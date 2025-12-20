from __future__ import annotations
import frappe
from frappe.utils.background_jobs import enqueue
from frappe.utils.redis_wrapper import RedisWrapper


from jellyfin_export.utils import get_settings, parse_allowed_exts, diagnose_and_heal_tree
from jellyfin_export.exporter import export_entity, export_subtree, remove_export

def _redis_lock(key: str, timeout: int = 300):
    r = RedisWrapper()
    return r.lock(key, timeout=timeout)

def _get_library_for_entity(entity_name: str):
    """
    Walk up parent_drive_entity until we hit a configured library root.
    Returns (library_name, root_entity, export_subdir, allowed_exts) or None.
    """
    settings = get_settings()
    libs = settings.libraries or []
    root_map = {}
    for lib in libs:
        if int(lib.enabled or 0) != 1:
            continue
        if lib.root_drive_entity:
            root_map[lib.root_drive_entity] = lib

    cur = entity_name
    seen = set()
    while cur and cur not in seen:
        seen.add(cur)
        if cur in root_map:
            lib = root_map[cur]
            allowed = parse_allowed_exts(lib.allowed_extensions)
            export_subdir = lib.export_subdir or lib.library_name
            return (lib.library_name, lib.root_drive_entity, export_subdir, allowed)
        cur = frappe.db.get_value("Drive Entity", cur, "parent_drive_entity")

    return None

def on_drive_entity_change(doc, method=None):
    settings = get_settings()
    if int(settings.enabled or 0) != 1:
        return

    # Handle soft deletion (Empty Trash)
    # When emptied from trash: is_active -> -1, path -> None
    if str(doc.is_active) == "-1":
        queue = settings.queue or "long"
        enqueue(
            "jellyfin_export.sync.run_delete_job",
            queue=queue,
            timeout=60 * 10,
            enqueue_after_commit=True,
            entity_name=doc.name,
        )
        return

    info = _get_library_for_entity(doc.name)
    if not info:
        return

    library_name, root_entity, export_subdir, allowed_exts = info

    queue = settings.queue or "long"
    enqueue(
        "jellyfin_export.sync.run_export_job",
        queue=queue,
        timeout=60 * 30,
        enqueue_after_commit=True,
        entity_name=doc.name,
        library_name=library_name,
        root_entity=root_entity,
        export_subdir=export_subdir,
    )

def on_drive_entity_trash(doc, method=None):
    settings = get_settings()
    if int(settings.enabled or 0) != 1:
        return

    # remove exported view if it exists
    queue = settings.queue or "long"
    enqueue(
        "jellyfin_export.sync.run_delete_job",
        queue=queue,
        timeout=60 * 10,
        enqueue_after_commit=True,
        entity_name=doc.name,
    )

def run_delete_job(entity_name: str):
    key = f"jellyfin_export:delete:{frappe.local.site}"
    with _redis_lock(key, timeout=300):
        remove_export(entity_name)



def run_export_job(entity_name: str, library_name: str, root_entity: str, export_subdir: str):
    # Self-heal tree if needed
    diagnose_and_heal_tree(root_entity)

    settings = get_settings()
    export_root = settings.export_root
    link_mode = settings.link_mode or "hardlink"
    include_images = int(settings.include_images or 0) == 1

    allowed_exts = None
    info = _get_library_for_entity(entity_name)
    if info:
        allowed_exts = info[3]

    # Decide whether entity is folder
    is_group = frappe.db.get_value("Drive Entity", entity_name, "is_group") or 0

    # Lock per-library to avoid stampedes during mass uploads
    lock_key = f"jellyfin_export:{frappe.local.site}:{library_name}"
    with _redis_lock(lock_key, timeout=900):
        if int(is_group) == 1:
            export_subtree(
                root_entity=entity_name,
                library_name=library_name,
                library_root_entity=root_entity,
                export_root=export_root,
                export_subdir=export_subdir,
                link_mode=link_mode,
                include_images=include_images,
                allowed_exts=allowed_exts,
            )
        else:
            export_entity(
                entity_name=entity_name,
                library_name=library_name,
                library_root_entity=root_entity,
                export_root=export_root,
                export_subdir=export_subdir,
                link_mode=link_mode,
                include_images=include_images,
                allowed_exts=allowed_exts,
            )
