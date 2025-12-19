from __future__ import annotations
import frappe
from frappe.utils.background_jobs import enqueue
from jellyfin_export.utils import get_settings, diagnose_and_heal_tree

@frappe.whitelist()
def sync_all():
    settings = get_settings()
    if int(settings.enabled or 0) != 1:
        frappe.throw("Jellyfin Export is disabled.")
    for lib in settings.libraries or []:
        if int(lib.enabled or 0) != 1 or not lib.root_drive_entity:
            continue
        # sync_library(lib.library_name)
        enqueue(
            "jellyfin_export.api.sync_library",
            queue=settings.queue or "long",
            timeout=60 * 60,
            library_name=lib.library_name,
        )
    return "Queued"

@frappe.whitelist()
def sync_library(library_name: str):
    settings = get_settings()
    lib = None
    for row in settings.libraries or []:
        if row.library_name == library_name and int(row.enabled or 0) == 1:
            lib = row
            break
    if not lib:
        frappe.throw(f"Library not found or disabled: {library_name}")

    # Full subtree export from configured root
    enqueue(
        "jellyfin_export.api._sync_library_job",
        queue=settings.queue or "long",
        timeout=60 * 60,
        library_name=lib.library_name,
        root_entity=lib.root_drive_entity,
        export_subdir=lib.export_subdir or lib.library_name,
    )
    return "Queued"

def _sync_library_job(library_name: str, root_entity: str, export_subdir: str):
    from jellyfin_export.exporter import export_subtree
    from jellyfin_export.utils import parse_allowed_exts

    settings = get_settings()
    include_images = int(settings.include_images or 0) == 1

    # find allowed_exts for this library
    allowed = None
    for row in settings.libraries or []:
        if row.library_name == library_name:
            allowed = parse_allowed_exts(row.allowed_extensions)
            break

    # Self-heal check
    diagnose_and_heal_tree(root_entity)

    export_subtree(
        root_entity=root_entity,
        library_name=library_name,
        library_root_entity=root_entity,
        export_root=settings.export_root,
        export_subdir=export_subdir,
        link_mode=settings.link_mode or "hardlink",
        include_images=include_images,
        allowed_exts=allowed,
    )

def nightly_resync():
    settings = get_settings()
    if int(settings.enabled or 0) != 1:
        return
    # Just queue a sync_all
    enqueue(
        "jellyfin_export.api.sync_all",
        queue=settings.queue or "long",
        timeout=60 * 60,
    )
