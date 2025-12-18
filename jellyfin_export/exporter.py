from __future__ import annotations
import os
import shutil
import frappe

from jellyfin_export.utils import (
    DEFAULT_VIDEO_EXTS, DEFAULT_SUB_EXTS, DEFAULT_IMG_EXTS,
    safe_name, ensure_dir, same_filesystem, split_ext, parse_allowed_exts
)

def _get_entity(entity_name: str):
    return frappe.get_doc("Drive Entity", entity_name)

def _build_rel_parts(entity_name: str, stop_at: str) -> list[str]:
    """
    Build path parts using Drive hierarchy (title + parent_drive_entity),
    stopping at stop_at (library root entity).
    """
    parts: list[str] = []
    cur = entity_name
    seen = set()

    while cur and cur not in seen:
        seen.add(cur)
        doc = _get_entity(cur)
        if doc.name == stop_at:
            break
        parts.append(safe_name(doc.title))
        cur = doc.parent_drive_entity

    return list(reversed(parts))

def _iter_descendants(root_entity: str):
    root = _get_entity(root_entity)

    # If lft/rgt are meaningful, use them (fast).
    if getattr(root, "lft", None) and getattr(root, "rgt", None) and root.lft < root.rgt:
        rows = frappe.get_all(
            "Drive Entity",
            filters=[
                ["lft", ">=", root.lft],
                ["rgt", "<=", root.rgt],
                ["is_active", "=", 1],
            ],
            fields=["name", "is_group", "title", "path", "file_ext", "mime_type", "trashed_on", "parent_drive_entity"],
            order_by="lft asc",
        )
        for r in rows:
            if r.get("trashed_on"):
                continue
            yield r
        return

    # Fallback BFS by parent_drive_entity
    queue = [root_entity]
    while queue:
        parent = queue.pop(0)
        children = frappe.get_all(
            "Drive Entity",
            filters={"parent_drive_entity": parent, "is_active": 1},
            fields=["name", "is_group", "title", "path", "file_ext", "mime_type", "trashed_on", "parent_drive_entity"],
        )
        for c in children:
            if c.get("trashed_on"):
                continue
            yield c
            if c.get("is_group"):
                queue.append(c["name"])

def _link_or_copy(src: str, dst: str, mode: str, export_root: str) -> str:
    """
    Returns actual mode used: hardlink/copy/symlink.
    IMPORTANT: symlink is usually NOT good when Jellyfin reads via NFS from another VM.
    Keep it for same-machine installs or when target paths are also mounted identically.
    """
    if os.path.lexists(dst):
        return "exists"

    if mode == "hardlink":
        if same_filesystem(src, export_root):
            os.link(src, dst)
            return "hardlink"
        # fallback
        mode = "copy"

    if mode == "copy":
        shutil.copy2(src, dst)
        return "copy"

    # mode == symlink
    os.symlink(src, dst)
    return "symlink"

def _upsert_map(entity_name: str, library_name: str, src: str, dst: str, export_type: str, status: str, err: str | None = None):
    now = frappe.utils.now_datetime()
    existing = frappe.db.get_value("Jellyfin Export Map", {"drive_entity": entity_name}, "name")
    doc = frappe.get_doc("Jellyfin Export Map", existing) if existing else frappe.new_doc("Jellyfin Export Map")
    doc.drive_entity = entity_name
    doc.library_name = library_name
    doc.src_path = src
    doc.export_path = dst
    doc.export_type = export_type
    doc.status = status
    doc.last_exported_on = now
    doc.last_error = err
    doc.save(ignore_permissions=True)

def _get_map(entity_name: str):
    return frappe.db.get_value(
        "Jellyfin Export Map",
        {"drive_entity": entity_name},
        ["name", "export_path", "src_path", "library_name", "status"],
        as_dict=True,
    )

def _same_inode(src: str, dst: str) -> bool:
    """
    True if src and dst are the same underlying file (hardlink case).
    """
    try:
        s1 = os.stat(src)
        s2 = os.stat(dst)
        return (s1.st_dev == s2.st_dev) and (s1.st_ino == s2.st_ino)
    except FileNotFoundError:
        return False

def _remove_path_safely(p: str):
    if not p:
        return
    try:
        if os.path.islink(p) or os.path.isfile(p):
            os.unlink(p)
        elif os.path.isdir(p):
            # remove only if empty
            try:
                os.rmdir(p)
            except OSError:
                pass
    except FileNotFoundError:
        pass

# def export_entity(entity_name: str, library_name: str, library_root_entity: str, export_root: str,
#                   export_subdir: str, link_mode: str, include_images: bool, allowed_exts: set[str] | None):
#     ent = _get_entity(entity_name)

#     # Skip inactive/trashed
#     if getattr(ent, "trashed_on", None) or getattr(ent, "is_active", 1) != 1:
#         return

#     export_base = os.path.join(export_root, export_subdir)
#     ensure_dir(export_base)

#     # Folder
#     if int(ent.is_group or 0) == 1:
#         rel_parts = _build_rel_parts(ent.name, stop_at=library_root_entity)
#         folder_dst = os.path.join(export_base, *rel_parts) if rel_parts else export_base
#         ensure_dir(folder_dst)
#         _upsert_map(ent.name, library_name, src="", dst=folder_dst, export_type="folder", status="exported")
#         return

#     # File
#     src = (ent.path or "").strip()
#     if not src or not os.path.exists(src):
#         _upsert_map(ent.name, library_name, src=src, dst="", export_type="file", status="error", err="Missing src path")
#         return

#     ext = split_ext(ent.title, ent.file_ext)
#     video_exts = allowed_exts or DEFAULT_VIDEO_EXTS
#     is_video = ext in video_exts
#     is_sub = ext in DEFAULT_SUB_EXTS
#     is_img = include_images and (ext in DEFAULT_IMG_EXTS)

#     if not (is_video or is_sub or is_img):
#         _upsert_map(ent.name, library_name, src=src, dst="", export_type="file", status="skipped")
#         return

#     rel_parts = _build_rel_parts(ent.name, stop_at=library_root_entity)
#     if not rel_parts:
#         rel_parts = [safe_name(ent.title)]

#     dst = os.path.join(export_base, *rel_parts)

#     # Collision policy: if dst exists but points to another entity, suffix with entity id
#     if os.path.lexists(dst):
#         base, e = os.path.splitext(dst)
#         dst = f"{base}__{ent.name}{e}"

#     ensure_dir(os.path.dirname(dst))

#     try:
#         used = _link_or_copy(src, dst, link_mode, export_root)
#         _upsert_map(ent.name, library_name, src=src, dst=dst, export_type="file", status="exported", err=None)
#     except Exception as ex:
#         _upsert_map(ent.name, library_name, src=src, dst=dst, export_type="file", status="error", err=str(ex))

def export_entity(entity_name: str, library_name: str, library_root_entity: str, export_root: str,
                  export_subdir: str, link_mode: str, include_images: bool, allowed_exts: set[str] | None):
    ent = _get_entity(entity_name)

    # Skip inactive/trashed
    if getattr(ent, "trashed_on", None) or getattr(ent, "is_active", 1) != 1:
        return

    export_base = os.path.join(export_root, export_subdir)
    ensure_dir(export_base)

    # Folder
    if int(ent.is_group or 0) == 1:
        rel_parts = _build_rel_parts(ent.name, stop_at=library_root_entity)
        folder_dst = os.path.join(export_base, *rel_parts) if rel_parts else export_base
        ensure_dir(folder_dst)

        # If folder moved/renamed, remove old exported folder path if it was mapped
        m = _get_map(ent.name)
        if m and m.get("export_path") and m["export_path"] != folder_dst:
            _remove_path_safely(m["export_path"])

        _upsert_map(ent.name, library_name, src="", dst=folder_dst, export_type="folder", status="exported")
        return

    # File
    src = (ent.path or "").strip()
    if not src or not os.path.exists(src):
        _upsert_map(ent.name, library_name, src=src, dst="", export_type="file", status="error", err="Missing src path")
        return

    ext = split_ext(ent.title, ent.file_ext)
    video_exts = allowed_exts or DEFAULT_VIDEO_EXTS
    is_video = ext in video_exts
    is_sub = ext in DEFAULT_SUB_EXTS
    is_img = include_images and (ext in DEFAULT_IMG_EXTS)

    if not (is_video or is_sub or is_img):
        _upsert_map(ent.name, library_name, src=src, dst="", export_type="file", status="skipped")
        return

    rel_parts = _build_rel_parts(ent.name, stop_at=library_root_entity)
    if not rel_parts:
        rel_parts = [safe_name(ent.title)]

    desired_dst = os.path.join(export_base, *rel_parts)
    ensure_dir(os.path.dirname(desired_dst))

    # Map-aware idempotency / move-rename handling
    m = _get_map(ent.name)
    if m and m.get("export_path"):
        old_dst = (m["export_path"] or "").strip()

        # If same path and already exported correctly, do nothing
        if old_dst == desired_dst and os.path.exists(old_dst):
            if link_mode != "hardlink" or _same_inode(src, old_dst):
                return

        # If path changed (move/rename), remove old exported path
        if old_dst and old_dst != desired_dst and os.path.exists(old_dst):
            _remove_path_safely(old_dst)

    dst = desired_dst

    # If destination exists, treat as already exported ONLY if it's the same file (hardlink case)
    if os.path.exists(dst):
        if link_mode == "hardlink" and _same_inode(src, dst):
            _upsert_map(ent.name, library_name, src=src, dst=dst, export_type="file", status="exported", err=None)
            return

        # Real collision: suffix with entity id
        base, e = os.path.splitext(dst)
        dst = f"{base}__{ent.name}{e}"

    ensure_dir(os.path.dirname(dst))

    try:
        _link_or_copy(src, dst, link_mode, export_root)
        _upsert_map(ent.name, library_name, src=src, dst=dst, export_type="file", status="exported", err=None)
    except Exception as ex:
        _upsert_map(ent.name, library_name, src=src, dst=dst, export_type="file", status="error", err=str(ex))

def export_subtree(root_entity: str, library_name: str, library_root_entity: str, export_root: str,
                   export_subdir: str, link_mode: str, include_images: bool, allowed_exts: set[str] | None):
    # Ensure the folder itself exists in export view
    export_entity(root_entity, library_name, library_root_entity, export_root, export_subdir, link_mode, include_images, allowed_exts)

    for row in _iter_descendants(root_entity):
        export_entity(
            row["name"],
            library_name,
            library_root_entity,
            export_root,
            export_subdir,
            link_mode,
            include_images,
            allowed_exts,
        )

def remove_export(entity_name: str):
    """
    Remove exported file/folder ONLY (never touch source).
    """
    m = frappe.db.get_value("Jellyfin Export Map", {"drive_entity": entity_name}, ["name", "export_path"], as_dict=True)
    if not m:
        return
    dst = (m.export_path or "").strip()
    if dst:
        try:
            if os.path.islink(dst) or os.path.isfile(dst):
                os.unlink(dst)
            elif os.path.isdir(dst):
                # Remove empty dirs only (don’t nuke recursively)
                try:
                    os.rmdir(dst)
                except OSError:
                    pass
        except FileNotFoundError:
            pass

    doc = frappe.get_doc("Jellyfin Export Map", m.name)
    doc.status = "deleted"
    doc.last_exported_on = frappe.utils.now_datetime()
    doc.save(ignore_permissions=True)
