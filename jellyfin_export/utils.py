from __future__ import annotations
import os
import re
import frappe
from frappe.utils.redis_wrapper import RedisWrapper
from frappe.utils.nestedset import rebuild_tree

DEFAULT_VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".webm", ".m4v"}
DEFAULT_SUB_EXTS = {".srt", ".ass", ".ssa", ".vtt", ".sub"}
DEFAULT_IMG_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

def get_settings():
    return frappe.get_single("Jellyfin Export Settings")

def safe_name(name: str) -> str:
    name = (name or "").replace("\x00", "").strip()
    # Avoid path separators and weird Windows-reserved characters (harmless on Linux, but good hygiene)
    name = re.sub(r'[\/\\:*?"<>|]', "_", name)
    return name or "untitled"

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def same_filesystem(path_a: str, path_b: str) -> bool:
    try:
        return os.stat(path_a).st_dev == os.stat(path_b).st_dev
    except FileNotFoundError:
        return False

def split_ext(title: str, file_ext: str | None) -> str:
    if file_ext:
        ext = file_ext.strip().lower()
        if not ext.startswith("."):
            ext = "." + ext
        return ext
    _, ext = os.path.splitext(title or "")
    return ext.lower()

def parse_allowed_exts(s: str | None) -> set[str] | None:
    if not s:
        return None
    exts = set()
    for part in s.split(","):
        p = part.strip().lower()
        if not p:
            continue
        if not p.startswith("."):
            p = "." + p
        exts.add(p)
    return exts or None

def _redis_lock(key: str, timeout: int = 300):
    r = RedisWrapper()
    return r.lock(key, timeout=timeout)

def diagnose_and_heal_tree(root_entity: str):
    """
    Check if Nested Set index (lft/rgt) matches the actual parent-child hierarchy.
    If not, rebuild the tree.
    """
    if not root_entity:
        return

    # 1. Quick check: Root must have lft/rgt
    root_doc = frappe.db.get_value("Drive Entity", root_entity, ["lft", "rgt", "is_group"], as_dict=True)
    if not root_doc:
        return

    # If root is missing lft/rgt, it's definitely broken
    if not root_doc.lft or not root_doc.rgt:
        print(f"Tree corruption detected (Root {root_entity} missing lft/rgt). Rebuilding...")
        rebuild_tree_safely()
        return

    # 2. Count Check: Compare Nested Set span vs Parent-Child traversal
    # Nested Set Count = (rgt - lft - 1) // 2
    ns_count = (root_doc.rgt - root_doc.lft - 1) // 2

    # Actual Recursive Count
    # Build adjacency list for ALL active entities to avoid thousands of queries
    # This is memory intensive but much faster than recursive DB calls
    all_entities = frappe.get_all(
        "Drive Entity", 
        filters={"is_active": 1}, 
        fields=["name", "parent_drive_entity"]
    )
    
    # parent -> [children]
    tree = {}
    for e in all_entities:
        p = e.parent_drive_entity
        if p:
            tree.setdefault(p, []).append(e.name)
            
    # Count descendants via BFS
    recursive_count = 0
    queue = [root_entity]
    while queue:
        curr = queue.pop(0)
        children = tree.get(curr, [])
        recursive_count += len(children)
        # Add children to queue for next level
        queue.extend(children)
        
    if ns_count != recursive_count:
        print(f"Tree corruption detected (Index Count: {ns_count}, Actual Count: {recursive_count}). Rebuilding...")
        rebuild_tree_safely()

def rebuild_tree_safely():
    # Global lock to prevent concurrent rebuilds
    lock_key = f"jellyfin_export:rebuild_tree:{frappe.local.site}"
    with _redis_lock(lock_key, timeout=600):
        # Double check inside lock in case someone else just fixed it? 
        # For now, just rebuild.
        try:
            rebuild_tree("Drive Entity")
            frappe.db.commit() # Ensure changes strictly saved
            print("Tree rebuild completed.")
        except Exception as e:
            print(f"Tree rebuild failed: {e}")
            frappe.log_error("Drive Entity Tree Rebuild Failed", str(e))
