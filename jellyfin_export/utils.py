from __future__ import annotations
import os
import re
import frappe

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
