import os
import sys
import plistlib
import subprocess
import logging

logger = logging.getLogger(__name__)

# Standard macOS Finder Label Colors and their numeric indices
FINDER_COLOR_MAP = {
    "none": 0,
    "orange": 1,
    "red": 2,
    "yellow": 3,
    "blue": 4,
    "purple": 5,
    "green": 6,
    "gray": 7,
    "grey": 7
}

INDEX_TO_COLOR = {
    0: "None",
    1: "Orange",
    2: "Red",
    3: "Yellow",
    4: "Blue",
    5: "Purple",
    6: "Green",
    7: "Gray"
}

def apply_finder_tag(file_or_dir_path, color_or_name):
    """
    Applies macOS Finder tag and color label to a file or directory.
    Combines modern Spotlight UserTags (xattr) with classic Finder label index (AppleScript).

    :param file_or_dir_path: Absolute or relative path to the file or directory.
    :param color_or_name: Color name string (e.g. 'Red', 'Yellow', 'Green') or int index (1-7).
    :return: bool indicating success.
    """
    if sys.platform != "darwin":
        logger.debug("Finder tags are only supported on macOS (Darwin).")
        return False

    abs_path = os.path.abspath(file_or_dir_path)
    if not os.path.exists(abs_path):
        logger.warning(f"Cannot apply Finder tag: Path does not exist: {abs_path}")
        return False

    if isinstance(color_or_name, int):
        label_index = color_or_name
        tag_name = INDEX_TO_COLOR.get(label_index, "Tag")
    elif isinstance(color_or_name, str):
        key = color_or_name.strip().lower()
        if key.isdigit():
            label_index = int(key)
            tag_name = INDEX_TO_COLOR.get(label_index, "Tag")
        elif key in FINDER_COLOR_MAP:
            label_index = FINDER_COLOR_MAP[key]
            tag_name = INDEX_TO_COLOR.get(label_index, color_or_name.capitalize())
        else:
            # Custom tag name without standard color; default to gray or 0
            label_index = 0
            tag_name = color_or_name.strip()
    else:
        logger.error(f"Invalid color_or_name type: {type(color_or_name)}")
        return False

    success = True

    # 1. Modern Spotlight UserTags via extended attribute: com.apple.metadata:_kMDItemUserTags
    try:
        tag_entry = f"{tag_name}\n{label_index}" if label_index > 0 else tag_name
        tag_data = [tag_entry] if tag_name != "None" else []
        plist_bytes = plistlib.dumps(tag_data, fmt=plistlib.FMT_BINARY)

        import ctypes
        libc = ctypes.cdll.LoadLibrary("libc.dylib")
        libc.setxattr.argtypes = [
            ctypes.c_char_p,
            ctypes.c_char_p,
            ctypes.c_void_p,
            ctypes.c_size_t,
            ctypes.c_uint32,
            ctypes.c_int
        ]
        libc.setxattr.restype = ctypes.c_int

        ret = libc.setxattr(
            abs_path.encode("utf-8"),
            b"com.apple.metadata:_kMDItemUserTags",
            plist_bytes,
            len(plist_bytes),
            0,
            0
        )
        if ret != 0:
            logger.debug(f"libc.setxattr returned non-zero code {ret} for {abs_path}")
    except Exception as e:
        logger.debug(f"Failed setting _kMDItemUserTags on {abs_path}: {e}")
        success = False

    # 2. Classic Finder label index via AppleScript
    try:
        script = f'tell application "Finder" to set label index of (POSIX file "{abs_path}" as alias) to {label_index}'
        res = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, timeout=5)
        if res.returncode != 0:
            logger.debug(f"osascript label failed for {abs_path}: {res.stderr.strip()}")
    except Exception as e:
        logger.debug(f"Failed running osascript label for {abs_path}: {e}")
        success = False

    logger.debug(f"🏷️ Applied Finder tag '{tag_name}' (color {label_index}) to: {abs_path}")
    return success
