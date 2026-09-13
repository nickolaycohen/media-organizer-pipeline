import os
import sys
import subprocess
import argparse
import logging
from constants import CURATED_LACIE_DIR, LOG_PATH

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger("move_to_be_curated_to_curated")


def run_applescript(script_str):
    try:
        p = subprocess.run(
            ['osascript', '-e', script_str],
            capture_output=True,
            text=True,
            check=True
        )
        return p.stdout.strip()
    except subprocess.CalledProcessError as e:
        logger.error(f"AppleScript error: {e.stderr.strip() or e.stdout.strip()}")
        return None


def get_tbc_albums():
    script = '''
    tell application "Photos"
        set topFolderName to "Media Organizer on LaCie"
        if not (exists folder topFolderName) then return ""
        set topFolder to folder topFolderName
        if not (exists folder "ToBeCurated" of topFolder) then return ""
        set tbcFolder to folder "ToBeCurated" of topFolder
        
        set output to ""
        repeat with alb in albums of tbcFolder
            set aName to name of alb
            if aName is not "SkipPublishing" and aName is not "Ignore" then
                set aCount to count of media items of alb
                set output to output & aName & "|" & (aCount as string) & "\\n"
            end if
        end repeat
        return output
    end tell
    '''
    out = run_applescript(script)
    albums = {}
    if out:
        for line in out.strip().split('\n'):
            if '|' in line:
                name, count_str = line.split('|', 1)
                albums[name.strip()] = int(count_str.strip())
    return albums


def move_album(moment_name):
    safe_name = moment_name.replace('"', '\\"')
    script = f'''
    tell application "Photos"
        set topFolderName to "Media Organizer on LaCie"
        if not (exists folder topFolderName) then return "ERROR: Top folder not found"
        set topFolder to folder topFolderName
        
        if not (exists folder "Curated" of topFolder) then
            make new folder named "Curated" at topFolder
        end if
        set curFolder to folder "Curated" of topFolder
        
        if not (exists folder "ToBeCurated" of topFolder) then return "ERROR: ToBeCurated not found"
        set tbcFolder to folder "ToBeCurated" of topFolder
        
        set albName to "{safe_name}"
        if exists album albName of tbcFolder then
            set tbcAlb to album albName of tbcFolder
            set mItems to media items of tbcAlb
            set itemCount to count of mItems
            if itemCount > 0 then
                set targetCurAlb to missing value
                if exists album albName of curFolder then
                    set targetCurAlb to album albName of curFolder
                else
                    make new album named albName at curFolder
                    set targetCurAlb to album albName of curFolder
                end if
                add mItems to targetCurAlb
            end if
            delete album id (id of tbcAlb)
            return "SUCCESS|" & (itemCount as string)
        else
            return "ERROR: Album not found in ToBeCurated"
        end if
    end tell
    '''
    res = run_applescript(script)
    if res and res.startswith("SUCCESS|"):
        count = int(res.split('|')[1])
        logger.info(f"✅ Moved {count} asset(s) from 'ToBeCurated/{moment_name}' to 'Curated/{moment_name}' in Apple Photos.")
        
        # Auto-export to update local disk folder and DB records
        script_dir = os.path.dirname(os.path.abspath(__file__))
        try:
            logger.info(f"🔄 Exporting updated Curated album for '{moment_name}' to disk...")
            subprocess.run([sys.executable, os.path.join(script_dir, "export_curated_album.py"), moment_name], check=True)
            logger.info(f"✅ Successfully exported and synced '{moment_name}'.")
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to export '{moment_name}': {e}")
            return False
    else:
        logger.warning(f"Could not move album '{moment_name}': {res}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Move/Copy assets from ToBeCurated to Curated in Apple Photos and export to disk.")
    parser.add_argument("moment_name", nargs="?", default="all", help="Moment name to move, or 'all' for all ToBeCurated albums")
    args = parser.parse_args()

    tbc_albums = get_tbc_albums()
    if not tbc_albums:
        logger.info("✨ No albums found in Apple Photos 'ToBeCurated' to move.")
        return

    target = args.moment_name.strip()
    if target.lower() == "all":
        logger.info(f"🚀 Moving all {len(tbc_albums)} album(s) from ToBeCurated to Curated: {', '.join(tbc_albums.keys())}")
        success_count = 0
        for name in list(tbc_albums.keys()):
            if move_album(name):
                success_count += 1
        logger.info(f"🎉 Successfully moved and exported {success_count}/{len(tbc_albums)} album(s).")
    else:
        # Match exact or case-insensitive
        match = None
        for name in tbc_albums.keys():
            if name.lower() == target.lower():
                match = name
                break
        if not match:
            # Check if target is part of album name
            candidates = [name for name in tbc_albums.keys() if target.lower() in name.lower()]
            if len(candidates) == 1:
                match = candidates[0]
            elif len(candidates) > 1:
                logger.error(f"Ambiguous moment name '{target}'. Matches: {', '.join(candidates)}")
                return
            else:
                match = target  # Attempt direct move with provided name

        move_album(match)


if __name__ == "__main__":
    main()
