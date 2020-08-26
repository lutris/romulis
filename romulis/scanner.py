"""Generate checksums for local files"""
import os
import hashlib
import shutil
import subprocess
from collections import defaultdict
from romulis.database import schema, sql

CUE_PATH = os.path.expanduser("~/.cache/lutris/redump/cuesheets/Sony - PlayStation/")
DEST_PATH = "/media/strider/Backup/Games/Sega/Dreamcast_redump"

def get_file_checksum_slow(filename, hash_type="sha1"):
    """Return the checksum hash of a given type"""
    hasher = hashlib.new(hash_type)
    with open(filename, "rb") as input_file:
        for chunk in iter(lambda: input_file.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def get_sha1_checksum(filename):
    """Use sha1sum binary to return a checksum"""
    try:
        return subprocess.check_output(["sha1sum", filename]).decode().split()[0]
    except subprocess.CalledProcessError:
        return ""


def extract_7z(archive_path):
    return subprocess.check_output(["7z", "x", "-aoa", archive_path])


def get_directory_checksums(directory):
    """Get checksums for files in a directory recursively"""

    schema.syncdb()
    with sql.db_cursor(schema.DB_PATH) as cursor:
        # with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
        for base, _dirs, files in os.walk(directory):
            for filename in files:
                file_path = os.path.join(base, filename)
                file_matches = sql.db_select(cursor, "local_files", condition=("path", file_path))
                if file_matches:
                    continue
                if file_path.lower().endswith((".zip", ".7z", ".rar", ".ecm", ".gz")):
                    print("Skipping %s, archives currently unsupported" % file_path)
                    continue
                sha1sum = get_sha1_checksum(file_path)
                if not sha1sum:
                    print("couldn't read %s" % file_path)
                    continue
                print("%s %s" % (file_path, sha1sum))
                sql.db_insert(cursor, "local_files", {
                    "path": file_path,
                    "sha1": sha1sum
                })

def get_game_by_id(cursor, game_id):
    games = sql.db_select(cursor, "games", condition=("id", game_id))
    for game in games:
        return game


def conflict_resolver(file_matches, roms_by_game):
    # Resolve conflicts
    conflicts = set()
    for path in file_matches:
        if len(file_matches[path]) == 1:
            continue
        game_ids = tuple(sorted({rom["game_id"] for rom in file_matches[path]}))
        conflicts.add(game_ids)
    conflict_resolution = {}
    for conflict in conflicts:
        game_ids = sorted(conflict, key=lambda gid: len(roms_by_game[gid]))
        conflict_resolution[conflict] = game_ids[0]
    return conflict_resolution


def do_match():
    """Match local files with known databases"""
    roms_by_game = defaultdict(set)
    games_by_roms = defaultdict(set)
    paths_by_roms = defaultdict(set)
    game_details = {}
    datsets = {}
    with sql.db_cursor(schema.DB_PATH) as cursor:
        local_files = sql.db_select(cursor, "local_files")
        file_matches = defaultdict(list)
        for local_file in local_files:
            matches = sql.db_select(cursor, "roms", condition=("sha1", local_file["sha1"]))
            for match in matches:
                file_matches[local_file["path"]].append((match))

        # Link games and roms together
        for path in file_matches:
            for rom in file_matches[path]:
                game_id = rom["game_id"]
                roms_by_game[game_id].add(rom["id"])
                games_by_roms[rom["id"]].add(game_id)
                paths_by_roms[rom["id"]].add(path)

        # Populate game details and datset
        for game_id in roms_by_game:
            games = sql.db_select(cursor, "games", condition=("id", game_id))
            for game in games:
                game_details[game_id] = game
                datset_id = game["datset_id"]
            if datset_id not in datsets:
                datsets_rows = sql.db_select(cursor, "datsets", condition=("id", datset_id))
                datsets[datset_id] = datsets_rows[0]

        romsets = {}
        for game_id in roms_by_game:
            romsets[game_id] = sql.db_select(cursor, "roms", condition=("game_id", game_id))

        romset_matches = defaultdict(list)
        for path in sorted(file_matches.keys()):
            for rom in file_matches[path]:
                game_id = rom["game_id"]
                if rom["name"] not in [rom["name"] for rom in romset_matches[game_id]]:
                    romset_matches[game_id].append(rom)

        for game_id in romsets:
            have = romset_matches[game_id]
            have_names = [rom["name"] for rom in have]
            need = romsets[game_id]
            need_names = [rom["name"] for rom in need]
            missing = [rom for rom in need if rom["name"] not in have_names]
            cuesheets = []
            if len(missing) == 1 and missing[0]["name"].endswith(".cue"):
                cue_path = os.path.join(CUE_PATH, missing[0]["name"])
                if os.path.exists(cue_path):
                    cuesheets.append(cue_path)
            if len(have) + len(cuesheets) == len(need):
                print("Set complete: %s" % game_details[game_id]["name"])

            destdir = os.path.join(DEST_PATH, game_details[game_id]["name"])
            if not os.path.isdir(destdir):
                os.makedirs(destdir)

            for rom in romset_matches[game_id]:
                srcpath = list(paths_by_roms[rom["id"]])[0]
                destpath = os.path.join(destdir, rom["name"])
                if os.path.exists(srcpath):
                    os.rename(srcpath, destpath)

            for cuesheet in cuesheets:
                shutil.copy(cuesheet, os.path.join(destdir, os.path.basename(cuesheet)))
