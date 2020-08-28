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
    """Extract a 7z archive"""
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


def do_match():
    """Match local files with known databases"""
    matcher = RomMatcher()
    matcher.rename_sets()


class RomMatcher:
    """Match local files with rom sets stored in database"""
    def __init__(self):
        self.connection = sql.get_connection(schema.DB_PATH)
        self.cursor = self.connection.cursor()
        self.roms_by_game = defaultdict(set)
        self.paths_by_roms = defaultdict(set)

    def __del__(self):
        self.connection.commit()
        self.connection.close()

    def rename_sets(self):
        """Rename (and move) ROMs to their destination"""
        romset_matches = self.get_fullset_matches()
        _datsets, game_details = self.get_datsets_and_game_details(self.roms_by_game)
        for game_id in romset_matches:
            romset = romset_matches[game_id]
            destdir = os.path.join(DEST_PATH, game_details[game_id]["name"])
            self.rename_roms(destdir, romset)

    def get_game_by_id(self, game_id):
        """Return a game by its ID"""
        games = sql.db_select(self.cursor, "games", condition=("id", game_id))
        for game in games:
            return game

    def get_file_matches(self):
        """Return file matched locally"""
        file_matches = defaultdict(list)
        local_files = sql.db_select(self.cursor, "local_files")
        for local_file in local_files:
            matches = sql.db_select(self.cursor, "roms", condition=("sha1", local_file["sha1"]))
            for match in matches:
                local_path = local_file["path"]
                if os.path.exists(local_path):
                    file_matches[local_path].append((match))
        return file_matches

    def set_path_and_rom_links(self, file_matches):
        """Create mappings to access roms by game ID and paths by ROM ID"""
        # Link games and roms together
        for path in file_matches:
            for rom in file_matches[path]:
                self.roms_by_game[rom["game_id"]].add(rom["id"])
                self.paths_by_roms[rom["id"]].add(path)

    def get_datsets_and_game_details(self, roms_by_game):
        """Populate game details and datset"""
        datsets = {}
        game_details = {}
        for game_id in roms_by_game:
            games = sql.db_select(self.cursor, "games", condition=("id", game_id))
            for game in games:
                game_details[game_id] = game
                datset_id = game["datset_id"]
            if datset_id not in datsets:
                datsets_rows = sql.db_select(self.cursor, "datsets", condition=("id", datset_id))
                datsets[datset_id] = datsets_rows[0]
        return datsets, game_details

    def get_romsets(self):
        """Return romsets found during the scan.
        The romsets are keys by game ID.
        """
        romsets = {}
        for game_id in self.roms_by_game:
            romsets[game_id] = sql.db_select(self.cursor, "roms", condition=("game_id", game_id))
        return romsets

    def get_romset_matches(self, romsets):
        """Return matches found for each romset"""
        romset_matches = {}
        for game_id in romsets:
            match = {}
            romset = romsets[game_id]
            for rom in romsets[game_id]:
                if rom["id"] in self.paths_by_roms:
                    match[rom["id"]] = self.paths_by_roms[rom["id"]]

            missing = [rom for rom in romset if rom["id"] not in match]
            if len(missing) == 1 and missing[0]["name"].endswith(".cue"):
                cue_path = os.path.join(CUE_PATH, missing[0]["name"])
                if os.path.exists(cue_path):
                    match[missing[0]["id"]] = cue_path
            if len(match) == len(romset):
                print("Full set detected: %s" % match)
                romset_matches[game_id] = match
        return romset_matches

    @staticmethod
    def rename_roms(destdir, romset):
        """Move and rename ROMs to their destination"""
        if not os.path.isdir(destdir):
            os.makedirs(destdir)

        for rom in romset:
            srcpath = romset[rom["id"]]
            destpath = os.path.join(destdir, rom["name"])
            if srcpath.startswith(CUE_PATH):
                shutil.copy(srcpath, destpath)
            else:
                os.rename(srcpath, destpath)

    def get_fullset_matches(self):
        """Return all full romsets found"""
        self.set_path_and_rom_links(self.get_file_matches())
        return self.get_romset_matches(self.get_romsets())
