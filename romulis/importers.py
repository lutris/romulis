"""Import various formats of DAT files to a local database"""
from xml.etree import ElementTree

from romulis.database import schema, sql


def get_datset(header_node):
    """Return datset payload"""
    return {
        "name": header_node.find("name").text,
        "description": header_node.find("description").text,
        "version": header_node.find("version").text,
        "date": header_node.find("date").text,
        "author": header_node.find("author").text,
        "url": header_node.find("url").text,
        "homepage": header_node.find("homepage").text,
    }


def get_rom(game_id, rom_node):
    """Return rom payload"""
    return {
        "game_id": game_id,
        "name": rom_node.get("name"),
        "size": int(rom_node.get("size")),
        "crc": rom_node.get("crc"),
        "md5": rom_node.get("md5"),
        "sha1": rom_node.get("sha1"),
    }


def get_game(datset_id, game_node):
    """Return game payload"""
    return {
        "datset_id": datset_id,
        "name": game_node.get("name"),
        "category": game_node.find("category").text,
        "description": game_node.find("description").text,
    }


def import_xml_dat(xml_content):
    """Import a XML dat file"""
    root_node = ElementTree.fromstring(xml_content)
    datafile_tags = {child.tag for child in root_node}
    if datafile_tags != {"header", "game"}:
        raise RuntimeError("Not a valid DAT file")

    schema.syncdb()

    with sql.db_cursor(schema.DB_PATH) as cursor:
        header_node = root_node.find("header")
        datset_id = sql.db_insert(cursor, "datsets", get_datset(header_node))
        for game_node in root_node.findall("game"):
            game_id = sql.db_insert(cursor, "games", get_game(datset_id, game_node))
            for rom_node in game_node.findall("rom"):
                sql.db_insert(cursor, "roms", get_rom(game_id, rom_node))
