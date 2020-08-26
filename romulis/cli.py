"""CLI tool to help manage ROMs"""
import os
import click
from romulis.importers import import_xml_dat
from romulis.scanner import get_directory_checksums, do_match

@click.group()
def main():
    """Entry point for Click commands"""


@main.command()
@click.argument("file_path")
def import_dat(file_path):
    """Import dat file"""
    if not os.path.exists(file_path):
        raise ValueError("Not a valid file: %s" % file_path)
    with open(file_path) as dat_file:
        dat_contents = dat_file.read()
    if dat_contents.startswith("<?xml"):
        import_xml_dat(dat_contents)
    else:
        raise RuntimeError("Unsupported dat file %s" % file_path)


@main.command()
@click.argument("directory")
def scan_dir(directory):
    """Scan a unorganized ROM directory"""
    if not os.path.isdir(directory):
        raise ValueError("Not a directory: %s" % directory)
    get_directory_checksums(directory)


@main.command()
def match():
    """Match local files to known checksums"""
    do_match()
