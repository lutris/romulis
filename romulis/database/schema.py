from romulis import settings
from romulis.database import sql

DB_PATH = settings.DB_PATH

DB_SCHEMA = {
    "datsets": [
        {
            "name": "id",
            "type": "INTEGER",
            "indexed": True
        },
        {
            "name": "name",
            "type": "TEXT"
        },
        {
            "name": "description",
            "type": "TEXT"
        },
        {
            "name": "version",
            "type": "TEXT"
        },
        {
            "name": "date",
            "type": "TEXT"
        },
        {
            "name": "author",
            "type": "TEXT"
        },
        {
            "name": "url",
            "type": "TEXT"
        },
        {
            "name": "homepage",
            "type": "TEXT"
        },
    ],
    "games": [
        {
            "name": "id",
            "type": "INTEGER",
            "indexed": True
        },
        {
            "name": "datset_id",
            "type": "INTEGER"
        },
        {
            "name": "name",
            "type": "TEXT"
        },
        {
            "name": "category",
            "type": "TEXT"
        },
        {
            "name": "description",
            "type": "TEXT"
        },
    ],
    "roms": [
        {
            "name": "id",
            "type": "INTEGER",
            "indexed": True
        },
        {
            "name": "game_id",
            "type": "INTEGER"
        },
        {
            "name": "name",
            "type": "TEXT"
        },
        {
            "name": "size",
            "type": "INTEGER",
        },
        {
            "name": "crc",
            "type": "TEXT",
        },
        {
            "name": "md5",
            "type": "TEXT",
        },
        {
            "name": "sha1",
            "type": "TEXT"
        },
    ],
    "local_files": [
        {
            "name": "id",
            "type": "INTEGER",
            "indexed": True
        },
        {
            "name": "path",
            "type": "TEXT",
        },
        {
            "name": "sha1",
            "type": "TEXT"
        }
    ]
}


def get_schema(tablename):
    """
    Fields:
        - position
        - name
        - type
        - not null
        - default
        - indexed
    """
    tables = []
    query = "pragma table_info('%s')" % tablename
    with sql.db_cursor(DB_PATH) as cursor:
        for row in cursor.execute(query).fetchall():
            field = {
                "name": row[1],
                "type": row[2],
                "not_null": row[3],
                "default": row[4],
                "indexed": row[5],
            }
            tables.append(field)
    return tables


def field_to_string(name="", type="", indexed=False, unique=False):  # pylint: disable=redefined-builtin
    """Converts a python based table definition to it's SQL statement"""
    field_query = "%s %s" % (name, type)
    if indexed:
        field_query += " PRIMARY KEY"
    if unique:
        field_query += " UNIQUE"
    return field_query


def create_table(name, schema):
    """Creates a new table in the database"""
    fields = ", ".join([field_to_string(**f) for f in schema])
    query = "CREATE TABLE IF NOT EXISTS %s (%s)" % (name, fields)
    with sql.db_cursor(DB_PATH) as cursor:
        cursor.execute(query)


def migrate(table, schema):
    """Compare a database table with the reference model and make necessary changes

    This is very basic and only the needed features have been implemented (adding columns)

    Args:
        table (str): Name of the table to migrate
        schema (dict): Reference schema for the table

    Returns:
        list: The list of column names that have been added
    """

    existing_schema = get_schema(table)
    migrated_fields = []
    if existing_schema:
        columns = [col["name"] for col in existing_schema]
        for field in schema:
            if field["name"] not in columns:
                migrated_fields.append(field["name"])
                sql.add_field(DB_PATH, table, field)
    else:
        create_table(table, schema)
    return migrated_fields


def syncdb():
    """Update the database to the current version, making necessary changes
    for backwards compatibility."""
    for table in DB_SCHEMA:
        migrate(table, DB_SCHEMA[table])
