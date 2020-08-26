import sqlite3


class db_cursor(object):

    def __init__(self, db_path):
        self.db_path = db_path
        self.db_conn = None

    def __enter__(self):
        self.db_conn = sqlite3.connect(self.db_path)
        cursor = self.db_conn.cursor()
        return cursor

    def __exit__(self, _type, value, traceback):
        self.db_conn.commit()
        self.db_conn.close()


def cursor_execute(cursor, query, params=None):
    """Function used to retry queries in case an error occurs"""
    return cursor.execute(query, params or ())


def bad_db_insert(db_path, table, fields):
    """Insert a row in a table"""
    columns = ", ".join(list(fields.keys()))
    placeholders = ("?, " * len(fields))[:-2]
    field_values = tuple(fields.values())
    with db_cursor(db_path) as cursor:
        cursor_execute(
            cursor,
            "insert into {0}({1}) values ({2})".format(table, columns, placeholders),
            field_values,
        )
        inserted_id = cursor.lastrowid
    return inserted_id

def db_insert(cursor, table, fields):
    columns = ", ".join(list(fields.keys()))
    placeholders = ("?, " * len(fields))[:-2]
    field_values = tuple(fields.values())
    cursor_execute(
        cursor,
        "insert into {0}({1}) values ({2})".format(table, columns, placeholders),
        field_values,
    )
    return cursor.lastrowid
    return inserted_id


def db_update(db_path, table, updated_fields, where):
    """Update `table` with the values given in the dict `values` on the
       condition given with the `row` tuple.
    """
    columns = "=?, ".join(list(updated_fields.keys())) + "=?"
    field_values = tuple(updated_fields.values())

    condition_field = "{0}=?".format(where[0])
    condition_value = (where[1], )

    with db_cursor(db_path) as cursor:
        query = "UPDATE {0} SET {1} WHERE {2}".format(table, columns, condition_field)
        cursor_execute(cursor, query, field_values + condition_value)


def db_delete(db_path, table, field, value):
    with db_cursor(db_path) as cursor:
        cursor_execute(cursor, "delete from {0} where {1}=?".format(table, field), (value, ))


def db_select(cursor, table, fields=None, condition=None):
    if fields:
        columns = ", ".join(fields)
    else:
        columns = "*"
    query = "SELECT {} FROM {}"
    if condition:
        condition_field, condition_value = condition
        if isinstance(condition_value, (list, tuple, set)):
            condition_value = tuple(condition_value)
            placeholders = ", ".join("?" * len(condition_value))
            where_condition = " where {} in (" + placeholders + ")"
        else:
            condition_value = (condition_value, )
            where_condition = " where {}=?"
        query = query + where_condition
        query = query.format(columns, table, condition_field)
        params = condition_value
    else:
        query = query.format(columns, table)
        params = ()
    cursor_execute(cursor, query, params)
    rows = cursor.fetchall()
    column_names = [column[0] for column in cursor.description]
    results = []
    for row in rows:
        row_data = {}
        for index, column in enumerate(column_names):
            row_data[column] = row[index]
        results.append(row_data)
    return results


def db_query(db_path, query, params=()):
    with db_cursor(db_path) as cursor:
        cursor_execute(cursor, query, params)
        rows = cursor.fetchall()
        column_names = [column[0] for column in cursor.description]
    results = []
    for row in rows:
        row_data = {}
        for index, column in enumerate(column_names):
            row_data[column] = row[index]
        results.append(row_data)
    return results


def add_field(db_path, tablename, field):
    query = "ALTER TABLE %s ADD COLUMN %s %s" % (
        tablename,
        field["name"],
        field["type"],
    )
    with db_cursor(db_path) as cursor:
        cursor.execute(query)
