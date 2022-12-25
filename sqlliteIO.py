import sqlite3
import datetime  # because timestamps help record keeping
# possibly modifying this in future to make use of peewee or sqlalchemy


class DataBaseIO:
    # this class should handle all of the sql work, and return only standard memory objects
    __db__ = None
    __cur__ = None

    def __init__(self, dbname):
        self.__db__ = sqlite3.connect(dbname, check_same_thread=False)
        self.__cur__ = self.__db__.cursor()

    def __str__(self):
        """
        refactored this to live in sql_handler
        :return:
        """
        tables = [str(each) for each in self.spew_tables()]
        verbose_table_data = ""
        for each in tables:
            fields = ", ".join(item for item in self.spew_header(each))
            contents = "\n".join("\t\t" + str(item) for item in self.execute_query(each))
            verbose_table_data += str("table: " + each + "\n\tfields: " + fields + "\n\tcontents:\n" + contents)

        return verbose_table_data

    def __execute_sql__(self, sql=""):
        with self.__db__:
            # other methods should have sanitized sql before it reaches this method.
            statement = str(sql).replace(";", "")  # some effort to prevent hostile inputs
            self.__cur__.execute(statement)

    def spew_header(self, table_name):
        with self.__db__:
            cur = self.__cur__
            # https://stackoverflow.com/questions/7831371/is-there-a-way-to-get-a-list-of-column-names-in-sqlite
            column_names = [str(row[1]) for row in cur.execute("PRAGMA table_info({})".format(table_name)).fetchall()]
        return column_names

    def spew_tables(self):
        with self.__db__:
            # https://stackoverflow.com/questions/305378/list-of-tables-db-schema-dump-etc-using-the-python-sqlite3-api
            tables = []
            tables_tup = self.__cur__.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            for each in tables_tup:
                tables.append(",".join(each))
        return tables

    def execute_query(self, table, select='*', parm='', regex=''):
        # returns data as a list, each row as a tuple
        # stripping out some characters known for sql injection attacks
        table = table.strip("'").strip(";")
        select = select.strip("'").strip(";")
        parm = parm.strip("'").strip(";")
        regex = regex.strip("'").strip(";")
        # results = None  # perhaps this isn't needed?
        if parm != '' and len(tuple(parm)) == 1:
            results = self.__cur__.execute(
                "select {} from {} where {} = '{}'".format(select, table, parm, regex)).fetchall()
        elif len(tuple(parm)) > 1:
            results = []
            i = 0
            while i < len(parm):
                results = self.__cur__.execute(
                    "select {} from {} where {} = '{}'".format(select, table, parm[i], regex[i])).fetchall()
                i += 1
        else:
            results = self.__cur__.execute('select {} from {}'.format(select, table)).fetchall()
        return results

    def create_table(self, table_name, *args):
        with self.__db__:
            table_name = table_name.strip("'").strip(";")
            args = str(args).replace("'", "").replace(";", "").strip("(").strip(")").strip(",")
            arg_list = [each for each in args.split(",")]
            sql = "Create Table if not exists {} ({})".format(table_name, ",".join(arg_list))
            self.__execute_sql__(sql)

    def add_record(self, table_name, columns='', *args):
        """
        https://www.jetbrains.com/help/pycharm/creating-documentation-comments.html
        :param table_name: any single string
        :param columns: use '' for implicit entries
        :param args: all values to insert, i.e: "'1','foo'" or "'bar'"
        :return: void
        """
        if columns != '':
            sql = "insert into {} ({}) values ({})".format(table_name, "".join(columns), "".join(args))

        else:
            sql = "insert into {} values ({})".format(table_name, "".join(args))
        self.__execute_sql__(sql)

    def update_record(self, table_name, column, unique_id="", new_value=""):
        """
        UPDATE table
        SET column_1 = new_value_1,
            column_2 = new_value_2
        WHERE
            search_condition
        """
        if unique_id != "":
            sql = "UPDATE {} SET {} = '{}' WHERE id = {}".format(table_name, column, new_value, unique_id)
            self.__execute_sql__(sql)

    def delete_record(self, table_name, parm, regex=""):
        if regex != "":
            self.__execute_sql__("delete from {} where {} = '{}'".format(table_name, parm, regex))
        else:
            print("Too broad of delete clause, aborting")

    def drop_table(self, table_name):
        # comment out this method unless absolutely needed. normally actions like this would
        # be for db admins only to perform
        # one could also do this action through a sql workbench
        with self.__db__:
            self.__execute_sql__("DROP TABLE IF EXISTS {}".format(table_name))

    def close(self):
        self.__db__ = None

    @staticmethod
    def get_date_time():
        # returning utc for consistency regardless of locale or daylight savings rules
        return datetime.datetime.utcnow()
