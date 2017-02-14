import pymysql.cursors
from pymysql.cursors import DictCursorMixin, Cursor

class LowercaseDictCursorMixin(DictCursorMixin):
    def _conv_row(self, row):
        if row is None:
            return None
        self._fields = [field.lower() for field in self._fields]
        return self.dict_type(zip(self._fields, row))


class LowercaseDictCursor(LowercaseDictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary, key is lowercase"""


class MysqlExe(object):

    def __init__(self, host, user, passwd, db):
        self.connection = pymysql.connect(
            host=host,
            user=user,
            password=passwd,
            db='skyeye',
            charset='utf8',
            cursorclass=LowercaseDictCursor)

    def __make_dict(self, cursor):
        cols = [d[0] for d in cursor.description]

        def create_row(*args):
            return dict(zip(cols, args))

        return create_row

    def close_connection(self):
        self.connection.close()

    def mycommit(self):
        try:
            self.connection.commit()
        except:
            self.connection.rollback()

    def fetch_records(self, sql, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            results = cursor.fetchall()
            return results

    def insert_record(self, table, cols, data_dict):
        fields = ','.join(cols)
        values = ','.join(map(lambda col: '%({})s'.format(col), cols))
        with self.connection.cursor() as cursor:
            sql = "INSERT INTO {0} ({1}) VALUES ({2})".format(
                table, fields, values)
            cursor.execute(sql, data_dict)
        self.connection.commit()

    def insert_records(self, table, cols, args):
        """Insert several data against one query

        :param table: insert records into this table
        :param cols: sequence of column_name
        :param args: [{...},{...},...]

        """
        fields = ','.join(cols)
        values = ','.join(map(lambda col: '%({})s'.format(col), cols))
        with self.connection.cursor() as cursor:
            sql = "INSERT INTO {0} ({1}) VALUES ({2})".format(
                table, fields, values)
            cursor.executemany(sql, args)
        self.mycommit()

    # def update_records(self, sql, params=None):
    #     with self.connection.cursor() as cursor:
    #         cursor.execute(sql, params)
    #         self.mycommit()

    def update_records(self, table, args, filters=[]):
        """
        update new fields
        :param table: insert records into this table
        :param args: SET [{key: value}]
        :param filters: list, some keys of args
        """
        try:
            pairs = ' , '.join(['{0} = %({0})s'.format(item) for item in args[0].keys() if item not in filters])
            filter = ' '.join(['AND {0} = %({0})s'.format(item) for item in filters])
            with self.connection.cursor() as cursor:
                sql = """
                    UPDATE {0}
                        SET {1}
                    WHERE
                        1 = 1
                        {2}
                """.format(table, pairs, filter)
                cursor.executemany(sql, args)
            self.mycommit()
        except Exception as e:
            print(e)


    def delete_records(self, sql, params=None):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, params)
            self.mycommit()

    def get_colname_list(self, db, table):
        sql = """
            SELECT COLUMN_NAME FROM information_schema.COLUMNS
            WHERE table_schema = '{0}'
            AND table_name = '{1}'
        """.format(db, table)
        results = self.fetch_records(sql)
        return [result['column_name'] for result in results]


if __name__ == '__main__':
    connection = MysqlExe('172.17.128.172', 'yxt', 'pwdasdwx', 'skyeye')
    print(connection.fetch_records('select userName, createDate from user limit 2'))
