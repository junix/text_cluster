# -*- coding:utf-8 -*-
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import cx_Oracle


class OracleExe(object):

    def __init__(self, host, port, sid, username, passwd):
        dsn = cx_Oracle.makedsn(host, port, service_name=sid)
        self.connection = cx_Oracle.connect(username, passwd, dsn)
        self.cursor = self.connection.cursor()

    def __make_dict(self, cursor):
        cols = [d[0].lower() for d in cursor.description]

        def create_row(*args):
            return dict(zip(cols, args))

        return create_row

    def close_connection(self):
        self.connection.close()

    def fetch_records(self, sql):
        self.cursor.execute(sql)
        self.cursor.rowfactory = self.__make_dict(self.cursor)
        results = self.cursor.fetchall()
        if results is None:
            results = []
        return results

    def get_column_list(self):
        sql = """
            SELECT
                column_name
            FROM
                user_tab_columns
            WHERE
                table_name = 'CORE_USERPROFILE'
        """
        return [col[0] for col in conn.fetch_records(sql)]


if __name__ == '__main__':
    conn = OracleExe('10.200.60.101', '1521', 'yxtdbdg',
                     'eldeveloper', 'el-developer')
    sql = """
        SELECT
            *
        FROM
            elearning.core_userprofile
        WHERE
            orgid = '71028353-7246-463f-ab12-995144fb4cb2'
    """
    print conn.fetch_records(sql)[0]
