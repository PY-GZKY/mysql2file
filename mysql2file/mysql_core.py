import os
import threading
import warnings
from typing import Optional

import pymysql
from dotenv import load_dotenv
from mongo2file.constants import *
from mongo2file.utils import to_str_datetime

load_dotenv(verbose=True)
lock_ = threading.Lock()


def check_folder_path(folder_path):
    if folder_path is None:
        _ = '.'
    elif not os.path.exists(folder_path):
        os.makedirs(folder_path)
        _ = folder_path
    else:
        _ = folder_path
    return _


class MysqlEngine:
    """
    MysqlEngine
    """

    def __init__(
            self,
            host: Optional[str] = None,
            port: Optional[int] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            database: Optional[str] = None,
            collection: Optional[str] = None,
            conn_timeout: Optional[int] = 30,
            conn_retries: Optional[int] = 5
    ):
        self.host = MYSQL_HOST if host is None else host
        self.port = MYSQL_PORT if port is None else port
        self.user = username
        self.password = password
        self.database = MYSQL_DATABASE if database is None else database
        self.collection = MYSQL_COLLECTION if collection is None else collection
        self.conn_timeout = MYSQL_CONN_TIMEOUT if conn_timeout is None else conn_timeout
        self.conn_retries = MYSQL_CONN_RETRIES if conn_retries is None else conn_retries
        self.mysql_core_ = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        self.cursor = self.mysql_core_.cursor(pymysql.cursors.DictCursor)
        self.collection_s_ = []
        self.collection_names = self.get_collection_names(self.cursor)

    def get_collection_names(self, cursor):
        cursor.execute('show tables')
        for core_ in cursor.fetchall():
            for k, v in core_.items():
                self.collection_s_.append(v)
        return self.collection_s_

    def to_csv(self, query=None,sql_:str=None, folder_path: str = None, filename: str = None, limit: int = 20,
               ignore_error: bool = False):
        """
        :param query: 查询字典
        :param sql_: 原生 sql 语句、str 类型
        :param folder_path: 指定导出的文件夹目录
        :param filename: 指定导出的文件名
        :param limit: 限制导出数量
        :param ignore_error: 是否忽略导出过程中出现的错误
        :return:
        """
        if query is None:
            query = {}

        if not isinstance(query, dict):
            raise TypeError('query must be of Dict type')
        if not isinstance(limit, int):
            raise TypeError("limit must be an integer type")
        if not isinstance(ignore_error, bool):
            raise TypeError("_id must be an boolean type")

        folder_path = check_folder_path(folder_path)

        if self.collection:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.csv'

            if sql_ is None:
                sql_ = f"select * from {self.collection};"
            else:
                if not isinstance(sql_, str):
                    raise TypeError("sql must be an str type")

            self.cursor.execute(sql_)
            doc_list_ = self.cursor.fetchall()

            import pyarrow as pa
            from pyarrow import csv as pa_csv_
            df_ = pa.Table.from_pylist(mapping=doc_list_, schema=None, metadata=None)
            with pa_csv_.CSVWriter(f'{folder_path}/{filename}', df_.schema) as writer:
                writer.write_table(df_)

        else:
            warnings.warn('No collection specified, All collections will be exported.', DeprecationWarning)
            # self.to_csv_s_()

    def __del__(self):
        #     self.cursor.close()
        self.mysql_core_.close()


if __name__ == '__main__':
    M = MysqlEngine(
        host=os.getenv('MYSQL_HOST', '192.168.0.141'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        username=os.getenv('MYSQL_USERNAME', 'root'),
        password=os.getenv('MYSQL_PASSWORD', '_admin_'),
        database=os.getenv('MYSQL_DATABASE', 'sm_admin'),
        collection=os.getenv('MYSQL_COLLECTION', 'sm_no_comment_scenic'),
    )

    M.to_csv()
    # M.to_json()
