import os
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor, ALL_COMPLETED, wait
from typing import Optional

import pandas
import pymysql
from dotenv import load_dotenv
from mongo2file.constants import *
from mongo2file.utils import to_str_datetime, serialize_obj

load_dotenv(verbose=True)
lock_ = threading.Lock()


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
            charset='utf8'
        )
        self.cursor = self.mysql_core_.cursor(pymysql.cursors.DictCursor)
        self.collection_s_ = []
        self.collection_names = self.get_collection_names(self.cursor)
        # print(self.collection_names)

    def get_collection_names(self, cursor):
        cursor.execute('show tables')
        for core_ in cursor.fetchall():
            for k, v in core_.items():
                self.collection_s_.append(v)
        return self.collection_s_

    def to_csv(self, query: dict, filename: str, limit: int = 20):
        if not isinstance(query, dict):
            raise TypeError('query must be of Dict type.')
        if self.collection:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}'
            sql = f"select * from {self.collection};"
            self.cursor.execute(sql)
            doc_list_ = self.cursor.fetchall()
            print(doc_list_)
            data = pandas.DataFrame(doc_list_)
            data.to_csv(path_or_buf=f'{filename}.csv')
        else:
            warnings.warn('No collection specified, All collections will be exported.', DeprecationWarning)
            self.to_csv_s_()

    def to_excel(self, query: dict, filename: str, limit: int = 20):
        if not isinstance(query, dict):
            raise TypeError('query must be of Dict type.')
        if self.collection:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}'
            sql = f"select * from {self.collection};"
            self.cursor.execute(sql)
            doc_list_ = self.cursor.fetchall()
            data = pandas.DataFrame(doc_list_)
            data.to_excel(path_or_buf=f'{filename}.csv')
        else:
            warnings.warn('No collection specified, All collections will be exported.', DeprecationWarning)
            self.to_excel_s_()

    def to_json(self, query: dict, filename: str = None, limit: int = 20):
        if not isinstance(query, dict):
            raise TypeError('query must be of Dict type.')
        if self.collection:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}'
            sql = f"select * from {self.collection};"
            self.cursor.execute(sql)
            doc_list_ = self.cursor.fetchall()
            with open(f'{filename}.json', 'w', encoding="utf-8") as f:
                [f.write(serialize_obj(data) + "\n") for data in doc_list_]
        else:
            warnings.warn('No collection specified, All collections will be exported.', DeprecationWarning)
            self.to_json_s_()

    def to_mongo(self):
        ...

    def no_collection_to_csv_(self, collection_: str, filename: str):
        if collection_:
            if filename is None:
                filename = f'{collection_}_{to_str_datetime()}'
            # cursor = self.mysql_core_.cursor(pymysql.cursors.DictCursor)
            lock_.acquire()  # get lock
            sql = f"select * from {collection_};"
            self.cursor.execute(sql)
            doc_list_ = self.cursor.fetchall()
            lock_.release()  # release lock
            data = pandas.DataFrame(doc_list_)
            data.to_csv(path_or_buf=f'{filename}.csv', encoding=PANDAS_ENCODING)

    def no_collection_to_excel_(self, collection_: str, filename: str):
        if collection_:
            if filename is None:
                filename = f'{collection_}_{to_str_datetime()}'

            cursor = self.mysql_core_.cursor(pymysql.cursors.DictCursor)
            sql = f"select * from {collection_};"
            cursor.execute(sql)
            doc_list_ = cursor.fetchall()
            data = pandas.DataFrame(doc_list_)
            data.to_excel(excel_writer=f'{filename}.xlsx', encoding=PANDAS_ENCODING)

    def no_collection_to_json_(self, collection_: str, filename: str):
        if collection_:
            if filename is None:
                filename = f'{collection_}_{to_str_datetime()}'
            sql = f"select * from {collection_};"
            self.cursor.execute(sql)
            doc_list_ = self.cursor.fetchall()
            with open(f'{filename}.json', 'w', encoding="utf-8") as f:
                [f.write(serialize_obj(data) + "\n") for data in doc_list_]

    def to_csv_s_(self):
        self.concurrent_(self.no_collection_to_csv_, self.collection_names)

    def to_excel_s_(self):
        self.concurrent_(self.no_collection_to_excel_, self.collection_names)

    def to_json_s_(self):
        self.concurrent_(self.no_collection_to_json_, self.collection_names)

    def concurrent_(self, func, collection_names):
        # todo 这里并发时需要加线程锁
        with ThreadPoolExecutor(max_workers=THREAD_POOL_MAX_WORKERS) as executor:
            futures_ = [executor.submit(func, collection_name, collection_name) for
                        collection_name in
                        collection_names]
            wait(futures_, return_when=ALL_COMPLETED)
            for future_ in futures_:
                print(future_)
                ...

    def __del__(self):
        # self.cursor.close()
        self.mysql_core_.close()
        ...


if __name__ == '__main__':
    M = MysqlEngine(
        host=os.getenv('MYSQL_HOST','192.168.0.141'),
        port=int(os.getenv('MYSQL_PORT',3306)),
        username=os.getenv('MYSQL_USERNAME','root'),
        password=os.getenv('MYSQL_PASSWORD',''),
        database=os.getenv('MYSQL_DATABASE',''),
        collection=os.getenv('MYSQL_COLLECTION',''),
    )
    M.to_csv(query={}, filename="_")
    # M.to_json()
