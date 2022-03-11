import os
import warnings
from typing import Optional

import alive_progress
import colorama
import pyarrow as pa
import pyarrow.csv as pa_csv_
import pyarrow.feather as pa_feather_
import pyarrow.parquet as pa_parquet_
import pymysql
import xlsxwriter

from .constants import *
from .utils import to_str_datetime, serialize_obj


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
    def __init__(
            self,
            host: Optional[str] = None,
            port: Optional[int] = None,
            username: Optional[str] = None,
            password: Optional[str] = None,
            database: Optional[str] = None,
            collection: Optional[str] = None,
            conn_timeout: Optional[int] = None,
            conn_retries: Optional[int] = None,
            charset: Optional[str] = None
    ):
        self.host = MYSQL_HOST if host is None else host
        self.port = MYSQL_PORT if port is None else port
        self.user = MYSQL_USERNAME if username is None else username
        self.password = MYSQL_PASSWORD if password is None else password
        self.database = MYSQL_DATABASE if database is None else database
        self.collection = MYSQL_COLLECTION if collection is None else collection
        self.connect_timeout = MYSQL_CONN_TIMEOUT if conn_timeout is None else conn_timeout
        self.conn_retries = MYSQL_CONN_RETRIES if conn_retries is None else conn_retries
        self.charset = MYSQL_CHARSET if charset is None else charset
        self.mysql_core_ = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
            charset=self.charset,
            connect_timeout=self.connect_timeout,
            cursorclass=pymysql.cursors.DictCursor
        )
        # _ = self.mysql_core_.get_server_info()
        # _ = self.mysql_core_.get_host_info()
        # _ = self.mysql_core_.get_proto_info()
        # _ = self.mysql_core_.host_info
        # _ = self.mysql_core_.connect_timeout
        # print(_)
        self.cursor = self.mysql_core_.cursor()
        self.collection_s_ = []
        self.collection_names = self.get_collection_names(self.cursor)

    def get_collection_names(self, cursor):
        cursor.execute('show tables')
        for core_ in cursor.fetchall():
            for k, v in core_.items():
                self.collection_s_.append(v)
        return self.collection_s_

    def to_csv(self, sql_: str = None, folder_path: str = None, filename: str = None,
               ignore_error: bool = False):
        """
        :param sql_: 原生 sql 语句、str 类型
        :param folder_path: 指定导出的文件夹目录
        :param filename: 指定导出的文件名
        :param ignore_error: 是否忽略导出过程中出现的错误
        :return:
        """
        if not isinstance(ignore_error, bool):
            raise TypeError("_id must be an boolean type")

        folder_path_ = check_folder_path(folder_path)

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

            df_ = pa.Table.from_pylist(mapping=doc_list_, schema=None, metadata=None)
            with pa_csv_.CSVWriter(f'{folder_path_}/{filename}', df_.schema) as writer:
                writer.write_table(df_)
            result_ = ECHO_INFO.format(colorama.Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_
        else:
            warnings.warn('No collection specified, All collections will be exported.', DeprecationWarning)

    def to_excel(self, sql_: str = None, folder_path: str = None, filename: str = None, mode: str = 'xlsx',
                 ignore_error: bool = True):
        """
        """
        folder_path_ = check_folder_path(folder_path)
        if self.collection:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.xlsx'
            if sql_ is None:
                sql_ = f"select * from {self.collection};"
            else:
                if not isinstance(sql_, str):
                    raise TypeError("sql must be an str type")

            self.cursor.execute(sql_)
            doc_objs_ = self.cursor.fetchall()
            with xlsxwriter.Workbook(f'{folder_path_}/{filename}') as work_book_:
                work_sheet_ = work_book_.add_worksheet('Sheet1')
                title_ = f'{colorama.Fore.GREEN} {self.collection} → {folder_path_}/{filename}'
                with alive_progress.alive_bar(len(doc_objs_), title=title_, bar="blocks") as bar:
                    header_ = list(doc_objs_[0].keys())
                    work_sheet_.write_row(f"A1", header_)
                    if ignore_error:
                        for index_, doc_ in enumerate(doc_objs_):
                            work_sheet_.write_row(f"A{index_ + 2}", list(doc_.values()))
                            bar()
            result_ = ECHO_INFO.format(colorama.Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_
        else:
            warnings.warn('No collection specified, All collections will be exported.', DeprecationWarning)

    def to_json(self, sql_: str = None, folder_path: str = None, filename: str = None):
        folder_path_ = check_folder_path(folder_path)
        if self.collection:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.json'
                if sql_ is None:
                    sql_ = f"select * from {self.collection};"
                else:
                    if not isinstance(sql_, str):
                        raise TypeError("sql must be an str type")
                self.cursor.execute(sql_)
                doc_objs_ = self.cursor.fetchall()
                data = {'RECORDS': doc_objs_}
                with open(f'{folder_path_}/{filename}', 'w', encoding="utf-8") as f:
                    f.write(serialize_obj(data))
                result_ = ECHO_INFO.format(colorama.Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
                return result_
        else:
            warnings.warn('No collection specified, All collections will be exported.', DeprecationWarning)

    def to_pickle(self, sql_: str = None, folder_path: str = None, filename: str = None):
        folder_path_ = check_folder_path(folder_path)
        if self.collection:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.pkl'
            if sql_ is None:
                sql_ = f"select * from {self.collection};"
            else:
                if not isinstance(sql_, str):
                    raise TypeError("sql must be an str type")
            self.cursor.execute(sql_)
            doc_objs_ = self.cursor.fetchall()

            import pickle
            with open(f'{folder_path_}/{filename}', 'wb') as f:
                pickle.dump(doc_objs_, f)

            result_ = ECHO_INFO.format(colorama.Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_

    def to_feather(self, sql_: str = None, folder_path: str = None, filename: str = None):
        """
        pip[conda] install pyarrow
        """
        folder_path_ = check_folder_path(folder_path)
        if self.collection:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.feather'
            if sql_ is None:
                sql_ = f"select * from {self.collection};"
            else:
                if not isinstance(sql_, str):
                    raise TypeError("sql must be an str type")
            self.cursor.execute(sql_)
            doc_objs_ = self.cursor.fetchall()
            df_ = pa.Table.from_pylist(mapping=doc_objs_, schema=None, metadata=None)
            with open(f'{folder_path_}/{filename}', 'wb') as f:
                pa_feather_.write_feather(df_, f)

            result_ = ECHO_INFO.format(colorama.Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_

    def to_parquet(self, sql_: str = None, folder_path: str = None, filename: str = None):
        folder_path_ = check_folder_path(folder_path)
        if self.collection:
            if filename is None:
                filename = f'{self.collection}_{to_str_datetime()}.parquet'
            if sql_ is None:
                sql_ = f"select * from {self.collection};"
            else:
                if not isinstance(sql_, str):
                    raise TypeError("sql must be an str type")
            self.cursor.execute(sql_)
            doc_objs_ = self.cursor.fetchall()
            df_ = pa.Table.from_pylist(mapping=doc_objs_, schema=None, metadata=None)
            with open(f'{folder_path_}/{filename}', 'wb') as f:
                pa_parquet_.write_table(df_, f)
            result_ = ECHO_INFO.format(colorama.Fore.GREEN, self.collection, f'{folder_path_}/{filename}')
            return result_

    def __del__(self):
        self.cursor.close()
        self.mysql_core_.close()
