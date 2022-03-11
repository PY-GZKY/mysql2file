import os

import dotenv
import pytest

from mysql2file import MysqlEngine

dotenv.load_dotenv(verbose=True)


def setup_function():
    global M
    M = MysqlEngine(
        host=os.getenv('MYSQL_HOST', '192.168.0.141'),
        port=int(os.getenv('MYSQL_PORT', 3306)),
        username=os.getenv('MYSQL_USERNAME', 'root'),
        password=os.getenv('MYSQL_PASSWORD', ''),
        database=os.getenv('MYSQL_DATABASE', ''),
        collection=os.getenv('MYSQL_COLLECTION', ''),
    )


def test_to_csv():
    result_ = M.to_csv(folder_path="_csv")
    print(result_)
    assert "successfully" in result_


def test_to_excel():
    result_ = M.to_excel(folder_path="_excel", mode='xlsx', ignore_error=True)
    print(result_)
    assert "successfully" in result_


def test_to_json():
    result_ = M.to_json(folder_path="./_json")
    print(result_)
    assert "successfully" in result_


def test_to_pickle():
    result_ = M.to_pickle(folder_path="./_pickle")
    print(result_)
    assert "successfully" in result_


def test_to_feather():
    result_ = M.to_feather(folder_path="./_feather")
    print(result_)
    assert "successfully" in result_


def test_to_parquet():
    result_ = M.to_parquet(folder_path="./_parquet")
    print(result_)
    assert "successfully" in result_


def teardown_function():
    ...


if __name__ == "__main__":
    pytest.main(["-s", "test_single.py"])
