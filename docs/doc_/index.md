<h1 align="center">Mysql2file</h1>
<p align="center">
  <img src="https://img.shields.io/badge/Python-3.7 | 3.8 | 3.9-blue" />
  <img src="https://img.shields.io/badge/license-Apache-green" />
  <img src="https://img.shields.io/badge/pypi-v1.0.1-red" />
</p>


↻ **_Mysql2file_** 是一个 `Mysql` 数据库转换为表格文件的库。

## 快速

`mysql2file` 依赖于 `PyArrow` 库。 它是 `C++ Arrow` 的 `Python` 版本实现。

`PyArrow` 目前与 `Python 3.7、3.8、3.9` 和 `3.10` 兼容。

如果您在 `Windows` 上遇到任何的导入问题或错误，您可能需要安装 `Visual Studio 2015`。

> 警告:
> `PyArrow` 目前只支持到 `win64` 位 ( Python `64bit` ) 操作系统。

其次, 除了常见的 `csv`、`excel`、以及 `json` 文件格式之外, `mysql2file` 还支持导出 `pickle`、`feather`、`parquet` 的二进制压缩文件。

`pickle`、`feather`、`parquet` 是 `Python` 序列化数据的一种文件格式, 它把数据转成二进制进行存储。 从而大大减少的读取的时间。

## 依赖

- `PyArrow >= 7.0.0`

#### 安装

```shell
pip install mysql2file
```

## 基本用法

#### 快速开始

```python
import os
from mysql2file import MysqlEngine
```


## Reference
...

## 致谢

- [Arrow](https://github.com/apache/arrow)

## License

- [Apache License]()
