from importlib.machinery import SourceFileLoader
from pathlib import Path

from setuptools import setup

description = '↻ 一个 Mysql 数据库转换为表格文件的库'
readme = Path(__file__).parent / 'README.md'
if readme.exists():
    long_description = readme.read_text(encoding='utf_8_sig')
else:
    long_description = description + '.\n\nSee https://mysql2file.readthedocs.io for documentation.'

version = SourceFileLoader('version', 'mysql2file/version.py').load_module()
readme = Path(__file__).parent / 'README.md'
if readme.exists():
    long_description = readme.read_text(encoding='utf-8')

setup(
    name='mysql2file',
    version=version.__version__,
    description=description,
    long_description=long_description,
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Framework :: AsyncIO',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: Unix',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Internet',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Clustering',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Monitoring',
        'Topic :: System :: Systems Administration',
    ],
    python_requires='>=3.7',
    author='PY-GZKY',
    author_email='341796767@qq.com',
    url='https://github.com/PY-GZKY/mysql2file',
    license='MIT',
    packages=['mysql2file'],
    include_package_data=True,
    zip_safe=True,
    entry_points="""
        [console_scripts]
        mysql2file=mysql2file.cli:cli
    """,
    install_requires=[
        'alive_progress==2.3.1',
        'colorama==0.4.4',
        'PyMySQL==0.9.3',
        'pyarrow==7.0.0',
        'XlsxWriter==3.0.2'
    ],
)
