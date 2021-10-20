#!/usr/local/bin/python
# -*- coding: utf-8 -*-

# apk add py-mysqldb or

import platform
import datetime
import time
import sys
import os
import MySQLdb
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR
from sqlalchemy import inspect
import numpy as np
import pandas as pd
import traceback
import akshare as ak
from chinese_calendar import is_workday

# 使用环境变量获得数据库。兼容开发模式可docker模式。
MYSQL_HOST = os.environ.get('MYSQL_HOST') if (os.environ.get('MYSQL_HOST') != None) else "mysqldb"
MYSQL_USER = os.environ.get('MYSQL_USER') if (os.environ.get('MYSQL_USER') != None) else "pythonstock"
MYSQL_PWD = os.environ.get('MYSQL_PWD') if (os.environ.get('MYSQL_PWD') != None) else "281208,,dxg"
MYSQL_DB = os.environ.get('MYSQL_DB') if (os.environ.get('MYSQL_DB') != None) else "pythonstock"

MYSQL_CONN_URL = "mysql+mysqldb://" + MYSQL_USER + ":" + MYSQL_PWD + "@" + MYSQL_HOST + ":3306/" + MYSQL_DB + "?charset=utf8"
print(MYSQL_CONN_URL)

__version__ = "2.0.0"
# 每次发布时候更新。

# configurations
TBL_NAME_DAILY_HIST = 'daily_hist'


def engine():
    engine = create_engine(
        MYSQL_CONN_URL,
        encoding='utf8', convert_unicode=True)
    return engine

def engine_to_db(to_db):
    MYSQL_CONN_URL_NEW = "mysql+mysqldb://" + MYSQL_USER + ":" + MYSQL_PWD + "@" + MYSQL_HOST + ":3306/" + to_db + "?charset=utf8mb4"
    engine = create_engine(
        MYSQL_CONN_URL_NEW,
        encoding='utf8', convert_unicode=True)
    return engine

# 通过数据库链接 engine。
def conn():
    try:
        db = MySQLdb.connect(MYSQL_HOST, MYSQL_USER, MYSQL_PWD, MYSQL_DB, charset="utf8")
        # db.autocommit = True
    except Exception as e:
        print("    ", __file__, " : ", "conn error :", e)
    db.autocommit(on=True)
    return db.cursor()


# 定义通用方法函数，插入数据库表，并创建数据库主键，保证重跑数据的时候索引唯一。
def insert_db(data, table_name, write_index, primary_keys):
    # 插入默认的数据库。
    insert_other_db(MYSQL_DB, data, table_name, write_index, primary_keys, rename_columns=True)


# 增加一个插入到其他数据库的方法。
def insert_other_db(to_db, data, table_name, write_index, primary_keys, rename_columns=False):
    # 定义engine
    engine_mysql = engine_to_db(to_db)
    # 使用 http://docs.sqlalchemy.org/en/latest/core/reflection.html
    # 使用检查检查数据库表是否有主键。
    insp = inspect(engine_mysql)
    col_name_list = data.columns.tolist()
    # 如果有索引，把索引增加到varchar上面。
    if write_index:
        # 插入到第一个位置：
        col_name_list.insert(0, data.index.name)
    # rename table column names if required
    if rename_columns:
        unify_db_table_name(to_db, table_name)
    # insert data to db
    data.to_sql(name=table_name, con=engine_mysql, schema=to_db, if_exists='append',
                dtype={col_name: NVARCHAR(length=255) for col_name in col_name_list}, index=write_index)

    # print(insp.get_pk_constraint(table_name))
    # print()
    # print(type(insp))
    # 判断是否存在主键
    if insp.get_pk_constraint(table_name)['constrained_columns'] == []:
        with engine_mysql.connect() as con:
            # 执行数据库插入数据。
            try:
                con.execute('ALTER TABLE `%s` ADD PRIMARY KEY (%s);' % (table_name, primary_keys))
            except  Exception as e:
                print("    ", __file__, " : ", "ADD PRIMARY KEY ERROR :", e)




# 插入数据。
def insert(sql, params=()):
    with conn() as db:
        print("    ", __file__, " : ", "insert : " + sql)
        try:
            db.execute(sql, params)
        except  Exception as e:
            print("    ", __file__, " : ", "insert : error : ", e)


# 查询数据
def select(sql, params=()):
    with conn() as db:
        print("    ", __file__, " : ", "select :" + sql)
        try:
            db.execute(sql, params)
        except  Exception as e:
            print("    ", __file__, " : ", "select : error :", e)
        result = db.fetchall()
        return result


# 计算数量
def select_count(sql, params=()):
    with conn() as db:
        print("    ", __file__, " : ", "select sql: " + sql)
        try:
            db.execute(sql, params)
        except  Exception as e:
            print("    ", __file__, " : ", "error : ", e)
        result = db.fetchall()
        # 只有一个数组中的第一个数据
        if len(result) == 1:
            return int(result[0][0])
        else:
            return 0


# 通用函数。获得日期参数。
def run_with_args(run_fun):
    tmp_datetime_show = datetime.datetime.now()  # 修改成默认是当日执行 + datetime.timedelta()
    tmp_hour_int = int(tmp_datetime_show.strftime("%H"))
    if tmp_hour_int < 12 :
        # 判断如果是每天 中午 12 点之前运行，跑昨天的数据。
        tmp_datetime_show = (tmp_datetime_show + datetime.timedelta(days=-1))
    tmp_datetime_str = tmp_datetime_show.strftime("%Y-%m-%d %H:%M:%S.%f")
    print("\n------------------------- common.run_with_args %s %s  -------------------------" % (run_fun, tmp_datetime_str))
    str_db = "MYSQL_HOST :" + MYSQL_HOST + ", MYSQL_USER :" + MYSQL_USER + ", MYSQL_DB :" + MYSQL_DB
    start = time.time()
    # 要支持数据重跑机制，将日期传入。循环次数
    if len(sys.argv) == 3:
        # python xxx.py 2017-07-01 10
        tmp_year, tmp_month, tmp_day = sys.argv[1].split("-")
        loop = int(sys.argv[2])
        tmp_datetime = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day))
        for i in range(0, loop):
            # 循环插入多次数据，重复跑历史数据使用。
            # time.sleep(5)
            tmp_datetime_new = tmp_datetime + datetime.timedelta(days=i)
            try:
                run_fun(tmp_datetime_new)
            except Exception as e:
                print("    ", __file__, " : ", "error :", e)
                traceback.print_exc()
    elif len(sys.argv) == 2:
        # python xxx.py 2017-07-01
        tmp_year, tmp_month, tmp_day = sys.argv[1].split("-")
        tmp_datetime = datetime.datetime(int(tmp_year), int(tmp_month), int(tmp_day))
        try:
            run_fun(tmp_datetime)
        except Exception as e:
            print("    ", __file__, " : ", "error :", e)
            traceback.print_exc()
    else:
        # tmp_datetime = datetime.datetime.now() + datetime.timedelta(days=-1)
        try:
            run_fun(tmp_datetime_show)  # 使用当前时间
        except Exception as e:
            print("    ", __file__, " : ", "error :", e)
            traceback.print_exc()
    print("------------------------- common.run_with_args finish %s , use time: %s -------------------------" % (
        tmp_datetime_str, time.time() - start))


# 设置基础目录，每次加载使用。
bash_stock_tmp = "/data/cache/hist_data_cache/%s/%s/"
if not os.path.exists(bash_stock_tmp):
    os.makedirs(bash_stock_tmp)  # 创建多个文件夹结构。
    print("------------------------- common.py : init tmp dir -------------------------")


# 增加读取股票缓存方法。加快处理速度。
def get_hist_data_cache(code, date_start, date_end):
    cache_dir = bash_stock_tmp % (date_end[0:7], date_end)
    # 如果没有文件夹创建一个。月文件夹和日文件夹。方便删除。
    # print("cache_dir:", cache_dir)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    cache_file = cache_dir + "%s^%s.gzip.pickle" % (date_end, code)
    # 如果缓存存在就直接返回缓存数据。压缩方式。
    if os.path.isfile(cache_file):
        # print("    ", __file__, " : ", "read from cache #########", cache_file)
        return pd.read_pickle(cache_file, compression="gzip")
    else:
        # print("    ", __file__, " : ", "get data, write cache #########", code, date_start, date_end)
        stock = ak.stock_zh_a_hist(symbol= code, start_date=date_start, end_date=date_end, adjust="")
        stock.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'quote_change',
                         'ups_downs', 'turnover']
        if stock is None:
            return None
        stock = stock.sort_index(0)  # 将数据按照日期排序下。
        # print(stock)
        stock.to_pickle(cache_file, compression="gzip")
        return stock

# 600开头的股票是上证A股，属于大盘股
# 600开头的股票是上证A股，属于大盘股，其中6006开头的股票是最早上市的股票，
# 6016开头的股票为大盘蓝筹股；900开头的股票是上证B股；
# 000开头的股票是深证A股，001、002开头的股票也都属于深证A股，
# 其中002开头的股票是深证A股中小企业股票；
# 200开头的股票是深证B股；
# 300开头的股票是创业板股票；400开头的股票是三板市场股票。
def stock_a(code):
    # print(code)
    # print(type(code))
    # 上证A股  # 深证A股
    if code.startswith('600') or code.startswith('6006') or code.startswith('601') or code.startswith('000') or code.startswith('001') or code.startswith('002'):
        return True
    else:
        return False
# 过滤掉 st 股票。
def stock_a_filter_st(name):
    # print(code)
    # print(type(code))
    # 上证A股  # 深证A股
    if name.find("ST") == -1:
        return True
    else:
        return False

# 过滤价格，如果没有基本上是退市了。
def stock_a_filter_price(latest_price):
    # float 在 pandas 里面判断 空。
    if np.isnan(latest_price):
        return False
    else:
        return True

def get_daily_hist(code, start_date_int, stop_date_int):
    '''
    Get daily hist of one stock from start_date_int to stop_date_int.
    '''
    uname_date = unify_names("date")
    uname_code = unify_names("code")
    try:
        # download
        hist_daily = ak.stock_zh_a_hist(symbol=code,\
                                        start_date=start_date_int,\
                                        end_date=stop_date_int,\
                                        adjust="")
        # unify column names
        hist_daily.columns = [unify_names(i) for i in hist_daily.columns]
        # convert date format form yyyy-mm-dd to yyyymmdd
        hist_daily[uname_date] = hist_daily[uname_date].apply(lambda x: x.replace("-", ""))
        # add code
        hist_daily[uname_code] = code
        # set date as index
        hist_daily.set_index(uname_date, inplace=True)
    except  Exception as e:
        print("    ", __file__, ": ak.stock_zh_a_hist():", e)
    # insert to db
    try:
        # delete data already downloaded before
        sql_del = "DELETE FROM `%s` WHERE `%s`=%s AND `%s`>=%s AND `%s`<=%s"
        sql_cmd = sql_del%(TBL_NAME_DAILY_HIST,\
                            uname_code,\
                            code,\
                            uname_date,\
                            start_date_int,\
                            uname_date,\
                            stop_date_int)
        insert(sql_cmd)
        # insert complete data just downloaded
        prim_keys = "`%s`,`%s`"%(uname_date, uname_code)
        insert_db(hist_daily, TBL_NAME_DAILY_HIST, True, prim_keys)
    except  Exception as e:
        print("    ", __file__, ": insert_db():", e)

def get_latest_stocks_list():
    '''
    Get a list of latest stocks.
    '''
    # all stocks of today
    try:
        print("    ", __file__, " : ", "get_latest_stocks_list()")
        trade_day = get_recent_trade_day(end_of_day=True)
        trade_day_int = trade_day.strftime("%Y%m%d")
        print("    ", __file__, " : ", "Recent trade day is", trade_day_int)
        # get all stocks of today
        data = ak.stock_zh_a_spot_em()
        data.columns = [unify_names(i) for i in data.columns]

        data = data.loc[data[unify_names("code")].apply(stock_a)]\
                   .loc[data[unify_names("name")].apply(stock_a_filter_st)]\
                   .loc[data[unify_names("latest_price")].apply(stock_a_filter_price)]
        data[unify_names("date")] = trade_day_int  # 修改时间成为int类型。

        # 删除老数据。
        del_sql = " DELETE FROM `stock_zh_ah_name` where `date` = '%s' " % trade_day_int
        insert(del_sql)

        # delete index
        data.drop(unify_names("index"), axis=1, inplace=True)

        print("    ", __file__, " : ", "Number of stocks is", len(data))

        # 删除index，然后和原始数据合并。
        insert_db(data, "stock_zh_ah_name", False, "`date`,`code`")
        
        # return data
        return trade_day, data
    except Exception as e:
        print("error :", e)
        return []

def is_trade_day(year, month, day):
    this_date = datetime.date(year, month, day)
    return is_workday(this_date) and this_date.weekday()<5

def get_recent_trade_day(end_of_day=False):
    '''
    Get the recent trade day. If today is the trade day and hour<17, return previous trade day.
    '''
    today = datetime.datetime.now() 
    one_day = datetime.timedelta(days=1)
    if end_of_day and today.hour < 17 :    # 
        today -= one_day
    while not is_trade_day(today.year, today.month, today.day):
        today -= one_day
    return today

def get_recent_trade_days(start_date, ndays=100):
    '''
    Get ndays trade days starting from start_date. start_date is the latest date.
    start_date should be valid trade day. If not, an empty list will be returned.
    '''
    trade_days = []
    if is_trade_day(start_date.year, start_date.month, start_date.day):
        one_day = datetime.timedelta(days=1)
        trade_days.append(start_date)
        while len(trade_days) < ndays:
            this_day = trade_days[-1] - one_day
            while not is_trade_day(this_day.year, this_day.month, this_day.day):
                this_day -= one_day
            trade_days.append(this_day)
    return trade_days

def unify_db_table_name(db_name, table_name):
    '''
    Change column names to unified names.
    '''
    # db engine
    engine_mysql = engine_to_db(db_name)
    # get a list of the column names in the table
    all_column_names, all_column_types = get_column_names_types_from_table(db_name, table_name)
    # change names
    new_all_column_names = [unify_names(raw_name) for raw_name in all_column_names]
    if all_column_names != new_all_column_names:
        alter_db_table_name(db_name, table_name, all_column_names, new_all_column_names, all_column_types)

def alter_db_table_name(db_name, table_name, old_names, new_names, col_types):
    '''
    Change a list of column names to new names in table_name of db_name
    '''
    # check input
    n_columns = len(old_names)
    if n_columns != len(new_names) or n_columns != len(col_types):
        print('old_names, new_names and col_types have different numbers of columns.')
        return False
    # db engine
    engine_mysql = engine_to_db(db_name)
    # change names
    sql = "ALTER TABLE `%s` CHANGE `%s` `%s` %s"
    with engine_mysql.connect() as conn:
        for i in range(n_columns):
            if old_names[i] != new_names[i]:
                this_sql = sql%(table_name, old_names[i], new_names[i], col_types[i])
                try:
                    res = conn.execute(this_sql)
                except  Exception as e:
                    print("    ", __file__, " : ", "alter_db_table_name : error :", e)
                    return False
    return True

def get_column_names_types_from_table(db_name, table_name):
    '''
    Get a list of the names of all the columns in the table_name in db_name
    '''
    column_names = []
    column_types = []
    sql = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'%s'"%table_name
    engine_mysql = engine_to_db(db_name)
    with engine_mysql.connect() as conn:
        try:
            res = conn.execute(sql)
            res = res.fetchall()
            column_names = [this_column[3] for this_column in res]
            column_types = ['%s(%d)'%(this_column[7],this_column[8]) for this_column in res]
        except  Exception as e:
            print("    ", __file__, " : ", "get_column_names_from_table : error :", e)
    return column_names, column_types


def unify_names(raw_name):
    '''
    Unify names of each column of data.
    '''
    mapping = {
        **dict.fromkeys(['序号'], 'index'),
        **dict.fromkeys(['代码', '股票代码', '证券代码'], 'code'),
        **dict.fromkeys(['名称', '股票名称', '证券名称', '证券简称'], 'name'),
        **dict.fromkeys(['日期', '交易日期'], 'date'),
        **dict.fromkeys(['价格', '最新价', '最新价格', 'latest_price', 'latest price'], 'price'),
        **dict.fromkeys(['最高', '最高价'], 'high'),
        **dict.fromkeys(['最低', '最低价'], 'low'),
        **dict.fromkeys(['今开', '开盘', '开盘价'], 'open'),
        **dict.fromkeys(['今收', '收盘', '收盘价', 'close_price'], 'close'),
        **dict.fromkeys(['昨收'], 'pre-close'),
        **dict.fromkeys(['涨跌幅', 'quote_change'], 'pct_change'),
        **dict.fromkeys(['涨跌额', 'ups_downs'], 'change'),
        **dict.fromkeys(['振幅', 'amplitude'], 'swing'),
        '成交量': 'volume',
        **dict.fromkeys(['成交额', 'turnover'], 'amount'),
        **dict.fromkeys(['成交均价', 'average_price'], 'avg_price'),
        **dict.fromkeys(['量比', 'quantity_ratio'], 'vol_ratio'),
        **dict.fromkeys(['换手率', 'turnover_rate'], 'turnover_ratio'),
        **dict.fromkeys(['市盈率-动态', '动态市盈率'], 'pe_dynamic'),
        '市净率': 'pb',
        '上榜次数': 'ranking_times',
        '累积购买额': 'sum_buy',
        '累积卖出额': 'sum_sell',
        '净额': 'net_amount',
        '买入席位数': 'buy_seat',
        '卖出席位数': 'sell_seat',
        '折溢率': 'overflow_rate',
        '成交笔数': 'trade_number',
        '成交总量': 'sum_volume',
        '成交总额': 'sum_amount',
        **dict.fromkeys(['成交总额/流通市值', 'turnover_market_rate'], 'sum_turnover_ratio')
    }
    try:
        return mapping[raw_name]
    except:
        return raw_name

if __name__ == '__main__':
    # print(is_trade_day(2021, 9, 17))
    # get_latest_stocks_list()
    # unify_db_table_name('pythonstock', TBL_NAME_DAILY_HIST)
    print(get_recent_trade_day(True))
