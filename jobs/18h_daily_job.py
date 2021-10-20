#!/usr/local/bin/python3
# -*- coding: utf-8 -*-


import libs.common as common
import sys
import time
import pandas as pd
import numpy as np
from sqlalchemy.types import NVARCHAR
from sqlalchemy import inspect
import datetime
import akshare as ak

import MySQLdb



####### 3.pdf 方法。宏观经济数据
# 接口全部有错误。只专注股票数据。
def stat_all(tmp_datetime):
    print('----', __file__ + ': 0: stat_all')
    datetime_str = (tmp_datetime).strftime("%Y-%m-%d")
    datetime_int = (tmp_datetime).strftime("%Y%m%d")
    print("datetime_str:", datetime_str, "    datetime_int:", datetime_int)

    # 股票列表
    try:
        print('----', __file__ + ': 1: stock_zh_a_spot_em')
        data = ak.stock_zh_a_spot_em()
        # print(data.index)
        # 解决ESP 小数问题。
        # data["esp"] = data["esp"].round(2)  # 数据保留2位小数
        data.columns = ['index', 'code', 'name', 'price', 'pct_change', 'change', 'volume', 'amount',
                        'swing', 'high', 'low', 'open', 'pre-close', 'vol_ratio', 'turnover_ratio', 'pe_dynamic',
                        'pb']

        data = data.loc[data["code"].apply(common.stock_a)].loc[data["name"].apply(common.stock_a_filter_st)].loc[
            data["latest_price"].apply(common.stock_a_filter_price)]
        data['date'] = datetime_int  # 修改时间成为int类型。

        # 删除老数据。
        del_sql = " DELETE FROM `stock_zh_ah_name` where `date` = '%s' " % datetime_int
        common.insert(del_sql)

        data.set_index('code', inplace=True)
        data.drop('index', axis=1, inplace=True)

        print('data count: %s'%len(data))

        # 删除index，然后和原始数据合并。
        common.insert_db(data, "stock_zh_ah_name", True, "`date`,`code`")
    except Exception as e:
        print("error :", e)



    # 龙虎榜-个股上榜统计
    # 接口: stock_sina_lhb_ggtj
    #
    # 目标地址: http://vip.stock.finance.sina.com.cn/q/go.php/vLHBData/kind/ggtj/index.phtml
    #
    # 描述: 获取新浪财经-龙虎榜-个股上榜统计
    #

    try:
        print('----', __file__ + ': 2: stock_sina_lhb_ggtj')
        stock_sina_lhb_ggtj = ak.stock_sina_lhb_ggtj(recent_day="5")

        stock_sina_lhb_ggtj.columns = ['code', 'name', 'ranking_times', 'sum_buy', 'sum_sell', 'net_amount', 'buy_seat',
                                       'sell_seat']

        stock_sina_lhb_ggtj = stock_sina_lhb_ggtj.loc[stock_sina_lhb_ggtj["code"].apply(stock_a)].loc[
            stock_sina_lhb_ggtj["name"].apply(stock_a_filter_st)]

        stock_sina_lhb_ggtj.set_index('code', inplace=True)
        # data_sina_lhb.drop('index', axis=1, inplace=True)
        # 删除老数据。
        stock_sina_lhb_ggtj['date'] = datetime_int  # 修改时间成为int类型。

        # 删除老数据。
        del_sql = " DELETE FROM `stock_sina_lhb_ggtj` where `date` = '%s' " % datetime_int
        common.insert(del_sql)

        print('stock_sina_lhb_ggtj count: %s'%len(stock_sina_lhb_ggtj))

        common.insert_db(stock_sina_lhb_ggtj, "stock_sina_lhb_ggtj", True, "`date`,`code`")

    except Exception as e:
        print("error :", e)


    # 每日统计
    # 接口: stock_dzjy_mrtj
    #
    # 目标地址: http://data.eastmoney.com/dzjy/dzjy_mrtj.aspx
    #
    # 描述: 获取东方财富网-数据中心-大宗交易-每日统计

    try:
        print('----', __file__ + ': 3: stock_dzjy_mrtj')
        stock_dzjy_mrtj = ak.stock_dzjy_mrtj(start_date=datetime_str, end_date=datetime_str)

        stock_dzjy_mrtj.columns = ['index', 'trade_date', 'code', 'name', 'quote_change', 'close_price', 'average_price',
                                   'overflow_rate', 'trade_number', 'sum_volume', 'sum_turnover',
                                   'turnover_market_rate']

        stock_dzjy_mrtj.set_index('code', inplace=True)
        # data_sina_lhb.drop('index', axis=1, inplace=True)
        # 删除老数据。
        stock_dzjy_mrtj['date'] = datetime_int  # 修改时间成为int类型。
        stock_dzjy_mrtj.drop('trade_date', axis=1, inplace=True)
        stock_dzjy_mrtj.drop('index', axis=1, inplace=True)

        # 数据保留2位小数
        try:
            stock_dzjy_mrtj = stock_dzjy_mrtj.loc[stock_dzjy_mrtj["code"].apply(stock_a)].loc[
                stock_dzjy_mrtj["name"].apply(stock_a_filter_st)]

            stock_dzjy_mrtj["average_price"] = stock_dzjy_mrtj["average_price"].round(2)
            stock_dzjy_mrtj["overflow_rate"] = stock_dzjy_mrtj["overflow_rate"].round(4)
            stock_dzjy_mrtj["turnover_market_rate"] = stock_dzjy_mrtj["turnover_market_rate"].round(6)
        except Exception as e:
            print("round error :", e)

        # 删除老数据。
        del_sql = " DELETE FROM `stock_dzjy_mrtj` where `date` = '%s' " % datetime_int
        common.insert(del_sql)

        print('stock_dzjy_mrtj count: %s'%len(stock_dzjy_mrtj))

        common.insert_db(stock_dzjy_mrtj, "stock_dzjy_mrtj", True, "`date`,`code`")

    except Exception as e:
        print("error :", e)

# main函数入口
if __name__ == '__main__':
    # 执行数据初始化。
    # 使用方法传递。
    tmp_datetime = common.run_with_args(stat_all)
