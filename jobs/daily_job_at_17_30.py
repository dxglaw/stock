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



def get_all_stocks(tmp_datetime):
    print('----', __file__ + ': 0: get_all_stocks')
    datetime_str = (tmp_datetime).strftime("%Y-%m-%d")
    datetime_int = (tmp_datetime).strftime("%Y%m%d")
    print("datetime_str:", datetime_str, "    datetime_int:", datetime_int)

    # all stocks of today
    try:
        print('----', __file__ + ': 1: stock_zh_a_spot_em')
        # get all stocks of today
        data = ak.stock_zh_a_spot_em()
        # print(data.index)
        # 解决ESP 小数问题。
        # data["esp"] = data["esp"].round(2)  # 数据保留2位小数
        data.columns = [common.unify_names(i) for i in data.columns]

        data = data.loc[data[common.unify_names("code")].apply(common.stock_a)]\
                   .loc[data[common.unify_names("name")].apply(common.stock_a_filter_st)]\
                   .loc[data[common.unify_names("latest_price")].apply(common.stock_a_filter_price)]
        data[common.unify_names("date")] = datetime_int  # 修改时间成为int类型。

        # 删除老数据。
        del_sql = " DELETE FROM `stock_zh_ah_name` where `date` = '%s' " % datetime_int
        common.insert(del_sql)

        data.set_index(common.unify_names("code"), inplace=True)
        data.drop(common.unify_names("index"), axis=1, inplace=True)

        print("data count: %s"%len(data))

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
        lhb_data = ak.stock_sina_lhb_ggtj(recent_day="5")

        lhb_data.columns = ['code', 'name', 'ranking_times', 'sum_buy', 'sum_sell', 'net_amount', 'buy_seat',
                            'sell_seat']

        lhb_data = lhb_data.loc[lhb_data[common.unify_names("code")].apply(common.stock_a)]\
                           .loc[lhb_data[common.unify_names("name")].apply(common.stock_a_filter_st)]

        lhb_data.set_index('code', inplace=True)
        # data_sina_lhb.drop('index', axis=1, inplace=True)
        # 删除老数据。
        lhb_data['date'] = datetime_int  # 修改时间成为int类型。

        # 删除老数据。
        del_sql = " DELETE FROM `stock_sina_lhb_ggtj` where `date` = '%s' " % datetime_int
        common.insert(del_sql)

        print('lhb_data count: %s'%len(lhb_data))

        common.insert_db(lhb_data, "stock_sina_lhb_ggtj", True, "`date`,`code`")

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
        dzjy_data = ak.stock_dzjy_mrtj(start_date=datetime_str, end_date=datetime_str)

        dzjy_data.columns = [common.unify_names(i) for i in dzjy_data.columns]

        dzjy_data.set_index('code', inplace=True)
        # 删除老数据。
        dzjy_data['date'] = datetime_int  # 修改时间成为int类型。
        dzjy_data.drop('index', axis=1, inplace=True)

        # 数据保留2位小数
        try:
            dzjy_data = dzjy_data.loc[dzjy_data[common.unify_names("code")].apply(common.stock_a)]\
                                 .loc[dzjy_data[common.unify_names("name")].apply(common.stock_a_filter_st)]

            dzjy_data[common.unify_names("average_price")] = \
                dzjy_data[common.unify_names("average_price")].round(2)
            dzjy_data[common.unify_names("overflow_rate")] = \
                dzjy_data[common.unify_names("overflow_rate")].round(4)
            dzjy_data[common.unify_names("turnover_market_rate")] = \
                dzjy_data[common.unify_names("turnover_market_rate")].round(6)
        except Exception as e:
            print("round error :", e)

        # 删除老数据。
        del_sql = " DELETE FROM `stock_dzjy_mrtj` where `date` = '%s' " % datetime_int
        common.insert(del_sql)

        print('dzjy_data count: %s'%len(dzjy_data))

        common.insert_db(dzjy_data, "stock_dzjy_mrtj", True, "`date`,`code`")

    except Exception as e:
        print("error :", e)

# main函数入口
if __name__ == '__main__':
    # 执行数据初始化。
    # 使用方法传递。
    tmp_datetime = common.run_with_args(get_all_stocks)

