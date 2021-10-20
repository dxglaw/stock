#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import libs.common as common
import time
import datetime
import pandas as pd
import libs.common
import akshare as ak

def wait_for_db(try_interval=60):
    '''
    Wait until db is ready.
    '''
    print('----', __file__, ': wait_for_db()')
    tries = 0
    while True:
        try:
            db = common.engine()
            conn = db.connect()
            conn.close()
            db.dispose()
            print("    Database is ready.")
            break
        except Exception as e:
            tries += 1
            print("\n    Check database error. Tried %s times. Try next time in %s seconds."%(tries, try_interval))
            print(e)
            time.sleep(try_interval)

def get_recent_hist(ndays=100):
    '''
    Get data of 100 trade days starting from the recent trade day.
    '''
    print('----', __file__, ': get_recent_hist()')
    
    #### get a list of all stocks
    (start_date, stocks_list) = common.get_latest_stocks_list()

    ##### get a list of trade day
    trade_days = common.get_recent_trade_days(start_date, ndays)
    start_date_int = trade_days[-1].strftime("%Y%m%d")
    stop_date_int = trade_days[0].strftime("%Y%m%d")

    #### download
    uname_date = common.unify_names("date")
    uname_code = common.unify_names("code")
    n_stocks = stocks_list.shape[0]
    codes_list = stocks_list[uname_code].tolist()
    engine = common.engine()
    sql_tpl = "SELECT `%s` FROM `%s` WHERE `%s`=%s AND `%s`>=%s AND `%s`<=%s"
    for i in range(n_stocks):
        # check data is already downloaded
        sql_cmd = sql_tpl%(uname_date,\
                           common.TBL_NAME_DAILY_HIST,\
                           uname_code,\
                           codes_list[i],\
                           uname_date,\
                           start_date_int,\
                           uname_date,\
                           stop_date_int)
        dates_list = []
        with engine.connect() as conn:
            try:
                dates_list = pd.read_sql(sql_cmd, conn)
                dates_list = dates_list[uname_date].to_list()
            except  Exception as e:
                print("    ", __file__, ": pd.read_sql():", e)
        # if not downloaded, download and save to db
        min_days = max(30, ndays-10) # at least need 30-day data, ndays-10 in case there is no data for some dates
        n_dates_in_db = len(dates_list)
        noLatestData = stop_date_int not in dates_list  # data of stop_date_int is not in DB
        noEnoughData = n_dates_in_db < min_days         # data in DB is not enough
        if noLatestData or noEnoughData:
            # download and process data
            print("    ", __file__, ": Download data for :", codes_list[i], n_dates_in_db, noLatestData)
            common.get_daily_hist(codes_list[i], start_date_int, stop_date_int)
        else:
            # print("    ", __file__, ": Data exist:", codes_list[i])
            pass

if __name__ == '__main__':
    # wait for db
    wait_for_db()
    # initialize data
    get_recent_hist(100)
    