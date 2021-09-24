#!/usr/local/bin/python3
# -*- coding: utf-8 -*-

import libs.common as common
import time

def wait_for_db():
    '''
    Wait until db is ready.
    '''
    tries = 0
    try_interval = 10
    while True:
        try:
            db = common.engine()
            conn = db.connect()
            conn.close()
            db.dispose()
            print("---- db exists.")
            break
        except Exception as e:
            tries += 1
            print("\n---- Check database error. Tried %s times. Try next time in %s seconds."%(tries, try_interval))
            print(e)
            time.sleep(try_interval)

def get_recent_hist(n):
    '''
    Get data from today to n days ago.
    '''
    # get a list of all stocks
    stocks_list = []
    # get data of each stock
    n_stocks = len(stocks_list)
    for i in range(n_stocks):
        # check data is already downloaded
        # download data
        pass

# main函数入口
if __name__ == '__main__':
    # wait for db
    wait_for_db()
    # initialize data
    get_recent_hist(100)
    