import psycopg2
import datetime
import pandas as pd
import numpy as np

def get_conn():
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="postgres"
        )
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn


def test_connect():
    """测试数据库连接"""
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = get_conn()
        # create a cursor
        cur = conn.cursor()

	    # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

	# close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def query(sql):
    """查询获取数据"""
    conn = None
    try:
        conn = get_conn()
        # create a cursor
        cur = conn.cursor()
        # execute a statement
        cur.execute(sql)
        rows = cur.fetchall()
        # close the communication with the PostgreSQL
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def insert(sql, data_array):
    """插入数据
    data_array: 每条数据是一个元组
    """
    conn = None
    try:
        conn = get_conn()
        # create a cursor
        cur = conn.cursor()
        # execute a statement
        cur.executemany(sql, data_array)
        # commit the changes to the database
        conn.commit()
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def insert_company_info(company_infos):
    """插入上市公司信息"""
    """ insert multiple stocks into the company_info table """
    sql = """INSERT INTO company_info(code, name, exchange, listed)
             VALUES(%s, %s, %s, %s)"""
    insert(sql, company_infos)


def get_company_infos():
    """
    返回股票代码和上市日期
    """
    sql = "SELECT code, listed, exchange from company_info order by code asc"
    return query(sql)


def get_existing_stock_infos():
    """
    返回当前数据库中的股票和最新的交易数据日期
    """
    sql = "select code, MAX(date) from stocks_daily_hfq group by code"
    return query(sql)


def insert_stock_data(stock_data):
    """插入股票数据，以天为单位"""
    """ insert multiple stocks data into the stocks table """
    sql = """INSERT INTO stocks_daily_hfq(code, date, open, high, low, close, volume)
             VALUES(%s, %s, %s, %s, %s, %s, %s)"""
    insert(sql, stock_data)


def get_stock_data(code):
    """获取股票交易数据"""
    sql = "select * from stocks_daily_hfq where code='{}'".format(code)

    return query(sql)


def get_stock_data_between(code, start_date, end_date=datetime.date.today().isoformat()):
    """获取股票交易数据
    code, date, open, high, low, close, volume
    """
    sql = "select * from stocks_daily_hfq where code='{}' and date>'{}' and date<'{}'".format(code, start_date, end_date)

    return query(sql)


def generate_avg_5_hdf(hdf_file):
    """生成5日均线数据"""
    companies = get_company_infos()

    for company in companies:
        code = company[0]
        exchange = company[2]
        data = get_stock_data(code)
        df = pd.DataFrame(data, columns=["code", "date", "open", "high", "low", "close", "volume"])
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

        close_price = df["close"]
        avg5 = np.zeros((len(close_price,)))
        avg5[:5] = np.nan

        for i in range(5, len(avg5)):
            avg5[i] = np.sum(close_price[i-5:i]) / 5.0

        df["avg5"] = avg5

        data = df.loc[:, "avg5"]

        data.to_hdf(hdf_file, key="{}_{}".format(exchange, code), mode='a', complevel=4, complib="zlib")
        print(code, " avg5 created")


def generate_avg_10_hdf(hdf_file):
    """生成10日均线数据"""
    companies = get_company_infos()

    for company in companies:
        code = company[0]
        exchange = company[2]
        data = get_stock_data(code)
        df = pd.DataFrame(data, columns=["code", "date", "open", "high", "low", "close", "volume"])
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

        close_price = df["close"]
        avg10 = np.zeros((len(close_price,)))
        avg10[:10] = np.nan

        for i in range(10, len(avg10)):
            avg10[i] = np.sum(close_price[i-10:i]) / 10.0

        df["avg10"] = avg10

        data = df.loc[:, "avg10"]

        data.to_hdf(hdf_file, key="{}_{}".format(exchange, code), mode='a', complevel=4, complib="zlib")
        print(code, " avg10 created")


def generate_avg_20_hdf(hdf_file):
    """生成20日均线数据"""
    companies = get_company_infos()

    for company in companies:
        code = company[0]
        exchange = company[2]
        data = get_stock_data(code)
        df = pd.DataFrame(data, columns=["code", "date", "open", "high", "low", "close", "volume"])
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

        close_price = df["close"]
        avg20 = np.zeros((len(close_price,)))
        avg20[:20] = np.nan

        for i in range(20, len(avg20)):
            avg20[i] = np.sum(close_price[i-20:i]) / 20.0

        df["avg20"] = avg20

        data = df.loc[:, "avg20"]

        data.to_hdf(hdf_file, key="{}_{}".format(exchange, code), mode='a', complevel=4, complib="zlib")
        print(code, " avg20 created")


def generate_avg_30_hdf(hdf_file):
    """生成30日均线数据"""
    companies = get_company_infos()

    for company in companies:
        code = company[0]
        exchange = company[2]
        data = get_stock_data(code)
        df = pd.DataFrame(data, columns=["code", "date", "open", "high", "low", "close", "volume"])
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

        close_price = df["close"]
        avg30 = np.zeros((len(close_price,)))
        avg30[:30] = np.nan

        for i in range(30, len(avg30)):
            avg30[i] = np.sum(close_price[i-30:i]) / 30.0

        df["avg30"] = avg30

        data = df.loc[:, "avg30"]

        data.to_hdf(hdf_file, key="{}_{}".format(exchange, code), mode='a', complevel=4, complib="zlib")
        print(code, " avg30 created")


def generate_close_price_hdf(hdf_file):
    """生成每日收盘数据"""
    companies = get_company_infos()

    for company in companies:
        code = company[0]
        exchange = company[2]
        data = get_stock_data(code)
        df = pd.DataFrame(data, columns=["code", "date", "open", "high", "low", "close", "volume"])
        df.set_index("date", inplace=True)

        data = df.loc[:, "close"]

        data.to_hdf(hdf_file, key="{}_{}".format(exchange, code), mode='a', complevel=4, complib="zlib")
        print(code, " price created")


def to_db_tuples(code, hist_df):
    result = [(code, listed, open, high, low, close, volume)
            for listed, open, high, low, close, volume
            in zip(hist_df['日期'], hist_df['开盘'], hist_df['最高'], hist_df['最低'], hist_df['收盘'], hist_df['成交额'])]
    return result


if __name__ == '__main__':
    test_connect()