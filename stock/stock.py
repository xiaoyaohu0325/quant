import pandas as pd
import seaborn as sns
import datetime
import db

avgs = ["avg_10", "avg_20", "avg_30"]

def find_cross():
    companies = db.get_company_infos()

    for company in companies:
        (code, _, exg) = company
        full_code = "{}_{}".format(exg, code)
        print("process", full_code)

        for avg in avgs:
            avg_series = pd.read_hdf("{}.h5".format(avg), key=full_code)
            price = pd.read_hdf("close_price.h5", key=full_code)
            size = len(avg_series)

            start_index = 0
            end_index = 0
            records = []

            for idx in range(1, size):
                if price[idx-1] < avg_series[idx] and price[idx] > avg_series[idx]:
                    start_index = idx

                if price[idx-1] > avg_series[idx] and price[idx] < avg_series[idx]:
                    end_index = idx
                    if start_index > 0: # find a pair of data
                        middle_price = 0.0
                        if end_index - start_index > 20:
                            middle_price = price[start_index + 20]
                        records.append((
                            full_code,
                            avg,
                            avg_series.index[start_index].isoformat(),
                            avg_series.index[end_index].isoformat(),
                            end_index - start_index,
                            price[start_index],
                            price[end_index],
                            middle_price
                        ))
                        start_index = 0
                        end_index = 0

            if len(records) > 0:
                output_df = pd.DataFrame(
                    records,
                    columns=["code", "avg_type", "start_date", "end_date", "days", "start_price", "end_price", "middle_price"]
                )
                output_df.to_hdf("cross_avg_stock.h5", key="cross_stock", format="table", mode='a', append=True, complevel=4, complib="zlib")


def plot_stock(full_code, start, end, span):
    offset = 5
    start_date = datetime.date.fromisoformat(start) - datetime.timedelta(days=offset)
    end_date = datetime.date.fromisoformat(end) + datetime.timedelta(days=offset)
    start_date_str = start_date.isoformat()
    end_date_str = end_date.isoformat()
    datalist = db.get_stock_data_between(full_code.split('_')[1], start_date_str, end_date_str)

    data = pd.read_hdf("avg_{}.h5".format(span), key=full_code)

    df = pd.DataFrame(datalist, columns=["code", "date", "open", "high", "low", "close", "volume"])
    df.set_index("date", inplace=True)
    df = df.loc[:, ["close"]]

    avg_data = data.loc[start_date:end_date]

    df["avg"] = avg_data
    df["id"] = df.index

    df2 = df.melt("id", var_name="type", value_name="price")

    fig = sns.relplot(data=df2, x="id", y="price", hue="type", kind="line")
    fig.set(title="{} cross avg {:2d}".format(full_code, span))
    fig.set_xticklabels(rotation=40, ha="right")
    fig.savefig("figure/{:2d}-{}-{}-{}.png".format(span, full_code, start_date_str, end_date_str), format="png")
    return

if __name__ == '__main__':
    find_cross()