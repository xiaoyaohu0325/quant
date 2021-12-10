import pandas as pd
import seaborn as sns

def plot(datatype, year):
    df = pd.read_hdf("avg_index.h5", key=datatype)
    df["id"] = df["date"]
    df.set_index("date", inplace=True)

    start_date = "{}-01-01".format(year)
    end_date = "{}-12-31".format(year)
    frag = df.loc[start_date:end_date]

    df_array = []
    for avgtype in ["avg5", "avg10", "avg20", "avg30"]:
        avg = frag.loc[:, ["close", avgtype, "id"]]
        avg.columns=["close", "avg", "id"]
        melt_avg = avg.melt("id", var_name="type", value_name="price")
        melt_avg["days"] = avgtype
        df_array.append(melt_avg)

    df = pd.concat(df_array)
    sns.relplot(data=df, x="id", y="price", hue="type", col="days", kind="line")


def plot_between(datatype, start_date, end_date):
    df = pd.read_hdf("avg_index.h5", key=datatype)
    df["id"] = df["date"]
    df.set_index("date", inplace=True)

    frag = df.loc[start_date:end_date]

    df_array = []
    for avgtype in ["avg5", "avg10", "avg20", "avg30"]:
        avg = frag.loc[:, ["close", avgtype, "id"]]
        avg.columns=["close", "avg", "id"]
        melt_avg = avg.melt("id", var_name="type", value_name="price")
        melt_avg["days"] = avgtype
        df_array.append(melt_avg)

    df = pd.concat(df_array)
    sns.relplot(data=df, x="id", y="price", hue="type", col="days", kind="line")