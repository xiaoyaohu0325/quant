import pandas as pd
import numpy as np

keys = ["sz50", "hz300", "zz500"]
avgs = ["avg5", "avg10", "avg20", "avg30"]

def process():
    data_frames = []
    for hdkey in keys:
        df = pd.read_hdf("avg_index.h5", key=hdkey)
        size = len(df)

        for avg in avgs:
            start_index = 0
            end_index = 0
            records = []

            for idx in range(1, size):
                pre = df.iloc[idx - 1]
                cur = df.iloc[idx]
                if pre["close"] < cur[avg] and cur["close"] > cur[avg]:
                    start_index = idx

                if pre["close"] > cur[avg] and cur["close"] < cur[avg]:
                    end_index = idx
                    if start_index > 0: # find a pair of data
                        row1 = df.iloc[start_index]
                        row2 = df.iloc[end_index]
                        records.append((hdkey, avg, row1.date, row2.date, end_index - start_index, row1.close, row2.close))
                        start_index = 0
                        end_index = 0

            output_df = pd.DataFrame(
                records,
                columns=["index_type", "avg_type", "start_date", "end_date", "days", "start_price", "end_price"]
                )
            data_frames.append(output_df)

    merged_df = pd.concat(data_frames)
    merged_df.index = pd.RangeIndex(0, len(merged_df))
    merged_df.to_hdf("cross_avg_index.h5", key="cross_index", complevel=4, complib="zlib")