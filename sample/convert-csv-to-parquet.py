import os
import argparse
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--csv", required=True, help="CSV file path")
args = vars(parser.parse_args())

csv = args["csv"]
parquet = "{}.par".format(os.path.splitext(csv)[0])

df = pd.read_csv(csv, sep='\t')
df.to_parquet(parquet, compression="gzip")
