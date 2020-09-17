import xlrd
import csv
import argparse
import os
import datetime

parser = argparse.ArgumentParser()
parser.add_argument("--xlsx", "-x", help="xlsx file path", required=True)
args = vars(parser.parse_args())
print(args)
xlsx_file = args["xlsx"]
csv_file = os.path.splitext(xlsx_file)[0] + ".csv"

def csv_from_excel(xlsx_file):
  wb = xlrd.open_workbook(xlsx_file)
  sh = wb.sheet_by_index(0)
  out_csv_file = open(csv_file, 'w')
  wr = csv.writer(out_csv_file)

  # print("Sampling:")
  # row10 = sh.row_values(10)
  # row10[4] = datetime.datetime(*xlrd.xldate_as_tuple(row10[4], wb.datemode)).strftime("%m/%d/%Y %H:%M")
  # print(row10)

  wr.writerow(sh.row_values(0))

  for rownum in range(1, sh.nrows):
    row = sh.row_values(rownum)
    if len(str(row[6])) > 0:
      row[0] = str(row[0]).split('.')[0]
      row[1] = str(row[1]).split('.')[0]
      row[3] = int(row[3])
      row[4] = datetime.datetime(*xlrd.xldate_as_tuple(row[4], wb.datemode)).strftime("%m/%d/%Y %H:%M")
      row[6] = int(row[6])
      wr.writerow(row)
    else:
      print(row)
  out_csv_file.close()

# runs the csv_from_excel function:
csv_from_excel(xlsx_file)

