import xlrd
import csv

with xlrd.open_workbook('mumbai.xls') as wb:
    sh = wb.sheet_by_index(0)  # or wb.sheet_by_name('name_of_the_sheet_here')
    with open('mumbai.csv', 'w', newline="") as f:   # open('a_file.csv', 'w', newline="") for python 3
        c = csv.writer(f)
        for r in range(sh.nrows):
            c.writerow(sh.row_values(r))