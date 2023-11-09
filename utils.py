import csv
import sys
from math import floor


def csv_read(row):
    csv.field_size_limit(sys.maxsize)
    return csv.reader(row, delimiter=",", quotechar='"')


def bin_data(curr, num):
    if curr is None:
        return None
    val = floor(curr)
    div = val // num + 1
    return f"{(div - 1) * num} - {div * num}"




