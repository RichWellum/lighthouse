#!/usr/local/bin/python3

import datetime
from datetime import date

import dateutil.relativedelta

# today = date.today()
# now = datetime.datetime.utcnow()
# pretty_now = str(now).split(".")[0]
# pn = str(datetime.datetime.utcnow()).split(".")[0]

pn = datetime.datetime.utcnow() - dateutil.relativedelta.relativedelta(months=1)

# print("Today's date:", today)
# print("Today's date:", now)
# print("Today's date:", pretty_now)
# print("Today's date:", pn)

# d = datetime.datetime.strptime(today, "%Y-%m-%d")
# d2 = now - dateutil.relativedelta.relativedelta(months=1)
# print("A month ago's date:", str(d2).split(".")[0])
print("A month ago's date:", str(pn).split(".")[0])

xx = str(datetime.datetime.utcnow() - dateutil.relativedelta.relativedelta(months=1).split(".")[0])