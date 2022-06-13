#!/usr/bin/env python
# coding: utf-8
import pandas as pd

events  = pd.read_csv('events.csv')


transactions = events[events.event == "transaction"]
transactions.to_csv("out_transaction.csv", index=False)
del transactions

events = events[events.event != "transaction"].drop('transactionid', axis=1)
events.to_csv("out_events.csv", index=False)
del events




