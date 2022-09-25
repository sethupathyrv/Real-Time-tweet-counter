from logging import warning
import streamlit as st
import datetime
import psycopg2
import pandas as pd
from psycopg2.extensions import AsIs
from psycopg2 import sql
from random import randrange
from datetime import date
con = psycopg2.connect(
    host='localhost',
    database='twitter',
    user="postgres",
    password=pw
)

f1 = open('sparkdata.txt', 'r')
count = 0
cur = con.cursor()
while True:
    count += 1

    line = f1.readline()
    if not line:
        break
    hashtag, counts = line.split(",")
    query = """ INSERT INTO hashcount (hashtag,hashtag_count) 
            VALUES (%s,%s)"""
    insert = (hashtag, counts)

    cur.execute(query, insert)
f1.close()

cur.execute("select * from hashcount")
df = pd.DataFrame(cur.fetchall())
df.columns = ['Hashtag', 'HashCount']
print(df)
cur.close()
con.close()
