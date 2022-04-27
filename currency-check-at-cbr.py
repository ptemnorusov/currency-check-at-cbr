import datetime
import time
import random
import os

import requests
import pyodbc
import smtplib

from bs4 import BeautifulSoup
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

currency_add_dict = {
    'CURRENCY_SITE_ID': 0,
    'COMPANY_ID': 0,
    'SITE_ID': 0,
    'CURRENCY_ID': 'USD',
    'CURRENCY_RATE_VALID_DATE': '',
    'CURRENCY_CONV_FACTOR': 1,
    'IN_CURRENCY_EXCH_RATE': '1.000000',
    'OUT_CURRENCY_EXCH_RATE': '1.000000',
    'DEFAULT_CURRENCY_ID': 'RUB',
    'USD': '',
    'EUR': '',
    'CHF': '',
    'AUD': '',
    'CAD': '',
    'JPY': '',
    'CNY': '',
    'GBP': '', 
    'NOK': '',
    'SGD': ''}

def get_new_currency_id():
    """
    Connet to DB and get last currency ID
    """
    global new_currency_id
    sqlserver = 'IP_ADDRESS'
    sqlusername = 'DB_USERNAME'
    sqldatabase = 'DB_NAME'
    sqlpassword = 'DB_PASSWORD'
    cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=' + sqlserver + ';DATABASE=' + sqldatabase + ';UID=' + sqlusername + ';PWD=' + sqlpassword)
    cursor = cnxn.cursor()
    get_last_currency_id = 'SELECT TOP 1 CURRENCY_SITE_ID FROM CURRENCY_SITE_VALIDATION ORDER BY CURRENCY_SITE_ID DESC'
    cursor.execute(get_last_currency_id)
    last_currency_id = int(cursor.fetchall()[0][0])
    new_currency_id = last_currency_id + 1
    return new_currency_id


def get_next_day():
    # Getting next day value
    weekday = datetime.datetime.today().weekday()
    currency_add_dict['CURRENCY_RATE_VALID_DATE'] = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


def main()
    # Getting currency values from CBR website
    url = 'https://www.cbr.ru/eng/currency_base/daily/'
    page = requests.get(url)
    time.sleep(5)
    soup = BeautifulSoup(page.text, 'lxml')
    price1 = list(soup.find_all('td'))
    for curr in range(0, len(price1), 5):
        if price1[curr].text == "840":
            currency_add_dict['USD'] = float(price1[curr + 4].text)/float(price1[curr + 2].text)
        if price1[curr].text == "978":
            currency_add_dict['EUR'] = float(price1[curr + 4].text)/float(price1[curr + 2].text)
        if price1[curr].text == "756":
            currency_add_dict['CHF'] = float(price1[curr + 4].text)/float(price1[curr + 2].text)
        if price1[curr].text == "036":
            currency_add_dict['AUD'] = float(price1[curr + 4].text)/float(price1[curr + 2].text)
        if price1[curr].text == "124":
            currency_add_dict['CAD'] = float(price1[curr + 4].text)/float(price1[curr + 2].text)
        if price1[curr].text == "392":
            currency_add_dict['JPY'] = float(price1[curr + 4].text)/float(price1[curr + 2].text)
        if price1[curr].text == "156":
            currency_add_dict['CNY'] = float(price1[curr + 4].text)/float(price1[curr + 2].text)
        if price1[curr].text == "826":
            currency_add_dict['GBP'] = float(price1[curr + 4].text)/float(price1[curr + 2].text)
        if price1[curr].text == "578":
            currency_add_dict['NOK'] = float(price1[curr + 4].text)/float(price1[curr + 2].text)
        if price1[curr].text == "702":
            currency_add_dict['SGD'] = float(price1[curr + 4].text)/float(price1[curr + 2].text)

    get_next_day()

    # SQL server requisites
    sqlserver = 'DB_SERVER_ADDRESS'
    sqlusername = 'DB_USER_NAME'
    sqldatabase = 'DB_NAME'
    sqlpassword = 'DB_USER_PASSWORD'
    cnxn = pyodbc.connect('DRIVER={SQL Server Native Client 11.0};SERVER=' + sqlserver + ';DATABASE=' + sqldatabase + ';UID=' + sqlusername + ';PWD=' + sqlpassword)
    cursor = cnxn.cursor()

    # Generating and running SQL query string
    get_new_currency_id()
    put_last_usd_currency = rf"""exec sp_executesql N'INSERT INTO CURRENCY_SITE_VALIDATION ( CURRENCY_SITE_ID, COMPANY_ID, SITE_ID, CURRENCY_ID, CURRENCY_RATE_VALID_DATE, CURRENCY_CONV_FACTOR, CURRENCY_EXCH_RATE, IN_CURRENCY_EXCH_RATE, OUT_CURRENCY_EXCH_RATE, DEFAULT_CURRENCY_ID ) VALUES ( @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10 )',N'@P1 int,@P2 int,@P3 int,@P4 nvarchar(3),@P5 datetime2(7),@P6 int,@P7 nvarchar(79),@P8 nvarchar(79),@P9 nvarchar(79),@P10 nvarchar(3)',{new_currency_id},{currency_add_dict['COMPANY_ID']},{currency_add_dict['SITE_ID']},N'USD','{currency_add_dict['CURRENCY_RATE_VALID_DATE']}',{currency_add_dict['CURRENCY_CONV_FACTOR']},N'{currency_add_dict['USD']}',N'{currency_add_dict['IN_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['OUT_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['DEFAULT_CURRENCY_ID']}'"""
    cursor.execute(put_last_usd_currency)
    cursor.commit()

    get_new_currency_id()
    put_last_eur_currency = rf"""exec sp_executesql N'INSERT INTO CURRENCY_SITE_VALIDATION ( CURRENCY_SITE_ID, COMPANY_ID, SITE_ID, CURRENCY_ID, CURRENCY_RATE_VALID_DATE, CURRENCY_CONV_FACTOR, CURRENCY_EXCH_RATE, IN_CURRENCY_EXCH_RATE, OUT_CURRENCY_EXCH_RATE, DEFAULT_CURRENCY_ID ) VALUES ( @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10 )',N'@P1 int,@P2 int,@P3 int,@P4 nvarchar(3),@P5 datetime2(7),@P6 int,@P7 nvarchar(79),@P8 nvarchar(79),@P9 nvarchar(79),@P10 nvarchar(3)',{new_currency_id},{currency_add_dict['COMPANY_ID']},{currency_add_dict['SITE_ID']},N'EUR','{currency_add_dict['CURRENCY_RATE_VALID_DATE']}',{currency_add_dict['CURRENCY_CONV_FACTOR']},N'{currency_add_dict['EUR']}',N'{currency_add_dict['IN_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['OUT_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['DEFAULT_CURRENCY_ID']}'"""
    cursor.execute(put_last_eur_currency)
    cursor.commit()

    get_new_currency_id()
    put_last_eur_currency = rf"""exec sp_executesql N'INSERT INTO CURRENCY_SITE_VALIDATION ( CURRENCY_SITE_ID, COMPANY_ID, SITE_ID, CURRENCY_ID, CURRENCY_RATE_VALID_DATE, CURRENCY_CONV_FACTOR, CURRENCY_EXCH_RATE, IN_CURRENCY_EXCH_RATE, OUT_CURRENCY_EXCH_RATE, DEFAULT_CURRENCY_ID ) VALUES ( @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10 )',N'@P1 int,@P2 int,@P3 int,@P4 nvarchar(3),@P5 datetime2(7),@P6 int,@P7 nvarchar(79),@P8 nvarchar(79),@P9 nvarchar(79),@P10 nvarchar(3)',{new_currency_id},{currency_add_dict['COMPANY_ID']},{currency_add_dict['SITE_ID']},N'CHF','{currency_add_dict['CURRENCY_RATE_VALID_DATE']}',{currency_add_dict['CURRENCY_CONV_FACTOR']},N'{currency_add_dict['CHF']}',N'{currency_add_dict['IN_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['OUT_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['DEFAULT_CURRENCY_ID']}'"""
    cursor.execute(put_last_eur_currency)
    cursor.commit()

    get_new_currency_id()
    put_last_eur_currency = rf"""exec sp_executesql N'INSERT INTO CURRENCY_SITE_VALIDATION ( CURRENCY_SITE_ID, COMPANY_ID, SITE_ID, CURRENCY_ID, CURRENCY_RATE_VALID_DATE, CURRENCY_CONV_FACTOR, CURRENCY_EXCH_RATE, IN_CURRENCY_EXCH_RATE, OUT_CURRENCY_EXCH_RATE, DEFAULT_CURRENCY_ID ) VALUES ( @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10 )',N'@P1 int,@P2 int,@P3 int,@P4 nvarchar(3),@P5 datetime2(7),@P6 int,@P7 nvarchar(79),@P8 nvarchar(79),@P9 nvarchar(79),@P10 nvarchar(3)',{new_currency_id},{currency_add_dict['COMPANY_ID']},{currency_add_dict['SITE_ID']},N'AUD','{currency_add_dict['CURRENCY_RATE_VALID_DATE']}',{currency_add_dict['CURRENCY_CONV_FACTOR']},N'{currency_add_dict['AUD']}',N'{currency_add_dict['IN_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['OUT_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['DEFAULT_CURRENCY_ID']}'"""
    cursor.execute(put_last_eur_currency)
    cursor.commit()

    get_new_currency_id()
    put_last_eur_currency = rf"""exec sp_executesql N'INSERT INTO CURRENCY_SITE_VALIDATION ( CURRENCY_SITE_ID, COMPANY_ID, SITE_ID, CURRENCY_ID, CURRENCY_RATE_VALID_DATE, CURRENCY_CONV_FACTOR, CURRENCY_EXCH_RATE, IN_CURRENCY_EXCH_RATE, OUT_CURRENCY_EXCH_RATE, DEFAULT_CURRENCY_ID ) VALUES ( @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10 )',N'@P1 int,@P2 int,@P3 int,@P4 nvarchar(3),@P5 datetime2(7),@P6 int,@P7 nvarchar(79),@P8 nvarchar(79),@P9 nvarchar(79),@P10 nvarchar(3)',{new_currency_id},{currency_add_dict['COMPANY_ID']},{currency_add_dict['SITE_ID']},N'CAD','{currency_add_dict['CURRENCY_RATE_VALID_DATE']}',{currency_add_dict['CURRENCY_CONV_FACTOR']},N'{currency_add_dict['CAD']}',N'{currency_add_dict['IN_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['OUT_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['DEFAULT_CURRENCY_ID']}'"""
    cursor.execute(put_last_eur_currency)
    cursor.commit()

    get_new_currency_id()
    put_last_eur_currency = rf"""exec sp_executesql N'INSERT INTO CURRENCY_SITE_VALIDATION ( CURRENCY_SITE_ID, COMPANY_ID, SITE_ID, CURRENCY_ID, CURRENCY_RATE_VALID_DATE, CURRENCY_CONV_FACTOR, CURRENCY_EXCH_RATE, IN_CURRENCY_EXCH_RATE, OUT_CURRENCY_EXCH_RATE, DEFAULT_CURRENCY_ID ) VALUES ( @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10 )',N'@P1 int,@P2 int,@P3 int,@P4 nvarchar(3),@P5 datetime2(7),@P6 int,@P7 nvarchar(79),@P8 nvarchar(79),@P9 nvarchar(79),@P10 nvarchar(3)',{new_currency_id},{currency_add_dict['COMPANY_ID']},{currency_add_dict['SITE_ID']},N'JPY','{currency_add_dict['CURRENCY_RATE_VALID_DATE']}',{currency_add_dict['CURRENCY_CONV_FACTOR']},N'{currency_add_dict['JPY']}',N'{currency_add_dict['IN_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['OUT_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['DEFAULT_CURRENCY_ID']}'"""
    cursor.execute(put_last_eur_currency)
    cursor.commit()

    get_new_currency_id()
    put_last_eur_currency = rf"""exec sp_executesql N'INSERT INTO CURRENCY_SITE_VALIDATION ( CURRENCY_SITE_ID, COMPANY_ID, SITE_ID, CURRENCY_ID, CURRENCY_RATE_VALID_DATE, CURRENCY_CONV_FACTOR, CURRENCY_EXCH_RATE, IN_CURRENCY_EXCH_RATE, OUT_CURRENCY_EXCH_RATE, DEFAULT_CURRENCY_ID ) VALUES ( @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10 )',N'@P1 int,@P2 int,@P3 int,@P4 nvarchar(3),@P5 datetime2(7),@P6 int,@P7 nvarchar(79),@P8 nvarchar(79),@P9 nvarchar(79),@P10 nvarchar(3)',{new_currency_id},{currency_add_dict['COMPANY_ID']},{currency_add_dict['SITE_ID']},N'CNY','{currency_add_dict['CURRENCY_RATE_VALID_DATE']}',{currency_add_dict['CURRENCY_CONV_FACTOR']},N'{currency_add_dict['CNY']}',N'{currency_add_dict['IN_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['OUT_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['DEFAULT_CURRENCY_ID']}'"""
    cursor.execute(put_last_eur_currency)
    cursor.commit()

    get_new_currency_id()
    put_last_eur_currency = rf"""exec sp_executesql N'INSERT INTO CURRENCY_SITE_VALIDATION ( CURRENCY_SITE_ID, COMPANY_ID, SITE_ID, CURRENCY_ID, CURRENCY_RATE_VALID_DATE, CURRENCY_CONV_FACTOR, CURRENCY_EXCH_RATE, IN_CURRENCY_EXCH_RATE, OUT_CURRENCY_EXCH_RATE, DEFAULT_CURRENCY_ID ) VALUES ( @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10 )',N'@P1 int,@P2 int,@P3 int,@P4 nvarchar(3),@P5 datetime2(7),@P6 int,@P7 nvarchar(79),@P8 nvarchar(79),@P9 nvarchar(79),@P10 nvarchar(3)',{new_currency_id},{currency_add_dict['COMPANY_ID']},{currency_add_dict['SITE_ID']},N'GBP','{currency_add_dict['CURRENCY_RATE_VALID_DATE']}',{currency_add_dict['CURRENCY_CONV_FACTOR']},N'{currency_add_dict['GBP']}',N'{currency_add_dict['IN_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['OUT_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['DEFAULT_CURRENCY_ID']}'"""
    cursor.execute(put_last_eur_currency)
    cursor.commit()

    get_new_currency_id()
    put_last_eur_currency = rf"""exec sp_executesql N'INSERT INTO CURRENCY_SITE_VALIDATION ( CURRENCY_SITE_ID, COMPANY_ID, SITE_ID, CURRENCY_ID, CURRENCY_RATE_VALID_DATE, CURRENCY_CONV_FACTOR, CURRENCY_EXCH_RATE, IN_CURRENCY_EXCH_RATE, OUT_CURRENCY_EXCH_RATE, DEFAULT_CURRENCY_ID ) VALUES ( @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10 )',N'@P1 int,@P2 int,@P3 int,@P4 nvarchar(3),@P5 datetime2(7),@P6 int,@P7 nvarchar(79),@P8 nvarchar(79),@P9 nvarchar(79),@P10 nvarchar(3)',{new_currency_id},{currency_add_dict['COMPANY_ID']},{currency_add_dict['SITE_ID']},N'NOK','{currency_add_dict['CURRENCY_RATE_VALID_DATE']}',{currency_add_dict['CURRENCY_CONV_FACTOR']},N'{currency_add_dict['NOK']}',N'{currency_add_dict['IN_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['OUT_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['DEFAULT_CURRENCY_ID']}'"""
    cursor.execute(put_last_eur_currency)
    cursor.commit()

    get_new_currency_id()
    put_last_eur_currency = rf"""exec sp_executesql N'INSERT INTO CURRENCY_SITE_VALIDATION ( CURRENCY_SITE_ID, COMPANY_ID, SITE_ID, CURRENCY_ID, CURRENCY_RATE_VALID_DATE, CURRENCY_CONV_FACTOR, CURRENCY_EXCH_RATE, IN_CURRENCY_EXCH_RATE, OUT_CURRENCY_EXCH_RATE, DEFAULT_CURRENCY_ID ) VALUES ( @P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10 )',N'@P1 int,@P2 int,@P3 int,@P4 nvarchar(3),@P5 datetime2(7),@P6 int,@P7 nvarchar(79),@P8 nvarchar(79),@P9 nvarchar(79),@P10 nvarchar(3)',{new_currency_id},{currency_add_dict['COMPANY_ID']},{currency_add_dict['SITE_ID']},N'SGD','{currency_add_dict['CURRENCY_RATE_VALID_DATE']}',{currency_add_dict['CURRENCY_CONV_FACTOR']},N'{currency_add_dict['SGD']}',N'{currency_add_dict['IN_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['OUT_CURRENCY_EXCH_RATE']}',N'{currency_add_dict['DEFAULT_CURRENCY_ID']}'"""
    cursor.execute(put_last_eur_currency)
    cursor.commit()

    # Generating and sending email
    sender = "Jarvis <jarvis@domain.dom>"
    receiver = "Receiver <receiver@domain.dom>"
    receiver2 = "Receiver 2 <receiver2@domain.dom>"
    subject = "Курсы валют на завтра"

    msg = MIMEMultipart()
    msg['From'] = sender
    msg['to'] = receiver
    msg['cc'] = receiver2
    msg['Subject'] = subject
    body = f"""Hello, 
    
    ЦБ опубликовал курсы валют на завтра.
    Добавил их в рабочую базу.

    Вот они:
    USD     {currency_add_dict['USD']}
    EUR     {currency_add_dict['EUR']}
    CHF     {currency_add_dict['CHF']}
    AUD     {currency_add_dict['AUD']}
    CAD     {currency_add_dict['CAD']}
    JPY     {currency_add_dict['JPY']}
    CNY     {currency_add_dict['CNY']}
    GBP     {currency_add_dict['GBP']}
    GBP     {currency_add_dict['NOK']}
    GBP     {currency_add_dict['SGD']}

    Хорошего дня"""

    msg.attach(MIMEText(body, "plain"))
    server = smtplib.SMTP('mail.domain.dom', 25, )
    text = msg.as_string()
    server.sendmail(sender, receiver, text)
    server.quit()

main()
