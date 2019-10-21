# -*- coding: utf-8 -*-
# Берем из монго, фильтруем по PRODUCTS, подставляем статусы и агентов из postgres, результат в Excel

import sys, argparse
from _datetime import datetime, timedelta, date
import os
from mysql.connector import MySQLConnection, Error
from collections import OrderedDict
import openpyxl
from pymongo import MongoClient
import psycopg2

from lib import read_config, l

#PRODUCTS = ['raiffeisen_loan_referral', 'raiffeisen_loan_lead_referral', 'raiffeisen_credit_card_referral',
#            'raiffeisen_credit_card_lead_referral']
PRODUCTS = ['alfabank_100_v2']
BAD_FIELDS = ['_id','owner_id','client','callcenter_status_code']

STATUSES = {0: 'Новая заявка', 100: 'Заявка отправлена в очередь', 110: 'Введен СМС код',
            120: 'Запрошена повторная СМС', 130: 'В процессе', 140: 'Одобрена', 150: 'Предварительно одобрена',
            160: 'Заявка заполнена', 200: 'Завершено', 210: 'Займ выдан', 220: 'Займ выдан повторно',
            230: 'Займ выдан через call-центр', 400: 'Удалена', 410: 'Неизвестный статус', 420: 'Ошибка выгрузки',
            430: 'Отказ', 450: 'Закрыта',460: 'Истек срок действия решения Банка', 470: 'Ошибка в заявке',
            500: 'Отладка', 510: 'Отложена'}

agents = {}
#q1 = """
# Загружаем агентов из postgres
cfg = read_config(filename='anketa.ini', section='postgresql')
conn = psycopg2.connect(**cfg)
cursor = conn.cursor()
cursor.execute('select ac.id, ac.lastname, ac.name, ac.middlename, di.title from account as ac '
               'left join division as di on ac.division_id = di.id;')
for row in cursor:
    family = ''
    name = ''
    lastname = ''
    if row[1]:
        family = row[1]
    if row[2]:
        name = ' ' + row[2]
    if row[3]:
        lastname = ' ' + row[3]
    agents[row[0]] = [family + name + lastname, row[4]]
#"""

# подключаемся к серверу
cfg = read_config(filename='anketa.ini', section='Mongo')
conn = MongoClient('mongodb://' + cfg['user'] + ':' + cfg['password'] + '@' + cfg['ip'] + ':' + cfg['port'] + '/'
                   + cfg['db'])
# выбираем базу данных
db = conn.saturn_v

# выбираем коллекцию документов
colls = db.Products

#product_alias: raiffeisen_loan_referral
#product_alias: raiffeisen_loan_lead_referral

wb_rez = openpyxl.Workbook(write_only=True)
ws_rez = []
for i, product in enumerate(PRODUCTS):
    ws_rez.append(wb_rez.create_sheet(product))
    fields = []
    for j, coll in enumerate(colls.find({'product_alias': product})):
        #print('\n\ncoll\n', coll, '\n', agents[485])
        if not j:
            for field in coll.keys():
                if field not in BAD_FIELDS:
                    fields.append(field)
            ws_rez[i].append(['ФИО Агента', 'Организация'] + fields)
            #print('\n\nЗАГОЛОВОК\n', ['ФИО Агента', 'Организация'] + fields)
        #print('\nBEFORE agents\n',  agents[coll['owner_id']], '\n', coll['owner_id'])
        if agents.get(coll['owner_id']):
            fields_rez = [agents[coll['owner_id']][0], agents[coll['owner_id']][1]]
            #print('\nagents\n', fields_rez, '\n', agents[coll['owner_id']], '\n', coll['owner_id'])
        else:
            fields_rez = [str(coll['owner_id']), str(coll['owner_id'])]
        for field in fields:
            if field == 'state_code':
                fields_rez.append(STATUSES[l(coll.get(field))])
            else:
                if str(type(coll.get(field))).find('str') < 0 and str(type(coll.get(field))).find('int') < 0:
                    fields_rez.append(str(coll.get(field)))
                else:
                    fields_rez.append(coll.get(field))
        #print('\n\nfields_rez\n',fields_rez)
        ws_rez[i].append(fields_rez)
wb_rez.save(datetime.now().strftime('%Y-%m-%d_%H-%M') + 'отчеты.xlsx')