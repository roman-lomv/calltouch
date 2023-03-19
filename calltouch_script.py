#!/usr/bin/python
# -*- coding: utf-8 -*-


import httplib2 
import apiclient
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import requests
import datetime
from datetime import timedelta, date
from json import dumps
from sqlalchemy import create_engine
import urllib.parse

# Наже следует ввести свои параметры

# Учетные данные БД
db_config = {
    'user': 'username',  # имя пользователя
    'pwd': urllib.parse.quote_plus('your_password'),  # пароль
    'host': '00.000.000.000',  # адрес сервера
    'port': 0000,  # порт подключения
    'db': 'database_name'  # название базы данных
}  

# id гугл таблицы
spreadsheetId = 'sheet_id'

# Поля Calltouch, которые необходимо выгрузить
fields = ['siteId', 'callId', 
          'date', 'callerNumber', 'redirectNumber',
          'phoneNumber', 'uniqTargetCall', 'source',
          'medium', 'keyword', 'ref', 'hostname',
          'utmSource', 'utmMedium', 'utmCampaign',
          'utmContent', 'utmTerm']

# Сохраненный файл с ключом 
CREDENTIALS_FILE = 'your_file.json'

""" 
На этом ввод параметров завершен, скрипт готов к работе
"""


connection_string = 'clickhouse://{}:{}@{}:{}/{}'.format(
    db_config['user'],
    db_config['pwd'],
    db_config['host'],
    db_config['port'],
    db_config['db']
)
# Подключаемся к БД.
try:
    engine = create_engine(connection_string)
except:
    print('Ошибка подключения к БД. Проверьте конфигурацию.')

# Формируем запросы на удаление старых данных и 
# получение последней даты по аккаунту
delete_query ='''ALTER TABLE calls DELETE WHERE siteId = '{}' and date BETWEEN '{}' and '{}' '''
last_date_query = '''SELECT MAX(CAST(date AS Date)) last_date FROM calls WHERE siteId = '{}' '''

credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, 
																['https://www.googleapis.com/auth/spreadsheets', 
																 'https://www.googleapis.com/auth/drive'])

try:
    httpAuth = credentials.authorize(httplib2.Http()) # Авторизуемся в системе
except:
    print('Ошибка авторизации в google.')

service = apiclient.discovery.build('sheets', 'v4', http = httpAuth) # Выбираем работу с таблицами и 4 версию API 

ranges = ["accounts!A2:F1000"] # Выбираем рабочий диапазон страницы accounts
          
# Получаем результаты          
results = service.spreadsheets().values().batchGet(spreadsheetId = spreadsheetId, 
                                     ranges = ranges, 
                                     valueRenderOption = 'FORMATTED_VALUE',  
                                     dateTimeRenderOption = 'FORMATTED_STRING').execute() 
sheet_values = results['valueRanges'][0]['values']

request = 'http://api.calltouch.ru/calls-service/RestAPI/{}/calls-diary/calls?clientApiId={}&dateFrom={}&dateTo={}&withCallTags=true&page={}'

n=2
# получаем данные от сервиса

for i in sheet_values:
    


    
    # код для первого запуска 
    if i[5] == '1':
        
        req = requests.get(request.format(i[1], 
                                          i[2],
                                          i[3],
                                          i[4],
                                          1))
        
        if req.status_code == 200:
            page_n = 1
            results=[]
            for page in range(1, req.json()['pageTotal'] + 1):
                
                page_req = requests.get(request.format(i[1], 
                                          i[2],
                                          i[3],
                                          i[4],
                                          page_n))
                results.append(page_req.json()['records'])
                page_n += 1
            status_update = (service.spreadsheets()
                             .values()
                             .batchUpdate(spreadsheetId = spreadsheetId, 
                                          body = {
                                              'valueInputOption': 'USER_ENTERED',
                                              'data': [{'range': 'accounts!F{}'.format(n),
                                                        'majorDimension': 'ROWS',
                                                        'values': [['2']]
                                                       }
                                                      ]
                                          })).execute()

            
            log_sting = 'Данные получены из кабинета за период {} - {}'.format(i[3], i[4])
            table_log = (service.spreadsheets()
                                 .values()
                                 .append(spreadsheetId = spreadsheetId,
                                         range = 'logs!A2:C30000',
                                         valueInputOption = 'RAW',
                                         body = {'values': [[i[1], 
                                                             dumps(datetime.datetime.now(), 
                                                                   sort_keys=True, 
                                                                   default=str),
                                                             log_sting]]})
                         .execute()
                        )
            
            engine.execute(delete_query.format(int(i[1]),
                                        format(pd.to_datetime(i[3], dayfirst=True, errors='ignore'), 
                                               '%Y-%m-%d'),
                                        format(pd.to_datetime(i[4], dayfirst=True, errors='ignore') + timedelta(days=1), 
                                               '%Y-%m-%d')
                                        )
                          )
            
            dict = {}
            for field in fields:
                dict[str(field)] = []
                for page in results:
                    for call in page:
                        dict[str(field)].append(call[field])
            calls = pd.DataFrame(dict)
            calls['date'] = pd.to_datetime(calls['date'], dayfirst=True, errors='ignore')
            calls.to_sql(name = 'calls', con = engine, if_exists = 'append', index = False)
            
            log_sting = 'Данные успешно загружены за период {} - {}'.format(i[3], i[4])
            table_log = (service.spreadsheets()
                                 .values()
                                 .append(spreadsheetId = spreadsheetId,
                                         range = 'logs!A2:C30000',
                                         valueInputOption = 'RAW',
                                         body = {"values": [[i[1], 
                                                             dumps(datetime.datetime.now(), 
                                                                   sort_keys=True, 
                                                                   default=str),
                                                             log_sting]]})
                         .execute()
                        )


            
                                                                       
            
        elif req.status_code == 403 or req.status_code == 500:
            status_update = (service.spreadsheets()
                             .values()
                             .batchUpdate(spreadsheetId = spreadsheetId, 
                                          body = {
                                              'valueInputOption': 'USER_ENTERED',
                                              'data': [{'range': 'accounts!F{}'.format(n),
                                                        'majorDimension': 'ROWS',
                                                        'values': [['ошибка']]
                                                       }]
                                          })
                             .execute()
                            )
            
            log_sting = 'Аккаунт не выгружен. Ошибка: {}'.format(req.json()['message'])
            table_log = (service.spreadsheets()
                                 .values()
                                 .append(spreadsheetId = spreadsheetId,
                                         range = 'logs!A2:C30000',
                                         valueInputOption = 'RAW',
                                         body = {"values": [[i[1], 
                                                             dumps(datetime.datetime.now(), 
                                                                   sort_keys=True, 
                                                                   default=str),
                                                             log_sting]]})
                         .execute()
                        )

        else:
            status_update = (service.spreadsheets()
                             .values()
                             .batchUpdate(spreadsheetId = spreadsheetId, 
                                          body = {
                                              'valueInputOption': 'USER_ENTERED',
                                              'data': [{'range': 'accounts!F{}'.format(n),
                                                        'majorDimension': 'ROWS',
                                                        'values': [['ошибка']]
                                                       }]
                                          })
                             .execute()
                            )
            
            log_sting = 'Аккаунт не выгружен. Ошибка: {}'.format(req.json()['data']['apiErrorData']['errorMessage'])
            table_log = (service.spreadsheets()
                                 .values()
                                 .append(spreadsheetId = spreadsheetId,
                                         range = 'logs!A2:C30000',
                                         valueInputOption = 'RAW',
                                         body = {"values": [[i[1], 
                                                             dumps(datetime.datetime.now(), 
                                                                   sort_keys=True, 
                                                                   default=str),
                                                             log_sting]]})
                         .execute()
                        )
            
    #код для ежедневного запуска и запуска после ошибки
    elif i[5] == '2' or i[5] == 'ошибка':
        
        last_date = pd.io.sql.read_sql(last_date_query.format(int(i[1])), con=engine)['last_date'][0]
        date_from = format(pd.to_datetime(last_date, dayfirst=True, errors='ignore'), 
                                               '%d/%m/%Y')
        date_to = format(pd.to_datetime(date.today(), dayfirst=True, errors='ignore'), 
                                               '%d/%m/%Y')
        
        
        req = requests.get(request.format(i[1], 
                                          i[2],
                                          date_from,
                                          date_to,
                                          1))
        
        if req.status_code == 200:
            page_n = 1
            results=[]
            for page in range(1, req.json()['pageTotal'] + 1):
                
                page_req = requests.get(request.format(i[1], 
                                          i[2],
                                          date_from,
                                          date_to,
                                          page_n))
                results.append(page_req.json()['records'])
                page_n += 1
            status_update = (service.spreadsheets()
                             .values()
                             .batchUpdate(spreadsheetId = spreadsheetId, 
                                          body = {
                                              'valueInputOption': 'USER_ENTERED',
                                              'data': [{'range': 'accounts!F{}'.format(n),
                                                        'majorDimension': 'ROWS',
                                                        'values': [['2']]
                                                       }]
                                          }
                                         )
                             .execute()
                            )

            
            log_sting = 'Данные получены из кабинета за период {} - {}'.format(date_from, date_to)
            table_log = (service.spreadsheets()
                                 .values()
                                 .append(spreadsheetId = spreadsheetId,
                                         range = 'logs!A2:C30000',
                                         valueInputOption = 'RAW',
                                         body = {'values': [[i[1], 
                                                             dumps(datetime.datetime.now(), 
                                                                   sort_keys=True, 
                                                                   default=str),
                                                             log_sting]]})
                         .execute()
                        )
            
            engine.execute(delete_query.format(int(i[1]),
                                        format(pd.to_datetime(last_date), '%Y-%m-%d'),
                                        format(pd.to_datetime(date.today()) + timedelta(days=1), '%Y-%m-%d')
                                        )
                          )
            
            dict = {}
            for field in fields:
                dict[str(field)] = []
                for page in results:
                    for call in page:
                        dict[str(field)].append(call[field])
            calls = pd.DataFrame(dict)
            calls['date'] = pd.to_datetime(calls['date'], dayfirst=True, errors='ignore')
            calls.to_sql(name = 'calls', con = engine, if_exists = 'append', index = False)
            
            log_sting = 'Данные успешно загружены за период {} - {}'.format(date_from, date_to)
            table_log = (service.spreadsheets()
                                 .values()
                                 .append(spreadsheetId = spreadsheetId,
                                         range = 'logs!A2:C30000',
                                         valueInputOption = 'RAW',
                                         body = {"values": [[i[1], 
                                                             dumps(datetime.datetime.now(), 
                                                                   sort_keys=True, 
                                                                   default=str),
                                                             log_sting]]})
                         .execute()
                        )
                                                                       
            
        elif req.status_code == 403 or req.status_code == 500:
            status_update = (service.spreadsheets()
                             .values()
                             .batchUpdate(spreadsheetId = spreadsheetId, 
                                          body = {'valueInputOption': 'USER_ENTERED',
                                                  'data': [{'range': 'accounts!F{}'.format(n),
                                                            'majorDimension': 'ROWS',
                                                            'values': [['ошибка']]
                                                           }]
                                                 })
                             .execute()
                            )
            
            log_sting = 'Аккаунт не выгружен. Ошибка: {}'.format(req.json()['message'])
            table_log = (service.spreadsheets()
                                 .values()
                                 .append(spreadsheetId = spreadsheetId,
                                         range = 'logs!A2:C30000',
                                         valueInputOption = 'RAW',
                                         body = {"values": [[i[1], 
                                                             dumps(datetime.datetime.now(), 
                                                                   sort_keys=True, 
                                                                   default=str),
                                                             log_sting]]
                                                }
                                        )
                         .execute()
                        )

        else:
            status_update = (service.spreadsheets()
                             .values()
                             .batchUpdate(spreadsheetId = spreadsheetId, 
                                          body = {'valueInputOption': 'USER_ENTERED',
                                                  'data': [{'range': 'accounts!F{}'.format(n),
                                                            'majorDimension': 'ROWS',
                                                            'values': [['ошибка']]
                                                           }]
                                                 }
                                         )
                             .execute()
                            )
            
            log_sting = 'Аккаунт не выгружен. Ошибка: {}'.format(req.json()['data']['apiErrorData']['errorMessage'])
            table_log = (service.spreadsheets()
                                 .values()
                                 .append(spreadsheetId = spreadsheetId,
                                         range = 'logs!A2:C30000',
                                         valueInputOption = 'RAW',
                                         body = {"values": [[i[1], 
                                                             dumps(datetime.datetime.now(), 
                                                                   sort_keys=True, 
                                                                   default=str),
                                                             log_sting]]})
                         .execute()
                        )
 
            
    # код для ежедневного запуска
    else:
        log_sting = 'Не заполнен статус для сайта {}'.format(i[0])
        table_log = (service.spreadsheets()
                                 .values()
                                 .append(spreadsheetId = spreadsheetId,
                                         range = 'logs!A2:C30000',
                                         valueInputOption = 'RAW',
                                         body = {"values": [[i[1], 
                                                             dumps(datetime.datetime.now(), 
                                                                   sort_keys=True, 
                                                                   default=str),
                                                             log_sting]]})
                         .execute()
                        )
            
    n += 1