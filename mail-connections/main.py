import datetime
import json
import pandas as pd
import eml_parser
import os
import glob
import mysql.connector
from mysql.connector import errorcode

TABLES = {}
TABLES['employees'] = (
    "CREATE TABLE `employees` ("
    "  `message_id` int(11) NOT NULL,"
    "  `sender` date NOT NULL,"
    "  `first_name` varchar(14) NOT NULL,"
    "  `last_name` varchar(16) NOT NULL,"
    "  `gender` enum('M','F') NOT NULL,"
    "  `hire_date` date NOT NULL,"
    "  PRIMARY KEY (`emp_no`)"
    ") ENGINE=InnoDB")

TABLES['departments'] = (
    "CREATE TABLE `departments` ("
    "  `dept_no` char(4) NOT NULL,"
    "  `dept_name` varchar(40) NOT NULL,"
    "  PRIMARY KEY (`dept_no`), UNIQUE KEY `dept_name` (`dept_name`)"
    ") ENGINE=InnoDB")

TABLES['salaries'] = (
    "CREATE TABLE `salaries` ("
    "  `emp_no` int(11) NOT NULL,"
    "  `salary` int(11) NOT NULL,"
    "  `from_date` date NOT NULL,"
    "  `to_date` date NOT NULL,"
    "  PRIMARY KEY (`emp_no`,`from_date`), KEY `emp_no` (`emp_no`),"
    "  CONSTRAINT `salaries_ibfk_1` FOREIGN KEY (`emp_no`) "
    "     REFERENCES `employees` (`emp_no`) ON DELETE CASCADE"
    ") ENGINE=InnoDB")
dic_list = []
path = 'C:\\Users\\Acer\\Desktop\\eml'

def dbc_connect():
    try:
      cnx = mysql.connector.connect(user='jessy', password='qwerty1234', host='127.0.0.1',
                                    database='employ')
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err)
    else:
      cnx.close()


def json_serial(obj):
    if isinstance(obj, datetime.datetime):
        serial = obj.isoformat()
        return serial


def parsing(path):
    for i in glob.glob(os.path.join(path, "*.eml")):
        print(i)
        with open(os.path.join(path, i), 'rb') as fhdl:
            print(fhdl)
            raw_email = fhdl.read()

            ep = eml_parser.EmlParser()
            parsed_eml = ep.decode_email_bytes(raw_email)

            x = json.dumps(parsed_eml, default=json_serial)
            y = json.loads(x)
            dict = {
                'message_id': y['header']['header']['message-id'][0],
                'sender': y['header']['from'],
                'sender_name': '',
                'receiver': y['header']['delivered_to'][0],
                'receiver_name': '',
                'date': y['header']['received'][0]['date'].rsplit('T')[0],
            }
            try:
                dict['receiver_name'] = y['header']['header']['to'][0].split(' <')[0],
                dict['sender_name'] = y['header']['header']['from'][0].split(' <')[0],
            except KeyError:
                print(y['header']['subject'])
            dic_list.append(dict)

    df = pd.DataFrame(dic_list)
    df.to_csv('df.csv')

    def creating_tables():
        for table_name in TABLES:
            table_description = TABLES[table_name]
            try:
                print("Creating table {}: ".format(table_name), end='')
                cursor.execute(table_description)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print("already exists.")
                else:
                    print(err.msg)
            else:
                print("OK")

        cursor.close()
        cnx.close()

if __name__ == '__main__':
    dbc_connect()
    os.chdir(path)