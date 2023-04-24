import datetime
import json
import pandas as pd
import eml_parser
import os
import glob
import mysql.connector

# from mysql.connector import errorcode

dic_list = []
path = 'C:\\Users\\Acer\\Desktop\\eml'
datadir_path = 'C:\\xampp\\mysql\\data'


class Data:
    def __init__(self, table_name):
        self.table_name = table_name

        # connection to database
        self.cnx = mysql.connector.connect(user='jessy', password='qwerty1234', host='127.0.0.1',
                                           database='wanderer_bd')
        '''
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            self.cnx.close()
            '''

    
    def load_dataframe(self):
        # Create a cursor object
        cur = self.cnx.cursor()

        query = f"LOAD DATA LOCAL INFILE 'df.csv' INTO TABLE emails FIELDS TERMINATED BY ',' IGNORE 1 LINES "
        cur.execute(query)
        self.cnx.commit()


# json parsing
def json_serial(obj):
    if isinstance(obj, datetime.datetime):
        serial = obj.isoformat()
        return serial


# parsing EMLs
def parsing(path):
    for i in glob.glob(os.path.join(path, "*.eml")):
        print(i)
        with open(os.path.join(path, i), 'rb') as fhdl:
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
                print(y['header']['from'])
            dic_list.append(dict)

    df = pd.DataFrame(dic_list)
    os.chdir(datadir_path)
    df.to_csv('df.csv')
    return df


if __name__ == '__main__':
    os.chdir(path)
    commit = Data('emails')
    try:
        df = parsing(path)
        commit.load_dataframe()
    except Exception as e:
        print(e)
