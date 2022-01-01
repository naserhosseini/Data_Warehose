import mysql.connector
import os, csv
from datetime import datetime, timedelta

from mysql.connector import errorcode

def load_data(table,data):
    mycrs.execute("SHOW columns FROM {}".format(table))
    col=mycrs.fetchone()
    col=''
    for x in mycrs.fetchall():
        col+=x[0]+','
    col=col[:-1]
    print(col)
    print(data)
    sql='INSERT INTO {}({}) VALUES{};'.format(table,col,data)
    print(sql)
    mycrs.execute(sql)



###############################################      main body

'''
Connect to MySQL database
The code readies to raise error for wrong credential
'''
try:
    config = {
        'user': 'root',
        'password': 'Password',
        'host': '127.0.0.1',
        'database': 'minioop',
        'raise_on_warnings': True,
        'autocommit':True}
    cnx = mysql.connector.connect(**config)
    mycrs=cnx.cursor()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
    quit()

'''
Initialize dimension table
'''

#Product
sql='SELECT distinct product_id, product_name FROM minioop.products;'
mycrs.execute(sql)
rows=mycrs.fetchall()
product={}
for row in rows:
    product[row[1]]=row[0]


#Type
sql='SELECT type_id, type FROM minioop.products;'
mycrs.execute(sql)
rows=mycrs.fetchall()
type_name={}
for row in rows:
    type_name[row[1]]=row[0]

path='D:/DE/Materials/Projects/Chapter02/'
parent=os.listdir(path)
for x in parent:
    if x.startswith('Sells'):
        f=path +'/'+x
        with open(f) as sells:
            rows=csv.reader(sells,delimiter=',')
            next(rows)
            for row in rows:
                #row.insert(0,row[4])
                row=[row[4],row[0],product[row[1]],type_name[row[2]],row[3]]
                delivery=datetime.strptime(row[1],'%Y-%m-%d')+timedelta(days=7)
                delivery=delivery.strftime('%Y-%m-%d')
                row.append(delivery)
                row.append(1)
                data=tuple(row)
                print(data)
#                load_data('orders',data=data)