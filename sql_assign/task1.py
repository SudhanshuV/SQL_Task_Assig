import mysql.connector as conn
import pandas as pd
import pymongo
import json
import logging

logging.basicConfig(filename='sql_log.log', level=logging.DEBUG,
                    format='%(levelname)s,%(asctime)s,%(name)s,%(message)s')

try:
    mydb = conn.connect(host='localhost', user='root', passwd='query', database='sql_task', use_pure=True)
    logging.info('Database connection successful')
    cur = mydb.cursor()
    q1 = 'create table if not exists attribute(Dress_ID int(15),Style varchar(20),Price varchar(10),Rating float(5,2),Size varchar(10),Season varchar(10),NeckLine varchar(20),SleeveLength varchar(20),waiseline varchar(20),Material varchar(20),FabricType varchar(20),Decoration varchar(20),PatternType varchar(20),Recommendation int(2),primary key(Dress_ID))'
    cur.execute(q1)
    logging.info('Attribute table schema created')
    q2 = 'create table if not exists dress_sale(Dress_ID int(15),`29-8-2013` int(10),`31-8-2013` int(10),`09-02-2013` int(10),`09-04-2013` int(10),`09-06-2013` int(10),`09-08-2013` int(10),`09-10-2013` int(10),`09-12-2013` int(10),`09-14-2013` int(10),`09-16-2013` int(10),`09-18-2013` int(10),`09-20-2013` int(10),`09-22-2013` int(10),`09-24-2013` int(10),`09-26-2013` int(10),`09-28-2013` int(10),`09-30-2013` int(10),`10-02-2013` int(10),`10-04-2013` int(10),`10-06-2013` int(10),`10-08-2013` int(10),`10-10-2013` int(10),`10-12-2013` int(10),primary key(Dress_ID))'
    cur.execute(q2)
    logging.info('Dress_sale table schema created')

    # Bulk loading has been done using mysql workbench and query solution is been done on jupyter for better readibilty .

except Exception as e:
    logging.error(e)
    mydb.close()

try:
    client = pymongo.MongoClient(
        "mongodb+srv://Sudhanshu_cluster:**urpasswd**@cluster0.qcrsb.mongodb.net/?retryWrites=true&w=majority")
    logging.info('Mongo_DB connection successful')
    at_table = pd.read_sql('select * from attribute', mydb)
    logging.info('attribute table -------------> attribute dataframe')
    ds_table = pd.read_sql('select * from dress_sale', mydb)
    logging.info('dress_sale table --------------> dress_sale dataframe')
    at_table.to_json('attri.json')
    logging.info('attribute dataframe ----------> attribute json')
    ds_table.to_json('dress.json')
    logging.info('dress_sale dataframe ----------> dress_sale json')
    database = client['Clothes']
    table1 = database['Attributes']
    table2 = database['Dress_sales']
    with open('attri.json') as f1:
        fd1 = json.load(f1)
    logging.info('string_format_json -------------------> dict_format_json conversion')

    if isinstance(fd1, list):
        table1.insert_many(fd1)
    else:
        table1.insert_one(fd1)
    logging.info('attribute data pushed successfully in mongodb')

    with open('dress.json') as f2:
        fd2 = json.load(f2)
    if isinstance(fd2, list):
        table2.insert_many(fd2)
    else:
        table2.insert_one(fd2)
    logging.info('dress_sale data pushed successfully in mongodb')

except Exception as e:
    logging.error(e)
