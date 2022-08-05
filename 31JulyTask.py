import logging
import pymongo
import pandas as pd
import mysql.connector as conn

logging.basicConfig(filename="task_31july_log.txt", level=logging.DEBUG)

# 1. Read this dataset in pandas , mysql and mongodb

# reading data from mysql
from pandas._testing import iloc

try:

    sql_db = conn.connect(host="localhost", user="root", passwd="root@051")
    logging.info('connected to mysql database:')
    logging.info(sql_db)
    cursor = sql_db.cursor()
    fitbitData = pd.read_sql('SELECT * FROM FitBit_data.FitBit_table', sql_db)
    logging.info("reading fitbit data from mysql")
    logging.info(fitbitData.head())
except Exception as e:
    logging.error(e)

# Reading the data using pandas
try:
    logging.info("reading data using pandas")
    p_df = pd.read_csv('/usr/local/mysql-8.0.30-macos12-arm64/data_set/FitBit_data.csv')
    logging.info(p_df.head())

except Exception as e:
    logging.error(e)

# Reading and Loading the data from mongo db


try:
    client = pymongo.MongoClient("mongodb+srv://root:root051@cluster0.dxerf.mongodb.net/?retryWrites=true&w=majority")
    db = client.test
    logging.info("Database Connected ")
    df = pd.read_csv('/usr/local/mysql-8.0.30-macos12-arm64/data_set/FitBit_data.csv')
    logging.info(df)  # reading data using pandas
    data = df.to_dict(orient="records")  # data converted from csv JSON
    logging.info("Converted df records into JSON :")

    """data loaded start """
    db1 = client['FitBit_DB']
    coll = db1["FitBit_collection"]
    # coll.insert_many(data)
    # logging.info("data inserted")

    """data load end"""

    """reading data from mongodb Start"""
    logging.info("reading data from mongodb atlas")
    monData = coll.find()
    for i in monData:
        logging.info(i)

    """reading data from mongodb Start"""

except Exception as e:
    logging.error(e)

# 2. while creting a table in mysql dont use manual approach to create it  ,always use a automation to create a table in mysql

"""

#create table using csvkit
 - Donload csvkit using (pip install csvkit)
 - by using ls and cd command went to the folder where we kept operational dataset   
 - by using command "csvsql --dialect mysql --snifflimit 100000 FitBit_data.csv > FitBitData.sql" created FitBitData.sql file and opened with sqlworkbench and created table. 

 #Dumping data into mysql

cursor.execute("LOAD DATA INFILE '/usr/local/mysql-8.0.30-macos12-arm64/data_set/FitBit_data.csv'
INTO TABLE FitBit_data FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';")

"""

# 3. convert all the dates available in dataset to timestamp format in pandas and in sql you to convert it in date format

try:
    df3 = pd.read_csv('/usr/local/mysql-8.0.30-macos12-arm64/data_set/FitBit_data.csv')
    logging.info(df3.dtypes)
    logging.info("converting ActivityDate object to datetime stamp format")
    df3['ActivityDate'] = pd.to_datetime(df3['ActivityDate'])  # Converting dates to timestamp
    logging.info(df3.dtypes)
except Exception as e:
    logging.error(e)

# 4 . Find out in this data that how many unique id's we have

try:
    logging.info("Count of unique Id's : ")
    logging.info(len(df3['Id'].unique()))  # here we find unique Id's
except Exception as e:
    print(e)

# 5 . which id is one of the active id that you have in whole dataset
try:
    logging.info("active id in whole dataset : ")
    df3['totalActiveMinute'] = (df3['VeryActiveMinutes'] + df3['FairlyActiveMinutes'] + df3['LightlyActiveMinutes'])
    activeID = df3.sort_values('totalActiveMinute', ascending=False).max()

    logging.info(activeID['Id'])
except Exception as e:
    logging.error(e)

# 6 . how many of them have not logged there activity find out in terms of number of ids
try:
    logging.info('Number of ids who not logged there activity :')
    logging.info(df3.groupby('Id')['LoggedActivitiesDistance'].sum() < 1)
    logging.info('Total Number of Id who not logged in : ')
    logging.info(len(df3.groupby('Id')['LoggedActivitiesDistance'].sum() < 1))
except Exception as e:
    logging.error(e)

# 7 . Find out who is the laziest person id that we have in dataset
try:
    logging.info("laziest person ID in the dataset :")
    df4 = pd.read_csv('/usr/local/mysql-8.0.30-macos12-arm64/data_set/FitBit_data.csv')
    logging.info(df4.groupby('Id')['VeryActiveMinutes'].sum().idxmin())
except Exception as e:
    logging.error(e)

# 8 . Explore over an internet that how much calories burn is required for a healthy person and find out how many healthy person we have in our dataset
try:
    df5 = pd.read_csv('/usr/local/mysql-8.0.30-macos12-arm64/data_set/FitBit_data.csv')
    c_df = pd.read_html('https://www.healthline.com/nutrition/how-many-calories-per-day#average-needs')
    logging.info("Standard calories required detail :")
    logging.info("calories required for age between 20yrs - 30yrs :")
    logging.info(c_df[0])
    logging.info("No of healthy person in our dataset : ")
    logging.info(df5.groupby('Id')['Calories'].sum().apply(lambda x: x >= 2200).value_counts())

except Exception as e:
    logging.error(e)

# 9. how many person are not a regular person with respect to activity try to find out those

try:
    logging.info("No of person who are irregular with respect to activities : ")
    df6 = pd.read_csv('/usr/local/mysql-8.0.30-macos12-arm64/data_set/FitBit_data.csv')
    logging.info(df6[df6["TotalSteps"] == 0])
except Exception as e:
    logging.error(e)

# 10 . Who is the third most active person in this dataset find out those in pandas

try:
    df7 = pd.read_csv('/usr/local/mysql-8.0.30-macos12-arm64/data_set/FitBit_data.csv')
    third_most = df7.groupby('Id')['VeryActiveMinutes'].sum().sort_values(ascending=False)[2:3]
    logging.info("third most active person in the data set: ")
    logging.info(third_most)
except Exception as e:
    logging.error(e)

# 11 . who is the 5th most laziest person avilable in dataset find it out

try:
    df8 = pd.read_csv('/usr/local/mysql-8.0.30-macos12-arm64/data_set/FitBit_data.csv')
    lazyPerson = df8.groupby('Id')['SedentaryMinutes'].sum().sort_values(ascending=True)[4:5]
    logging.info("5th Most laziest person in the dataset")
    logging.info(lazyPerson)
except Exception as e:
    logging.error(e)

# 12 . what is a total cummulative calories burn for a person find out

try:
    df9 = pd.read_csv('/usr/local/mysql-8.0.30-macos12-arm64/data_set/FitBit_data.csv')
    result = df9.groupby('Id', as_index=False)['Calories'].sum()
    logging.info("total cummulative calories burn for a person :")
    logging.info(result)
except Exception as e:
    logging.error(e)
