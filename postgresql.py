#Menu driven PostgreSql CRUD operation in python

import psycopg2 as psql     #postgresql library import
import os,time,csv

connection = psql.connect(database="mydb",
                         user = "tonmoy",
                         host = "localhost",    #defining database settings
                         password = "root",
                         port = 5432
                         )
cursor = connection.cursor()


def checkPostgresql(tblName):                   #checking if the given table name is exist in the database or not
    cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '"+tblName+"');")
    result = cursor.fetchone()
    return result[0]


def createPostgresql(tblName,columns):         
    cursor.execute("Create table "+tblName+"("+columns+");")     #table creation query
    connection.commit()


def readPostgresql(tblName):
    cursor.execute("Select *from "+tblName+" order by id desc")                     #fetching table data
    result = cursor.fetchall()
    return result


def insertPostgresql(tblName,tblValues):
    cursor.execute("Insert into "+tblName+" values("+tblValues+");")           #inserting data into table
    connection.commit()


def updatePostgresql(tblName,tblValues):
    cursor.execute("Update "+tblName+" Set "+tblValues+";")             #updating data from the table
    connection.commit()


def deletePostgresql(tblName,tblValues):
    cursor.execute("Delete from "+tblName+" where "+tblValues+"")       #deleting specific data/row from the table
    connection.commit()


def deleteAllPostgresql(tblName):
    cursor.execute("Delete from "+tblName+" where 1")               #deleting all the rows/data from the table
    connection.commit()


def dropTablePostgresql(tblName):
    cursor.execute("Drop table "+tblName+"")
    connection.commit()

def csvInsertPostgresql(tblName,fileName):                          #importing data through a csv file
    with open(''+fileName+'.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row.
        for row in reader:
            cursor.execute("INSERT INTO "+tblName+" VALUES (%s, %s, %s, %s)",row)
    connection.commit()


option = -1
while option != 0 :
    print("Choose an option:")
    print("1.Create a table")
    print("2.Show a table info")
    print("3.Insert Data into a table")
    print("4.Update Data into a table")
    print("5.Delete a row from a table")
    print("6.Delete entire row from a table")
    print("7.Insert data from CSV to table")
    print("8.Drop a table")
    print("Enter 0 to exit!")
    option = int(input("-> "))
    match option:
        case 1:
            os.system("clear")
            tblName = input("Enter name of the table: ")
            result = checkPostgresql(tblName)
            if result != True:
                print("Enter single/multi column name with their data types according to postgresql syntax without brackets(ex: id int,name varchar(40))")
                columns = input("-> ")
                createPostgresql(tblName,columns)
                print("Created!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tblName+" Table already exists in database")
                time.sleep(2)
                os.system("clear")
        case 2:
            os.system("clear")
            tblName = input("Enter name of the table: ")
            result = checkPostgresql(tblName)
            if result == True:
                startTime = time.time()
                print("Table info: ")
                rows = readPostgresql(tblName)
                for row in rows:
                    print(row)
                endTime = time.time()
                execTime = float(endTime - startTime)
                print("Results showed in : ",execTime,"s")
                time.sleep(8)
                os.system("clear")
            else:
                print(""+tblName+" Table doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 3:
            os.system("clear")
            tblName = input("Enter name of the table: ")
            result = checkPostgresql(tblName)
            if result == True:
                print("Enter Values for the given table's column according to postgresql syntax without brackets(ex: 8,'name')")
                tblValues = input("-> ")
                insertPostgresql(tblName,tblValues)
                print("Inserted!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tblName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 4:
            os.system("clear")
            tblName = input("Enter name of the table: ")
            result = checkPostgresql(tblName)
            if result == True:
                print("Enter Values for the given table's column according to postgresql syntax without brackets(ex: name='Kurama',id=08 Where your_condition)")
                tblValues = input("-> ")
                updatePostgresql(tblName,tblValues)
                print("Updated!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tblName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 5:
            os.system("clear")
            tblName = input("Enter name of the table: ")
            result = checkPostgresql(tblName)
            if result == True:
                print("Enter condition values for the given table's column according to postgresql syntax without brackets(ex: name='Kurama',id=08)")
                tblValues = input("-> ")
                deletePostgresql(tblName,tblValues)
                print("Deleted!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tblName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 6:
            os.system("clear")
            tblName = input("Enter name of the table: ")
            result = checkPostgresql(tblName)
            if result == True:
                deleteAllPostgresql()
                print("Deleted!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tblName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 7:
            os.system("clear")
            tblName = input("Enter name of the table: ")
            result = checkPostgresql(tblName)
            if result == True:
                fileName = input("Enter CSV file name without extension name for table: "+tblName+": ")
                csvInsertPostgresql(tblName,fileName)
                print("Imported!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tblName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 8:
            os.system("clear")
            tblName = input("Enter name of the table: ")
            result = checkPostgresql(tblName)
            if result == True:
                dropTablePostgresql(tblName)
                print("Dropped!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tblName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case _:
            print("Wrong Input")
            time.sleep(2)
            os.system("clear")
