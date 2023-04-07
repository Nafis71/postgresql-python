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


def checkPostgresql(tableName):                   #checking if the given table name is exist in the database or not
    cursor.execute("SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '"+tableName+"');")
    result = cursor.fetchone()
    return result[0]


def createPostgresql(tableName,columns):         
    cursor.execute("Create table "+tableName+"("+columns+");")     #table creation query
    connection.commit()


def readPostgresql(tableName):
    cursor.execute("Select *from "+tableName+" order by id desc")                     #fetching table data
    result = cursor.fetchall()
    return result


def insertPostgresql(tableName,tblValues):
    cursor.execute("Insert into "+tableName+" values("+tblValues+");")           #inserting data into table     
    connection.commit()
                                                                            

def updatePostgresql(tableName,tblValues):
    cursor.execute("Update "+tableName+" Set "+tblValues+";")             #updating data from the table
    connection.commit()


def deletePostgresql(tableName,tblValues):
    cursor.execute("Delete from "+tableName+" where "+tblValues+"")       #deleting specific data/row from the table
    connection.commit()


def deleteAllPostgresql(tableName):
    cursor.execute("Delete from "+tableName+" where True")               #deleting all the rows/data from the table
    connection.commit()


def dropTablePostgresql(tableName):
    cursor.execute("Drop table "+tableName+"")
    connection.commit()

def csvInsertPostgresql(tableName,fileName):                          #importing data through a csv file
    with open(''+fileName+'.csv', 'r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row.
        for row in reader:
            cursor.execute("INSERT INTO "+tableName+" VALUES (%s, %s, %s)",row)
    connection.commit()


def check(tableName):
     result = checkPostgresql(tableName)
     return result


choosen = -100
while choosen != 0 :
    print("Choose an option:")
    print("1.Create a table")
    print("2.Show a table info")
    print("3.Insert Data into a table")
    print("4.Update Data into a table")
    print("5.Delete a row from a table")
    print("6.Delete all rows from a table")
    print("7.Insert data from CSV to table")
    print("8.Drop a table")
    print("Enter 0 to exit!")
    choosen = int(input("-> "))
    match choosen:
        case 1:
            os.system("clear")
            tableName = input("Enter name of the table: ")
            if check(tableName) != True:
                print("Enter single/multi column name with their data types according to postgresql syntax without brackets(ex: id int,name varchar(40))")
                columns = input("-> ")
                createPostgresql(tableName,columns)
                print("Created!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tableName+" Table already exists in database")
                time.sleep(2)
                os.system("clear")
        case 2:
            os.system("clear")
            tableName = input("Enter name of the table: ")
            if check(tableName) == True:
                startTime = time.time()
                print("Table info: ")
                rows = readPostgresql(tableName)
                for row in rows:
                    print(row)
                endTime = time.time()
                execTime = float(endTime - startTime)
                print("Results showed in : ",execTime,"s")
                time.sleep(8)
                os.system("clear")
            else:
                print(""+tableName+" Table doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 3:
            os.system("clear")
            tableName = input("Enter name of the table: ")
            if check(tableName) == True:
                print("Enter Values for the given table's column according to postgresql syntax without brackets(ex: 8,'name')")
                tblValues = input("-> ")
                insertPostgresql(tableName,tblValues)
                print("Inserted!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tableName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 4:
            os.system("clear")
            tableName = input("Enter name of the table: ")
            if check(tableName) == True:
                print("Enter Values for the given table's column according to postgresql syntax without brackets(ex: name='Kurama',id=08 Where your_condition)")
                tblValues = input("-> ")
                updatePostgresql(tableName,tblValues)
                print("Updated!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tableName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 5:
            os.system("clear")
            tableName = input("Enter name of the table: ")
            if check(tableName) == True:
                print("Enter condition values for the given table's column according to postgresql syntax without brackets(ex: name='Kurama',id=08)")
                tblValues = input("-> ")
                deletePostgresql(tableName,tblValues)
                print("Deleted!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tableName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 6:
            os.system("clear")
            tableName = input("Enter name of the table: ")
            if check(tableName) == True:
                deleteAllPostgresql(tableName)
                print("Deleted!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tableName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 7:
            os.system("clear")
            tableName = input("Enter name of the table: ")
            if check(tableName) == True:
                fileName = input("Enter CSV file name without extension name for table: "+tableName+": ")
                csvInsertPostgresql(tableName,fileName)
                print("Imported!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tableName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case 8:
            os.system("clear")
            tableName = input("Enter name of the table: ")
            if check(tableName) == True:
                dropTablePostgresql(tableName)
                print("Dropped!")
                time.sleep(2)
                os.system("clear")
            else:
                print(""+tableName+" doesn't exists")
                time.sleep(2)
                os.system("clear")
        case _:
            print("Wrong Input")
            time.sleep(2)
            os.system("clear")
                
                
            
                
                
            
            
                
                
                
                
                
                
                

            
            
        
    
        
    

