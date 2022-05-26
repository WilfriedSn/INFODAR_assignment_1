from multiprocessing import connection
import sqlite3
import os.path # for createSQLITEDB

#returns all lines from a file
def readFile(fileName):
	with open(fileName, 'r') as f:
		lines = f.read().split("\n")
		return lines


def getSqliteInsertCode(filename):
    lines = readFile(filename)
    newLines = []
    for line in lines:
        #make new list with all non empty lines
        if line != "":
            newLines.append(line)
    return newLines

#returns a sqliteconnection to use
def createSQLITEDB(filename):
    #check if file exists in the current directory
    if os.path.exists(filename):
        print(f'DBFile {filename} got deleted and recreated')
        os.remove(filename)
    return sqlite3.connect(filename)

def openSQLITEDB(filename):
    #not?, ! doesnt work
    if not os.path.exists(filename):
        print(f'dbfile {filename} did not exsist, one was created')
    return sqlite3.connect(filename)


#probaly not needed
def closeSQLITEDB(con):
    con.close()

#transform CEQ to SQLITEcode
def transformCEQ(line, table):
    k = 10
    #split line by "," 
    line = line.split(",")
    #check if k is predefined or not
    if line[0][0] == "k":
        k = int(line[0][4:])
        #get the rest of line combined
        andQuery = " AND ".join(line[1:])
    else:
        andQuery = " AND ".join(line)
    result = (k, f"SELECT * FROM {table} WHERE {andQuery}")
    return result

#|----------------------------------------------------------------------------------------------------------------------|
#| db4. Voor elke auto, kijk op hoeveel van de workload queries de auto applied, sla een lookup van auto naar dit op.   |
#|----------------------------------------------------------------------------------------------------------------------|

#transforms workload to a usable 2d list
def transFormWorkload(fileName):
    lines = readFile(fileName)
    workload = list()
    for line in lines:
        #check if it is intended as code
        if ' times: ' in line:
            #remove trailing spaces and \n, add semicolon make the second part runnable sqlitecode,
            #   split by " times: " 
            #   first index (amount of times) as a int
            #   second index (the Sqlite code) as a string
            line = line.strip()
            line += ';'
            line = line.split(' times: ')
            whereClause = line[1].split('FROM autompg')[1]
            whereClause = "SELECT ID FROM autompg" + whereClause
            workload.append((int(line[0]), whereClause))
    return workload

#get ids from sqlite querry
def getIdsFormDB(query, connection):
    cursor = connection.cursor()
    cursor.execute(query)
    ids = cursor.fetchall()
    return ids

def fillDB4WorkloadData(filename, connection):
    workload = transFormWorkload(filename)
    #create a dictionairy <ID,value>
    workloadArray = [0]
    #add 500 0 to workloadArray //shouldbechanged
    for i in range(400):
        workloadArray.append(0)

    for line in workload:
        ids = getIdsFormDB(line[1], connection)
        for id in ids:
            workloadArray[id[0]] += line[0]
    return workloadArray

connection = openSQLITEDB('test.db')

test = fillDB4WorkloadData("workload.txt", connection)
for i in range(len(test)):
	print(f"{i} : {test[i]}")

test.sort()
for i in range(len(test)):
	print(f"{i} : {test[i]}")


#print(transformCEQ("k = 9, model = ding, year = 1999, price = 50", "table"))

#createSQLITEDB("test.db")
#print(getSqliteInsertCode("test.db"))