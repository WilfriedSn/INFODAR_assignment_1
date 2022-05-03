import sqlite3
import os.path # for createSQLITEDB

#returns all lines from a file
def readFile(fileName):
    file = open(fileName, 'r')
    lines = file.readlines()
    file.close()
    return lines


#transforms workload to a usable 2d list
def transFormWorkload(fileName):
    lines = readFile(fileName)
    workload = [[]]
    for line in lines:
        #check if it is intended as code
        if line.__contains__(' times: '):
            #remove trailing spaces and \n, add semicolon make the second part runnable sqlitecode,
            #   split by " times: " 
            #   first index (amount of times) as a int
            #   second index (the Sqlite code) as a string
            line = line.strip()
            line += ';'
            line = line.split(' times: ')
            line[0] = int(line[0])
            workload.append(line)
    return workload

def getSqliteInsertCode(filename):
    lines = readFile(filename)
    newLines = []
    for line in lines:
        #make new list with all non empty lines
        if line != '\n':
            newLines.append(line)
    return newLines

#returns a sqliteconnection to use
def createSQLITEDB(filename):
    #check if file exists in the current directory
    if os.path.exists(filename):
        print('DBFile ' + filename + ' got deleted and recreated')
        os.remove(filename)
    con = sqlite3.connect(filename)
    return con

def openSQLITEDB(filename):
    #not?, ! doesnt work
    if not(os.path.exists(filename)):
        print('dbfile ' + filename + ' did not exsist, one was created')
    con = sqlite3.connect(filename)


#probaly not needed
def closeSQLITEDB(con):
    con.close()




    
    
        

