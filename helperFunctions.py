from multiprocessing import connection
import sqlite3
import os.path
from unittest import result # for createSQLITEDB

#Made by:
#Olivia Lohmann: (8742779)
#Wifried Snijders(8657078)


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

#get results from sqlite querry
def getResultOfQuery(connection, query):
	cursor = connection.cursor()
	cursor.execute(query)
	ids = cursor.fetchall()
	return ids

def insertDataIntoQuery(connection, query):
	cursor = connection.cursor()
	cursor.execute(query)
	connection.commit()

