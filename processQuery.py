from asyncio import open_connection
from multiprocessing import connection
import sqlite3
import os.path
from unittest import result # for createSQLITEDB
import helperFunctions

categorischeData = ['brand','model','type']
metaDBCon = helperFunctions.openSQLITEDB('metaDB.db')

#transform CEQ to SQLITEcode
def transformCEQ(line):
	k = 10
	#split line by "," 
	line = line.split(",")
	#list of all dependenties
	dependenties = []
	#check if k is predefined or not
	if line[0][0] == "k":
		k = int(line[0][4:])
		#get the rest of line combined
		andQuery = " AND ".join(line[1:])
		dependenties = line[1:]
	else:
		andQuery = " AND ".join(line)
		dependenties = line
	result = (k, f"SELECT * FROM autompg WHERE {andQuery}", dependenties)
	return result


"""
def processQuery(query, con):
	extendedQuery = transformCEQ(query)
	resultOfBasicQuery = helperFunctions.procesQuery(extendedQuery[1], con)
	if (resultOfBasicQuery.count() < extendedQuery[0]):
		...
	elif(resultOfBasicQuery.count() == extendedQuery[0]):
		displayResult(resultOfBasicQuery, extendedQuery[0])	
	else:
		...

def topK(k, extendedQuery, con):
	...

def displayResult(result, perfectMachtCount):
	i = 0
	for line in result:
		if i < perfectMachtCount:
			print(f"* {line}")
		else:
			print(f"  {line}")
	print(f"amount of perfect matches found: {perfectMachtCount}")




output < k => difficult shit
output = k => result of transfromCEQ
output > k => Top K
"""