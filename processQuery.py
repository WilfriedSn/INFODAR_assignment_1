from asyncio import open_connection
from multiprocessing import connection
import random
import sqlite3
import os.path
from unittest import result # for createSQLITEDB
import helperFunctions

categorischeData = ['brand','model','type']
metaDBCon = helperFunctions.openSQLITEDB('metaDatabase.db')

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

#attr[0] = brand
#attr[1] = weight
#attr[2] = wanted value (ford)
def topK(attrNeededValues,k):
	x = attrNeededValues.length()*k*1.5
	topK = []
	for attr in attrNeededValues:
		if attr[0] in categorischeData:
			topK.append(getTopXCatagoricalData(attr[0],attr[2]))
		else:
			topK.append(getTopXNumericalData(attr[0],attr[2]))
	...

def getTopXNumericalData(table, x, expectedValue):
	helperFunctions.getResultOfQuery(metaDBCon, f"SELECT val, QFScore FROM {table}QF WHERE val >= {expectedValue}")
	helperFunctions.getResultOfQuery(metaDBCon, f"SELECT val, QFScore FROM {table}QF WHERE val < {expectedValue}")
	


def getTopXCatagoricalData(attr, expectedValue):
	helperFunctions.getResultOfQuery(metaDBCon, f"Select val2, IDF from {attr}IDF where val1 = '{expectedValue}' order by IDF desc;")

def tieBreaker(id1,id2,attr):
	if (attr == 'NONE'):
		#a random choice is the same as always choosing the first one
		return id1
	else:
		id1val = helperFunctions.getResultOfQuery(metaDBCon, f"SELECT QFScore FROM {attr}QF WHERE val = {id1}")
		id2val = helperFunctions.getResultOfQuery(metaDBCon, f"SELECT QFScore FROM {attr}QF WHERE val = {id2}")
		if (id1val > id2val):
			return id1
		else:
			return id2



if __name__ == "__main__": #only execute this code when this file is ran directly incase we want to import functions from here
	...

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