from asyncio import open_connection
from json import tool
from multiprocessing import connection
import random
import re
import sqlite3
import os.path
from unittest import result
from xml.dom.minidom import Attr # for createSQLITEDB
import helperFunctions

categorischeData = ['brand','model','type']
metaDBCon = helperFunctions.openSQLITEDB('metaDatabase.db')
mainDBCon = helperFunctions.openSQLITEDB('mainDatabase.db')
attrMaxDiffDictionairy = {}

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
def topK(attrNeededValuess,k):
	fillMinMaxDictionairy
	x = attrNeededValuess.length()*k*1.5
	topK = []
	categorischeIDFDictionairy = {}

	for attr in attrNeededValuess:
		if attr[0] in categorischeData:
			temp = (getTopXCatagoricalData(attr[0] ,x ,attr[2]))
			topK.append(temp[0])
			for cata in temp[1]:
				categorischeIDFDictionairy[cata[0]] = cata[1]
		else:
			topK.append(getTopXNumericalData(attr[0] ,x ,attr[2]))
	
	result = []
	buffer = []
	#calculate max value
	maxValueHelper = []
	for i in range(len(attrNeededValuess)):
		if attrNeededValuess[i][0] not in categorischeData:
			maxValueHelper.append = attrNeededValuess[i][2]
		else:
			maxValueHelper.append = 1
	maxvalue = getIdValue(maxValueHelper,attrNeededValuess)
	while (len(result)<k):
		for i in len(attrNeededValuess):
			if (topK[i][0] not in result and topK[i][0] not in buffer):
				...


def fillMinMaxDictionairy():
	helper = helperFunctions.getResultOfQuery(metaDBCon, f"SELECT attr,diff FROM minMax;")
	for i in range(len(helper)):
		attrMaxDiffDictionairy[helper[i][0]] = helper[i][1]

			
#returns the value of the ID
#input [edited value of all parts], score function
def getIdValue(values, attrNeededValuess):
	result = 0
	for i in range(len(attrNeededValuess)):
		if attrNeededValuess[i][0] not in categorischeData:
			#(attr max diff-(attrQueryvalue - attrRealValue)^2/maxdiff) with a mininmum of 0 and a max of 1
			values[i] = max(0, ((attrMaxDiffDictionairy[attrNeededValuess[i][0]] - pow((attrNeededValuess[i][2]-values[i]),2))/attrMaxDiffDictionairy[attrNeededValuess[i][0]]))
		result += values[i]*attrNeededValuess[1][i]
	return result


def getTopXNumericalData(attr, x, expectedValue):
	toHigherValues = helperFunctions.getResultOfQuery(mainDBCon, f"SELECT id, {attr} FROM autompg WHERE {attr} >= {expectedValue} order by {attr} asc limit {round(x)};")
	toLowerValues = helperFunctions.getResultOfQuery(mainDBCon, f"SELECT id, {attr} FROM autompg WHERE {attr} < {expectedValue} order by {attr} desc limit {round(x)};")
	result = []
	j = 0
	k = 0
	for i in range(len(toHigherValues)+ len(toLowerValues)):
		if (abs(toHigherValues[j][1] - expectedValue) <= abs(toLowerValues[k][1] - expectedValue)):
			result.append(toHigherValues[j])
			j += 1
		else:
			result.append(toLowerValues[k])
			k += 1
		if (j >= len(toHigherValues)):
			result.append(toLowerValues[k:])
			break
		if (k >= len(toLowerValues)):
			result.append(toHigherValues[j:])
			break
	return result
		


	


def getTopXCatagoricalData(attr ,x ,expectedValue):
	Catagoricals = helperFunctions.getResultOfQuery(metaDBCon, f"Select val2, IDF from {attr}IDF where val1 = '{expectedValue}' order by IDF desc;")
	result = []
	for i in range(len(Catagoricals)):
		if (len(result) < x):
			helpers = (helperFunctions.getResultOfQuery(mainDBCon, f"Select id, {attr} from autompg where {attr} = '{Catagoricals[i][0]}';"))
			for helper in helpers:
				result.append([helper[0], Catagoricals[i][1]])
		else:
			break
	result = [result, Catagoricals]
	return result

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
	print(getTopXCatagoricalData('brand',300 ,'ford'))
	print(getTopXNumericalData('weight',20 ,3000))

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