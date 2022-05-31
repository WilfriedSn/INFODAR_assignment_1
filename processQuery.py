from ast import While
from asyncio import open_connection
from json import tool
from logging import BufferingFormatter
from math import log2
from multiprocessing import connection
import random
import re
import sqlite3
import os.path
from tkinter import W
from tkinter.tix import Select
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
	line = line.replace(";","")
	line = line.replace(" ","")
	#split line by "," 
	line = line.split(",")
	#list of all dependenties
	dependenties = []
	#check if k is predefined or not
	if line[0][0] == "k":
		k = int(line[0][2:])
		#get the rest of line combined
		dependenties = line[1:]
	else:
		dependenties = line
	result = (dependenties, k)
	return result


def dependentiesToTopK(dependenties,k):
	result = []
	for dep in dependenties:
		dep = dep.split("=")
		result.append([dep[0],1,dep[1]])
	return topK(result,k)

#attr[0] = brand
#attr[1] = how importand this querry is 
#attr[2] = wanted value ('ford')
def topK(attrNeededValuess,k):
	fillMinMaxDictionairy()
	x = len(attrNeededValuess)*k*1.5
	topK = []
	categorischeIDFDictionairy = {}
	attrList = []

	for attr in attrNeededValuess:
		if attr[0] in categorischeData:
			temp = (getTopXCatagoricalData(attr[0] ,x ,attr[2]))
			topK.append(temp[0])
			for cata in temp[1]:
				categorischeIDFDictionairy[cata[0]] = cata[1]
		else:
			topK.append(getTopXNumericalData(attr[0] ,x ,attr[2]))
		attrList.append(attr[0])
		
	
	tempresult = []
	buffer = []
	#calculate max value
	maxValueHelper = []
	result = []
	for attr in attrNeededValuess:
		maxValueHelper.append(attr[1])
	maxValue = sum(maxValueHelper)
	
	print(f"maxvalutehelper: {maxValueHelper}")
	for valIndex in range(min(map(lambda x : len(x), topK))):
		for attrID in range(len(attrNeededValuess)):
			if (topK[attrID][valIndex][0] not in tempresult and topK[attrID][valIndex][0] not in buffer):
				#print(f"SELECT {','.join(attrList)} FROM autompg WHERE id = {topK[i][0][0]}")
				#print(topK[attrID][valIndex][0])
				helper = helperFunctions.getResultOfQuery(mainDBCon, f"SELECT {','.join(attrList)} FROM autompg WHERE id = {topK[attrID][valIndex][0]}")
				helper = getIdValue(helper,attrNeededValuess,categorischeIDFDictionairy, attrID)
				buffer.append([topK[attrID][valIndex][0],helper[0]])
				maxValueHelper[attrID] = helper[1]
		maxValue = sum(maxValueHelper)
		for bufferItem in buffer:
			if (bufferItem[1] >= maxValue):
				tempresult.append(bufferItem)
				buffer.remove(bufferItem)
		if (len(tempresult)>k):
			break
	tempresult.sort(key=lambda x:x[1],reverse=True) #hopefully this works
	if len(tempresult) > k:
		nonties = list(filter(lambda x : x[1] > tempresult[k-1][1], tempresult))
		tiesSection = tieBreaker(list(filter(lambda x : x[1] == tempresult[k-1][1], tempresult)), k - len(nonties), attrNeededValuess)
		result = nonties + tiesSection
	else:
		result = tempresult
	return result
		

def tieBreaker(ids,x,attrNeededValuess):
	categorischeAtributes = []
	score = ids[0][1]
	for i in range(len(attrNeededValuess)):
		if attrNeededValuess[i][1] in categorischeData:
			categorischeAtributes.append[attrNeededValuess[i][1]]
	if len(categorischeAtributes)>0:
		for id in ids:
			id[1] = 0
			for i in range(len(categorischeAtributes)):
				id[1] += helperFunctions.getResultOfQuery(metaDBCon, f"SELECT idf FROM idf WHERE attr = '{categorischeAtributes[i]}'")[0][0]
		ids.sort(key=lambda x:x[1],reverse=True)
	#in case of only numbers return only the first x
	return ids[0:x]







def fillMinMaxDictionairy():
	helper = helperFunctions.getResultOfQuery(metaDBCon, f"SELECT attr,diff FROM minMax;")
	for i in range(len(helper)):
		attrMaxDiffDictionairy[helper[i][0]] = helper[i][1]

			
#returns the value of the ID
#input [edited value of all parts], score function
def getIdValue(values, attrNeededValuess, categorischeIDFDictionairy, importantValueIndex):
	result = 0
	values[0] = list(values[0])
	for i in range(len(attrNeededValuess)):
		if attrNeededValuess[i][0] not in categorischeData:
			#(attr max diff-(attrQueryvalue - attrRealValue)^2/maxdiff) with a mininmum of 0 and a max of 1
			values[0][i] = 1 / (1+log2(1+abs( int(attrNeededValuess[i][2]) - values[0][i])/  (int(attrNeededValuess[i][2])+values[0][i])))
			#max(0, ((attrMaxDiffDictionairy[attrNeededValuess[i][0]] - pow((attrNeededValuess[i][2]-values[i]),2))/attrMaxDiffDictionairy[attrNeededValuess[i][0]]))
		else:
			values[0][i] = categorischeIDFDictionairy[values[0][i]]
		if i == importantValueIndex:
			importantValue = values[0][i]*attrNeededValuess[i][1]
		result += values[0][i]*attrNeededValuess[i][1]
	
	return [result,importantValue]


def getTopXNumericalData(attr, x, expectedValue):
	expectedValue = int(expectedValue)
	toHigherValues = helperFunctions.getResultOfQuery(mainDBCon, f"SELECT id, {attr} FROM autompg WHERE {attr} >= {expectedValue} order by {attr} asc limit {round(x)};")
	toLowerValues = helperFunctions.getResultOfQuery(mainDBCon, f"SELECT id, {attr} FROM autompg WHERE {attr} < {expectedValue} order by {attr} desc limit {round(x)};")
	result = []
	j = 0
	k = 0
	if toHigherValues == []:
		return toLowerValues
	elif toLowerValues == []:
		return toHigherValues
	for i in range(len(toHigherValues)+ len(toLowerValues)):
		if (abs(toHigherValues[j][1] - expectedValue) <= abs(toLowerValues[k][1] - expectedValue)):
			result.append(toHigherValues[j])
			j += 1
		else:
			result.append(toLowerValues[k])
			k += 1
		if (j >= len(toHigherValues)):
			result.extend(toLowerValues[k:])
			break
		if (k >= len(toLowerValues)):
			result.extend(toHigherValues[j:])
			break
	return result
		


	


def getTopXCatagoricalData(attr ,x ,expectedValue):
	Catagoricals = helperFunctions.getResultOfQuery(metaDBCon, f"Select val2, IDF from {attr}IDF where val1 = {expectedValue} order by IDF desc;")
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




if __name__ == "__main__": #only execute this code when this file is ran directly incase we want to import functions from here
	#fillMinMaxDictionairy()
	#print(getIdValue(list(helperFunctions.getResultOfQuery(mainDBCon, "SELECT mpg, cylinders, displacement, brand from autompg where id = 10;")[0]), [['mpg',1,100],['cylinders',1, 3],['displacement',1, 5],['brand',1, 'ford']],0))
	temp = transformCEQ("k = 10")
	print(dependentiesToTopK(temp[0],temp[1]))
	#print(getTopXCatagoricalData('brand',300 ,'ford'))
	#print(getTopXNumericalData('weight',20 ,3000))
	print("finished")

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