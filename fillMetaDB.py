from multiprocessing import connection
import sqlite3
import os.path
from unittest import result # for createSQLITEDB
import helperFunctions


#transforms workload to a usable 2d list
def transFormWorkload(fileName):
	lines = helperFunctions.readFile(fileName)
	workload = []
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
			workload.append([int(line[0]), line[1]])
	return workload

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

def workLoadToDB4(workload):
	for line in workload:
		whereClause = line[1].split('FROM autompg')[1]
		line[1] = "SELECT ID FROM autompg" + whereClause
	return workload

#get ids from sqlite querry
def getIdsFormDB(query, connection):
	cursor = connection.cursor()
	cursor.execute(query)
	ids = cursor.fetchall()
	return ids

def fillDB4WorkloadData(filename, connection):
	workload = workLoadToDB4(transFormWorkload(filename))
	#create a dictionairy <ID,value>
	workloadArray = list()
	#add 500 0 to workloadArray //shouldbechanged
	for i in range(400):
		workloadArray.append([i,0])

	for line in workload:
		ids = getIdsFormDB(line[1], connection)
		for id in ids:
			workloadArray[id[0]][1] += line[0]
	return workloadArray


# connection = openSQLITEDB('test.db')
# test = fillDB4WorkloadData("workload.txt", connection)
# for i in range(len(test)):
# 	print(f"{i} : {test[i]}")


#|----------------------------------------------------------------------------------------------------------------------|
#| db3. Voor elke waarde van elk categorische datapunt, vindt log(QF(alles)/QF(dit))									|
#|----------------------------------------------------------------------------------------------------------------------|
"""def workLoadToDB3(catagorishDataPoint, workload):
	#transform workload to "select catagorishDataPoint, count(id) from workload group by catagorishDataPoint;"
	for line in workload:
		whereClause = line[1].split('FROM autompg')[1]
		whereClause = whereClause.replace(';','')
		line[1] = f"SELECT {catagorishDataPoint} ,count(ID) FROM autompg {whereClause} group by {catagorishDataPoint};"
	return workload

def fillDB3WorkloadData(catagorishDataPoint, connection):
	workload = workLoadToDB3(catagorishDataPoint, transFormWorkload("workload.txt"))
	#create a list <catagorishDataPoint,amount of times mentioned>
	workloadArray = list()
	#create a list <catagorischdataPoint,<workloadarray>>
	result = list()
	cursor = connection.cursor()
	cursor.execute(f"SELECT {catagorishDataPoint},count(ID) FROM autompg group by {catagorishDataPoint};")
	catagorishDataPoints = cursor.fetchall()
	print(catagorishDataPoints)
	for line in catagorishDataPoints:
		workloadArray.append([line[0], 0])
	for line in workloadArray:



	return result
	
connection = openSQLITEDB('test.db')
test = fillDB3WorkloadData("brand", connection)"""



#print(transformCEQ("k = 9, model = ding, year = 1999, price = 50", "table"))

#createSQLITEDB("test.db")
#print(getSqliteInsertCode("test.db"))