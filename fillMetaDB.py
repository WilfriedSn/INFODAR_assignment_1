from multiprocessing import connection
import sqlite3
import os.path
from unittest import result # for createSQLITEDB
import json
import math
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
#| Create workload.json to more easily read the workload data later													    |
#|----------------------------------------------------------------------------------------------------------------------|

def createWorkloadJson():
	# 124 times: SELECT * FROM autompg WHERE model_year = '82' AND type = 'sedan'
	# {"count": 124, "model_year" : ["82"], "type" : ["sedan"] }
	
	with open("workload.txt", "r") as workload:
		result = []

		lines = workload.read().split("\n")[2:]

		for line in lines:
			if line != "":
				element = {}
				element["count"] = int(line.split(" times:")[0])
				queries = line.split(" WHERE ")[1].split(" AND ")
				for query in queries:
					if " IN " in query:
						name, vals = query.split(" IN ")
						element[name] = vals[2:-2].split("','")
					else:
						name, vals = query.split(" = '")
						element[name] = [vals[:-1]]
				result.append(element)
		
		with open("workload.json", "w") as file:
			json.dump(result, file)

#|----------------------------------------------------------------------------------------------------------------------|
#| Helper functions specific to this file																			    |
#|----------------------------------------------------------------------------------------------------------------------|

def getAllUniqueVals(db, attr):
	cur = db.cursor()
	result = set()

	for row in cur.execute(f"SELECT {attr} FROM autompg"):
		result.add(row[0])
	
	return list(result)

#|----------------------------------------------------------------------------------------------------------------------|
#| IDFTable: Foreach pair of values, Calc  IDF(tu) / IDF(t) union IDF(u)											    |
#|----------------------------------------------------------------------------------------------------------------------|

def createIDFTables(sourcedb, outputdb):
	for attr in ["brand", "model", "type"]:
		createIDFTable(sourcedb, outputdb, attr)

def getIDFScore(attr, val1, val2, workload):
	if val1 == val2:
		return 1
	intersect = 0
	union = 0
	for query in workload:
		if attr in query.keys():
			if val1 in query[attr] or val2 in query[attr]:
				union += query["count"]
				if val1 in query[attr] and val2 in query[attr]:
					intersect += query["count"]
	if union > 0:
		return intersect / union
	else:
		return 0


def createIDFTable(sourcedb, outputdb, attr):
	cur = outputdb.cursor()
	cur.execute(f"""CREATE TABLE {attr}IDF (
		val1 text NOT NULL,
		val2 text NOT NULL,
		IDF real,
		PRIMARY KEY (val1, val2)
	)""")
	
	values = getAllUniqueVals(sourcedb, attr)
	with open("workload.json", "r") as file:
		workload = json.load(file)
		for val1 in values:
			for val2 in values:
				if val1 <= val2: #To save a bit of space we don't store the difference between ab and ba, the one that comes first alphabically goes first
					score = getIDFScore(attr, val1, val2, workload)
					cur.execute(f"INSERT INTO {attr}IDF VALUES ('{val1}', '{val2}', {score})")

	outputdb.commit()	

#|----------------------------------------------------------------------------------------------------------------------|
#| QF Table, for every attribute value calculate log(QF(alles)/QF(dit))													|
#|----------------------------------------------------------------------------------------------------------------------|

def createQFTables(sourcedb, outputdb):
	totalQF = sourcedb.cursor().execute("SELECT COUNT(*) FROM autompg").fetchone()[0]
	for attr in ["brand", "model", "type"]:
		createQFTable(sourcedb, outputdb, attr, totalQF)

def createQFTable(sourcedb, outputdb, attr, totalQF):
	sCur = sourcedb.cursor()
	oCur = outputdb.cursor()

	oCur.execute(f"""CREATE TABLE {attr}QF (
		val text NOT NULL,
		QFScore real,
		PRIMARY KEY (val)
	)""")

	values = getAllUniqueVals(sourcedb, attr)

	for val in values:
		valQf = sCur.execute(f"SELECT COUNT(*) FROM autompg WHERE {attr} = '{val}'").fetchone()[0]
		QFScore = math.log(totalQF/valQf, 10)
		oCur.execute(f"INSERT INTO {attr}QF VALUES ('{val}', {QFScore})")
	
	outputdb.commit()

#|----------------------------------------------------------------------------------------------------------------------|
#| Execute all the required code																					    |
#|----------------------------------------------------------------------------------------------------------------------|

if __name__ == "__main__": #only execute this code when this file is ran directly incase we want to import functions from here
	createWorkloadJson()

	metaDb = helperFunctions.createSQLITEDB("metaDatabase.db")
	mainDb = helperFunctions.openSQLITEDB("mainDatabase.db")

	createQFTables(mainDb, metaDb)

	createIDFTables(mainDb, metaDb)

	metaDb.close()
	mainDb.close()



"""
Stuff behind here is obsolite 

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
	workloadArray = []
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
def workLoadToDB3(catagorishDataPoint, workload):
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
test = fillDB3WorkloadData("brand", connection)



#print(transformCEQ("k = 9, model = ding, year = 1999, price = 50", "table"))

#createSQLITEDB("test.db")
#print(getSqliteInsertCode("test.db"))

"""