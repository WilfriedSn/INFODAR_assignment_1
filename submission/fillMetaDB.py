import json
import math
import helperFunctions
from statistics import stdev

#Made by:
#Olivia Lohmann: (8742779)
#Wifried Snijders(8657078)

#commented due to being used to generate metadb.txt and metaload.txt not needed for users
def executeQuery(cur, query):
	#filename = None
	#if query[0] == "C":
	#	filename = "metadb.txt"
	#if query[0] == "I":
	#	filename = "metaload.txt"
	#if filename:
	#	with open(filename, "a") as file:
	#		file.write(query + "\n")
	return cur.execute(query)

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

#|--------------------------------------------------------------------|
#| Create a table with info on the max and min values of each atribute|
#|--------------------------------------------------------------------|



def createMinMaxTable(mainDBConnection, metaDBConnection):
	helperFunctions.getResultOfQuery(metaDBConnection, "CREATE TABLE minMax (attr text NOT NULL, min real, max real, diff read, PRIMARY KEY (attr))")
	for attr in ['mpg', 'cylinders', 'displacement', 'horsepower', 'weight', 'acceleration', 'model_year', 'origin']:
		minVal = helperFunctions.getResultOfQuery(mainDBConnection, f"SELECT MIN({attr}) FROM autompg;")[0][0]
		maxVal = helperFunctions.getResultOfQuery(mainDBConnection, f"SELECT MAX({attr}) FROM autompg;")[0][0]
		helperFunctions.insertDataIntoQuery(metaDBConnection, f"INSERT INTO minMax VALUES ('{attr}', {minVal}, {maxVal}, {maxVal - minVal});")




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

	for row in executeQuery(cur, f"SELECT {attr} FROM autompg"):
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
	executeQuery(cur, f"""CREATE TABLE {attr}IDF (
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
				score = getIDFScore(attr, val1, val2, workload)
				executeQuery(cur, f"INSERT INTO {attr}IDF VALUES ('{val1}', '{val2}', {score})")

	outputdb.commit()	

#|----------------------------------------------------------------------------------------------------------------------|
#| QF Table, for every attribute value calculate log(QF(alles)/QF(dit))													|
#|----------------------------------------------------------------------------------------------------------------------|

def createQFTables(sourcedb, outputdb):
	totalQF = executeQuery(sourcedb.cursor(), "SELECT COUNT(*) FROM autompg").fetchone()[0]
	for attr in ["brand", "model", "type"]:
		createQFTable(sourcedb, outputdb, attr, totalQF)

def createQFTable(sourcedb, outputdb, attr, totalQF):
	sCur = sourcedb.cursor()
	oCur = outputdb.cursor()

	executeQuery(oCur, f"""CREATE TABLE {attr}QF (
		val text NOT NULL,
		QFScore real,
		PRIMARY KEY (val)
	)""")

	values = getAllUniqueVals(sourcedb, attr)

	for val in values:
		valQf = executeQuery(sCur, f"SELECT COUNT(*) FROM autompg WHERE {attr} = '{val}'").fetchone()[0]
		QFScore = math.log(totalQF/valQf, 10)
		executeQuery(oCur, f"INSERT INTO {attr}QF VALUES ('{val}', {QFScore})")
	
	outputdb.commit()

#|----------------------------------------------------------------------------------------------------------------------|
#| For numerical attrs, calculate stdev																				    |
#|----------------------------------------------------------------------------------------------------------------------|

def createStDevTable(sourcedb, outputdb):
	sCur = sourcedb.cursor()
	oCur = outputdb.cursor()

	executeQuery(oCur, f"""CREATE TABLE stDev (
		attr text NOT NULL,
		stDev real,
		PRIMARY KEY (attr)
	)""")

	for attr in ["mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model_year", "origin"]:
		scores = list(map(lambda x : x[0], executeQuery(sCur, f"SELECT {attr} FROM autompg").fetchall()))
		executeQuery(oCur, f"INSERT INTO stDev VALUES ('{attr}', {stdev(scores)})")

	outputdb.commit()


#|----------------------------------------------------------------------------------------------------------------------|
#| Execute all the required code																					    |
#|----------------------------------------------------------------------------------------------------------------------|

if __name__ == "__main__": #only execute this code when this file is ran directly incase we want to import functions from here
	with open("metadb.txt", "w") as file:
		file.write("")

	with open("metaload.txt", "w") as file:
		file.write("")
	
	createWorkloadJson()

	metaDb = helperFunctions.createSQLITEDB("metaDatabase.db")
	mainDb = helperFunctions.openSQLITEDB("mainDatabase.db")

	createQFTables(mainDb, metaDb)

	createIDFTables(mainDb, metaDb)

	createMinMaxTable(mainDb,metaDb)

	createStDevTable(mainDb,metaDb)

	metaDb.close()
	mainDb.close()
	print("DBS created")


