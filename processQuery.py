import math
import helperFunctions
import functools
import pprint as pp


categorischeData = ['brand','model','type']
allPossibleAttributes = ['mpg','cylinders','displacement','horsepower','weight','acceleration','model_year','origin','brand','model','type']
metaDBCon = helperFunctions.openSQLITEDB('metaDatabase.db')
mainDBCon = helperFunctions.openSQLITEDB('mainDatabase.db')

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

#splits dependeties on = and transports the K
def dependentiesToTopK(dependenties,k):
	result = []
	for dep in dependenties:
		dep = dep.split("=")
		result.append([dep[0],1,dep[1]])
	return [result,k]


def calcWeightedScore(scores, weights):
	return sum([x * y for (x, y) in zip(scores, weights)])


@functools.cache
def calculateIDFNumerical(queryVal, attr, h, n):
	cur = mainDBCon.cursor()
	values = list(map(lambda x : x[0], cur.execute(f"SELECT {attr} FROM autompg").fetchall()))
	divider = 0
	for v in values:
		divider += math.pow(math.e, -0.5 * math.pow((v-queryVal)/h, 2))
	return math.log(n/divider, 10)

@functools.cache
def getNandH(attr):
	metaCur = metaDBCon.cursor()
	sourceCur = mainDBCon.cursor()
	n = sourceCur.execute("SELECT COUNT(*) FROM autompg").fetchone()[0]
	h = metaCur.execute(f"SELECT stDev FROM stDev WHERE attr = '{attr}'").fetchone()[0]
	return n, h


def calculateQFNumerical(targetVal, queryVal, attr):
	queryVal = int(queryVal)
	n, h = getNandH(attr)
	result = math.pow(math.e, -0.5 * math.pow((targetVal-queryVal)/h, 2)) * calculateIDFNumerical(queryVal, attr, h, n)
	return result


#attr[0] = brand
#attr[1] = how importand this atribute is not used but good for future data experts (it is always 1 in out program)
#attr[2] = wanted value ('ford')
def topK(attrNeededValuess,k):
	x = len(attrNeededValuess)*k*1.5 + 5
	topK = []
	categorischeIDFDictionairy = {}
	attrList = []
	QFWights = []
	for attr in attrNeededValuess:
		if attr[0] in categorischeData:
			QFWights.append(helperFunctions.getResultOfQuery(metaDBCon, f"SELECT QFScore FROM {attr[0]}QF WHERE val = {attr[2]}")[0][0])
			temp = (getTopXCatagoricalData(attr[0] ,x ,attr[2]))
			topK.append(temp[0])
			for cata in temp[1]:
				categorischeIDFDictionairy[cata[0]] = cata[1]
		else:
			QFWights.append(2)
			topK.append(getTopXNumericalData(attr[0] ,x ,attr[2]))
		attrList.append(attr[0])

	#setup for topK
	tempResult = []
	buffer = []
	maxValueHelper = []
	#fill maxvalueHelper with the possible modified values of the importance
	for attr in attrNeededValuess:
		maxValueHelper.append(attr[1])
	maxValue = calcWeightedScore(maxValueHelper, QFWights)

	print(min(map(lambda x : len(x), topK)))

	#get the topK
	for valIndex in range(min(map(lambda x : len(x), topK))):
		for attrID in range(len(attrNeededValuess)):
			if (topK[attrID][valIndex][0] not in list(map(lambda x:x[0], tempResult)) and topK[attrID][valIndex][0] not in list(map(lambda x:x[0], buffer))):
				carValues = helperFunctions.getResultOfQuery(mainDBCon, f"SELECT {','.join(attrList)} FROM autompg WHERE id = {topK[attrID][valIndex][0]}") 
				#calculate the weighted carscore of car and the updateable maxvalue
				carScore, newTopKMaxVal = getIdValue(carValues,attrNeededValuess,categorischeIDFDictionairy, attrID, QFWights)
				carId = topK[attrID][valIndex][0]
				buffer.append([carId,carScore])
				maxValueHelper[attrID] = newTopKMaxVal
		maxValue = calcWeightedScore(maxValueHelper, QFWights)
		for bufferItem in buffer:
			if (bufferItem[1] >= maxValue):
				tempResult.append(bufferItem)
				buffer.remove(bufferItem)
	#break when topK is full
		if (len(tempResult)>k):
			break
	
	tempResult.sort(key=lambda x:x[1],reverse=True) 
	
	return tempResult[:k]
			
#returns the score of the id and returns the new local max value
def getIdValue(values, attrNeededValuess, categorischeIDFDictionairy, importantValueIndex, weights):
	result = 0
	values[0] = list(values[0])
	for i in range(len(attrNeededValuess)):
		if i == importantValueIndex:
			if attrNeededValuess[i][0] not in categorischeData:
				importantValue = calculateQFNumerical(values[0][i], attrNeededValuess[i][2], attrNeededValuess[i][0])
			else:
				importantValue = categorischeIDFDictionairy[values[0][i]]
		if attrNeededValuess[i][0] not in categorischeData:
			values[0][i] = weights[i] * calculateQFNumerical(values[0][i], attrNeededValuess[i][2], attrNeededValuess[i][0])
		else:
			values[0][i] = weights[i] * categorischeIDFDictionairy[values[0][i]]
		result += values[0][i]*attrNeededValuess[i][1]

	return [result,importantValue]

#gets the top X*2 numerical values from the database
def getTopXNumericalData(atribute, x, expectedValue):
	expectedValue = int(expectedValue)
	toHigherValues = helperFunctions.getResultOfQuery(mainDBCon, f"SELECT id, {atribute} FROM autompg WHERE {atribute} >= {expectedValue} order by {atribute};")
	toLowerValues = helperFunctions.getResultOfQuery(mainDBCon, f"SELECT id, {atribute} FROM autompg WHERE {atribute} < {expectedValue} order by {atribute} ;")
	result = []
	j = 0
	k = 0
	if toHigherValues == []:
		return toLowerValues
	elif toLowerValues == []:
		return toHigherValues
	result = []
	j = 0
	k = 0
	#combining the 2 sorted lists into one sorted list (sorted by distance from expected value)
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
		

#gets the top X CatagoricalDataValues from the database
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


#check for human errors
def checkIfValidQuery(userInput):
	if userInput[1] <=0:
		userInput[1] = 10
		print("Invalid number of results, defaulting to 10")
	for i in range(len(userInput[0])):
		if userInput[0][i][0] not in allPossibleAttributes:
			print(f"Invalid attribute: {userInput[0][i][0]}")
			userInput.remove(userInput[0][i])
	if len(userInput[0]) ==0:
		print("Invalid query, no attributes given")
		return False

	return userInput


#display a beautiful table
def displayResult(result):
	maxscore = len(userInput[0])
	seperationString = "    |"
	header = f"ID{seperationString+seperationString.join(allPossibleAttributes)+seperationString}"
	header = header.replace("|type", "            |type       ")
	print (header)
	print ('_'*len(header))
	headerCount = header.split("|")
	for i in range(len(result)):
		resultQuery = helperFunctions.getResultOfQuery(mainDBCon, f"SELECT * FROM autompg WHERE id = {result[i][0]};")
		resultString = list(resultQuery[0])
		resultString = str(resultString)
		resultString = resultString.split(",")
		totalResultString = ""
		if result[i][1] == maxscore:
			resultString[0] = "*" + resultString[0]
		for j in range(len(resultString)):
			resultString[j] = resultString[j].replace("[", "")
			resultString[j] = resultString[j].replace("]", "")
			resultString[j] = resultString[j].replace("'", "")
			resultString[j] = resultString[j].replace(" ", "")
			totalResultString += resultString[j].ljust(len(headerCount[j]))+ "|"
		print(totalResultString)

if __name__ == "__main__": #only execute this code when this file is ran directly incase we want to import functions from here
	#userInput = input()
	userInput = "k = 10, horsepower = 100, brand = 'ford'"
	userInput = transformCEQ(userInput)
	userInput = checkIfValidQuery(dependentiesToTopK(userInput[0], userInput[1]))
	if userInput != False:
		result = topK(userInput[0], userInput[1])
		displayResult(result)