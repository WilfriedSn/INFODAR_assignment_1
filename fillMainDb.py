import sqlite3
import helperFunctions

with open("autompg.txt", "r") as file:
	
	db = helperFunctions.createSQLITEDB("mainDatabase.db")
	cur = db.cursor()

	lines = file.read().split(";")
	lines = list(map((lambda x : x.replace("\n", "")), lines))

	for line in lines:
		cur.execute(line)
	db.commit()
	db.close()
