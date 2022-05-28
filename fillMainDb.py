from multiprocessing import connection
import sqlite3
import os.path
from unittest import result # for createSQLITEDB
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
