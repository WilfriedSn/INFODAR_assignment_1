#returns all lines from a file
def readFile(fileName):
    file = open(fileName, 'r')
    lines = file.readlines()
    file.close()
    return lines


#transforms workload to a usable 2d list
def transFormWorkload(fileName):
    lines = readFile(fileName)
    workload = [[]]
    for line in lines:
        #check if it is intended as code
        if line.__contains__(' times: '):
            #remove trailing spaces and \n, add semicolon make the second part runnable sqlitecode,
            #   split by " times: " 
            #   first index (amount of times) as a int
            #   second index (the Sqlite code) as a string
            line = line.strip()
            line += ';'
            line = line.split(' times: ')
            line[0] = int(line[0])
            workload.append(line)
    return workload


