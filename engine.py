import os
import sys
import copy
import sqlparse
import re
import csv

cartesianTable = []
tabledict = {}
tables = {}

class Table:
    def __init__(self, name, cols, data):
        self.name = name
        self.columns = cols
        self.data = data


def fullJoin():
    global tables
    global cartesianTable

    final_cols = []
    table_l = list(tables.keys())
    cartesianTable = copy.deepcopy(tables[table_l[0]].data)

    for i in range(1, len(table_l)):
        cartesianTable = [[*row1, *row2] for row1 in cartesianTable for row2 in tables[table_l[i]].data]

    # for row in cartesianTable:
    #     print((str(row)))


def extractMetadata():
    f = open("./files/metadata.txt", "r")
    lines = f.readlines()
    global tabledict

    tname,tcols = [],[]
    start, cstart = 0,0
    temp = []

    name = ""
    for l in lines:
        if l[-1:] == "\n":
            l = l[0:-1]
        if l == "<begin_table>":
            start = 1
            continue

        if l == "<end_table>":
            start = 0
            cstart = 0
            tcols.append(temp)

            tabledict[name] = temp
            name = ""
            temp = []
            continue

        if start and cstart:
            temp.append(str(l))

        if start and not cstart:
            tname.append(str(l))
            name = str(l)
            cstart = 1

    # print (tabledict)

# def distinctQuery(querypart, files):
#     dist_set = set()
#     maxl = 0
#     for i in range(len(querypart)):
#         if len(querypart[i] > maxl):
#             maxl = len(querypart[i])

#     for j in range(maxl):
#         temp = []
#         for i in range(len(querypart)):
#             temp.append(querypart[i])
#         dist_set.add(temp) #if not there already

def checkColumn(col):
    global tabledict
    k = list(tabledict.keys())
    temp = col.split(".")
    found = 0
    if (len(temp) > 1):
        tab = temp[0]
        column = temp[1]
        #check if col exists
        if column not in tabledict[str(tab)]:
            print ("Error: column " + str(column) + " not found in the given table.")
            sys.exit(0)
    else:
        column = temp[0]
        for x in k:
            if column in tabledict[x]:
                if not found:
                    tab = str(x)
                    found = 1
                else:
                    print ("Error: not specified which table column " + str(column) + " belongs to.")
                    sys.exit(0)


 
def whereQuery(query):
    #split on basis of and and or conditions
    condition = query[-1]
    condition = condition.split()[1:]
    print (condition)
    #split based on or
    
    pass

def selectQuery(querybits):
    if querybits[1] == "distinct":
        files = querybits[4]
    else:
        files = querybits[3]
    files = re.findall("[\'\w]+", files)
    # print (files)

    global tables
    for i in range(len(files)):
        lis = []
        fin = open(os.path.join("./files/", str(files[i]) + ".csv"), "r")
        data = fin.readlines()
        for dat in data:
            lis.append([int(u) for u in dat.strip().split(',')])

        tables[str(files[i])] = Table(str(files[i]), tabledict[str(files[i])], lis)

    #create big cartesian table
    fullJoin()

    #check for where conditions
    last = querybits[-1]
    temp = last.split()
    print (temp)
    if (temp[0] == "where"):
        whereQuery(querybits)

    # if query[1] == "distinct":
        #add operations on distincted col
        # cols = query[2]
        # distinctQuery(cols, fhandlers)


def processQuery(raw):
    queries = sqlparse.split(raw)
    for q in queries:
        if q[-1] != ';':
            print ("Syntax error: end SQL command with semi colon")
            return
        # q = q.lower()
        parsed = sqlparse.parse(q)[0].tokens
        # print (parsed)
        plist = sqlparse.sql.IdentifierList(parsed).get_identifiers()
        querybits = []
        for cmd in plist:
            querybits.append(str(cmd))
        # print (querybits[0])
        # print (querybits[1])
        # print (querybits[2])

        # print (querybits[3])
        # print (querybits[4])

        # print (querybits[5])
        # print (querybits[6])

        if querybits[0] == "select":
            selectQuery(querybits)
        else:
            print ("Query type is incorrect or not suppported.")
            continue


def main():
    if len(sys.argv) != 2:
        print ("Incorrect number of arguments passed")
        return
    query = sys.argv[1]
    extractMetadata()
    processQuery(query)


if ( __name__ == "__main__"):
    main()
