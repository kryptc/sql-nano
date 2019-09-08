import os
import sys
import copy
import sqlparse
import re
import csv
from functools import reduce

cartesianTable = []             #joined table of cartesian product of all rows of tables being used in the query
tabledict = {}                  #dictionary of table name to list of columns it has
tables = {}                     #dictionary of table name to Table object with name, cols and data in cols
final_cols = []                 #list of all columns in order of cartesian product for all tables


class Table:
    def __init__(self, name, cols, data):
        self.name = name
        self.columns = cols
        self.data = data


def fullJoin():
    global tables
    global cartesianTable
    global final_cols

    table_l = list(tables.keys())
    for i in range(len(table_l)):
        tab = str(table_l[i])
        for j in range(len(tabledict[tab])):
            colname = tab + "." + str(tabledict[tab][j])
            final_cols.append(colname)
    # print (final_cols) 
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


def checkColumn(col):
    global tables
    k = list(tables.keys())
    # print (k)
    temp = col.split(".")
    # print (temp)
    found = 0
    if (len(temp) > 1):
        tab = temp[0]
        column = temp[1]
        #check if table name exists
        if tab not in k:
            print ("Error: The table named " + str(tab) + " doesn't exist.")
            sys.exit(0)
        #check if col exists
        if column not in tables[str(tab)].columns:
            print ("Error: Column " + str(column) + " not found in the given table.")
            sys.exit(0)
    else:
        column = temp[0]
        for x in k:
            if column in tables[x].columns:
                if not found:
                    tab = str(x)
                    found = 1
                else:
                    print ("Error: Not specified which table column " + str(column) + " belongs to.")
                    sys.exit(0)
            if not found:
                print ("Error: Column " + str(column) + " not found in the given table.")
                sys.exit(0)


def findColNum(arg):
    global final_cols
    global tables

    k = list(tables.keys())
    temp = arg.split(".")
    if (len(temp) > 1):
        tab = temp[0]
        column = temp[1]
    else:
        column = temp[0]
        for x in k:
            if column in tables[x].columns:
                tab = str(x)
    # fullTable = tables[tab].data
    reqCol = []
    arg = tab + "." + column
    for i in range(len(final_cols)):
        if final_cols[i] == str(arg):
            colInd = i
            return colInd

def evaluate(arg1, ind1, operator, arg2, ind2, i):
    global cartesianTable
    if ind1 == -1:      #arg1 is number
        if ind2 == -1:
            pass        #arg2 is number
        else:           #arg2 is a column
            arg2 = cartesianTable[i][ind2]
    else:               #arg1 is column
        arg1 = cartesianTable[i][ind1]
        if ind2 == -1:
            pass
        else:
            arg2 = cartesianTable[i][ind2]
    if operator == "=":
        return arg1 == arg2
    elif operator == ">":
        return arg1 > arg2
    elif operator == ">=":
        return arg1 >= arg2
    elif operator == "<":
        return arg1 < arg2
    elif operator == "<=":
        return arg1 <= arg2

            
def preEvaluate(cond):
    global cartesianTable

    operator = cond[1]

    ind1,ind2 = -1,-1
    arg = cond[0]
    no = 0
    try:
        arg = int(arg)
    except:
        checkColumn(arg)
        ind1 = findColNum(arg)

    arg2 = cond[2]
    try:
        arg2 = int(arg2)
    except:
        checkColumn(arg2)
        ind2 = findColNum(arg2)

    finalData = []
    finalRows = []
    for i in range(len(cartesianTable)):
        bool_var = evaluate(arg, ind1, operator, arg2, ind2, i)
        # print (bool_var)
        if bool_var:
            finalData.append(cartesianTable[i])
            finalRows.append(i)
    # print (finalRows)
    return finalRows


def checkJoin(cond):
    redundant = []
    operator = cond[1]
    ind1,ind2 = -1,-1
    arg = cond[0]
    try:
        arg = int(arg)
    except:
        ind1 = findColNum(arg)

    arg2 = cond[2]
    try:
        arg2 = int(arg2)
    except:
        ind2 = findColNum(arg2)
        if operator == "=":
            redundant.append(ind2)
            # print (ind2)
    return redundant


def parseWhere(cond):
    part = cond
    temp = []

    for k in range(len(part)):
        minitemp = ""
        for j in range(len(part[k])):
            if part[k][j] in ["=",">",">=","<","<="]:
                if (minitemp != ""):        
                    temp.append(minitemp)
                temp.append(part[k][j])
                minitemp = ""
            else:
                minitemp += part[k][j]
        if (minitemp != ""):        
            temp.append(minitemp)
    return temp

 
def whereQuery(query):
    #split on basis of and and or conditions
    condition = query[-1]
    condition = condition.split()[1:]

    beg = 0
    cond = []
    # OR = 0
    # AND = 0

    #only for purposes of constraints of the given problem statement
    #otherwise outer loop contains all OR statements
    #inner loop contains all AND statements
    #last if condition checks if it is the end of the string, and adds the last condition command
    for c in range(len(condition)):
        if condition[c] == "or" or condition[c] == "OR":
            # OR = 1
            x = slice(beg,c)
            beg = c + 1
            cond.append(condition[x])
            x = slice(c+1, len(condition))
            cond.append(condition[x])

            for i in range(len(cond)):
                cond[i] = parseWhere(cond[i])

            res1 = set(preEvaluate(cond[0]))
            res2 = set(preEvaluate(cond[1]))
            res = res1 | res2

            red1 = set(checkJoin(cond[0]))
            red2 = set(checkJoin(cond[1]))
            red = red1 | red2
            break


        elif condition[c] == "and" or condition[c] == "AND":
            # AND = 1
            x = slice(beg,c)
            cond.append(condition[x])
            x = slice(c+1, len(condition))
            cond.append(condition[x])

            for i in range(len(cond)):
                cond[i] = parseWhere(cond[i])
            res1 = set(preEvaluate(cond[0]))
            res2 = set(preEvaluate(cond[1]))
            res = res1 & res2

            red1 = set(checkJoin(cond[0]))
            red2 = set(checkJoin(cond[1]))
            red = red1 | red2
            break


        elif c == len(condition)-1:
            x = slice(beg,c+1)
            cond.append(condition[x])

            for i in range(len(cond)):
                cond[i] = parseWhere(cond[i])
            res = set(preEvaluate(cond[0]))
            red = checkJoin(cond[0])
            break
    return res,red

def checkAggregate(query_list):
    #aggregate only on single column
    temp = -1
    agg = -1
    for i in range(len(query_list)):
        x = query_list[i]
        temp = x.find("(")
        if temp > -1:
            agg = x[0:temp]
            agg = agg.lower()
            if agg == "max":
                agg = 1
            elif agg == "min":
                agg = 2
            elif agg == "avg":
                agg = 3
            elif agg == "sum":
                agg = 4
            else:
                agg = -1
            query_list[i] = x[temp+1:-1]
            
    return agg, query_list


def projectColumns(query_cols, table_cols, distinct, redundant):
    global cartesianTable
    global final_cols
    global tables
    global tabledict

    col_ind = []
    col_name = []

    query_cols = query_cols.replace(",", " ")
    query_cols = query_cols.split()
    # print (query_cols)

    agg, query_cols = checkAggregate(query_cols)

    for i in range(len(query_cols)):
        checkColumn(query_cols[i])

        temp = findColNum(query_cols[i])
        if temp in redundant:
            # print (redundant, temp)
            continue
        col_ind.append(temp)
        # print (col_ind)
        col_name.append(final_cols[col_ind[-1]])

    if len(query_cols) > 1 or distinct:
        disp_set = set()

        for i in range(len(cartesianTable)):
            row = ""
            if i in table_cols:
                for j in range(len(col_ind)):
                    if j == len(col_ind) - 1:
                        row += str(cartesianTable[i][col_ind[j]])
                    else:
                        row += str(cartesianTable[i][col_ind[j]]) + ", "
            if row != "":
                disp_set.add(row)
    else:
        disp_set = []
        k = list(tables.keys())
        tab = col_name[0].split(".")[0]
        col = col_name[0].split(".")[1]

        data = tables[tab].data
        ind = 0
        for i in range(len(tabledict[tab])):
            if tabledict[tab][i] == str(col):
                ind = i

        for i in range(len(data)):
            disp_set.append(str(data[i][ind]))

    if agg == -1:
        print (str(col_name)[1:-1])
        for i in disp_set:
            print (i)

    elif agg == 1:
        print ("max(" + str(col_name)[1:-1] + ")")
        print (max(disp_set))
    elif agg == 2:
        print ("min(" + str(col_name)[1:-1] + ")")
        print (min(disp_set))
    elif agg == 3:
        print ("avg(" + str(col_name)[1:-1] + ")")
        print (reduce(lambda x,y:float(x) + float(y), disp_set)/float(len(disp_set)))
    elif agg == 4:
        print ("sum(" + str(col_name)[1:-1] + ")")
        print (reduce(lambda x,y:float(x) + float(y), disp_set))
        

def selectQuery(querybits):
    global tabledict

    if querybits[1] in ["distinct", "DISTINCT"]:
        files = querybits[4]
        display = querybits[2]
        dist = 1
    else:
        files = querybits[3]
        display = querybits[1]
        dist = 0

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

    global cartesianTable
    res = [x for x in range(len(cartesianTable))]
    redundant = set()
    #check for where conditions
    last = querybits[-1]
    temp = last.split()
    if (temp[0] in  ["where", "WHERE"]):
        res, redundant = whereQuery(querybits)

    #project columns
    if display == "*":
        display = ""
        k = list(tabledict.keys())
        for x in k:
            if x in files:
                for j in range(len(tabledict[x])):
                    display += (str(x) + "." + str(tabledict[x][j]) + ",")

    # print (display)
    projectColumns(display, res, dist, redundant)


def processQuery(raw):
    queries = sqlparse.split(raw)
    for q in queries:
        if q[-1] != ';':
            print ("Syntax error: end SQL command with semi colon")
            return
        q = q[:-1]
        # q = q.lower()
        parsed = sqlparse.parse(q)[0].tokens
        # print (parsed)
        plist = sqlparse.sql.IdentifierList(parsed).get_identifiers()
        querybits = []
        for cmd in plist:
            querybits.append(str(cmd))

        if querybits[0] in ["select", "SELECT"]:
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

