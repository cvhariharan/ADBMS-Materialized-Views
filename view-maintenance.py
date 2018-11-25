# Imports the Google Cloud client library
from google.cloud import bigquery
import os, sqlparse, json, re
from moz_sql_parser import parse
import mysql.connector

database = "sakila"

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="",
    database=database
)

print(mydb)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './View-Maintenance-52cc59f31237.json'

# Instantiates a client
bigquery_client = bigquery.Client()

# query = bigquery_client.query("SELECT * FROM `view-maintenance-216508.StackOverflow.important_answers_table` LIMIT 1000")
  
# results = query.result()

# for row in results:
#     print(row.questions_title+" - "+row.questions_body);
cursor = mydb.cursor()
query = input("Query: ")
oldQuery = query

#Remove excess whitespaces
query = re.sub(r' +', " ", query)
query = re.sub(r"`", "", query)

viewName = query.split()[2]
print("View: "+viewName)
tempQuery = query.split()
query = ""
for i in range(4, len(tempQuery)):
    query = query+" "+tempQuery[i]

def getColumns(cols, colType):
    if isinstance(cols, dict):
        # print(cols["value"])
        return [cols["value"]]
    elif isinstance(cols, list):
        t = []
        for i in range(0, len(cols)):
            if "name" in cols[i]:
                t.append(cols[i][colType])
            else:
                t.append(cols[i]["value"])
        # print(t)
        return t

parsed = parse(query)

print(parsed)

fromTable = parsed["from"]
if isinstance(fromTable, dict):
    fromTable = fromTable["value"]
viewColumns = getColumns(parsed["select"], "name")
tableColumns = getColumns(parsed["select"], "value")
mapping = {}

#Create view table
cursor.execute(oldQuery)

print(viewColumns)
print(tableColumns)
for i in range(len(viewColumns)):
    mapping[viewColumns[i]] = tableColumns[i]

print(mapping)

#Set up triggers
#Insert Trigger
insertTrigger = "create trigger trig_"+fromTable+"_ins after insert on "+fromTable+" for each row begin insert into "+viewName+" ("
for i in range(len(viewColumns)):
    if i != 0:
        insertTrigger = insertTrigger+","
    insertTrigger = insertTrigger+" "+viewColumns[i]
insertTrigger = insertTrigger + ") values ("
for i in range(len(tableColumns)):
    if i != 0:
        insertTrigger = insertTrigger+","
    insertTrigger = insertTrigger+" NEW."+tableColumns[i]
insertTrigger = insertTrigger + "); end;"
print("TRIGGER: "+insertTrigger)

#Delete trigger
# table1 = ''
# table1Alias = ''
# table2 = ''
# table2Alias = ''
# tableDict = {}
# viewColumns = []
# print(query)
# parsed = parse(query)
# print(parsed)
# mapping = {}
# # print(len(parsed["from"]))
# 

# if isinstance(parsed["from"], list):
#     fromArr = parsed["from"]
#     viewColumns = getColumns(parsed["select"])
#     if isinstance(fromArr[0], dict):
#         table1 = fromArr[0]["value"]
#         table1Alias = fromArr[0]["name"]
#     else:
#         table1 = fromArr[0]
#         table1Alias = table1
    
#     if isinstance(fromArr[1], dict):
#         function = list(fromArr[1].keys())[0]
#         functionVal = fromArr[1][function]
#         if isinstance(functionVal, dict):
#             table2 = fromArr[1][function]["value"]
#             table2Alias = fromArr[1][function]["name"]
#         else:
#             table2 = fromArr[1][function]
#             table2Alias = table2

#     tableDict[table1Alias] = table1
#     tableDict[table2Alias] = table1
#     for col in viewColumns:
#         tAlias = col.split('.')[0]
#         tColumn = col.split('.')[1]

#         # tableDict[table1Alias] = table1
#         # function = list(fromArr[1].keys())[0]
#         # table2 = fromArr[1][function]["value"]
#         # table2Alias = fromArr[1][function]["name"]
#         # tableDict[table2Alias] = table2
    
    
#     # if "as" in query:
#     #     fromArr = parsed["from"]
#     #     viewColumns = getColumns(parsed["select"])
#     #     table1 = fromArr[0]["value"]
#     #     table1Alias = fromArr[0]["name"]
#     #     tableDict[table1Alias] = table1
#     #     function = list(fromArr[1].keys())[0]
#     #     # print(function)
#     #     table2 = fromArr[1][function]["value"]
#     #     table2Alias = fromArr[1][function]["name"]
#     #     tableDict[table2Alias] = table2
       
#     # else:
#     #     fromArr = parsed["from"]
#     #     viewColumns = getColumns(parsed["select"])
#     #     table1 = fromArr[0]
#     #     table1Alias = table1
#     #     tableDict[table1Alias] = table1
#     #     function = list(fromArr[1].keys())[0]
#     #     # print(function)
#     #     table2 = fromArr[1][function]
#     #     table2Alias = table2
#     #     tableDict[table2Alias] = table2
#     print(table1+" "+table1Alias+" "+function+" "+table2+" "+table2Alias)
#     print("Columns: "+str(viewColumns))
#     print(tableDict)