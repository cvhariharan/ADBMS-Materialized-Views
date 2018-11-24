# Imports the Google Cloud client library
from google.cloud import bigquery
import os, sqlparse, json, re
from moz_sql_parser import parse

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './View-Maintenance-52cc59f31237.json'

# Instantiates a client
bigquery_client = bigquery.Client()

# query = bigquery_client.query("SELECT * FROM `view-maintenance-216508.StackOverflow.important_answers_table` LIMIT 1000")
  
# results = query.result()

# for row in results:
#     print(row.questions_title+" - "+row.questions_body);

query = input("Query: ")

#Remove excess whitespaces
query = re.sub(r' +', " ", query)

viewName = query.split()[2]
print("View: "+viewName)
tempQuery = query.split()
query = ""
for i in range(4, len(tempQuery)):
    query = query+" "+tempQuery[i]

table1 = ''
table1Alias = ''
table2 = ''
table2Alias = ''
tableDict = {}
viewColumns = []
print(query)
parsed = parse(query)
print(parsed)
# print(len(parsed["from"]))
def getColumns(cols):
    if isinstance(cols, dict):
        # print(cols["value"])
        return [cols["value"]]
    elif isinstance(cols, list):
        t = []
        for i in range(0, len(cols)):
            t.append(cols[i]["value"])
        # print(t)
        return t

if isinstance(parsed["from"], list):
    fromArr = parsed["from"]
    viewColumns = getColumns(parsed["select"])
    if isinstance(fromArr[0], dict):
        table1 = fromArr[0]["value"]
        table1Alias = fromArr[0]["name"]
    else:
        table1 = fromArr[0]
        table1Alias = table1
    
    if isinstance(fromArr[1], dict):
        function = list(fromArr[1].keys())[0]
        functionVal = fromArr[1][function]
        if isinstance(functionVal, dict):
            table2 = fromArr[1][function]["value"]
            table2Alias = fromArr[1][function]["name"]
        else:
            table2 = fromArr[1][function]
            table2Alias = table2
    tableDict[table1Alias] = table1
    tableDict[table2Alias] = table1
        # tableDict[table1Alias] = table1
        # function = list(fromArr[1].keys())[0]
        # table2 = fromArr[1][function]["value"]
        # table2Alias = fromArr[1][function]["name"]
        # tableDict[table2Alias] = table2
    
    
    # if "as" in query:
    #     fromArr = parsed["from"]
    #     viewColumns = getColumns(parsed["select"])
    #     table1 = fromArr[0]["value"]
    #     table1Alias = fromArr[0]["name"]
    #     tableDict[table1Alias] = table1
    #     function = list(fromArr[1].keys())[0]
    #     # print(function)
    #     table2 = fromArr[1][function]["value"]
    #     table2Alias = fromArr[1][function]["name"]
    #     tableDict[table2Alias] = table2
       
    # else:
    #     fromArr = parsed["from"]
    #     viewColumns = getColumns(parsed["select"])
    #     table1 = fromArr[0]
    #     table1Alias = table1
    #     tableDict[table1Alias] = table1
    #     function = list(fromArr[1].keys())[0]
    #     # print(function)
    #     table2 = fromArr[1][function]
    #     table2Alias = table2
    #     tableDict[table2Alias] = table2
    print(table1+" "+table1Alias+" "+function+" "+table2+" "+table2Alias)
    print("Columns: "+str(viewColumns))
    print(tableDict)