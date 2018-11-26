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

selectQuery = query
print("Create:  "+selectQuery)

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

# print(parsed)


def simpleQuery(parsed):
    fromTable = parsed["from"]
    if isinstance(fromTable, dict):
        fromTable = fromTable["value"]
    viewColumns = getColumns(parsed["select"], "name")
    tableColumns = getColumns(parsed["select"], "value")
    mapping = {}

    #Create view table
    # cursor.execute(oldQuery)

    # print(viewColumns)
    # print(tableColumns)
    for i in range(len(viewColumns)):
        mapping[viewColumns[i]] = tableColumns[i]

    print("Mapping from MV -> Table: "+ json.dumps(mapping))

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
    print("INSERT TRIGGER: "+insertTrigger)

    # cursor.execute(insertTrigger)

    #Delete trigger
    deleteTrigger = "create trigger trig_"+fromTable+"_del after delete on "+fromTable+" for each row begin delete from "+viewName+" where "+viewColumns[0]+" = OLD."+tableColumns[0]+"; end;"
    print("DELETE TRIGGER: "+deleteTrigger)

    # cursor.execute(deleteTrigger)

#Update trigger
    updateTrigger = "create trigger trig_"+fromTable+"_upd after update on "+fromTable+" for each row begin update "+viewName+" set "
    for i in range(len(viewColumns)):
        if i != 0:
            updateTrigger = updateTrigger+","
        updateTrigger = updateTrigger+" "+viewColumns[i]+" = NEW."+tableColumns[i]
    updateTrigger = updateTrigger + "; end;"
# for i in range(len(tableColumns)):
#     if i != 0:
#         updateTrigger = updateTrigger+","
#     updateTrigger = updateTrigger+" NEW."+tableColumns[i]
# updateTrigger = updateTrigger + "); end;"
    print("UPDATE TRIGGER: "+updateTrigger)

# cursor.execute(updateTrigger)


def complexQuery(parsed):
    fromArr = parsed["from"]
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

    #Create stored procedure
    sqlProcedure = "create procedure clean"+viewName+"() begin drop table "+viewName+"; create table "+viewName+" as "+selectQuery+"; end;"
    print("SQL clean up procedure: "+sqlProcedure)

    #Setup triggers
    insertTriggerTable1 = "create trigger trigq_"+table1+"_ins after insert on "+table1+" for each row begin call clean"+viewName+"(); end;"
    insertTriggerTable2 = "create trigger trigq_"+table2+"_ins after insert on "+table2+" for each row begin call clean"+viewName+"(); end;"

    updateTriggerTable1 = "create trigger trigq_"+table1+"_upd after update on "+table1+" for each row begin call clean"+viewName+"(); end;"
    updateTriggerTable2 = "create trigger trigq_"+table2+"_upd after update on "+table2+" for each row begin call clean"+viewName+"(); end;"
    
    deleteTriggerTable1 = "create trigger trigq_"+table1+"_del after delete on "+table1+" for each row begin call clean"+viewName+"(); end;"
    deleteTriggerTable2 = "create trigger trigq_"+table2+"_del after delete on "+table2+" for each row begin call clean"+viewName+"(); end;"
    # print(table1+" "+table1Alias+" "+table2+" "+table2Alias)
    print("Insert triggers: "+insertTriggerTable1);
    print("Insert triggers: "+insertTriggerTable2);

    print("Update triggers: "+updateTriggerTable1);
    print("Update triggers: "+updateTriggerTable2);

    print("Delete triggers: "+deleteTriggerTable1);
    print("Delete triggers: "+deleteTriggerTable2);
fromTable = parsed["from"]
if isinstance(fromTable, list):
    #Complex query
    complexQuery(parsed)
else:
    simpleQuery(parsed)

# for i in range(len(viewColumns)):
#     if i != 0:
#         deleteTrigger = deleteTrigger+","
#     deleteTrigger = deleteTrigger+" "+viewColumns[i]
# deleteTrigger = deleteTrigger + ") values ("
# for i in range(len(tableColumns)):
#     if i != 0:
#         deleteTrigger = deleteTrigger+","
#     deleteTrigger = deleteTrigger+" NEW."+tableColumns[i]
# deleteTrigger = deleteTrigger + "); end;"

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