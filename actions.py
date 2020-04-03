from typing import Any, Text, Dict, List
import pymysql
from rasa_sdk import Action
from rasa_sdk.events import SlotSet
import mysql.connector
from mysql.connector import Error
from rasa_sdk.forms import FormAction
from rasa_sdk import Tracker
from rasa_sdk.executor import CollectingDispatcher
from sqlalchemy import create_engine 
from sqlalchemy.engine import reflection
import yaml
import shutil
# import spacy 
# nlp = spacy.load('en_core_web_md') 
import os.path
from os import path
import requests
import sys
import nltk 
from nltk.tokenize import word_tokenize 
from nltk.corpus import wordnet 
from random import randint
# import json
global engine

class ActinConnectDb(Action):
    global L

    global engine
    global inspector
    def name(self):
        return "action_connect_db"   
    

    def run(self, dispatcher, tracker, domain):
        dbhost=tracker.get_slot('dbhost')
        dbdriver=tracker.get_slot('dbdriver')
        dbdialect=tracker.get_slot('dbdialect')
        dbusername=tracker.get_slot('dbusername')
        dbpassword=tracker.get_slot('dbpassword')
        dbname=tracker.get_slot('dbname')
        try:
            entities=""
            engine = create_engine(str(dbdriver)+'+'+str(dbdialect)+'://'+str(dbusername)+':'+str(dbpassword)+'@'+str(dbhost)+'/'+str(dbname))
            inspector = reflection.Inspector.from_engine(engine)
            tables_names=(inspector.get_table_names())
            column_names=[]
            for table in tables_names:  
                cols = inspector.get_columns(table)
                for col in cols:                    
                    column_names.append(col['name'])

            path1="data/lookup"
            if path.exists(path1) == True:
                try:
                    shutil.rmtree(path1)
                except Error as er:
                    path1.rmdir()
                os.makedirs(path1)
            else:
                os.makedirs(path1)

            domain='domain.yml'
            stream = open(domain, 'r')
            data = yaml.load(stream)
            
            for table in tables_names:  
                path2=path1+"/"+table
                os.makedirs(path2)
                tb=path2+"/"+table+"s.txt"
                t= open(tb,'a+')
                t.write(table)
                t.close()
                hello=path2+"/columns"
                os.makedirs(hello)
                cols = inspector.get_columns(table)
                for col in cols:                    
                    # data['entities'].append(col['name'])
                    # data['slots'][col['name']]={'type':'unfeaturized'}
                    with open('domain.yml', 'w') as fp:
                        fp.write(yaml.dump(data))
                    with engine.connect() as con:
                        rs = con.execute('SELECT {0} FROM {1}'.format(col['name'], table))
                        resultset = [dict(row) for row in rs]
                    
                    yo=hello+"/"+col['name']
                    os.makedirs(yo)
                    fl=yo+"/"+col['name']                    
                    cl= fl+"synonyms.txt"       
                    clmn=open(cl,'a')   
                    clmn.write(col['name'])
                    clmn.close()
                                
                    filename=fl+".txt"                   
                    with open(filename,'a+') as file:
                        for dic in resultset:   
                            b=(str)(dic[col['name']])+"\n"
                            file.write(b) 
                    file.close() 
                    writer = open('data/nlu.md','a+')
                    r="\t- lookup/"+table+"/columns/"+col['name']+"/"+col['name']+".txt"
                    writer.write("\n## synonym:"+col['name']+"\n\t"+r)  
                        
            def  occurences(searched_word): 
                L=[]
                with open("data/nlu.md",newline='') as file:
                    result=file.read()
                    words=word_tokenize(result)
                    for index,word in enumerate (words,0):
                        if word == searched_word:
                            L.append(index)       
                return L
            
            l= occurences("table")
            lcol= occurences("column")
            with open("data/nlu.md",newline='') as file1:
                result=file1.read()
                words=word_tokenize(result)
                    
                for i in l:
                    x=randint(0,(len(tables_names)-1))
                    syn = list()
                    for synset in wordnet.synsets(tables_names[x]):
                        for lemma in synset.lemmas():
                            syn.append(lemma.name())
                    syn.append(tables_names[x])
                    syn.append(tables_names[x])
                    syn.append(tables_names[x])
                    for j in syn:
                        k=randint(0,(len(syn)-1))
                        if words[i-3]=='[':
                            words[i-3]=words[i-3]+str(syn[k])
                        else:
                            words[i-3]=str(syn[k])
                    if words[i+1] != ')':                        
                        words[i]= "table"
                        words[i+1]=":"
                        words[i+2]=tables_names[x]
                    else:
                        words[i]="table:"+tables_names[x]
                for j in lcol:
                    x=randint(0,(len(column_names)-1))
                    synC = list()
                    for synset in wordnet.synsets(column_names[x]):
                        for lemma in synset.lemmas():
                            synC.append(lemma.name())
                    synC.append(column_names[x])
                    synC.append(column_names[x])
                    synC.append(column_names[x])
                    for a in synC:
                        b=randint(0, (len(synC)-1))
                        if words[j-3]=='[':
                            words[j-3]= '['+ str(synC[b])
                        else:
                            words[j-3]= str(synC[b])                        
                    if words[j+1] != ')':                        
                        words[j]= "column"
                        words[j+1]=":"
                        words[j+2]=column_names[x]
                    else:
                        words[j]="column:"+column_names[x]                         
                for j in range (len(words)):
                    if (words[j]=='-'):
                        words[j-1]=str(words[j-1])+'\n'
                    if (words[j]=='#' and words[j-1]=='#'):
                        words[j-2]=str(words[j-2])+'\n'
                                
                    
                writer = open('data/nlu.md','w')
                for m in range(len(words)-1):
                    if words[m]=='[' or words[m+1]==']' or words[m+1]=='(' or words[m]=='('or words[m+1]==')' or words[m]=='intent'or words[m+1]==')\n' or (words[m]=='#' and words[m+1]=='#') or words[m]==':' or words[m+1]==":":
                        writer.write(words[m])  
                    else:
                        writer.write(words [m]+' ')
                writer.write(words[len(words)-1])
        except Error as e:
            print("Error while connecting to database", e)
        return [SlotSet("tableNames",list(tables_names)),SlotSet("columnNames",list(column_names))]
                    
class ActionSearchCondition(Action):
    def name(self):
        return "action_search_condition"

    def run(self, dispatcher, tracker, domain):
        tableSlot=tracker.get_slot('table')
        columnSlot=tracker.get_slot('column')
        valSlot=(tracker.get_slot('val'))
        tab1=(tracker.get_slot('tableNames'))
        col1=(tracker.get_slot('columnNames'))
        operationsSlot=(tracker.get_slot('operations'))
        dbhost=tracker.get_slot('dbhost')
        dbdriver=tracker.get_slot('dbdriver')
        dbdialect=tracker.get_slot('dbdialect')
        dbusername=tracker.get_slot('dbusername')
        dbpassword=tracker.get_slot('dbpassword')
        dbname=tracker.get_slot('dbname')
        # elements=tracker.get_slot('elements') 
        # engine = create_engine(str(dbdriver)+'+'+str(dbdialect)+'://'+str(dbusername)+':'+str(dbpassword)+'@'+str(dbhost)+'/'+str(dbname))
        # inspector = reflection.Inspector.from_engine(engine)
        msg=tracker.latest_message
        L=os.listdir('./data/lookup/')
        dict={}
        h=[]
        for i in L:
            h=[]
            p=('./data/lookup/'+i+'/columns/')
            c=os.listdir(p)
            for j in c:
                if(j.endswith('.txt')==False):
                    h.append(j)
            m={i:h}
            dict.update(m) 
        # dict is the dictionary of tables and columns 
        values=[]
        
        # values is the dictionary of the element in the message
        for i in range (len(msg['entities'])):
            vals={} 
            vals[msg['entities'][i]['entity']]=msg['entities'][i]['value']
            values.append(vals)
        keys=[]
        for d in values:
            L=list(d.keys())
            keys.append(L[0])
        sql="SELECT"
        list_cols=[]
        z=0
        check=True #false if we insert a word and rasa considert in as table or column
        find = False
        j= 0
        tab=[]
        n=0
        hi=False
        find2=False
        find3=False
        hiList=[]
        op=False #to check if we got operations in request
        op1=False # IN done so must z-1
        op2=False #pour ajouter AND s'il n'a pas ecrit AND/OR écrit
        opand=False # in [] and and name
        eq=True
        # waitH=False #wait till having ends
        waitG=False #wait till Group by ends
        Grp=0 # nbr de group 
        Hvng=0 # nbr having
        group="" # phrase group by
        having="" #phrase having
        keywrd=0
        for k1 in keys:
            if k1 == "table":
                n=n+1
            if k1 =="keyword":
                keywrd=keywrd+1        
        x=0  
        try: 
            v=[]
            for key,value in dict.items():
                if keywrd>0:
                    if value=="GROUP BY":
                        Grp=Grp+1
                    if value=="HAVING":
                        Hvng=Hvng+1
                v=v+value
            for index2 in range (len(keys)):
                if keys[index2]=="operations" and values[x][keys[index2]] not in ["AND","OR"] :
                    op=True 
                x=x+1 
            while z < len (keys) and check==True:
                if keys[z] in ["column" , "table" ]:
                    if (keys[z]=="table" and values[z][keys[z]] not in dict) or Hvng>1 or Grp>1:
                        check=False
                    if keys[z]=="column" :
                        if values[z][keys[z]] not in v:
                            check=False


# # order by and half of having hihih
#                 if ("keyword" in keys ):
#                     if Grp == 1:
#                         waitG=True
#                         if keys[z]=="GROUP BY":
#                             group=group+" GROUP BY "
#                             zgrp=z+1
#                             while zgrp<len(keys) and keys[zgrp]== "column":
#                                 group=group+values[zgrp][keys[zgrp]]
#                                 zgrp=zgrp+1
#                     if Hvng == 1:
#                         if keys[z]=="HAVING":
#                             having = having + " HAVING "
#                             zhv=z+1
#                             while zhv<len(keys) and keys[zhv]== "functions":
#                                 if (zhv+3<len(keys) and keys(zhv+1)=="column" and keys(zhv+2)=="operations" and keys(zhv+3)=="val") :
#                                      #prolem here if operation IN or BETWEEN
#                                      # i can trun the where tests to condition methon and run it here
#                                     having = having + values[zhv][keys(zhv+1)] + values[zhv][keys(zhv+2)] + values[zhv][keys(zhv+1)]
#                                 else:
#                                     check=False
#                             # a test here donoo yetttttttttttttttttttttttttttttttttttttttttttttttt
#                     print ("grp: "+group)
#                     print("hvn : "+having)




                if keys[z]=="table" and z==0:
                    tab.append(values[z][keys[z]])
                    sql=sql+" * FROM "+values[z][keys[z]]
                    
                    for index2 in range (len(keys)):
                        if keys[index2]=="column":
                            if values[index2]['column'] not in list_cols:
                                list_cols.append(values[index2]['column'])
                
                    for column in list_cols:
                        for key1,value in dict.items():
                                if column in value and key1 not in tab  :                                      
                                    tab.append(key1)
                                    sql=sql+", "+key1                             

                elif keys[z]=="column" and z==0 :
                    find3=True
                    j=j+1
                    sql=sql+" "+values[z][keys[z]]
                    list_cols.append(values[z][keys[z]])
                    index=z+1

                    while ((index<len(keys)) and ((values[index][keys[index]] in ["AND","OR"] or keys[index]=='column'))):                    
                        if (keys[index]=='column'): 
                            if ((index+1 <len(keys)) and (values[index+1][keys[index+1]] not in ["AND","OR"]) and keys[index+1] == "operations") :                        
                                if values[index][keys[index]] not in list_cols:
                                    list_cols.append(values[index]['column'])
                                hiList.append(values[index]['column'])
                            else:    
                                j=j+1   
                                if values[index][keys[index]] not in list_cols:                   
                                    sql=sql+","+values[index]['column']
                                    list_cols.append(values[index]['column'])   
                        else:
                            j=j+1   
 
                        print(z)
                        print(j)                                                  
                        index=index+1                    
                    for x in range (index+1, len(keys)):                    
                        if keys[x] == "column" :
                            hiList.append(values[x]['column'])
                    for column in list_cols:
                        for key,value in dict.items():
                                if column in value and key not in tab :
                                    tab.append(key)
                    sql= sql+ " FROM "
                
                    if len (tab) > 1 :
                        sql= sql+tab[0]
                        for t in range (1,len(tab)):                        
                            sql=sql+","+tab[t]
                    else:            
                        sql=sql+tab[0]      

                if n ==1: 
                    find2=True                
                elif keys[z] == "table" and find2 == False and z!=0 and values[z][keys[z]] not in tab: 
                    sql=sql+" ,"+values[z][keys[z]]
                    n=n-1
                    if n==1:
                        find2=True
                elif keys[z]== "table" and find2 == False and z!=0 :
                    n=n-1
                    if n==1:
                        find2=True        
                #find2 = True if all the tables are treated 
                #j <= z bech ma yetraitich des colonnes that he alredy treated before

                #where condition column
                if find3 and j<=z : 
                    # find == True he already wrote Where 
                    if ("operations" in keys and find==False and  (values[z][keys[z]] not in ["AND","OR","NOT"]) and op) :
                        sql = sql+" WHERE"
                        find = True
                    if find :
                        if values[z][keys[z]] == "IN":
                            x=z+1                        
                            sql= sql + ' IN ['
                            valeurs=[]
                            if ( (keys[x]=="val" or values[x][keys[x]] in ["AND","OR","NOT"] )):                            
                                if values[x][keys[x]] not in ["AND","OR","NOT"]:
                                    sql=sql + values[x][keys[x]]
                                    valeurs.append (values[x][keys[x]])
                                    x=x+1
                                    z=z+1
                            while (x < len(keys)  and (keys[x]=="val" or values[x][keys[x]] in ["AND","OR","NOT"] ) ):                                                
                                if ( x+1 <len(keys) and  (keys[x+1]=="val" or values[x+1][keys[x+1]] in ["AND","OR","NOT"])):                                                                                   
                                    if values[x][keys[x]] not in ["AND","OR","NOT"]:                                  
                                        if (values[x][keys[x]] not in valeurs):
                                            valeurs.append(values[x][keys[x]])
                                            sql = sql + " , "+ values[x][keys[x]]
                                elif x+1 <len(keys) and  keys[x+1]=="column" and keys[x]=="val":
                                        if (values[x][keys[x]] not in valeurs):
                                            valeurs.append(values[x][keys[x]])
                                            sql = sql + ","+ values[x][keys[x]]
                                            op2=True 
                                elif(x+1 == len (keys)):
                                    if keys[x] == "val" and (values[x][keys[x]] not in valeurs):
                                        sql = sql + " , "+ values[x][keys[x]]                               
                                                                            
                                x=x+1
                                z=z+1  
                            sql= sql + ']'  
                            op1=True                      
                        else:                                          
                            #if (values[z][keys[z]] not in tab ): # c'est bon il a ecrit tout les tables
                            #     if keys[z]=='column' and values[z][keys[z]] in hiList:
                            #         if op2 == True:
                            #             sql = sql + " AND "+ values[z][keys[z]]
                            #             op2 = False  
                            #         else:
                            #             sql = sql + " "+ values[z][keys[z]]   

                            #     else:
                            if  opand == False and keys[z]=="column" and keys[z-1]=="val":
                                    sql = sql + " AND "+ values[z][keys[z]]
                            elif opand ==True and keys[z]=="column" and keys[z-1]=="val":
                                sql = sql + values[z][keys[z]]
                                opand=False
                             
                            elif op2 == True:
                                sql = sql + " AND " 
                                op2 = False  
                                opand = True
                            else:
                                sql = sql + " "+ values[z][keys[z]] 
                       
                        if op1 == True and x!=len(keys):
                            z=z-1
                            op1= False    
                    
                #find2 kamel 5dem les stables mteeou lkol
                # find wakt elli kteb Where already

                #where condition table
                elif find3==False : 
                    if "operations" in keys and values[z][keys[z]] not in ["AND","OR"]  and find2 and find == False and op:                                
                        sql = sql+" WHERE"
                        find = True
                    elif find and find2 == True :                 
                        if ( z+2 <= len (keys) and  values[z][keys[z]] in [">", "<"] and values[z+1][keys[z+1]] in ["AND","OR"] and values[z+2][keys[z+2]]=="=") :
                            sql = sql + " "+ values[z][keys[z]]
                            z=z+1
                        elif z+1 <= len(keys) and values[z][keys[z]] == "NOT" :
                            if values[z+1][keys[z+1]] == "=":
                                sql = sql + " !"
                            elif keys[z+1] == "val":
                                sql = sql + " !="                                      
                        elif  z < len(keys) and values[z][keys[z]] == "=" and values[z-1][keys[z-1]] in ["AND","OR","NOT"]:                        
                            sql = sql + values[z][keys[z]]     # supprimer le and ou od ou not in the fist condition here                                                                                      
                        elif values[z][keys[z]] == "IN":
                            x=z+1                        
                            sql= sql + ' IN ['
                            valeurs=[]
                            if ( (keys[x]=="val" or values[x][keys[x]] in ["AND","OR","NOT"] )):                            
                                if values[x][keys[x]] not in ["AND","OR","NOT"]:
                                    sql=sql + values[x][keys[x]]
                                    valeurs.append (values[x][keys[x]])
                                    x=x+1
                                    z=z+1
                            while (x < len(keys)  and (keys[x]=="val" or values[x][keys[x]] in ["AND","OR","NOT"] ) ):                                                
                                if ( x+1 <len(keys) and  (keys[x+1]=="val" or values[x+1][keys[x+1]] in ["AND","OR","NOT"])):                                                                                   
                                    if values[x][keys[x]] not in ["AND","OR","NOT"]:                                  
                                        if (values[x][keys[x]] not in valeurs):
                                            valeurs.append(values[x][keys[x]])
                                            sql = sql + " , "+ values[x][keys[x]]

                                elif x+1 <len(keys) and  keys[x+1]=="column" and keys[x]=="val":
                                        if (values[x][keys[x]] not in valeurs):
                                            valeurs.append(values[x][keys[x]])
                                            sql = sql + ","+ values[x][keys[x]]
                                            op2=True #pour ajouter and s'il n'a rien écrit a la fin de IN

                                elif(x+1 == len (keys)):
                                    if keys[x] == "val" and (values[x][keys[x]] not in valeurs):
                                        sql = sql + " , "+ values[x][keys[x]]   
                                x=x+1
                                z=z+1  
                            sql= sql + ']'   
                            op1 = True # IN done so must z-1
                        else :
                            if  opand == False and keys[z]=="column" and keys[z-1]=="val":
                                sql = sql + " AND "+ values[z][keys[z]]
                            elif opand ==True and keys[z]=="column" and keys[z-1]=="val":
                                sql = sql + values[z][keys[z]]
                                opand=False
                             
                            elif op2 == True:
                                sql = sql + " AND " 
                                op2 = False  
                                opand = True
                            else:
                                sql = sql + " "+ values[z][keys[z]] 
                               
                    print(values[z][keys[z]])
                    if op1 == True and x!=len(keys):
                        z=z-1
                        op1= False
                
                z=z+1
            if check == False:
                print("Uncorrect request ; cannot find objet in the data!")
            else:
                if Grp==1:
                    sql = group.replace(group, "")
                    sql = sql+ " "+group
                if Hvng==1:
                    sql = having.replace(having, "")
                    sql = sql+ " "+having
                sql=sql.upper()+";"
                print(sql)
        except IndexError as error:
            print("Uncorrect request!")
              
        # text=msg['text']      
        # operations=[]
        # tables_names=[]
        # columns_names=[]
        # values=[]
        # tables=[]
        # columns=[]              
        # for t in tab1:
        #     tables_names.append(nlp(t))            
        # for c in col1:
        #     columns_names.append(nlp(c)) 
        # for op in list(operationsSlot):
        #     operations.append(nlp(op))
        # for column in columnSlot:
        #     columns.append(nlp(column))
        # for table in tableSlot:
        #     tables.append(nlp(table)) 
        # for val in valSlot:
        #     values.append(nlp(val)) 
        # valuesDb=[]
        # tablesDb=[]
        # columnsDb=[]
        # operationsDb=[]     
        
        # for table in tables:                           
        #     stab=0.0
        #     for tab in tables_names:
        #         s1=table.similarity(tab)
        #         if (s1>stab):
        #             tablee=tab
        #             stab=s1
        #     tablesDb.append(tablee) 

        # for column in columns:                           
        #     scol=0.0
        #     for col in columns_names:
        #         s1=column.similarity(col)
        #         if (s1>scol):
        #             colu=col
        #             scol=s1
        #         print(column)    
        #     columnsDb.append(colu)                             
        

        
        # def findOp(dict,value):
        #     key=''
        #     sv=0.0
        #     for k, v in dict.items(): 
        #         if(key==''):
        #             key=k  
        #         for i in v:
        #             s=value.similarity(nlp(i))
        #             if (s>sv):
        #                 sv=s
        #                 print(sv)
        #                 key=k
        #                 print(key)
        #     return key 
        # # with open('data/op.json') as json_data:
        # #     data_dict = json.load(json_data)
        # #     operationsDb=[]
        # #     for op in operations:
        # #         opDb=findOp(data_dict,op)
        # #         operationsDb.append(opDb)
        # # print( tablesDb)   

        # print( tablesDb)   
        # print(operationsDb)
        # print(columnsDb)
        # # file = open("data/lookups/"+str(column)+".txt", "r")
        # # lines = file.readlines()           
        # dbhost=tracker.get_slot('dbhost')
        # dbdriver=tracker.get_slot('dbdriver')
        # dbdialect=tracker.get_slot('dbdialect')
        # dbusername=tracker.get_slot('dbusername')
        # dbpassword=tracker.get_slot('dbpassword')
        # dbname=tracker.get_slot('dbname') 
                
        # engine = create_engine(str(dbdriver)+'+'+str(dbdialect)+'://'+str(dbusername)+':'+str(dbpassword)+'@'+str(dbhost)+'/'+str(dbname))
        # with engine.connect() as con:
        #     # rs = con.execute('{0} * FROM {1} WHERE {2} {3} {4} AND {5}'.format(str(oprrr(0)),str(table),str(column),str(oprrr(1)),float(val1.text),float(val2.text)))
        #     rs = con.execute('SELECT * FROM {0} WHERE {1} BETWEEN {2} AND {3}'.format(str(table),str(column),float(val1.text),float(val2.text)))
        #     resultset = [dict(row) for row in rs]
        #     print (resultset) 
        return[]