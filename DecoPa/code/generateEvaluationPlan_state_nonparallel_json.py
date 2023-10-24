#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

"""

import sys
import time 
import pickle
from estimateLatency import *
import random as rd
import json


with open('placementDict_stateparallel',  'rb') as placementDictFile:
        placementDict = pickle.load(placementDictFile)
        
with open('processingConstraints_stateparallel', 'rb') as constraintFile:
        constraintDict = pickle.load(constraintFile)        
        


count = 0
for i in placementDict:
    placementDict[i] = ([count], placementDict[i][1], placementDict[i][2])
    count += 1
    
parallelizedPrimTypes = [placementDict[x][1] for x in placementDict.keys() if len(placementDict[x][1])==1]
ressources = len(sum([x[0] for x in list(placementDict.values())],[]))

filteredProjs  = {x: x for x in placementDict.keys()}
filteredProjs.update({x: x for x in list(set(sum([y.leafs() for y in wl],[])))})


def inputTo(eventtype):
    return [x for x in placementDict.keys() if eventtype in placementDict[x][2]]

def generateNW():
    nwsize =  ressources # max node ide from Placement Dict
    myTypes = [x for x in list(set(sum([y.leafs() for y in wl],[])))]
    localRates = {}
    sources = {}
    complexParallelTypes = {}
    parallelTypes = [placementDict[x][1] for x in placementDict.keys() if len(placementDict[x][1]) == 1]

    for i in sorted(placementDict.keys(), key = len):
        if placementDict[i][1]:
            if len(placementDict[i][1])==1:
                sources[placementDict[i][1]] =  placementDict[i][0] # this assumes that maximal once parallellized on one type
                localRates[placementDict[i][1]] = totalRate(placementDict[i][1])/len(placementDict[i][0])
    rest = [x for x in myTypes if not x in localRates.keys()]      
    
    for etype in rest:
        sources[etype]  = []
        mysources = sum([placementDict[x][0] for x in inputTo(etype)],[])
        for source in mysources:
            sources[etype].append(source)
        localRates[etype] = totalRate(etype)/len(sources[etype])

    totalRest = [x for x in rates.keys() if not x in localRates.keys()]
    for etype in totalRest:
        sources[etype] = [0]
        localRates[etype] = totalRate(etype)
    mynodes = list(range(nwsize)) 
    print(sources, localRates)
    nw = [[localRates[eventtype] if x in sources[eventtype] else 0 for eventtype in sorted(localRates.keys())] for x in mynodes]
    return nw, sources, complexParallelTypes 


def networkText_new(stretch=1):
    myTypes = sorted(list(set(sum([x.leafs() for x in wl], []))))
    mystr = ""
    for etype in myTypes:
        for proj in placementDict.keys():
            if etype in placementDict[proj][2]:
                if etype == placementDict[proj][1]: # is parallized
                    mystr += " ".join([str(y) for y in placementDict[proj][0]])
                    mystr += ";"
                else:
                    mystr += ";".join([str(y) for y in placementDict[proj][0]])
                    mystr += ";"
        mystr = mystr[:-1]
        mystr+="\n"
        
    mystr += "---------------------------------\n"
    mystr += " ".join([str(totalRate(x)/stretch) for x in sorted(myTypes)])        
    return mystr 


def networkText(stretch):
    myTypes = sorted(list(set(sum([x.leafs() for x in wl], []))))
    #mystr = "network \n"
    mystr = ""
    for i in mynw:        
        
        for j in range(len(i)):
           if string.ascii_uppercase[j] in myTypes:
              mystr += str(i[j]/stretch) + " "
              #mystr += str(1) + " "
           else:
                mystr += "0" + " "   
        mystr +="\n"
    mystr =  mystr[:-1]    + "\n"
    mystr += "".join(["------" for x in list(set(sum([y.leafs() for y in wl],[])))]) + "\n"
    parallelTypes =  [placementDict[x][1] for x in placementDict.keys() if len(placementDict[x][1]) == 1]
    mystr +="".join(["P " if x in parallelTypes else "B " for x in sorted(string.ascii_uppercase[:len(mynw[0])])])
    print(mystr)
    return mystr    

def singleSelecText():
    return "Single Selectivities:" + str(singleSelectivities) 

def singleSelecText():
    return "Single Selectivities:" + str(singleSelectivities)


## generate processing rules
def processingRules(i):  
    
   
   
    
    projection = [x for x in placementDict.keys() if i in placementDict[x][0]][0]
    text = ""
    text = "Projections to process:\n"       
    mycombination = placementDict[projection][2]
    if projection in projrates.keys():
        mySelRate = projrates[projection][0]                        
    else:
        mySelRate =   return_selectivity(projection.leafs())
    predicate_checks =   return_selectivity_num(projection.leafs())    
    text += "SELECT " + str(projection) + " FROM "
    for sub in mycombination: 
                        if  filteredProjs[sub] != sub: #TODO doesnt work
                            text += str(filteredProjs[sub]) + "|" 
                        text+=str(sub) +"; "
                        if len(sub) > 1:
                            if sub in projrates.keys():
                                mySelRate = mySelRate / projrates[sub][0] #Correct?
                                predicate_checks -=1 
                            else:
                                mySelRate = mySelRate /  return_selectivity(sub.leafs())
                                predicate_checks -=1 
    text = text[:-2]    
    text += " WITH selectionRate= " + str(mySelRate) + "\n"  
    
    if filteredProjs[projection] != projection:
            name = str(filteredProjs[projection]) + "|" + str(projection) 
    else:
        name = str(projection)
    output_selection = filteredProjs[projection].leafs()
    input_1 = str(mycombination[0])
    input_2 = str(mycombination[1])
    selectivity = mySelRate
    
    actual_combination = [filteredProjs[x].leafs() if len(x) > 1 else x for x in mycombination]
    id_constraints = list(set(actual_combination[0]).intersection(set(actual_combination[1])))
    
    
    return name, output_selection, input_1, input_2, selectivity,constraintDict[projection][0],id_constraints,predicate_checks,projection


# for each prim event and projection, list of nodes who neeed as input -> nodes who have input and are not source of event type
def generateForwardingDict():
    forwardingDict = {x:[] for x in placementDict.keys()}
    forwardingDict.update({x:[] for x in list(set(sum([y.leafs() for y in wl],[])))})
    for proj in sorted(placementDict.keys(), key = len):
        for ingredient in placementDict[proj][2]:   
            if not ingredient in parallelizedPrimTypes:
                    forwardingDict[ingredient] +=  placementDict[proj][0]
    forwardingDict = {key: val for key, val in forwardingDict.items() if val}           
    return forwardingDict

def generateSourceDict():
    sourceDict = {}
    
    #complex event generation
    for proj in placementDict.keys():
        for node in placementDict[proj][0]:
            if not node in sourceDict.keys():
                sourceDict[node] = [proj]
            else:
                sourceDict[node] += [proj]
    #add prim event generation from nw
    for etype in primSources.keys():
        for node in primSources[etype]:
            sourceDict[node].append(etype)
            
    return sourceDict
    
    


mynw = generateNW()  
primSources = mynw[1]

mynw = mynw[0]    
complexParallelTypes = [placementDict[x][1] for x in placementDict.keys() if len(x)>1]


with open('network', 'wb') as network_file:
          pickle.dump(mynw, network_file)

sourceDict = generateSourceDict()

#for i in sourceDict.keys():
#    print(i, list(map(lambda x:str(x), sourceDict[i])))

forwardingDict = generateForwardingDict()

#for i in forwardingDict.keys():
 #   print(i, list(map(lambda x:str(x), forwardingDict[i])))
    
def getETB(projection):
    text = ""
    if len(projection)>1:
        myTuple = ["("+x+":ANY)" for x in projection.leafs()]
    else:
        myTuple = ["("+projection+":ANY)"]
    for x in myTuple:
        text+= x +";"
    return text[:-1]

def getETBInstantiated(projection, etype, node):
    text = ""
    if len(projection)>1:
        myTuple = ["("+x+":ANY)" if not x==etype else "("+x+":node"+str(node)+")"  for x in projection.leafs()]
    else:
        myTuple = ["("+projection+":ANY)"]
    for x in myTuple:
        text+= x +";"
    return text[:-1]

def getSources(etype):
    return [x for x in sourceDict.keys() if etype in sourceDict[x]]



def forwardingRule(i):
    # matched projection is parallelized, get sources of non parallelized prim input used for etb distinguishing, generate etb for each placement based on respective sources
    text = "Forward rules:\n"
    recipients = []
    sendmode = ""
    for projection in sourceDict[i]: #everything locally produced
          if not projection in wl:
              if len(projection) > 1: 
                if not projection in complexParallelTypes:
                    sendmode = "broadcast"
                    recipients = [x for x in forwardingDict[projection]]
                    # if  filteredProjs[projection] != projection: #TODO doesnt work
                    #             text += str(filteredProjs[projection]) + "|" 
                    # text += str(projection) + " - "
                    # text += "[ETB:" + getETB(projection)
                    # text += " FROM:[node" +str(i)+"] TO:[" 
                    # recipients = ["node"+str(x) for x in forwardingDict[projection] if not x == i]
                    # for x in recipients:            
                    #     text+= x +";"
                    # text= text[:-1]
                    # text+="]]\n"
                    
                else:
                    sendmode = "roundrobin"
                    recipients = [x for x in forwardingDict[projection]]
                    # sources = getSources(complexParallelTypes[projection])
                    # for source in range(len(sources)): 
                    #     # generate ETB text for source of complexParallelTypes[projection]
                    #         if  filteredProjs[projection] != projection: #TODO doesnt work
                    #             text += str(filteredProjs[projection]) + "|"
                    #         text += str(projection) + " - "    
                    #         text += "[ETB:" + getETBInstantiated(projection, complexParallelTypes[projection], sources[source])
                    #         text += " FROM:[node" +str(i)+"] TO:[" 
                    #         recipients = ["node"+str(x) for x in [forwardingDict[projection][source]]]
                    #         for x in recipients:            
                    #             text+= x +";"
                    #         text= text[:-1]
                    #         text+="]]\n"
    return recipients, sendmode



sendTo = {x: forwardingRule(x)[0] for x in range(len(mynw))}        
print(sendTo)
def getPredecessors(node):
    return  [x for x in range(len(mynw)) if node in sendTo[x]]

def main():
    stretch = 1
    path = ""
    if len(sys.argv) > 1: 
            path = str(sys.argv[1])
    
    # print nw file matrix to path
    f = open('../plans/'+ path +'/matrix.txt', "w")   
    f.write(networkText_new(stretch)) 
    f.close()
    
    f = open('../plans/'+ path +'/sinks.txt', "w")   
    f.write(str(sum([x for x in [placementDict[y][0]for y in wl]],[]))) 
    f.close()
    
    
    for node in range(len(mynw)):
        connected_to = getPredecessors(node)
        kleeneType = 0
        recipient, send_mode = forwardingRule(node)
        
        
        
        query_name, output_selection, input_1, input_2, selectivity, sequence_constraints, id_constraints,predicate_checks, query = processingRules(node)
        
         # TODO ADJUST TO KLEENE CASE!
        actual_selectivity =  totalRate(query) / (2* totalRate(placementDict[query][2][0]) * totalRate(placementDict[query][2][1]))
        selectivity = actual_selectivity
        
        if query.hasKleene():
            myinputs = [input_1, input_2]
            if query.kleene_components()[0] in myinputs:
                kleeneType =  2
            input_1 = str(query.kleene_components()[0])
            input_2 = [x for x in myinputs if not x == input_1][0]
            selectivity = selectivities[str(query.kleene_components()[0])] * math.prod([selectivities[str(query.kleene_components()[0])+x]for x in query.leafs() if not x == query.kleene_components()[0]])
            
       
            
        config = {
            "forwarding": {
                "send_mode": send_mode,
                "recipient": recipient,
                "connections_to_establish": connected_to + [node],
                "node_id": node
            },
            "processing": {
                "query_length": len(query.leafs()),
                "predicate_checks": 1,
                "query_name": query_name,
                "output_selection": output_selection,
                "input_1": input_1,
                "input_2": input_2,
                "selectivity": selectivity,
                "sequence_constraints": sequence_constraints,
                "id_constraints": id_constraints,
                "is_negated": 0,
                "kleene_type": kleeneType,
                "context": [],
                "time_window_size": 60 * stretch
            }
        } 
            
        with open('../plans/'+ path +'/config_' + str(node) +'.json', 'w') as f:
            json.dump(config, f)
    
    
if __name__ == "__main__":
    main()
