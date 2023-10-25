#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 15 11:19:49 2023

"""

import sys
import time 
import math
import pickle
from generate_projections import *
import random as rd
import json

with open('current_wl',  'rb') as  wl_file:
    wl = pickle.load(wl_file)

with open('placementDictLatency',  'rb') as placementDictFile:
        placementDict = pickle.load(placementDictFile)
        
with open('scalingFactor',  'rb') as scalingData: # new for outputrate
    myscaling = pickle.load(scalingData)            
        
with open('osDict',  'rb') as filterDict_file:
     osDict = pickle.load(filterDict_file)
        

with open('processingConstraints', 'rb') as constraintFile:
        constraintDict = pickle.load(constraintFile)        

def getfiltered(): #ACHTUNG: unterstützt nicht mehrfach os    
    filterDict = {x:x for x in list(set(sum([y.leafs() for y in wl],[])))}
    # for each projection generate subprojection to be sent
    if list(osDict.keys()):
        for proj in osDict.keys():
            if not proj in filterDict.keys():
                filterDict[proj] = proj
            if osDict[proj]:
                combi = placementDict[proj][2]
                for ingredient in combi:
                    toSend = [x for x in ingredient.leafs() if not x in [y[0] for y in osDict[proj] if y[1] != ingredient]]
                    filterDict[ingredient] =  settoproj(toSend,ingredient)
    else:
        filterDict.update({x: x for x in placementDict.keys()})
    return filterDict
    # use "subprojection|projection" for writing output to combination and forward dict
    # make sure that if subprojectio != projection, combination has adapted selectionRate
    
    
filteredProjs = getfiltered()

        
# placementDict proj: ([nodes], partType)
# first of all generate network > all nodes with part type being a prim event generate type with partial rate
# for rest of events let randomly choose nodes as single sources
# take care of parallelized projections rates later
parallelizedPrimTypes = [placementDict[x][1] for x in placementDict.keys() if len(placementDict[x][1])==1]
ressources = len(sum([x[0] for x in list(placementDict.values())],[]))


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
            if not placementDict[i][1] in sources.keys():
                sources[placementDict[i][1]] = []
            if len(placementDict[i][1])==1:
                sources[placementDict[i][1]]+=placementDict[i][0] # this assumes that maximal once parallellized on one type
                localRates[placementDict[i][1]] = totalRate(placementDict[i][1])/len(placementDict[i][0])
    rest = [x for x in myTypes if not x in localRates.keys()]      
    for etype in rest:
        sources[etype]  = []
        mysources = sum([placementDict[x][0] for x in inputTo(etype)],[])
        
        print(etype, mysources, inputTo(etype))        
        for source in mysources:
            sources[etype].append(source)
        localRates[etype] = totalRate(etype)/len(sources[etype])

    totalRest = [x for x in rates.keys() if not x in localRates.keys()]
    for etype in totalRest:
        sources[etype] = [0]
        localRates[etype] = totalRate(etype)
    mynodes = list(range(nwsize)) 
    print(sources, localRates)
    localRates = {x:localRates[x] for x in localRates if not isinstance(x,Tree)}
    nw = [[localRates[eventtype] if x in sources[eventtype] else 0 for eventtype in sorted(localRates.keys())] for x in mynodes]
    return nw, sources, complexParallelTypes 

def networkText_new(stretch=1):
    myTypes = sorted(list(set(sum([x.leafs() for x in wl], []))))
    mystr = ""
    for etype in myTypes:
        for proj in placementDict.keys():
            if etype in placementDict[proj][2]:
                if etype == placementDict[proj][1]: # is parallized
                    #for k in sources[etype]:
                        mystr += " ".join([str(y) for y in placementDict[proj][0]])
                        #mystr += " ".join(k)
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
    myTypes = list(set(sum([x.leafs() for x in wl], [])))
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
    return mystr    

def singleSelecText():
    return "Single Selectivities:" + str(singleSelectivities)


## generate processing rules


def getPredicateConstraints_adjust(left,right):
    predicateConstraints = []
    #combination = sorted(combination, key = lambda x:  len(x.leafs()) if isinstance(x, Tree) else len(x))
    #left = [combination[0].leafs() if len(combination[0])>1 or isinstance(combination[0], Tree) else combination[0]][0]
    #right = []
    #if len(combination)>1:
     #   right = [combination[1].leafs() if len(combination[1])>1 else combination[1]][0]
    left = [left.leafs() if len(left)>1 or isinstance(left, Tree) else left][0]
    right = [right.leafs() if len(right)>1 or isinstance(right, Tree) else right][0]

    if right:
        for k in left:
            for i in right:
                #print(i,k)
                if not  k in right and not i in left and not i ==  k:
                #if not i in right:
                    predicateConstraints.append((k,i))
    return predicateConstraints

def processingRules(i):  
    
    projection = [x for x in placementDict.keys() if i in placementDict[x][0]][0]
    text = ""
    text = "Projections to process:\n"       
    mycombination = placementDict[projection][2]
    mySelRate = return_selectivity(projection.leafs())      
    predicate_checks =   return_selectivity_num(projection.leafs())           
    text += "SELECT " + str(projection) + " FROM "
    for sub in mycombination: 
                        if  filteredProjs[sub] != sub: #TODO doesnt work
                            text += str(filteredProjs[sub]) + "|" 
                        text+=str(sub) +"; "
                        if sub in projrates.keys():
                                print(sub)
                                mySelRate = mySelRate / projrates[sub][0] #Correct?
                                predicate_checks -=1 
                        elif len(sub) > 1:
                                mySelRate = mySelRate /  return_selectivity(sub.leafs())
                                predicate_checks -=1 
    text = text[:-2]    
    text += " WITH selectionRate= " + str(mySelRate) + "\n"  
    id_constraints = constraintDict[projection][1]
    sequence_constraints = constraintDict[projection][0]
    predicate_constraints = constraintDict[projection][2]
    
    if projection.hasKleene() and len(mycombination) == 1: # this is the kleene matching projection
       kleeneType =  projection.kleene_components()[0] 
       return str(projection), projection.leafs(), str(mycombination[0]), str(mycombination[0]), selectivities[str(kleeneType)],constraintDict[projection][0],[x for x in projection.leafs() if not x == str(kleeneType)],predicate_constraints,1,projection,1

        
    if filteredProjs[projection] != projection:
            name = str(filteredProjs[projection]) + "/" + str(projection) 
    else:
        name = str(projection)
    output_selection = filteredProjs[projection].leafs()

    inputs = [mycombination[x] for x in range(len(mycombination))]
    
    input_1 = str(inputs[0])
    input_2 = str(inputs[1])
    predicate_constraints  = getPredicateConstraints_adjust(inputs[0], inputs[1])
    
    selectivity = mySelRate
    actual_combination = [filteredProjs[x].leafs() if len(x) > 1 else x for x in mycombination]
    if len(filteredProjs[mycombination[0]]) > 1 and len(filteredProjs[mycombination[1]]) > 1:
        id_constraints = list(set(actual_combination[0]).intersection(set(actual_combination[1])))
        if not id_constraints:
            id_constraints = []
    else:
        id_constraints = []
    
    print(projection, constraintDict[projection])
    return name, output_selection, input_1, input_2, selectivity,sequence_constraints,id_constraints,predicate_constraints,predicate_checks,projection,0, 

# for each prim event and projection, list of nodes who neeed as input -> nodes who have input and are not source of event type
def generateForwardingDict():
    forwardingDict = {x:[] for x in placementDict.keys()}
    forwardingDict.update({x:[] for x in list(set(sum([y.leafs() for y in wl],[])))})
    for proj in sorted(placementDict.keys(), key = len):
        for ingredient in placementDict[proj][2]:   
            if not ingredient in parallelizedPrimTypes:
                    forwardingDict[ingredient] +=  placementDict[proj][0]
    
    forwardingDict = {key: val for key, val in forwardingDict.items() if val}      
    for i in forwardingDict:
        print(i, forwardingDict[i])
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
complexParallelTypes = [placementDict[x][1] for x in placementDict.keys() if len(x)>1]
mynw = mynw[0] 


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
                    
                else:
                    sendmode = "roundrobin"
                    recipients = [x for x in forwardingDict[projection]]

    return recipients, sendmode
            
      
sendTo = {x: forwardingRule(x)[0] for x in range(len(mynw))}        
print(sendTo)
def getPredecessors(node):
    return  [x for x in range(len(mynw)) if node in sendTo[x]]



def generatePlan():
    text  = ""
    text +=networkText_new(stretch) + "\n"
    text +="-----------\n"
    text +="Randomized Rate-Based Primitive Event Generation\n"
    text +="-----------\n"
    text += singleSelecText() + "\n" # remove?
    text +="-----------\n"
    for node in range(len(mynw)):
        text += "~~\n"
        text += "node" + str(node) +"\n"
        text += "--\n" 
        text += forwardingRule(node) + "\n"
        text += "--\n" 
        text += processingRules(node) + "\n"

    return text


def main():
    path = ""
    stretch =1/myscaling
    if len(sys.argv) > 1: 
            path = str(sys.argv[1])
    if len(sys.argv)>2:
        stretch = int(sys.argv[2])
    
        
    # print nw file matrix to path
    print(networkText_new(stretch))
    
    f = open('../plans/'+ path +'/matrix.txt', "w")   
    f.write(networkText_new(stretch)) 
    f.close()
    
    f = open('../plans/'+ path +'/sinks.txt', "w")   
    f.write(str(sum([x for x in [placementDict[y][0]for y in wl]],[])))
    f.close()
       
    f = open('../plans/'+ path +'/alphabet.txt', "w")   
    f.write(''.join(sorted(list(set(sum([x.leafs() for x in wl], [])))))) 
    f.close()
    
    kleeneNodes = sum([placementDict[y][0] for y in [x for x in placementDict.keys() if len(placementDict[x][2])==1]],[])
    kleeneTypes = [x.kleene_components()[0] for x in wl  if x.hasKleene() ]
    
    partitioning_key = ""
    for node in range(len(mynw)):
        connected_to = getPredecessors(node)
    
    
        recipient, send_mode = forwardingRule(node)
        
        query_name, output_selection, input_1, input_2, selectivity, sequence_constraints, id_constraints,  predicateConstraints,predicate_checks, query, kleene_matching = processingRules(node)
        
        if [x for x in recipient if x in kleeneNodes]:           
            send_mode = "partition"
            partitioning_key = [x for x in output_selection if not x in kleeneTypes]
        if not kleene_matching:
            actual_selectivity =  totalRate(query) / (2* totalRate(placementDict[query][2][0]) * totalRate(placementDict[query][2][1]))
           # print("actual", actual_selectivity, selectivity)
            selectivity = actual_selectivity
            
        
        if len(output_selection) < len(query.leafs()):
          myproj = settoproj(output_selection, query)
          selectivity *= (myproj.evaluate() * math.prod([singleSelectivities[getKeySingleSelect(x,query)] for x in output_selection])/totalRate(query))
        #print("Predicate", input_1, input_2, predicateConstraints)    
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
                 "is_negated": 0,
                 "kleene_type": kleene_matching,
                 "context": [],
                "id_constraints": id_constraints,
                "time_window_size": 60 * stretch,
                "predicate_constraints": predicateConstraints,
              #  "partitioning_key": partitioning_key
            }
        } 
            
        #print(config)
        with open('../plans/'+ path +'/config_' + str(node) +'.json', 'w') as f:
            json.dump(config, f)
    
    
if __name__ == "__main__":
    main()