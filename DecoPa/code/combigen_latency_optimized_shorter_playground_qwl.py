#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 27 14:16:13 2021


Latency bounded version of combination generation.
"""
import sys
import time 
import math
from generate_projections import *
import random as rd
from itertools import permutations
import numpy as np


numberCombis = 0
projFilterDict =  {}  

#ressources = 15
os_combigen = 0
os_afterprocessing = os_combigen # if os_combigen = 1 -> os_afterprocessing = 1

    
ressources = 20
parallel = 1
combiDict = {}
stateCombiDict = {}

totalEventRates = {x:instances[x]*rates[x] for x in rates.keys()}
totalProjRates = {x: totalRate(x) for x in projrates.keys()}


def totalRate(projection):

    if len(projection) == 1:
        if isinstance(projection, str) or  isinstance(projection, PrimEvent):
            return rates[str(projection)] * len(nodes[str(projection)])
        elif projection.hasKleene():
            return getKleeneRate(projection) *scaling
   
    elif projection.hasKleene():
         return getKleeneRate(projection)*scaling
    else:
        projection = projection.strip_NSEQ()
        # add case for nseq with two events only, treat as normal sequence
        outrate = projection.evaluate() *  getNumETBs(projection)  
        selectivity =  return_selectivity(projection.leafs())
        myrate = outrate * selectivity  
        return myrate*scaling


def unfold_combi(query, combination): #unfolds a combination, however in the new version we will have only one combination which is provided in the same format as the unfolded dict
    unfoldedDict = {}
    unfoldedDict[query] = combination    
    unfoldedDict.update(unfold_combiRec(combination, unfoldedDict))
    return unfoldedDict

def unfold_combiRec(combination, unfoldedDict): 
    
    for proj in combination:
        if len(proj) > 1 or (isinstance(proj, Tree) and proj.hasKleene()): 
            if proj in combiDict.keys():
                mycombination =  combiDict[proj][0]
            else:
                mycombination = proj.leafs() # this is the case if proj is a single sink projection, and we have to decide how to match it later
            unfoldedDict[proj] = mycombination
            unfoldedDict.update(unfold_combiRec(mycombination, unfoldedDict))
    return unfoldedDict 


def depth(projection, mycombi, myCombidict):
     mydepth = 1
     if set(mycombi) == set(projection.leafs()): # has no complex ancestors
         return mydepth   
     else:   
         mydepth += max([depth(x, myCombidict[x],myCombidict) for x in   mycombi if len(x)>1])
         return mydepth 
           

def depth_reverse(projection, myCombidict):
     mydepth = 1
     if projection not in sum(list(myCombidict.values()),[]): # has no complex ancestors
         return mydepth   
     else:   
         
         mydepth += max([depth_reverse(x, myCombidict) for x in myCombidict.keys() if projection in myCombidict[x]])
         return mydepth 

def grandchildren(projection, combi):
    local_combi_dict = unfold_combi(projection, combi)
    return [x for x in local_combi_dict.keys() if depth(x, local_combi_dict[x], local_combi_dict) == 1]

def getComparisons(proj, combi):
    latency = 1
    if proj.hasKleene():
        return getComparisons_kleene(proj, combi)
    for subproj in combi:      
        latency *= totalRate(subproj) #* sum(list(map(lambda x: myrates[x], myPMS)))
    latency *=2
    latency *= myscaling
    latency += sum([myscaling * totalRate(x) for x in combi]) #inputrate as additional component in cost function
    return latency

def getComparisons_kleene(proj, combi):
    latency = 1
    kleeneType = []
    if proj.hasKleene():
        kleeneType = proj.kleene_components()[0]        
        otherType = [x for x in combi if not x == kleeneType]     
            
    if kleeneType in combi and otherType:    # KL(A,B) = [A,B]    
        latency = totalRate(kleeneType) * (totalRate(settoproj([kleeneType], proj)) + totalRate(otherType[0]) + totalRate(proj)) + totalRate(otherType[0]) * totalRate(settoproj([kleeneType], proj))
    elif kleeneType in combi and not otherType: # KL(A) = [A], 
         #latency = totalRate(kleeneType) * (totalRate(settoproj([kleeneType], proj)) + totalRate(proj)) 
         latency = totalRate(kleeneType) * totalRate(proj)
    elif proj.hasKleene() and len(combi) ==1: #SEQ(KL(A),B) = [SEQ(A,B)]
         return totalRate(proj) * totalRate(combi[0])
    else:         
        for subproj in combi:    
            latency *= totalRate(subproj) #* sum(list(map(lambda x: myrates[x], myPMS)))
        latency =latency*2    
    return latency * myscaling#* scaling


def getComparisons_simple(proj, combi): # with output selection, only 2 part combinations, no kleene hhandling
    latency = 1
    for subproj in combi:
        latency *=  totalRate(subproj)               
    return latency*2 * myscaling


def getCompleteLatency_noRessources(completeCombi):
    latency = 0
    myRessources = 1
    for proj in sorted(completeCombi.keys(), key = lambda x: len(x.leafs())):
        latency += getComparisons(proj, completeCombi[proj])
    latency += sum([totalRate(x) for x in completeCombi[proj]])
    return latency

def getCompleteLatency_noRessources_kleene(completeCombi): # should have the same behaviour as version without _kleene
    latency = 0
    myRessources = 1
    for proj in sorted(completeCombi.keys(), key = lambda x: len(x.leafs())):
        latency += getComparisons_kleene(proj, completeCombi[proj])
    return latency



def getCompleteLatency_stateparallel(projection, completeCombi, ressourceDict): 
    myLatencyDict = {}
    myRessources = 1
    for proj in sorted(completeCombi.keys(), key = lambda x: len(x.leafs())):
        if ressourceDict[proj] <2:
            myRessources = 1
        else:
            myRessources = ressourceDict[proj]
        if not projection.hasKleene():    
            latency = getComparisons(proj, completeCombi[proj]) /  myRessources
        else:
            if proj in stateCombiDict.keys():
                latency = stateCombiDict[proj][0]/myRessources
            else:
                latency = getBestStateParallel_kleene(proj)[0]/myRessources
        mySubs = [x for x in myLatencyDict.keys() if x in completeCombi[proj] and len(x)>1]
        if mySubs:
            latency += max(myLatencyDict[x] for x in mySubs)
        myLatencyDict[proj] = latency   
        
    return myLatencyDict[projection] 


def sample(mylist, num):
    newlist = []
    for i in range(num):
        k = int(np.random.uniform(0,len(mylist)-1))       
        if not mylist[k] in newlist:
            newlist.append(mylist[k])
    return newlist

def getBestStateParallel(projection):
    myOrder = projection.leafs()    
    mycombi = {}
    oldcosts = np.inf
    if len(projection) > 1:
        myOrders = permutations(myOrder)
        myOrders = sample(list(myOrders),40)
    elif not parallel: #?
        myOrders = [myOrder]
    for order in myOrders:
        combi = {}
        for i in range(1,len(order)):
             order = list(order)
             myproj = settoproj(order[0:i+1], projection)
             combi[myproj] = [settoproj(order[0:i], projection), order[i]] #TODO: set to string
             combi[myproj] = [x.leafs()[0] if isinstance(x,PrimEvent)  else x for x in combi[myproj]]
        if getCompleteLatency_noRessources(combi) < oldcosts:
            mycombi = combi
            oldcosts =  getCompleteLatency_noRessources(combi)
    for i in mycombi.keys():
        if str(i) == str(projection):
            projection = i
    return getCompleteLatency_noRessources(mycombi), mycombi[projection], [x for x in mycombi[projection] if len(x)==1][0], mycombi


def getBestStateParallel_kleene(projection):
    myOrder = projection.leafs()
    kleeneType = None
    if projection.hasKleene():
        kleeneType = str(projection.kleene_components()[0])
        
    if len(myOrder) > 2:
        myOrders = permutations(myOrder)
        mycombi = {}
   
        oldcosts = np.inf
        for order in myOrders:       # TODO, for kleene query, add projection KL(X) with X as input to Combination compute costs accordingly
            combi = {}
            if projection.hasKleene():
                     combi[settoproj(kleeneType,projection)] = [kleeneType]
            for i in range(1,len(order)):
                 order = list(order)
                 myproj = settoproj(order[0:i+1], projection)
                 if str(myproj) == str(projection):
                     myproj = projection
                 combi[myproj] = [settoproj(order[0:i], projection), order[i]]
                 # if i is kleene component then use settoproj(i, projection) and add combination for kleene type
                 combi[myproj] = [x.leafs()[0] if isinstance(x,PrimEvent)  else x for x in combi[myproj]]                 
                 combi[myproj]= [x if not x==kleeneType else settoproj(kleeneType,projection) for x in combi[myproj]]
            if getCompleteLatency_noRessources_kleene(combi) < oldcosts:
                mycombi = combi
                oldcosts =  getCompleteLatency_noRessources_kleene(combi)                
    else:
        mycombi = {projection:projection.leafs()}  
    return getCompleteLatency_noRessources_kleene(mycombi), mycombi[projection], [x for x in mycombi[projection] if len(x)==1][0], mycombi

def kleeneUnarySingle(proj,combi):
    if len(combi) == 1 and combi[0] == proj.kleene_components()[0]:
        return True
    else:
        return False

def getRessourcesProportional(completeCombi, ressources): #without depth
    portions = {x: getComparisons(x,completeCombi[x]) if not kleeneUnarySingle(x, completeCombi[x]) else 0 for x in completeCombi.keys()}
    total = sum(list(portions.values()))
    ressourceDict = {x:round((portions[x]/total)*ressources)  for x in portions.keys()}
    ressourceDict = {x: ressourceDict[x] if ressourceDict[x]>1 else 1 for x in ressourceDict.keys()}
    if sum(list(ressourceDict.values())) > ressources:
       singleSinks = [x for x in ressourceDict.keys() if ressourceDict[x] <2 ]
       multiSinks = list(set(list(ressourceDict.keys())).difference(set(singleSinks)))
       portions = {x: getComparisons(x,completeCombi[x]) for x in multiSinks}
       total = sum(list(portions.values()))
       for i in multiSinks:
           ressourceDict[i] = round((portions[i]/total)*(ressources - len(singleSinks)) )
    #DIRTY
    potentialRest = ressources - sum(list(ressourceDict.values()))
    if potentialRest > 0:
       maximalParallel = [x for x in ressourceDict.keys() if ressourceDict[x] == max(list(map(lambda x: ressourceDict[x], ressourceDict.keys())))][0]
       ressourceDict[maximalParallel] = ressourceDict[maximalParallel] + potentialRest
  
    return ressourceDict 



def getBestStateParallel_negated(projection):
    myOrder = projection.leafs()
    myOrders = permutations(myOrder)
    mycombi = {}
    oldcosts = np.inf
    negated = projection.get_negated()
    
    negated_dict = {x: projection.get_context(x) for x in negated}
    
    for order in [list(x) for x in myOrders]:
        correctOrder = True 
        for neg in negated_dict.keys():
            if order.index(str(neg)) < order.index(str(negated_dict[neg][0])) or order.index(str(neg)) < order.index(str(negated_dict[neg][1])):
                correctOrder = False
        # check for order if for each negated all events in context are before, otherwise dont use
        if correctOrder:
            combi = {}
            for i in range(1,len(order)):
                 order = list(order)
                 myproj = settoproj(order[0:i+1], projection)
                 combi[myproj] = [settoproj(order[0:i], projection), order[i]] #TODO: set to string
                 combi[myproj] = [x.leafs()[0] if isinstance(x,PrimEvent)  else x for x in combi[myproj]]
            if getCompleteLatency_noRessources(combi) < oldcosts:
                mycombi = combi
                oldcosts =  getCompleteLatency_noRessources(combi)
    return getCompleteLatency_noRessources(mycombi), mycombi[projection], [x for x in mycombi[projection] if len(x)==1][0],mycombi


for proj in projlist: # THE RESULTING COMBINATIONS and COSTS ARE RESSOURCE OBLIVIOUS! 
    if proj.get_negated():
        stateCombiDict[proj] = getBestStateParallel_negated(proj)
    elif proj.hasKleene():
        stateCombiDict[proj] = getBestStateParallel_kleene(proj)
    else:
        stateCombiDict[proj] = getBestStateParallel(proj)
 
state_parallel_query_latencies = {}

for query in wl: #get non cost oblivious best state combo for complete query
    myRessourceDict = getRessourcesProportional(stateCombiDict[query][3], ressources)
    state_parallel_query_latencies[query] = getCompleteLatency_stateparallel(query, stateCombiDict[query][3],  myRessourceDict)
    
    
def assignstateCombiDict(projection): #should we make this ressource aware?
                if projection.get_negated():
                    if projection in stateCombiDict.keys():
                        mylatency = stateCombiDict[projection] 
                    else:
                        mylatency = getBestStateParallel_negated(projection)
                elif projection.hasKleene():
                    if projection in stateCombiDict.keys():
                        mylatency = stateCombiDict[projection] 
                    else:
                        mylatency = getBestStateParallel_kleene(projection)
                else:
                    if projection in stateCombiDict.keys():
                        mylatency = stateCombiDict[projection] 
                    else:
                        mylatency = getBestStateParallel(projection)  
                return mylatency
    
def getBestLatency(query, projection, mylist):
    
    # ALL KLEENE
    if projection.hasKleene():
        if projection in stateCombiDict.keys():
            mylatency_stateBased = stateCombiDict[projection] 
        else:
            mylatency_stateBased  = getBestStateParallel_kleene(projection)        
        myParallels = [mylatency_stateBased[2]]    
        completeChainCombi = mylatency_stateBased[3]  
        myRessources = (math.prod([totalRate(x) for x in projection.leafs()])/math.prod([totalRate(x) for x in query.leafs()])) * ressources 
        myRessourceDict = getRessourcesProportional(completeChainCombi,myRessources)
        mylatency_stateBased = (getCompleteLatency_stateparallel(projection, completeChainCombi, myRessourceDict), mylatency_stateBased[1], mylatency_stateBased[2])           
        
        myLatency = getLatency_new(projection, [projection.stripKL_simple()], query, 0, projection)
        # it can also be the case that this kind of combination is worse than a state based one which has the projection KL(A) = [A]
        if myLatency[0] < mylatency_stateBased[0]:
            combiDict[projection] = ([projection.stripKL_simple()],projection.stripKL_simple(),0,myLatency[0])
            return myLatency[0]
        else:
            for i in completeChainCombi.keys():
                    #print("miau", i)
                    if not i in combiDict.keys():
                        combiDict[i] = (completeChainCombi[i],[],0,np.inf) 
                # costs of stateParallel are ressource unaware, for bestLatency they should be aware to be comparable with results from other branches
            #print("huhU", projection, mylatency_stateBased[0])
            combiDict[projection] = (mylatency_stateBased[1],list(set(myParallels)),0,mylatency_stateBased[0])                    
            return mylatency_stateBased[0]
        
    # simple two event projs without choices
    elif len(projection) ==2 :
        latency = math.prod([totalRate(x) for x in projection.leafs()]) *2*myscaling
        mycombi = projection.leafs()
        # this is without parallelization
        myParallel = [x for x in projection.leafs() if totalRate(x) == max([totalRate(x) for x in projection.leafs()])]
        combiDict[projection] = (mycombi,myParallel,0,latency)
        return latency
    
    
    # take longest element of mylist, if multiple (choose the one with minimal latency value)
    else:
        # longest element kann auch die gleichen kinder haben, wenn einmal seq(a,b) und einmal and(a,b) 
        if mylist:
            longest_element_ = sorted(mylist, key = lambda x: len(x.leafs()), reverse= True)[0] # USE MINIMAL LONGEST
            longest_element = sorted([x for x in mylist if len(x) == len(longest_element_)], key= lambda x: totalRate(x))[0]    
            # get remaining elements, check if projection over remaining elements in mylist
            rest = list(set(projection.leafs()).difference(set(longest_element.leafs())))            
            # projections over rest
            myProjs = [x for x in mylist if set(rest).issubset(set(x.leafs()))] 
        else:
            rest = projection.leafs()
            myProjs = []
        if len(rest) == 1: # rest contains a single prim event

            mycombi = [longest_element] + rest # use chain combi for rest  
            if numberProjections(mycombi, projection)<=ressources: #myRessources
                
                mylatency = getLatency_new(projection, mycombi, query,0)
                myParallels = [mylatency[1]]
                for ingredient in [x for x in mycombi if len(x)>1]:
                    myParallels += combiDict[ingredient][1]
                combiDict[projection] = (mycombi,list(set(myParallels)),0,mylatency[0])
            else:
                mylatency = assignstateCombiDict(projection)
                myParallels = [mylatency[2]] 
                
                completeChainCombi = mylatency[3]  #add best chain combination of proj to combidict, if subprojs not in combidict, add as well       
                myRessources = (math.prod([totalRate(x) for x in projection.leafs()])/math.prod([totalRate(x) for x in query.leafs()])) * ressources 
                myRessourceDict = getRessourcesProportional(completeChainCombi,myRessources)
                mylatency = (getCompleteLatency_stateparallel(projection, completeChainCombi,  myRessourceDict), mylatency[1], mylatency[2])
                for i in completeChainCombi.keys():
                    if not i in combiDict.keys():
                        combiDict[i] = (completeChainCombi[i],[],0,np.inf) 
                # costs of stateParallel are ressource unaware, for bestLatency they should be aware to be comparable with results from other branches
                #print("huhU", projection, mylatency[0])
                combiDict[projection] = (mylatency[1],list(set(myParallels)),0,mylatency[0])    
                
          
        
        else: # rest yields no beneficial projection and is not only a prim event
                print("This is the case where there is no valid combination, as there are not enough beneficial projections")
                mylatency = assignstateCombiDict(projection)
                
                myParallels = [mylatency[2]]            
                
                completeChainCombi = mylatency[3]  #add best chain combination of proj to combidict, if subprojs not in combidict, add as well
                
                # apply function, such that costs are adjusted, using ressources for estimation
                myRessources = (math.prod([totalRate(x) for x in projection.leafs()])/math.prod([totalRate(x) for x in query.leafs()])) * ressources 
                myRessourceDict = getRessourcesProportional(completeChainCombi,myRessources)
                mylatency = (getCompleteLatency_stateparallel(projection, completeChainCombi,  myRessourceDict), mylatency[1], mylatency[2])
                for i in completeChainCombi.keys():
                    if not i in combiDict.keys():
                        combiDict[i] = (completeChainCombi[i],[],0,np.inf) 
                # costs of stateParallel are ressource unaware, for bestLatency they should be aware to be comparable with results from other branches
                #print("huhU", projection, mylatency[0])
                combiDict[projection] = (mylatency[1],list(set(myParallels)),0,mylatency[0])    
    return mylatency[0]  

def addSingleToKleeneCombiDict(projection):
    myType = projection.leafs()[0]
    myLatency = totalRate(myType) * totalRate(projection) * myscaling
    myCombination = [myType]
    if projection in combiDict.keys():
        if myLatency < combiDict[projection][2]:
            combiDict[projection] = (myCombination, [], 0, myLatency)
    else:
        combiDict[projection] = (myCombination, [], 0, myLatency)
    
 
   
def correctNegated(projection, mycombi):
    neg = projection.get_negated()
    negated_dict = {x: projection.get_context(x) for x in neg}
    for element in mycombi:
        #in a combination negated events shall only appear in projections which also contain their context, or as single prim events
        if len(element)>1:
            for i in negated_dict.keys():
                if i in element.leafs():
                    if not (negated_dict[i][0] in element.leafs() and negated_dict[i][1] in element.leafs()):
                        return False
    return True     
             
def getBestChainCombis(query, shared, ressourceLimit):     
    #myprojlist = [x for x in projsPerQuery[query]] # HERE WE NEED TO RESPECT OPERATOR SEMANTIC -> new function
    if combiDict.keys():
        myprojlist = [x for x in combiDict.keys() if combiDict[x][3] == 0 and query.can_be_used(x)] + [query]
    else:
        myprojlist = [x for x in projsPerQuery[query]]
   
    for projection in sorted(myprojlist, key = lambda x: len(x.leafs())+ int(x.hasKleene())):    # start with all non_kleene + 1 if x.hasKleene()
            #ressourceLimit =int( len(projection) - 1 + ((ressources-len(query)-1)/len(query)-1))
            
            #  print("COMBINATIONS FOR ", projection)
            if len(projection)==1 and isinstance(projection, KL):
                addSingleToKleeneCombiDict(projection)
                continue
            
            mylist = [x for x in myprojlist if len(x.leafs()) < len(projection.leafs()) and set(x.leafs()).issubset(projection.leafs()) and projection.can_be_used_combi(x)] #and not combiDict[x][3] > centralLatency ]     
            if projection.hasKleene():
                mylist += [x for x in [projection.stripKL_simple()] if projection.stripKL_simple() in projrates.keys()] #add projection without kleene
            # for matching SEQ(A,B,C) with AND(A,B,C)
            #mylist = [x for x in myprojlist if (set(x.leafs()).issubset(projection.leafs()) and not x == projection)] #and not combiDict[x][3] > centralLatency ]                 
            if projection in combiDict.keys():
                bestLatency = combiDict[projection][3]
            else:
            #    bestLatency = min([getBestLatency(query, projection, mylist), stateCombiDict[projection][0]])    
                bestLatency = getBestLatency(query, projection, mylist)
            if projection in wl:
               print("QUERY ALERT", combiDict[projection][0])
            #print("OBEN " + str(projection) + ":" + str(bestLatency) + " < " + str(state_parallel_query_latencies[query]) + " ?" + " ressources" + str(ressourceLimit))
            #print([str(x) for x in mylist])
            #print("=============================================")
            #if bestLatency <=  state_parallel_query_latencies[query] and bestLatency > 0: # pruning step, but ressource planning necessary
            getBestTreeCombiRec(query, projection, mylist, [], 0, shared , ressourceLimit, bestLatency)
            #elif bestLatency > state_parallel_query_latencies[query]:                
             #   myprojlist = [x for x in myprojlist if not x == projection]
    return combiDict

def getBestTreeCombiRec(query, projection, mylist, mycombi, mycosts, shared, ressourceLimit, bestLatency): # atm combinations are generated redundantly and also performance could be improved with a hashtable [ -> the projections with which ABC could be combined in a combination for ABCDE are a subset of the projections AB can be combined...]
    if mylist and len(mycombi)<2  and set(sum([[x] if len(x) == 1 else x.leafs() for x in mycombi],[])) != set(projection.leafs()): 
        
        for i in range(len(sorted(mylist, key = lambda x: len(x.leafs()),reverse = True))):            
            proj = mylist[i]
           # print("projection ", projection, " extending" , proj)
            subProjections = [x for x in mylist[i+1:]]
            combiBefore = [x for x in  mycombi]
            mycombi.append(proj)
            # also for redundancy
            subProjections = [x for x in subProjections  if not set(mylist[i].leafs()).issubset(set(x.leafs())) or set(x.leafs()).issubset(set(mylist[i].leafs()))]
            mycombiEvents = ''.join(list(set(list("".join(list(map(lambda x: ''.join(x.leafs()), mycombi)))))))
            subProjections = [x for x in subProjections if not set(x.leafs()).issubset(set(list(mycombiEvents))) and not set(list(mycombiEvents)).issubset(set(x.leafs()))]            
            
            if len(mycombi) == 1:
                currentProj = mycombi[0]
            else:
                currentProj = settoproj(list(set(sum([x.leafs() for x in mycombi],[]))),projection)      
            if projection in combiDict.keys() and combiDict[projection][3] < bestLatency:
               bestLatency = combiDict[projection][3]
            if getLatency_new(currentProj, mycombi,query, ressourceLimit,projection)[0] < bestLatency:
                ##### fill each intermediate combination with primitive events to generate new combination 
                _missingEvents =   list(set(projection.leafs()).difference(set(''.join(map(lambda x: ''.join(x.leafs()), mycombi)))))
                if _missingEvents and len(_missingEvents) + len(mycombi) < 3:
                    _missingEvents += mycombi
                    
                    getBestTreeCombiRec(query, projection, [], _missingEvents, mycosts, shared, ressourceLimit, bestLatency)
                    
                #exclude redundant combinations
                mycombiEvents = ''.join(list(set(list("".join(list(map(lambda x: ''.join(x.leafs()), mycombi)))))))
                subProjections = [x for x in subProjections if not set(x.leafs()).issubset(set(list(mycombiEvents))) and not set(list(mycombiEvents)).issubset(set(x.leafs()))]
               
                #exclude subprojections containing parallelized type but not parallelizing respective type
                #myParallelized = list(set(sum([combiDict[x][1] for x in mycombi if not isinstance(combiDict[x][1],Tree)],[])))
                #subProjections = [x for x in subProjections if (not list(set(x.leafs()).intersection(set(myParallelized)))) or  (list(set(x.leafs()).intersection(set(myParallelized))) and set(x.leafs()).intersection(set(myParallelized)).issubset(set(combiDict[x][1])))]
                
                # maybe: exclude combination which parallelize multiple times on same typ with different projs                
                # subProjections = [x for x in subProjections if not list(set(combiDict[x][1]).intersection(set(myParallelized)))]
                
                getBestTreeCombiRec(query, projection, subProjections, mycombi, mycosts, shared, ressourceLimit, bestLatency)
                mycombi =  combiBefore
            else:
                mycombi =  combiBefore
                continue
    else: 
       
       if not mycombi or set(sum([[x] if len(x) == 1 else x.leafs() for x in mycombi],[])) != set(projection.leafs()):  #not even one ms placeable subprojection exists ?
           return       
       else: # only correct combination which match the projection 
                         
                        if correctNegated(projection, mycombi):       
                             
                           (mycosts, myLatency) = costsOfCombination(projection, mycombi, shared, ressourceLimit)      
                         #  print(list(map(lambda x:str(x), mycombi)), mycosts)
                          # if projection == wl[0]:
                           #    print("HMMIIIMAIMAMIAAM",  numberProjections(mycombi, projection), ressourceLimit)
                           if myLatency[0] < combiDict[projection][3] and numberProjections(mycombi, projection) < ressourceLimit: # somehow support sharing -> numberProjections - size of subtree of shared projection
                               
                               combiDict[projection] = (mycombi, myLatency[1], mycosts, myLatency[0])
                 

def numberProjections(combi, projection):
    mySubCombi = unfold_combi(projection, combi)
    return len(list(set(list(mySubCombi.keys())))) +1 


def costsOfCombination(projection, mycombi, shared, ressourceLimit): # here is a good spot to check the combinations that are actually enumerated during our algorithm
       mycosts = 0
       #myLatency = getLatency_new_(projection, mycombi) # ressource unaware/oblivious
       myLatency = getLatency_new(projection, mycombi,query, ressourceLimit,projection) # ressource aware
       myParallelEvents = [myLatency[1]]       
       for ingredient in [x for x in mycombi if len(x) > 1]:
           if not isinstance(combiDict[ingredient][1], Tree):
               myParallelEvents += combiDict[ingredient][1]
       return (mycosts, (myLatency[0],myParallelEvents)) # add sum of rates of inputs
   
def completeCombi(combi, proj):
    return set(proj.leafs()) == set(sum([x.leafs() if len(x)>1  else [x] for x in combi],[]))
   
def getLatency_new_(proj, combi): 
    if len(combi) ==1:
        if len(combi[0].leafs()) == 0:
            return 
        elif len(combi[0])>1 and combiDict[combi[0]][3] == 0 and completeCombi(combi, proj): # e.g. use AND(A,B,C) for SEQ(A,B,C) as combination after AND(A,B,C) was chosen as projection in combination of previous query
            return 0, combiDict[combi[0]][1]
   # print(proj, list(map(lambda x: str(x), combi)))   
    # if combi comes from bestchain, then we cannot take combiDict here    
    latency = max([combiDict[x][3] for x in combi if len(x) > 1]+[0]) # use total latency instead
    #latency = sum([combiDict[projection][3] for projection in unfold_combi(proj, combi).keys() if not projection == proj])

    send_reduction = {x:1 for x in combi}
    if os_combigen:
        if len(combi) > 1 and  (len(combi[0])>1 and len(combi[1])>1):
            overlap = list(set(combi[0].leafs()).intersection(set(combi[1].leafs())))
            for i in overlap:
                myproj = [x for x in combi if singleSelectivities[getKeySingleSelect(i,x)] == min(list(map(lambda y: singleSelectivities[getKeySingleSelect(i,y)], combi)))][0]
                for proj in combi:
                    if not proj == myproj:
                        send_reduction[proj] =  send_reduction[proj] * singleSelectivities[getKeySingleSelect(i,proj)] * totalRate(i)
                        
    myrates = {x:totalRate(x)/send_reduction[x] for x in combi}

    latency += 2*math.prod(list(myrates.values())) 

    # compute actual latency: for each element in combi, generate list of partial matches / respective projections and multiply with rates
    maximal_primitive_rate = [x for x in combi if myrates[x] == max(list(map(lambda x: myrates[x],combi)))] #maximal type
    return latency,maximal_primitive_rate[0]


def getKleeneLatency(proj, myinput, query): #unary combination
    # get myRessources    
    latency = totalRate(proj) * totalRate(myinput)
    if latency <  stateCombiDict[query][0]:
        myRessources = round(ressources * (latency/ stateCombiDict[query][0]))+1
    else: 
        myRessources = 1
    if myinput == proj.kleene_components()[0]:
        myRessources = 1
    return latency/myRessources + combiDict[myinput][3] , ''


def getLatency_new(proj, combi, query, ressourceLimit, projection = None): # adjust ressources per proj by number of projections, automatically favors sharing # ADD VERSION THAT RECOMPUTES FOR ALL PROJS
    #print(proj, projection,[str(x) for x in combi])
    if len(combi) ==1:   
       
       if len(combi[0].leafs()) == 0:           
            return 0
       elif len(combi[0])>1 and combiDict[combi[0]][3]==0 and completeCombi(combi, proj):  # e.g. use AND(A,B,C) for SEQ(A,B,C) as combination after AND(A,B,C) was chosen as projection in combination of previous query
           return 0, combiDict[combi[0]][1]
       elif proj in combiDict.keys(): #this should only be the case 
           if set(projection.leafs()) == set(proj.leafs()) and projection.hasKleene():
               # costs for ac -> ac+ computation
               return getKleeneLatency(projection,proj,query)
           else:    
              return combiDict[proj][3],  combiDict[proj][1]
       elif set(projection.leafs()) == set(combi[0].leafs()) and projection.hasKleene():
           return getKleeneLatency(projection,combi[0],query)
    if proj.hasKleene(): #projection with non kleene types to obtain kleene
       # prim event in combi can nnot has kleene
        
        if (isinstance(combi[0],str) or not combi[0].hasKleene()) and (isinstance(combi[1],str) or not combi[1].hasKleene()):
            
            myRessources = ressources * ( getBestStateParallel(proj)[0]/ stateCombiDict[query][0])  
            # TODO COMPUTE COSTS FOR SEQ(A,KL(B),C) FROM A, SEQ(BC)
            return np.inf, ''
            # add latency of child 
            
    # use ratio of latency of ressource unaware best state parallel combination
    if proj in stateCombiDict.keys():
        latency = stateCombiDict[proj][0] 
    else:
        if proj in stateCombiDict.keys():
            latency = stateCombiDict[proj]
        elif proj.get_negated():
            latency = getBestStateParallel_negated(proj)[0]
        else:
            latency = getBestStateParallel(proj)[0]
            
    #if proj == query:
    #        myRessources  = ressources
    #else:
    #        myRessources = ressources * (latency/ stateCombiDict[query][0])   
        
    if proj == query:
             myRessources  = ressourceLimit
    else:
             myRessources = ressourceLimit * (latency/ stateCombiDict[query][0])  
        
    #print("HIIII "  + str(proj) + "state parallel latency: " + str(latency) + " combi: " + str(list(map(lambda x: str(x), combi))))# " Ressources: " + str(myRessources) )
  
    
    #get ressource Dict -> first ressource dict, then os dict actually! 
    if os_combigen:
        os_dict = get_os(unfold_combi(proj, combi))
        ressourceDict =  getRessourcesProportional_os(unfold_combi(proj, combi), myRessources, os_dict)
    else:
        os_dict = {}
        ressourceDict =  getRessourcesProportional(unfold_combi(proj, combi), myRessources)   
        
    #ressourceDict = getRessources_depth(proj, unfold_combi(proj, combi), myRessources)     # TODO FIX   
        
    latency = getCompleteLatency_os_proj(proj, unfold_combi(proj, combi), ressourceDict, os_dict)
 
    myrates = {x:totalRate(x) for x in combi}    
    maximal_primitive_rate = [x for x in combi if myrates[x] == max(list(map(lambda x: myrates[x],combi)))] #maximal type
    return latency,maximal_primitive_rate[0]




def getRessources_depth(projection, myCombidict, ressources):
    myressources = ressources - len(myCombidict.keys())
    
    # for multi-query, it can be that there are not even enough ressources 
    if myressources <= 0:
         ressourceDict =  {x: 1 for x in myCombidict.keys()}
         return ressourceDict
     #if this is the case return one ressource per projection
     
    comparisons = [getComparisons_simple(x,  myCombidict[x]) for x in myCombidict.keys() if len(x)>1]
    portion_comparisons = {x: (getComparisons_simple(x, myCombidict[x]) / sum(comparisons)) * myressources for x in myCombidict.keys() }
    # tiefe jeder projection, die keine projektionen als kinder hat, summe davon für nenner
    leafs = [x for x in myCombidict.keys() if depth(x, myCombidict[x],myCombidict) == 1]
    leaf_depth = {x: depth_reverse(x,myCombidict) for x in leafs}
    
    # zuordnung einer projektion ihres urenkels mit der tiefsten tiefe
    grandchildren_dict = {x: [y for y in grandchildren(x,myCombidict[x]) if leaf_depth[y] == max([leaf_depth[z] for z in grandchildren(x,myCombidict[x])])][0] for x in myCombidict.keys()}
    # jede projektion bekommt lokale tiefe / tiefe aller längsten pfade (summe über die blätter)       
    totaldepth = sum(list(leaf_depth.values())) #TODO  what about branching! 
    depth_ratio = {x: leaf_depth[grandchildren_dict[x]]/totaldepth for x in grandchildren_dict.keys()}
    # für keden knoten anzahl der vergleiche/totale_vergleiche * ressourcen * anteilige tiefe
    portions_comparisons_depth = {x: portion_comparisons[x]  * depth_ratio[x] for x in portion_comparisons.keys()}
    # verbrauchte ressourcen aufsummieren
    ressources_sofar = sum(list(portions_comparisons_depth.values()))
    #anteil lokale verbrauchter / total verbrauchte ressourcen * gesamt ressourcen pro knoten ausgeben
    final_ressourceDict = {x: round((portions_comparisons_depth[x]/ressources_sofar)* myressources) for x in portions_comparisons_depth}
    rest = myressources  - sum(list(final_ressourceDict.values()))
    # add rest to maximal 
    ressourceDict = {x:final_ressourceDict[x] if final_ressourceDict[x]> 1 else 1 for x in final_ressourceDict.keys()}
    return ressourceDict



def getRessources_depth_os_final(myCombidict, os_dict):
    myressources = ressources - len(myCombidict.keys())
    comparisons = [getComparisons_simple_os(x,  myCombidict[x], os_dict) for x in myCombidict.keys() if len(x)>1]
    portion_comparisons = {x: (getComparisons_simple_os(x,  myCombidict[x], os_dict) / sum(comparisons)) * myressources for x in myCombidict.keys() }
    # tiefe jeder projection, die keine projektionen als kinder hat, summe davon für nenner
    leafs = [x for x in myCombidict.keys() if depth(x, myCombidict[x],myCombidict) == 1]
    leaf_depth = {x: depth_reverse(x,myCombidict) for x in leafs}

    # zuordnung einer projektion ihres urenkels mit der tiefsten tiefe
    grandchildren_dict = {x: [y for y in grandchildren(x, myCombidict[x]) if leaf_depth[y] == max([leaf_depth[z] for z in grandchildren(x,myCombidict[x])])][0] for x in myCombidict.keys()}
    # jede projektion bekommt lokale tiefe / tiefe aller längsten pfade (summe über die blätter)
    
    
    totaldepth = sum(list(leaf_depth.values())) #TODO what about branching
    
    
    depth_ratio = {x: leaf_depth[grandchildren_dict[x]]/totaldepth for x in grandchildren_dict.keys()}
    # für keden knoten anzahl der vergleiche/totale_vergleiche * ressourcen * anteilige tiefe
    portions_comparisons_depth = {x: portion_comparisons[x]  * depth_ratio[x] for x in portion_comparisons.keys()}
    # verbrauchte ressourcen aufsummieren
    ressources_sofar = sum(list(portions_comparisons_depth.values()))
    #anteil lokale verbrauchter / total verbrauchte ressourcen * gesamt ressourcen pro knoten ausgeben
    final_ressourceDict = {x: round((portions_comparisons_depth[x]/ressources_sofar)*myressources) for x in portions_comparisons_depth}
    rest = myressources - sum(list(final_ressourceDict.values()))
    # add rest to maximal 
    ressourceDict = {x:final_ressourceDict[x] +1 for x in final_ressourceDict.keys()}
    return ressourceDict


def getRessources_depth_final(myCombidict):
    myressources = ressources - len(myCombidict.keys())
    comparisons = [getComparisons_simple(x,  myCombidict[x]) for x in myCombidict.keys() if len(x)>1]
    portion_comparisons = {x: (getComparisons_simple(x,  myCombidict[x]) / sum(comparisons)) * myressources for x in myCombidict.keys() }
    # tiefe jeder projection, die keine projektionen als kinder hat, summe davon für nenner
    leafs = [x for x in myCombidict.keys() if depth(x, myCombidict[x],myCombidict) == 1]
    leaf_depth = {x: depth_reverse(x,myCombidict) for x in leafs}

    # zuordnung einer projektion ihres urenkels mit der tiefsten tiefe
    grandchildren_dict = {x: [y for y in grandchildren(x, myCombidict[x]) if leaf_depth[y] == max([leaf_depth[z] for z in grandchildren(x,myCombidict[x])])][0] for x in myCombidict.keys()}
    # jede projektion bekommt lokale tiefe / tiefe aller längsten pfade (summe über die blätter)
    
    totaldepth = sum(list(leaf_depth.values())) #TODO what about branching
        
    depth_ratio = {x: leaf_depth[grandchildren_dict[x]]/totaldepth for x in grandchildren_dict.keys()}
    # für keden knoten anzahl der vergleiche/totale_vergleiche * ressourcen * anteilige tiefe
    portions_comparisons_depth = {x: portion_comparisons[x]  * depth_ratio[x] for x in portion_comparisons.keys()}
    # verbrauchte ressourcen aufsummieren
    ressources_sofar = sum(list(portions_comparisons_depth.values()))
    #anteil lokale verbrauchter / total verbrauchte ressourcen * gesamt ressourcen pro knoten ausgeben
    final_ressourceDict = {x: round((portions_comparisons_depth[x]/ressources_sofar)*myressources) for x in portions_comparisons_depth}
    rest = myressources - sum(list(final_ressourceDict.values()))
    # add rest to maximal 
    ressourceDict = {x:final_ressourceDict[x] +1 for x in final_ressourceDict.keys()}
    return ressourceDict

  

def getInputRates_os(proj,combi,os_dict,nodes):
    if os_dict:
        my_os = os_dict[proj]      
    else: 
        my_os = []
    inputrates = []
    sendReduction = 1
    for subproj in combi:
        if my_os:
            sendReduction = [x[0] for x in my_os if subproj != x[1]]
            sendReduction = math.prod([totalRate(x) * singleSelectivities[getKeySingleSelect(x,subproj)] for x in sendReduction])
        inputrates.append(totalRate(subproj) / sendReduction)    
    inputrates = sorted(inputrates)
    return inputrates[0]/len(nodes) + inputrates[1]


def getComparisons_simple_os(proj, combi, os_dict): # with output selection, only 2 part combinations
    latency = 1
    if os_dict:
        my_os = os_dict[proj]      
    else: 
        my_os = []
    if not my_os:
        return getComparisons(proj,combi)
    for subproj in combi:
        if my_os:
            sendReduction = [x[0] for x in my_os if subproj != x[1]]
            sendReduction = math.prod([totalRate(x) * singleSelectivities[getKeySingleSelect(x,subproj)] for x in sendReduction])
            # todo: problematic if sendReduction < 1
            # os_dict should be updated to avoid send reductions smaller that 1
            if sendReduction <1:
                sendReduction = 1
        else:
            sendReduction = 1
        latency *=  (totalRate(subproj) / sendReduction)       
    latency*=2
    latency *= myscaling
    #myMax = [x for x in combi if totalRate(x) == max([totalRate(y) for y in combi])][0]# if totalRate(x) == max(sum([totalRate(y) for y in completeCombi[proj]]))][0]
    #latency += totalRate(myMax) + totalRate([x for x in combi if not x == myMax][0])
    latency += sum([myscaling * totalRate(x) for x in combi])
    return latency



def getRessourcesProportional_os(completeCombi, ressources,os_dict): #without depth
    portions = {x: getComparisons_simple_os(x,completeCombi[x],os_dict) for x in completeCombi.keys()}
    total = sum(list(portions.values()))
    ressourceDict = {x:round((portions[x]/total)*ressources)  for x in portions.keys()}
    ressourceDict = {x: ressourceDict[x] if ressourceDict[x]>1 else 1 for x in ressourceDict.keys()}
    if sum(list(ressourceDict.values())) > ressources:
       singleSinks = [x for x in ressourceDict.keys() if ressourceDict[x] <2 ]
       multiSinks = list(set(list(ressourceDict.keys())).difference(set(singleSinks)))
       portions = {x: getComparisons_simple_os(x,completeCombi[x],os_dict) for x in multiSinks}
       total = sum(list(portions.values()))
       for i in multiSinks:
           ressourceDict[i] = round((portions[i]/total)*(ressources - len(singleSinks)) )
    #DIRTY
    potentialRest = ressources - sum(list(ressourceDict.values()))
    if potentialRest > 0:
       maximalParallel = [x for x in ressourceDict.keys() if ressourceDict[x] == max(list(map(lambda x: ressourceDict[x], ressourceDict.keys())))][0]
       ressourceDict[maximalParallel] = ressourceDict[maximalParallel] + potentialRest
    return ressourceDict 



def getKeySingleSelect(primEvent, projection):
    myString = primEvent + "|" + "".join(sorted(projection.leafs()))
    return myString

def get_os(mycombi):
    filterDict = {}
#    if len(mycombi) > 1:
    for projection in mycombi:
            filterDict[projection] = []
            if len(mycombi[projection]) > 1:
                if (len(mycombi[projection][0])>1 and len(mycombi[projection][1])>1):
                    combi = mycombi[projection]
                    overlap = list(set(combi[0].leafs()).intersection(set(combi[1].leafs())))
                    for i in overlap:
                        myproj = [x for x in combi if singleSelectivities[getKeySingleSelect(i,x)] == min(list(map(lambda y: singleSelectivities[getKeySingleSelect(i,y)], combi)))][0]
                        filterDict[projection].append((i,myproj))
    return filterDict



        
def getCompleteLatency(completeCombi, ressourceDict):
    myLatencyDict = {}
    myRessources = 1
    for proj in sorted(completeCombi.keys(), key = lambda x: len(x.leafs()) + int(x.hasKleene())):
        if ressourceDict[proj] <2:
            myRessources = 1
        else:
            myRessources = ressourceDict[proj]
        latency = getComparisons(proj, completeCombi[proj]) /  myRessources
        latency += sum([totalRate(x) for x in completeCombi[proj]])
        mySubs = [x for x in completeCombi[proj] if len(x)>1]
        if mySubs:
            latency += max(myLatencyDict[x] for x in mySubs)
            #latency += sum(myLatencyDict[x] for x in mySubs)
        myLatencyDict[proj] = latency   
        
    return max([myLatencyDict[query] for query in wl])



def getCompleteLatency_os_proj(proj, completeCombi, ressourceDict,os_dict): 
    myLatencyDict = {}
    myRessources = 1

    # if we have a projection which has another projection as only input to its combination, then start with the one having two inputs 
    fixList = sorted(list(completeCombi.keys()), key = lambda x: len(x.leafs()))
   
    order = {x:  fixList.index(x) for x in completeCombi.keys()}
    # get all tuples of projection where one projection is the only input of the combination of another one, 
    singles = [(x, completeCombi[x][0]) for x in completeCombi.keys()  if len(completeCombi[x]) == 1 ]
    
    for pair in singles:
      if pair[0] in order.keys() and pair[1] in order.keys():
            if order[pair[0]] < order[pair[1]]:
                inter = order[pair[1]] 
                order[pair[1]] = order[pair[0]]
                order[pair[0]] = inter
    #for proj in sorted(completeCombi.keys(), key = lambda x: len(x.leafs())): #add tie breaker
    
    for proj in sorted(completeCombi.keys(), key = lambda x: order[x]):     
        if ressourceDict[proj] <2:
            myRessources = 1
        else:
            myRessources = ressourceDict[proj]
        latency = getComparisons_simple_os(proj, completeCombi[proj],os_dict) / myRessources 
        
        # add sum of input, divide rate of max input by myRessources
        myMax = [x for x in completeCombi[proj] if totalRate(x) == max([totalRate(y) for y in completeCombi[proj]])][0]# if totalRate(x) == max(sum([totalRate(y) for y in completeCombi[proj]]))][0]
        latency += totalRate(myMax) / myRessources 
        if len(completeCombi[proj])>1:
            latency +=  totalRate([x for x in completeCombi[proj] if not x == myMax][0])
        
        mySubs = [x for x in completeCombi[proj] if len(x)>1]
        if mySubs:
            latency += max(myLatencyDict[x] for x in mySubs) #TODO here we can substitute with sum!!!!
           # latency += sum(myLatencyDict[x] for x in mySubs)
        myLatencyDict[proj] = latency       
    return myLatencyDict[proj]

def getCompleteLatency_os(completeCombi, ressourceDict,os_dict):
    myLatencyDict = {}
    myRessources = 1
    for proj in sorted(completeCombi.keys(), key = lambda x: len(x.leafs()) + int(x.hasKleene())):
        if ressourceDict[proj] <2:
            myRessources = 1
        else:
            myRessources = ressourceDict[proj]
        latency = getComparisons_simple_os(proj, completeCombi[proj],os_dict) /  myRessources
        mySubs = [x for x in completeCombi[proj] if len(x)>1]
        if mySubs:
            #latency += max(myLatencyDict[x] for x in mySubs)
            latency += sum(myLatencyDict[x] for x in mySubs)
        myLatencyDict[proj] = latency       
    return max([myLatencyDict[query] for query in wl]) # here this may be also sum


def getSequenceTuples(sequenceDict):
    sequences = []
    for event in sequenceDict.keys():
        if sequenceDict[event]:
            for second in sequenceDict[event]:
                sequences.append((event,second))
    #todo: minimal set of sequences - eliminate what is given by transitivity
    #for constraint in totalSequences:
        # get all tuples which have constraint[0] as tuple[0] and constraint[1] as tuple[1], in the list of those tuples, there is pair x,y which has x[1]=[y] then remove constraint
    
    return sequences
      
def getIDConstraint(combination, query): # enrich with osdict
    if len(combination)>1:
        combi = [x.leafs() if len(x) > 1 else [x] for x in combination]
        return list(set(combi[0]).intersection(set(combi[1])))  
    elif query.hasKleene():
        return [x for x in query.leafs() if not x == query.kleene_components()[0]]
  #  return []   

def getSequenceCostraints(combination, query):
    querySequences = getSequenceTuples(query.getsequences())
    firstSequences = []
    secondSequences=[]
    if len(combination) > 1:
        if len(combination[0])>1:
            firstSequences = getSequenceTuples(combination[0].getsequences())
        else:
            firstSequences = []
    
        if len(combination[1])>1:
            secondSequences = getSequenceTuples(combination[1].getsequences())
        else:
            secondSequences = []
    totalSequences = [x for x in querySequences if not x in firstSequences + secondSequences]     
    return totalSequences

def getPredicateConstraints(combination, query):
    predicateConstraints = []
    combination = sorted(combination, key = lambda x:  len(x.leafs()) if isinstance(x, Tree) else len(x))
    left = [combination[0].leafs() if len(combination[0])>1 or isinstance(combination[0], Tree) else combination[0]][0]
    right = []
    if len(combination)>1:
        right = [combination[1].leafs() if len(combination[1])>1 else combination[1]][0]
    if right:
        for i in left:
            for k in right:
                #print(i,k)
                if not  i in right and not k in left and not i ==  k:
                #if not i in right:
                    predicateConstraints.append((k,i))
    else:
        kleeneType = query.kleene_components()[0] 
        predicateConstraints.append((str(kleeneType),str(kleeneType)))
    return predicateConstraints

# def getPredicateConstraints_statebased(combination, query):
#     predicateConstraints = []
    
        
#     left = [combination[0].leafs() if len(combination[0])>1 else combination[0]][0]
#     right = []
#     if len(combination)>1:
#         right = [combination[1].leafs() if len(combination[1])>1 else combination[1]][0]
#     if right:
#         for i in left:
#             for k in right:
#                 #print(i,k)
#                 if not  i in right and not k in left and not i ==  k:
#                 #if not i in right:
#                     predicateConstraints.append((k,i))

#     return predicateConstraints


def getStateParallelCombi(query):
     combi = {}
     for i in range(1,len(query.leafs())):
         myproj = settoproj(query.leafs()[0:i+1], query)
         combi[myproj] = [settoproj(query.leafs()[0:i], query), query.leafs()[i]] #TODO: set to string
         combi[myproj] = [x.leafs()[0] if (isinstance(x,PrimEvent) or (isinstance(x,Tree) and len(x)==1 and x.hasKleene()))  else x for x in combi[myproj]]
     return combi
 
def plotCombi(combi):
    G = nx.Graph()
    G.add_nodes_from(list(map(lambda x: str(x), combi.keys())))
    for query in wl:
        for e in query.leafs():
            if not e in G.nodes:
                G.add_node(e)
    for i in combi.keys():
        for k in combi[i]:
            G.add_edge(str(i),str(k))
    nx.draw(G, with_labels=True, font_weight='bold')
    plt.show() 
    

def main():     
    # add ressources as additional parameter
    global ressources
    global totalProjRates
    ressources = 20
    global parallel
    global os_combigen
    global os_afterprocessing
    
    os_combigen = 0
    parallel = 1 # if parallel = 0, only for single, stateparalle, llsf plans computed (no combination enumeration)
    tw = 1
    if len(sys.argv) > 1: #save to file
         ressources =int(sys.argv[1])
    if len(sys.argv) > 2: #save to file
         parallel=int(sys.argv[2])     
    if len(sys.argv)>3:
        tw = int(sys.argv[3])     
    if len(sys.argv)>4:
        os_combigen = int(sys.argv[4])
        
    os_afterprocessing = os_combigen
    
    # initialize for binary search
    stateparallelOk = 0
    parallelOk = 0
    singleOk = 1
    llsfOk = 1
   
    shared = 2
    start_time = time.time()
    state_parallel_combi = {}
    available_ressources = ressources - (sum([len(x.leafs()) for x in wl]) - len(wl))
    
    # set for server / pi experiments accordingly -> as long as a comparison takes 1ms, compLimit is fix
    rateLimit = 6000
    compLimit = 60000/tw
    
    singleRate = 0
    singleComps = 0
    
    portions = {query: round((state_parallel_query_latencies[query]/sum(list(state_parallel_query_latencies.values())))*available_ressources)  for query in wl} # use portion based on stateparallel estimate, to estimate available ressources per query
    print(portions)
    

    
    # if available_resources > 0 
   # for query in sorted(wl, key = (lambda x: len(projsPerQuery[x])), reverse=True): #start with queries having the least projections, try other sortings...
    for query in sorted(wl, key = (lambda x: state_parallel_query_latencies[x]), reverse = True):
            state_parallel_combi.update(getStateParallelCombi(query))         #getStateParallelCombi, for final result
            #print("STARTING : ", query, " inputs: ", list(map(lambda x:str(x),projsPerQuery[query] )))
            if parallel:
                getBestChainCombis(query, shared, ressources) # use only already used projections
            
                print("---------")
                print(query, combiDict[query][3])            
                myprojections = unfold_combi(query,combiDict[query][0]).keys()
                for i in myprojections:
                    combiDict[i] = (combiDict[i][0],combiDict[i][1] ,0,0) # set latencies to 0 to foster decision to share        
    
    for i in portions.keys():
        print(i, portions[i])

    end_time = time.time() 
    combigenTime = round(end_time - start_time,2)
     
    if parallel:       # if parallelplan necessary
        curcombi = {}         
        for i in range(len(wl)):        
            if wl[i] in combiDict.keys():
                curcombi.update(unfold_combi(wl[i], combiDict[[wl[i]][0]][0]))    
        mycombi = curcombi    
    else:
        mycombi = {}
        for query in wl:
            mycombi.update(stateCombiDict[query][3])
        curcombi = mycombi
        
    if os_afterprocessing:
        os_dict = get_os(mycombi)
        print(os_dict)
        ressourceDict = getRessourcesProportional_os(mycombi,ressources,os_dict)   
        for i in mycombi.keys():
            print("Complete Comparisons (/os): ", i, getComparisons(i,mycombi[i]),getComparisons_simple_os(i,mycombi[i],os_dict))  
    else:
         os_dict ={}
         for i in mycombi.keys():
            print("Complete Comparisons (/os): ", i, getComparisons(i,mycombi[i]))
            ressourceDict = getRessourcesProportional_os(mycombi,ressources,os_dict)           
       #  ressourceDict = getRessourcesProportional_final(mycombi,ressources)        
        # ressourceDict =  getRessources_depth_final(mycombi)
    count = 0
    PlacementDict = {} # PlacementDict is input for Evaluation Plan Generation and contains, projection, placement, combi, partitioning type
    for pro in curcombi.keys():
        mynodes = []        
        for i in range(count, count+ressourceDict[pro]):
            mynodes.append(i)
            count+=1            
        #  compute parallelized type of projection      
        if ressourceDict[pro]>1:            
            myPart = [x for x in curcombi[pro] if totalRate(x) == max(list(map(lambda x: totalRate(x),curcombi[pro])))][0]
        else:
            myPart = ''
        print("----------------")
        if not list(os_dict.keys()):
            maxRate =  max([totalRate(x)*myscaling for x in mycombi[pro]])/len(mynodes) + sum([totalRate(x)*myscaling  for x in mycombi[pro]]) - max([totalRate(x)*myscaling  for x in mycombi[pro]])
            maxComps =  (math.prod([totalRate(x) for x in mycombi[pro]])*2 *myscaling)/len(mynodes)
        else:
            maxRate =  getInputRates_os(pro,mycombi[pro],os_dict,mynodes)
            maxComps = getComparisons_simple_os(pro, mycombi[pro],os_dict)  /len(mynodes)
        if pro.hasKleene() and len(mycombi[pro]) == 1:
            maxComps = (myscaling * totalRate(pro) * totalRate(mycombi[pro][0]))/len(mynodes)
            
        if maxRate > rateLimit:
            parallelOk = 1
            print("INRATE NOT OK ")
        if maxComps > compLimit:
            parallelOk = 2
            print("COMPS NOT OK ")
        if maxComps > compLimit and maxRate > rateLimit:
           parallelOk = 3
           print("BOTH NOT OK ") 
           
        print(str(pro) + " " + str(list(map(lambda x: str(x), mycombi[pro]))), ressourceDict[pro], mynodes, myPart, maxRate, maxComps)
        PlacementDict[pro] = (mynodes, myPart,mycombi[pro])
    
    if os_afterprocessing:    
        print("TOTAL LATENCY: ", getCompleteLatency_os(curcombi, ressourceDict,os_dict))
        print("TOTAL LATENCY: ", getCompleteLatency(curcombi, ressourceDict))
    else:
        print("TOTAL LATENCY: ", getCompleteLatency(curcombi, ressourceDict))
    
    constraintDict = {} # for the flink based evaluation, used to evaluate sequences and overlapping combinations    
    for i in curcombi.keys():    
        constraintDict[i] = (getSequenceCostraints(curcombi[i],i),getIDConstraint(curcombi[i], i),getPredicateConstraints(curcombi[i],i))
       # print(i, constraintDict[i])
    #### STATE PARALLEL VERSION 
    print("---------------")
    print("STATE PARALLEL")
    print("---------------")
    constraintDict_stateParallel = {}
    count = 0
    ressourceDict_state_parallel = getRessourcesProportional(state_parallel_combi,ressources) # ressourceDict State Parallel
    for i in state_parallel_combi.keys():
        print(i, getPredicateConstraints(state_parallel_combi[i],i))
        constraintDict_stateParallel[i] = (getSequenceCostraints(state_parallel_combi[i],i),getIDConstraint(state_parallel_combi[i], i),getPredicateConstraints(state_parallel_combi[i],i))
    
    
    PlacementDict_state_parallel = {} # PlacementDict is input for Evaluation Plan Generation and contains, projection, placement, combi, partitioning type
    for pro in state_parallel_combi.keys():
        mynodes = []        
        for i in range(count, count+ressourceDict_state_parallel[pro]):
            mynodes.append(i)
            count+=1     
            
        # PARTPROJ COMPUTATION     
        if  ressourceDict_state_parallel[pro]>1:     #TODO generate constraint on  llsf here     
            if pro.hasKleene() and pro.kleene_components()[0] in state_parallel_combi[pro]:
                myPart = [x for x in  state_parallel_combi[pro] if not x in pro.kleene_components()][0]
            else:    
                myPart = [x for x in state_parallel_combi[pro] if totalRate(x) == max(list(map(lambda x: totalRate(x),state_parallel_combi[pro])))][0]
        else:
            myPart = ''
        
        print("----------------")
        
        maxRate =  max([totalRate(x)*myscaling for x in state_parallel_combi[pro]])/len(mynodes) + sum([totalRate(x) *myscaling for x in state_parallel_combi[pro]]) - max([totalRate(x)*myscaling for x in state_parallel_combi[pro]])
        maxComps =  (math.prod([totalRate(x) for x in state_parallel_combi[pro]])*2*myscaling)/len(mynodes)

        if pro.hasKleene() and pro.kleene_components()[0] in state_parallel_combi[pro]:
            maxComps = totalRate([x for x in state_parallel_combi[pro] if not x == pro.kleene_components()[0]][0])  * totalRate(settoproj(str(pro.kleene_components()[0]),pro)) 
            maxComps += totalRate(pro.kleene_components()[0]) * (totalRate(pro) + totalRate([x for x in state_parallel_combi[pro] if not x == pro.kleene_components()[0]][0]) +totalRate(settoproj(str(pro.kleene_components()[0]),pro)))
            maxComps = maxComps/len(mynodes)
            
        if maxRate > rateLimit:
            stateparallelOk = 1
            print("INRATE NOT OK ")
        if maxComps > compLimit:
            stateparallelOk = 2
            print("COMPS NOT OK ")
        if maxComps > compLimit and maxRate > rateLimit:
           stateparallelOk = 3
           print("BOTH NOT OK ") 
            
        maxRate_llsf =  max([totalRate(x) for x in state_parallel_combi[pro] if len(x) == 1])/len(mynodes) + sum([totalRate(x) for x in state_parallel_combi[pro]]) - max([totalRate(x) for x in state_parallel_combi[pro] if len(x) == 1])
        if maxRate_llsf > rateLimit or maxComps > compLimit: #maxcomps > 2000000
            llsfOk = 0
            
        singleRate += sum([totalRate(x)*myscaling for x in state_parallel_combi[pro]])
        if pro.hasKleene() and pro.kleene_components()[0] in state_parallel_combi[pro]:
            singleComps += totalRate([x for x in state_parallel_combi[pro] if not x == pro.kleene_components()[0]][0])  * totalRate(settoproj(str(pro.kleene_components()[0]),pro)) 
            singleComps += totalRate(pro.kleene_components()[0]) * (totalRate(pro) + totalRate([x for x in state_parallel_combi[pro] if not x == pro.kleene_components()[0]][0]) +totalRate(settoproj(str(pro.kleene_components()[0]),pro)))
        else:
            singleComps += math.prod([totalRate(x) for x in state_parallel_combi[pro]])*2*myscaling
        if singleRate>rateLimit or singleComps>compLimit:
            singleOk = 0 #is it sum of inputs/comparisons or a single automaton?
            
        print(str(pro) + " " + str(list(map(lambda x: str(x), state_parallel_combi[pro]))), ressourceDict_state_parallel[pro], mynodes, myPart, maxRate, maxComps)
        
        PlacementDict_state_parallel[pro] = (mynodes, myPart, state_parallel_combi[pro])
    print("TOTAL LATENCY: ", getCompleteLatency(state_parallel_combi, ressourceDict_state_parallel))
   

  
    print("time: " + str(end_time - start_time))   
    print("CURRENT SCALING: " + str(myscaling))
    f = open('singleOk.txt', "w")   
    f.write(str(singleOk)) 
    f.close()
    
    f = open('stateparallel.txt', "w")   
    f.write(str(stateparallelOk)) 
    f.close()
    
    f = open('parallel.txt', "w")   
    f.write(str(parallelOk)) 
    f.close()
    
    f = open('llsfOk.txt', "w")   
    f.write(str(llsfOk)) 
    f.close()
    
    with open('placementDictLatency', 'wb') as placementDictParallel:
        pickle.dump(PlacementDict, placementDictParallel)
        
    with open('placementDict_stateparallel', 'wb') as placementDictStateParallel:
        pickle.dump(PlacementDict_state_parallel, placementDictStateParallel)    
    
    with open('osDict',  'wb') as filterDict_file:
        pickle.dump(os_dict , filterDict_file)  
    
    with open('processingConstraints', 'wb') as constraintFile:
        pickle.dump(constraintDict, constraintFile)
        
    with open('processingConstraints_stateparallel', 'wb') as constraintFile:
        pickle.dump(constraintDict_stateParallel, constraintFile)    
    
    with open('curcombi',  'wb') as newcombi:
        pickle.dump(mycombi, newcombi)
        
    with open('originalCombiDict', 'wb') as combidict:
        pickle.dump(combiDict, combidict)
        
  
    # export number of queries, computation time combination, maximal query length, TODO: maximal depth combination tree, portion of rates saved by multi-sink eventtypes
    combiExperimentData = [len(wl), combigenTime, max(len(x) for x in wl), len(projlist)] 
    with open('combiExperimentData',  'wb') as combiExperimentData_file:
        pickle.dump(combiExperimentData , combiExperimentData_file)  
        
   
if __name__ == "__main__":
    main()