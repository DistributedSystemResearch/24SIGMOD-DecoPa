#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


Generate beneficial projections for given query workload.

"""
import subsets as sbs 
import multiprocessing
import pickle
from structures import *
import random as rd
import math
from decimal import Decimal


scaling = 1

with open('current_wl',  'rb') as  wl_file:
    wl = pickle.load(wl_file)
    
with open('scalingFactor',  'rb') as scalingData: # new for outputrate
    myscaling = pickle.load(scalingData)    
    
with open('selectivities', 'rb') as selectivity_file:
    selectivities = pickle.load(selectivity_file) 


for i in wl:
    if i.hasKleene():
        kleeneType = i.kleene_components()[0]
        myKey = str(kleeneType)
        #selectivities.update({myKey:rd.uniform(0.1,0.01)}) # set selectivity for kleene projection
        selectivities.update({myKey:0.0039})  #FIXED FOR EXPERIMENTS
# structures for speedup in partInput function
MSTrees  = {}
DistMatrices =  {}

def getNumETBs(projection):
    num = 1
    for etype in projection.leafs():
        num *= len(IndexEventNodes[etype])
    return num
def getANDQuery(query):
    mychildren = sorted(query.leafs())
    leaflist = []
    for i in mychildren:
        leaflist.append(PrimEvent(i))
    newquery = AND()
    newquery.children = leaflist
    if query == newquery:
        return query
    else:
        return newquery

#for multiquery, generate workload of AND queries to make all shareable
if len(wl)>1:
    original_wl = [x for x in wl]
    for x in [getANDQuery(x) for x in wl]:    
        if not x in wl:
            wl.append(x)

def binom(n, k):
    return math.comb(n, k)

def powerset(s):
    x = len(s)
    masks = [1 << i for i in range(x)]
    for i in range(1 << x):
        yield [ss for mask, ss in zip(masks, s) if i & mask]
        

    
# def optimisticTotalRate(projection): # USE FILTERED RATE FOR ESTIMATION 
#     if projection in projlist: # is complex event        
#         for i in projFilterDict.keys():
#             if i  == projection: 
#                 myproj = i
#                 if getMaximalFilter(projFilterDict, myproj):                                                
#                         return getDecomposedTotal(getMaximalFilter(projFilterDict, myproj), myproj)    
#                 else:
#                         return projFilterDict[myproj][getMaximalFilter(projFilterDict, myproj)][0] * getNumETBs(myproj) #TODO change
#     else:
#         return rates[projection.leafs()[0]] * len(nodes[projection.leafs()[0]])
    
    
def min_max_doubles(query,projevents):
    doubles = getdoubles_k(projevents)
    leafs = map(lambda x: filter_numbers(x), query.leafs())
    for event in doubles.keys():
        if not doubles[event] == leafs.count(event):
            return False
    return True
 
    
def settoproj(evlist,query):
    """ take query and list of prim events and return projection"""
    leaflist = []
    evlist = sepnumbers(evlist)  
    evlist = list(map(lambda x: str(x), evlist))
    for i in evlist:   
        leaflist.append(PrimEvent(i))  
    newproj = query.getsubop(leaflist)  
 
    return newproj



def isBeneficial(projection, rate):
    """ determines for a projection based on the if it is beneficial """
    totalProjrate =  rate * getNumETBs(projection)
    #sumrates = sum(map(lambda x: rates[x] * float(len(nodes[x])), projection.leafs()))
    sumrates = max([rates[x] * float(len(nodes[x])) for x in projection.leafs()]) # PARALLEL NEW : outrate < maxrate
    if sumrates > totalProjrate:
        return True
    else:
        return False
    
def isBeneficial_sharing(projection, rate):
    """ determines for a projection based on the if it is beneficial """
    totalProjrate = rate * getNumETBs(projection)
    #sumrates = sum(map(lambda x: rates[x] * float(len(nodes[x])), projection.leafs()))
    sumrates = max([rates[x] * float(len(nodes[x])) for x in projection.leafs()]) # PARALLEL NEW : Condition for beneficial
    if 2*sumrates > totalProjrate:
        return True
    else:
        return False    

def totalRate(projection):

    if len(projection) == 1:
        if isinstance(projection, str) or  isinstance(projection, PrimEvent):
            return rates[str(projection)] * len(nodes[str(projection)])
        elif projection.hasKleene():
            return scaling * getKleeneRate(projection) 
   
    elif projection.hasKleene():
         return scaling * getKleeneRate(projection)
    else:
        projection = projection.strip_NSEQ()
        # add case for nseq with two events only, treat as normal sequence
        outrate = projection.evaluate() *  getNumETBs(projection)  
        selectivity =  return_selectivity(projection.leafs())
        myrate = outrate * selectivity  
        return myrate * scaling


    
def return_selectivity(proj):
    
    """ return selectivity for arbitrary projection """
    proj = list(map(lambda x: filter_numbers(x), proj))
    two_temp = sbs.printcombination(proj,2)    
    selectivity = 1
    for two_s in two_temp:       
        if two_s in selectivities.keys():           
         #  if selectivities[two_s]!= 1:
               selectivity *= selectivities[two_s]
    return selectivity

def return_selectivity_num(proj):
    
    """ return selectivity for arbitrary projection """
    proj = list(map(lambda x: filter_numbers(x), proj))
    two_temp = sbs.printcombination(proj,2)    
    selectivity = 1
    num = 0
    for two_s in two_temp:       
        if two_s in selectivities.keys():           
           if selectivities[two_s]!= 1:
              num +=1
    return num

def generate_projections(query):  
    """ generates list of benecifical projection """    
    
    negated = query.get_negated()
    projections = []
    projrates = {}
    match = query.leafs()
    projlist = match
    for i in range(2, len(match)):
           iset =  sbs.boah(match, i) 
           for k in range(len(iset)):  
                    nseq_violated = False
                    curcom = list(iset[k].split(","))  
                    projevents = rename_without_numbers("".join(sorted(list(set(curcom))))) #A1BC becomes ABC and A1B1CA2 becomes A1BCA2                    
                    mysubop = settoproj(curcom, query) 
                    for neg in negated: #if negated type in projection
                        if neg in mysubop.getleafs():                            
                            mycontext = query.get_context(neg) 
                            if not set(mycontext).issubset(set(mysubop.getleafs())): # if conext of negated event not in projection, exclude projection
                                nseq_violated = True
                    if mysubop.hasKleene():
                        rate = getKleeneRate(mysubop) *scaling
                        selectivity = return_selectivity(mysubop.leafs())
                    else:
                        outrate = mysubop.evaluate()                          
                        selectivity =  return_selectivity(curcom)
                        rate = outrate * selectivity  *scaling
                    placement_options = isBeneficial(mysubop, rate)                
                    
                    if placement_options and min_max_doubles(query, projevents) and not nseq_violated:  # if the projection is beneficial (yields a placement option) and minmax?                         
                                projrates[mysubop] = (selectivity, rate)         
                                projections.append(mysubop) # do something to prevent a1a2b and a2a3b to be appended to dictionary
                                if mysubop.hasKleene(): # to ensure the projection needed to generate combinations for kleene subop is added SEQ(KLA(A),B,C) <- SEQ(A,B,C)
                                    mysubop_wthoutKleene = mysubop.stripKL_simple()
                                    projections.append(mysubop_wthoutKleene) 
                                    outrate = mysubop_wthoutKleene.evaluate()                          
                                    selectivity =  return_selectivity(mysubop_wthoutKleene.leafs())
                                    rate = outrate * selectivity  *scaling
                                    projrates[mysubop_wthoutKleene] = (selectivity, rate)    
    projections.append(query)
    if query.hasKleene():
         rate = getKleeneRate(query) * scaling
         kleeneType = query.kleene_components()[0]
         selectivity = return_selectivity(query.leafs())
         projrates[query] = (selectivities[kleeneType], rate)          
         kleeneProj = settoproj([kleeneType], query)        
         projrates[kleeneProj] = (selectivities[kleeneType],  getKleeneRate(kleeneProj))
         projections.append(kleeneProj)
         
    else:
        outrate = query.evaluate()                          
        selectivity =  return_selectivity(query.leafs())
        rate = outrate * selectivity*scaling                   
        projrates[query] = (selectivity, rate) 
   
    
    return projections, projrates

kleeneRates = {}

def getKleeneRate(projection):
     global kleeneRates
     rate = 0
     kleeneType = str(projection.kleene_components()[0])     
     rest = 1
     if projection in kleeneRates.keys():
         return kleeneRates[projection]
     if len(projection.leafs()) > 1:
         rest = settoproj([x for x in projection.leafs() if not x == kleeneType], projection) # this should be stripKL and then costs with kl rate = 1
         sels = [selectivities[y] for y in[kleeneType+x for x in projection.leafs() if not x == kleeneType]]
         secondfactor =  totalRate(rest)
         totalSels = [kleeneType+x for x in projection.leafs() if not x == kleeneType]
         rest = totalRate(projection.stripKL_simple()) / (totalRate(kleeneType) * math.prod([selectivities[x] for x in totalSels]))
         
     else:
         sels = [1]
         secondfactor = 1
     for i in range(0,int(round(totalRate(kleeneType)))):
         #rate += binom(int(round(totalRate(kleeneType))),(i+1)) * (math.prod(sels)**(i+1)) * (selectivities[kleeneType] **(i))
         rate += Decimal(binom(int(round(totalRate(kleeneType))),(i+1))) * Decimal((math.prod(sels)**(i+1))) * Decimal((selectivities[kleeneType] **(i)))
     kleeneRates[projection] = float(rate*Decimal(rest))    
     return float(rate *Decimal(rest))# *kleene_sel


def returnSubProjections(proj, projlist):
    """ return list of projection keys that can be used in a combination of a given projection"""    
    myprojlist = [x for x in projlist if len(x.leafs()) <= len(proj.leafs()) and set(x.leafs()).issubset(set(proj.leafs()))]
    outputlist = []                          
    for i in myprojlist:
                if not proj == i:
                 if i.can_be_used(proj):    
                     outputlist.append(i)

    return outputlist

sharedProjectionsDict = {}
sharedProjectionsList = []
projsPerQuery = {}
#query = wl[0]
projlist = []
projrates = {}


# if respective projection is in projections, do nothing
for query in wl + [x.stripKL_simple() for x in wl if x.hasKleene()]:
    #print(query)
    #query = query.stripKL_simple()
    result = generate_projections(query)
    #projsPerQuery[query] = result[0]
   
    for i in result[0]:   
        if not i in projlist:       
            projlist.append(i)
            projrates[i] = result[1][i]
            if query in wl:
                sharedProjectionsDict[i] = [query]
        else:
            for mykey in sharedProjectionsDict.keys():
                if mykey == i and query in wl:   
                    sharedProjectionsDict[mykey].append(query)

# else, generate AND, check if is beneficial, if yes add to projections
#ADD Projections for shared subsets with and operator!!! 
for query1 in wl:
    for query2 in wl:
        if not query1 ==query2:
            # get overlap between each pair of queries
            overlappingEvents = list(set(query1.leafs()).intersection(set(query2.leafs())))
            if len(overlappingEvents) > 1:
                # for each subset of overlap
                subsets = [x for x in powerset(overlappingEvents) if len(x)>1]
                for subset in subsets:
                    # instantiate AND
                    mysubop = AND()
                    mysubop.children = [PrimEvent(x) for x in subset]                    
                    outrate = mysubop.evaluate()                          
                    selectivity =  return_selectivity(subset)
                    rate = outrate * selectivity     
                    if isBeneficial_sharing(mysubop, rate): 
                        # order of event types in AND
                        if not mysubop in projlist:
                              projlist.append(mysubop)
                              projrates[mysubop] = (selectivity, rate)
                              sharedProjectionsDict[mysubop] = [query1, query2]
                              #TODO fix this situation
                    
                
                             



# change workload to workload of AND queries for better sharing
# change order, key should be the new query and value list of old queries
if len(wl) > 1: 
    originalMapping = {}
    for i in original_wl:
        if not  getANDQuery(i) in originalMapping.keys():
            originalMapping[getANDQuery(i)] = [i]
        else:
            originalMapping[getANDQuery(i)].append(i)
            
    wl = list(originalMapping.keys()) #THIS SHOULD BE TURNED OUT FOR SINGLE QUERY EXPERIMENTS

#print([str(x[0]) for x in originalMapping.values()])
#print([str(x) for x in projlist])
# add and for overlapping seqs




for query in wl:
   # query = query.stripKL_simple() #TODO not really strip kleene
    projsPerQuery[query] = [x for x in projlist if query.can_be_used(x)]


for projection in sharedProjectionsDict.keys():
    if len(sharedProjectionsDict[projection]) > 1:
        sharedProjectionsList.append(projection) 



#for i in projrates.keys():
   # print(i, projrates[i][1])

with open('projrates',  'wb') as projratesfile:
    pickle.dump(projrates, projratesfile)

projratesOk = 0
print(len(projrates.keys()))

for i in wl:    
    print(i, " : ", totalRate(i))
    projratesOk = 1
    if totalRate(i) > 100: # this is for ensuring matches during experiments
        print("JAAA")
        projratesOk = 1
        
f = open('projratesOk.txt', "w")   
f.write(str(projratesOk)) 
f.close()