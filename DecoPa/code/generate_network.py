
"""
Generate network with given size (nwsize), node-event ratio (node_event_ratio), 
number of event types (num_eventtypes), event rate skew (eventskew)-
"""
import sys
import pickle
import numpy as np
import string
import random

""" Experiment network rates 

Citibike:    eventrates = [4.5, 348, 2854, 35, 3120,20833,6,244,5950] 
Google First 48h:       eventrates = [5002.8, 4761.0, 575.6, 190.8, 2849.6, 1265.0, 0.64, 0.01, 0.22080000000000002]

"""    



       # event_rates_file = res[0]
       # event_node_assignment = res[1]
        

def generate_eventrates(eventskew,numb_eventtypes):
    
    eventrates = np.random.zipf(eventskew,numb_eventtypes)
    while max(eventrates)>50000:
        eventrates = np.random.zipf(eventskew,numb_eventtypes)
    return eventrates


def generate_events(eventrates, n_e_r):
    myevents = []
    for i in range(len(eventrates)):
        x = np.random.uniform(0,1)
        if x < n_e_r:
            myevents.append(eventrates[i])
        else:
            myevents.append(0)
    
    return myevents

def regain_eventrates(nw):
    eventrates = [0 for i in range(len(nw[0]))]
    interdict = {}
    for i in nw:
        for j in range(len(i)):
            if i[j] > 0 and not j in interdict.keys():
                interdict[j] = i[j]
    for j in sorted(interdict.keys()):
        eventrates[j] = interdict[j]
    return eventrates 

def allEvents(nw):
    for i in range(len(nw[0])) :
        column = [row[i] for row in nw]
        if sum(column) == 0:
            return False
    return True

def swapRatesMax(eventtype, rates, maxmin):
    rates = list(rates)
    if maxmin == 'max':
        maxRate = max(rates)
    else: 
        maxRate = min(rates)
    maxIndex = rates.index(maxRate)
    eventTypeIndex = string.ascii_uppercase.index(eventtype)
    newRates = [x for x in rates]
    newRates[maxIndex], newRates[eventTypeIndex] =   newRates[eventTypeIndex], newRates[maxIndex]
    return newRates

def swapRates(numberofswaps,rates):
    newRates = [x for x in rates]
    for i in range(numberofswaps):        
        newRates = [x for x in newRates]
        index = int(len(newRates)/2)
        left = index - (i+1) 
        right = index + i
        newRates[left], newRates[right] = newRates[right], newRates[left]
    return newRates

def generate_assignment(nw, eventtypes):
    assignment = {k: [] for k in range(eventtypes)}
    for node in range(len(nw)):
        for eventtype in range(len(nw[node])):
            if nw[node][eventtype] > 0:
                assignment[eventtype].append(node)        
    return assignment

def generateFromAssignment(assignment, rates, nwsize):
    return [[rates[eventtype]  if x in assignment[eventtype] else 0 for eventtype in assignment.keys()] for x in range(nwsize)]

def main():

    
    #default values for simulation 
    nwsize = 1
    node_event_ratio = 1.0
    num_eventtypes = 6
    eventskew = 1.1
    toFile = True
    swaps = 0   
    scaling = 10
    miau = True
    kleeneOk = 1

      
    if len(sys.argv) > 1: #save to file
        toFile =int(sys.argv[1])
    if len(sys.argv) > 2:
        if "." in str(sys.argv[2]): #all but 1           
            miau = True # readFromFile
            scaling = float(sys.argv[2]) # scaling, if != 1 read from file     
    if len(sys.argv) > 3:
        num_eventtypes = int(sys.argv[3])
    
    
    if toFile:
        totalEventrates = generate_eventrates(eventskew,num_eventtypes) #generate new set of event types
        

            
        nw= []
        totalEventrates = sorted(totalEventrates, reverse=True)
        for node in range(nwsize):
                nw.append(generate_events(totalEventrates, node_event_ratio))

        
        with open('rates', 'wb') as rates_file:
              pickle.dump(totalEventrates, rates_file) 
     
    if miau: #read from file and scale
        with open('rates',  'rb') as  rates_file:
            totalEventrates = pickle.load(rates_file)
        
        print(totalEventrates)

    
    f = open('kleeneRate.txt', "w")   
    f.write(str(kleeneOk)) 
    f.close()    
  
    eventrates = [5002.8, 4761.0, 575.6, 190.8, 2849.6, 1265.0, 0.64, 0.01, 0.22080000000000002] # Google First 48h        
    #eventrates = [4.5, 348, 2854, 35, 3120,20833,6,244,5950] # Citibike
    totalEventrates = eventrates

    print(totalEventrates)
    assignment = {}
    
    for etype in range(len(eventrates)):
        if totalEventrates[etype] == min(totalEventrates):
            partitioning = 1 
        else:
            partitioning = nwsize
            while totalEventrates[etype]/partitioning < min(totalEventrates):
                partitioning -= 1        
        localRate = totalEventrates[etype]/partitioning
        totalEventrates[etype] = localRate
        nodes_for_etype = []
        while len(nodes_for_etype) < partitioning:
            source = np.random.randint(0,nwsize)
            if not source in nodes_for_etype:
                nodes_for_etype.append(source)
        assignment[etype] = nodes_for_etype        
    nw = []    
    nw = generateFromAssignment(assignment, totalEventrates, nwsize) #UNCOMMENT FOR RANDOM PARTITIONING
    print(nw)
   
    #export eventskew, node_eventratio, networksize, maximal difference in rates
    networkExperimentData = [eventskew, num_eventtypes, node_event_ratio, nwsize, min(eventrates)/max(eventrates)]
    with open('networkExperimentData', 'wb') as networkExperimentDataFile:
        pickle.dump(networkExperimentData, networkExperimentDataFile)
        
    with open('scalingFactor', 'wb') as scalingData:
        pickle.dump(scaling, scalingData)
    
    with open('network', 'wb') as network_file:
          pickle.dump(nw, network_file)      
          
      
        
if __name__ == "__main__":
    main()


        



