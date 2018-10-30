#!/usr/bin/env python2.7

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd




def import_results(testname):
    # return pandas array for whole results
    data = pd.read_csv('./output/'+testname, header=None,
                        names=['type','objsize','rank','datasize','dataread','buffersize','readpattern','CPU','wall','rate'])

    return data

#def parse_results(req_param, par):
#    # get the req param out of pandas

def count_repeats(data):
    rank = data['rank']
    count = 0
    for i in rank:
        if i == 0:
            count+=1

    if count == 0:
        count = 1

    return count

def agg_rate(rates,repeats,par):
    try:
        len_rate = float(len(rates))
        reshaped =  rates.values.reshape(repeats,int(np.ceil(len_rate/repeats)))
        rate = np.array(reshaped)
        return np.sum(rate,axis=1)
    except ValueError:
        agg = np.zeros(repeats)
        
        for i in range(repeats-1):
            #print i
            for j in range(par):
                #print j+i*par-2
                agg[i] += rates.values[j+i*par-1]
        if agg[-1] == 0:
            agg[-1] = np.mean(rates.values[j+i*par+1-1:])*par
        
        return agg

def agg_rate_from_median(rates,repeats,par):
    #print par
    
    agg = np.zeros(repeats)

    grouped = [[] for x in range(repeats)]
    #print(grouped)
    i = 0
    j = 0
    for rate in rates:
        #print(grouped)
        #print(i,j)
        try:
            grouped[i].append(rate)
        except IndexError:
            grouped.append([])
            grouped[i].append(rate)            
            agg = np.zeros(len(grouped))
        j += 1
        if j==par:
            i += 1
            j = 0
    for i,group in enumerate(grouped):
        
        agg[i] = np.median(group)*par

    return agg
   
def testplot(test,pars,subplot_list):


    plt.subplot(subplot_list[0],subplot_list[1],subplot_list[2])
    results = []
    for par in pars:
        testname = test.replace('?',par)
        data = import_results(testname)
        #print data
        repeats = count_repeats(data)

        rates = data['rate']
        try: 
            par_ = int(par.replace('_p',''))
        except ValueError:
            par_ = 1
        results.append(agg_rate_from_median(rates,repeats,par_))

    plt.boxplot(results)
    axislabels = [par.replace('_p','') for par in pars]
    if axislabels[0] == '':
        axislabels[0] = '1'
    #print results
    print test
    print 'Reapeats for test:'
    for a,v in zip(axislabels,results):
        print 'par: %s, count: %s' %(a,len(v))
    
    plt.xticks(np.arange(len(pars))+1,axislabels)
    plt.ylabel('Aggregate rate (MB/s)')
    plt.xlabel('Concurrent requests')


def plot_dd_results():
    
    tests = ['lotus serial', 'lotus par 16','HP node serial', 'HP node par 16']
    res = [ [358,358,367,367,152,338],
            [1037,1019,897,970,914,979],
            [134,164,160,163],
            [1007,957,972]]
    plt.subplot(1,1,1)
    plt.boxplot(res)
    plt.xticks(np.arange(len(tests))+1,tests)
    plt.ylabel('Aggregate rate (MB/s)')
    plt.xlabel('Test')
    plt.title('dd tests from high performance node and LOTUS')
    
def main():
    plt.figure() # multithreading comparison
    #testplot(testnamewithmissing,pars,subplot_list)
    testplot('results_OpenDAP_100g_PFS?_MT',['','_p4','_p16','_p64','_p128','_p256'],[2,1,1])
    plt.title('Multithreading')
    #testplot('results_OpenDAP_100g_PFS?_s1',['','_p4','_p16','_p64'],[3,1,2])
    testplot('results_OpenDAP_100g_PFS?_s4',['','_p4','_p16','_p64','_p128','_p256'],[2,1,2])
    plt.title('Non multithreading')
    plt.tight_layout()

    plt.figure() # 1 server vs 4 servers
    #testplot(testnamewithmissing,pars,subplot_list)
    testplot('results_OpenDAP_100g_PFS?_s1',['','_p4','_p16','_p64'],[2,1,1])
    plt.title('1 Servers')
    #testplot('results_OpenDAP_100g_PFS?_s1',['','_p4','_p16','_p64'],[3,1,2])
    testplot('results_OpenDAP_100g_PFS?_s4',['','_p4','_p16','_p64'],[2,1,2])
    plt.title('4 Servers')
    plt.tight_layout()

    plt.figure() # HP vs dap4gws
    #testplot(testnamewithmissing,pars,subplot_list)
    testplot('results_OpenDAP_100g_PFS?_MT',['','_p4','_p16','_p64','_p128','_p256'],[2,1,1])
    plt.title('High performance server')
    #testplot('results_OpenDAP_100g_PFS?_s1',['','_p4','_p16','_p64'],[3,1,2])
    testplot('results_OpenDAP_dap4gws_PFS?',['','_p4','_p16','_p64','_p128','_p256'],[2,1,2])
    plt.title('OpenDAP4GWS server')
    plt.tight_layout()

    plt.figure() # HP vs dap4gws
    #testplot(testnamewithmissing,pars,subplot_list)
    testplot('results_OpenDAP_100g_QB?_MT',['','_p4','_p16','_p64','_p128','_p256'],[1,1,1])
    plt.title('High performance server (QuoByte)')
    plt.tight_layout()
    

    plt.figure()
    plot_dd_results()

    plt.show()

if __name__ == '__main__':
    main()
