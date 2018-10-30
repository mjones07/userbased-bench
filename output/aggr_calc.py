#!/usr/bin/env python2.7


import sys
import numpy as np

test = sys.argv[1]
if len(sys.argv) == 3:
    p = int(sys.argv[2])

    fname = './results_%s_p%s' %(test,p)
else:
    fname = test
    p = test.split('_')
    for i in p:
        if i[0] == 'p':
            p = int(i[1:])
            break
        else:
            p=1
print p
data = 0
time = []
rate = 0
with open(fname) as f:
    n = 0
    for line in f:
        
        if n >= p:
            break
        try:
            data += float(line.split(',')[3])
            time.append(float(line.split(',')[8]))
            rate += float(line.split(',')[-1])
            n += 1
        except IndexError:
            pass
            

aggr_rate = data/10**6/max(time)
print 'Calculated Aggr rate (data total / max time): %s' % aggr_rate
print 'summed',rate
print 'max', data/10**6/min(time)
print 'avg', data/10**6/np.mean(time)
