#!/usr/bin/env python2.7

import sys
import numpy as np
from netCDF4 import Dataset
from time import time, clock

def seq_read_1d(mpirank, fid, num_elements):
    print fid
    f = Dataset(fid,'r')
    var = f.variables['var']

    num_reads = var.shape/num_elements

    bytes_read = 0
    start = time()
    cpu_time = clock()
    for i in xrange(num_reads):
        ind_slice0 = long(i*num_elements)
        ind_slice1 = long(i*num_elements+num_elements)
        bytes_read += 8*len(var[ind_slice0:ind_slice1])

    cpu_time = clock() - cpu_time
    wall_time = time()-start
    rate = bytes_read/wall_time

    return '%s,%s,%s,%s,%s,sequential,%s,%s,%s'\
        % (mpirank,var.shape[0]*8, bytes_read, num_elements*8, num_reads[0], cpu_time, wall_time, rate/1000**2)

def hop_read_1d(mpirank, fid, num_elements):

    f = Dataset(fid,'r')
    var = f.variables['var']

    num_reads = var.shape/num_elements

    bytes_read = 0
    start = time()
    cpu_time = clock()
    for i in xrange(0,num_reads,4):
        ind_slice0 = long(i*num_elements)
        ind_slice1 = long(i*num_elements+num_elements)
        bytes_read += 8*len(var[ind_slice0:ind_slice1])

    cpu_time = clock() - cpu_time
    wall_time = time()-start
    rate = bytes_read/wall_time

    return '%s,%s,%s,%s,%s,hopping,%s,%s,%s'\
        % (mpirank, var.shape[0]*8, bytes_read, num_elements*8, num_reads[0], cpu_time, wall_time, rate/1000**2)

def rand_read(mpirank, fid, num_elements, rand_num):

    f = Dataset(fid,'r')
    var = f.variables['var']


    rand_starts = [int(np.ceil(x)) for x in np.random.random(rand_num)*(var.shape-num_elements)]
    if rand_starts <0:
        rand_starts = 0

    bytes_read = 0
    start = time()
    cpu_time = clock()
    for ind1 in rand_starts:
        ind2 = int(ind1+num_elements)
        bytes_read += 8*len(var[ind1:ind2])

    cpu_time = clock() - cpu_time
    wall_time = time()-start
    rate = bytes_read/wall_time

    return '%s,%s,%s,%s,%s,random,%s,%s,%s'\
        % (mpirank, var.shape[0]*8, bytes_read, num_elements*8, rand_num, cpu_time, wall_time, rate/1000**2)

def readfile_1d(mpirank, fid, readmode, readsize, rand_num):
    # Readsize in elements
    num_elements = np.ceil(readsize/8.)

    if readmode == 's':
        results = seq_read_1d(mpirank, fid, num_elements)
    elif readmode == 'h':
        results = hop_read_1d(mpirank, fid, num_elements)
    elif readmode == 'r':
        assert rand_num != None, 'For random read the number of reads needs to be specified'
        results = rand_read(mpirank, fid, num_elements, rand_num)
    return results

if __name__ == '__main__':
    fid = sys.argv[1]
    readmode = sys.argv[2]
    readsize = float(sys.argv[3])
    if len(sys.argv) == 5:
        rand_num = int(sys.argv[4])
    else:
        rand_num = None

    readfile_1d(mpirank, fid, readmode, readsize, rand_num)
