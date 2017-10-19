#!/usr/bin/env python2.7

import os
import sys
import numpy as np
from time import time, clock
from numpy.random import random


def hop_read(fpath, block_size, file_size):
    """
    Hopping read mode: reads one block size then skips three times the block size.

    :param fpath: path to the file
    :param block_size: size of the blocks to read
    :param file_size: total size of the file
    :return: wall_time: wall time for the total read
    :return: cpu_time: cpu time for the total read (on windows system this will also be wall time)
    :return: total_bytes_read: total number of bytes read from the file
    :return: buffers_read: total number of blocks read from the file
    """

    num_buffers = np.floor(file_size/block_size)
    hop_size = 4
    wall_time = time()
    cpu_time = clock()
    total_bytes_read = 0L
    buffers_read = 0
    f = open(fpath, 'rb')
    # read one buffer, skip three then read the next
    for i in np.arange(0,num_buffers,hop_size):
        f.seek(i*block_size)
        bytes_read = len(f.read(block_size))
        total_bytes_read += bytes_read
        buffers_read += 1
    f.close()

    wall_time = time()-wall_time
    cpu_time = clock()-cpu_time

    return wall_time, cpu_time, total_bytes_read, buffers_read


def rand_read(fpath, block_size, file_size, block_num):
    """
    Random read mode: read the specified number of blocks from the file at random positions.
    Uses uniform random numbers.

    :param fpath: path to the file
    :param block_size: size of the blocks to read
    :param file_size: total size of the file
    :param block_num: number of blocks to read from file
    :return: wall_time: wall time for the total read
    :return: cpu_time: cpu time for the total read (on windows system this will also be wall time)
    :return: total_bytes_read: total number of bytes read from the file
    :return: buffers_read: total number of blocks read from the file
    """
    # Generate random numbers
    rand_starts = [np.ceil(x) for x in random(block_num)*file_size]
    f_rand = open('rand_nums','a')
    print >> f_rand, rand_starts
    f_rand.close()
    wall_time = time()
    cpu_time = clock()
    total_bytes_read = 0L
    buffers_read = 0

    f = open(fpath, 'rb')
    for i in rand_starts:
        f.seek(i)
        bytes_read = len(f.read(block_size))
        total_bytes_read += bytes_read
        buffers_read += 1

    f.close()
    wall_time = time()-wall_time
    cpu_time = clock()-cpu_time

    return wall_time, cpu_time, total_bytes_read, buffers_read


def seq_read(fpath, block_size, file_size):
    """
    Sequential read mode: reads whole file sequentially.

    :param fpath: path to the file
    :param block_size: size of the blocks to read
    :param file_size: total size of the file
    :return: wall_time: wall time for the total read
    :return: cpu_time: cpu time for the total read (on windows system this will also be wall time)
    :return: total_bytes_read: total number of bytes read from the file
    :return: buffers_read: total number of blocks read from the file
    """
    num_buffers = file_size/block_size
    total_bytes_read = 0L
    wall_time = time()
    cpu_time = clock() # returns cpu time on linux, wall time on windows
    full_block_read = True
    f = open(fpath, 'rb')
    while full_block_read:
        #for i in np.arange(num_buffers): #~~~ change to while
        bytes_read = len(f.read(block_size))
        full_block_read = bytes_read == block_size
        total_bytes_read += bytes_read

    f.close()

    wall_time = time()-wall_time
    cpu_time = clock()-cpu_time

    return wall_time, cpu_time, total_bytes_read, num_buffers


def main(rank, fpath, read_mode, block_size, block_num):

    # Find the size of the file
    file_size = float(os.path.getsize(fpath))

    if read_mode == 's':
        read_time, cpu_time, bytes_read, num_buffers = seq_read(fpath, block_size, file_size)
        print_read_mode = 'sequential'
    elif read_mode == 'r':
        read_time, cpu_time, bytes_read, num_buffers = rand_read(fpath, block_size, file_size, block_num)
        print_read_mode = 'random'
    elif read_mode == 'h':
        read_time, cpu_time, bytes_read, num_buffers = hop_read(fpath, block_size, file_size)
        print_read_mode = 'hopping'
    else:
        raise ValueError('Read mode error.\n\tUsage: python readfile.py filename s|r|h blocksize [randnum]\n')

    rate = (bytes_read/read_time)/1000**2
    return '%d,%d,%d,%d,%d,%s,%f,%f,%f' % (rank,file_size, bytes_read, block_size, num_buffers, print_read_mode, cpu_time, read_time, rate)

if __name__ == '__main__':
    # Usage: python readfile.py filename readmode blocksize randnum
    if len(sys.argv)<3:
        raise ValueError('Not enough arguments.\n\tUsage: python readfile.py filename s|r|h blocksize [randnum]\n')
    fpath = sys.argv[1]
    read_mode = sys.argv[2]
    block_size = long(sys.argv[3])
    if len(sys.argv) == 5:
        block_num = int(sys.argv[4])
    else:
        block_num = None

    if read_mode == 'r' and block_num == None:
        raise ValueError('Needs number of buffers for random mode.\n\tUsage: python readfile.py filename s|r|h blocksize [randnum]\n')

    main(fpath, read_mode, block_size, block_num)
