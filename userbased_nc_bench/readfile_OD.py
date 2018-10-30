#!/usr/bin/env python2.7

import sys
import numpy as np
from netCDF4 import Dataset
from time import time, clock
from pydap.client import open_url

def readfile(fid, pattern, buffersize, v):
    #print fid
    f = open_url(fid)
    var = f[v]
    start = time()
    st_clock = clock()
    dataread = 0
    try:
        dim1 = var.shape[0]
        dim2 = var.shape[1]
        dim3 = var.shape[2]
        dim4 = var.shape[3]
    except IndexError:
        pass
    start = time()
    cpu_time = clock()

    if buffersize == 'all':
        data = var[:]
        dataread = np.prod(data.shape) * 8
        datareadMi = np.prod(data.shape) * 8 / 1024 ** 2
        cpu_time = clock() - cpu_time
        wall_time = time() - start
        rate = dataread / wall_time
        return '%s,%s,%s,%s,%s,%s,%s,%s' \
               % (
               mpirank, np.prod(data.shape) * 8, dataread, buffersize, pattern, cpu_time, wall_time, rate / 1000 ** 2)

    elif pattern == 's':  # serial write
        # if the buffer is larger than one dim, but smaller than two
        one_size = dim4 * 8
        two_size = dim4 * dim3 * 8
        three_size = dim4 * dim3 * dim2 * 8
        four_size = dim4 * dim3 * dim2 * dim1 * 8
        if buffersize >= one_size and buffersize < two_size:
            # buffersize must be whole fraction of the dimensions
            assert (dim4 * dim3) % (buffersize / 8.) == 0, ValueError(
                'Buffersize must be whole fraction of dimensions 3 and 4')
            # then iterate over the remainder of dim3
            # calculate number of whole buffers in dim3
            num_buff = int(np.floor(dim4 * dim3 / (buffersize / 8.)))
            buff_el = buffersize / dim3 / 8
            rem_from_dim = dim3 - num_buff * buff_el

            print
            'Reading by iterating over dims 1,2,3 with %s*%s*%s buffers of size %sx%s elements, and a remainder of %sx%s elements' % (
            num_buff, dim1, dim2, buff_el, dim4, rem_from_dim, dim4)

            dataread = 0
            for i1 in range(dim1):
                for i2 in range(dim2):
                    for i3 in range(num_buff):
                        data = var[i1, i2, int(i3 * buff_el):int((i3 + 1) * buff_el), :]
                        dataread += reduce(lambda x, y: x * y, data.shape) * 8
            datareadMi = dataread / 1024 ** 2
            print
            'Bytes read = %s' % datareadMi

        elif buffersize >= two_size and buffersize < three_size:
            # buffersize must be whole fraction of the dimensions
            assert (dim4 * dim3 * dim2) % (buffersize / 8.) == 0, ValueError(
                'Buffersize must be whole fraction of dimensions 3 and 4')
            # then iterate over the remainder of dim3
            # calculate number of whole buffers in dim3
            num_buff = int(np.floor(dim4 * dim3 * dim2 / (buffersize / 8.)))
            buff_el = buffersize / (dim4 * dim3) / 8
            rem_from_dim = dim2 - num_buff * buff_el

            print
            'Filling by iterating over dims 1,2 with %s*%s buffers of size %sx%sx%s elements, and a remainder of %sx%sx%s elements' % (
            num_buff, dim1, buff_el, dim3, dim4, rem_from_dim, dim3, dim4)

            dataread = 0
            for i1 in range(dim1):
                for i2 in range(num_buff):
                    data = var[i1, int(i2 * buff_el):int((i2 + 1) * buff_el), :, :]
                    dataread += reduce(lambda x, y: x * y, data.shape) * 8
            datareadMi = dataread / 1024 ** 2
            print
            'Bytes read = %s' % datareadMi



        elif buffersize >= three_size and buffersize < four_size:
            # buffersize must be whole fraction of the dimensions
            assert (dim1 * dim2 * dim3 * dim4) % (buffersize / 8.) == 0, ValueError(
                'Buffersize must be whole fraction of dimension 4')
            # then iterate over the remainder of dim3
            # calculate number of whole buffers in dim3
            num_buff = int(np.floor(dim1 * dim2 * dim3 * dim4 / (buffersize / 8.)))
            buff_el = buffersize / (dim2 * dim3 * dim4) / 8
            rem_from_dim = dim1 - num_buff * buff_el

            print
            'Filling by iterating over dim 1 with %s buffers of size %sx%sx%sx%s elements, and a remainder of %sx%sx%sx%s elements' % (
            num_buff, buff_el, dim2, dim3, dim4, rem_from_dim, dim2, dim3, dim4)

            dataread = 0
            for i1 in range(num_buff):
                data = var[int(i1 * buff_el):int((i1 + 1) * buff_el), :, :, :]
                dataread += reduce(lambda x, y: x * y, data.shape) * 8
            datareadMi = dataread / 1024 ** 2
            print
            'Bytes read = %s' % datareadMi

        else:
            raise ValueError('not impl')
    elif pattern == 'h':
        ''' Striding pattern takes whole of first dim to provide poor read pattern
        '''
        # if the buffer is larger than one dim, but smaller than two
        one_size = dim1 * 8
        two_size = dim1 * dim4 * 8
        three_size = dim4 * dim3 * dim1 * 8
        four_size = dim4 * dim3 * dim2 * dim1 * 8
        if buffersize >= one_size and buffersize < two_size:
            # buffersize must be whole fraction of the dimensions
            assert (dim4 * dim1) % (buffersize / 8.) == 0, ValueError(
                'Buffersize must be whole fraction of dimensions 3 and 4')
            # then iterate over the remainder of dim3
            # calculate number of whole buffers in dim3
            num_buff = int(np.floor(dim1 * dim4 / (buffersize / 8.)))
            buff_el = buffersize / dim1 / 8
            rem_from_dim = dim4 - num_buff * buff_el

            print
            'Reading by iterating over dims 2,3,4 with %s*%s*%s buffers of size %sx%s elements, and a remainder of %sx%s elements' % (
            num_buff, dim2, dim3, buff_el, dim1, rem_from_dim, dim1)

            dataread = 0
            for i2 in range(dim2):
                for i3 in range(dim3):
                    for i4 in range(num_buff):
                        data = var[:, i2, i3, int(i4 * buff_el):int((i4 + 1) * buff_el)]
                        dataread += reduce(lambda x, y: x * y, data.shape) * 8
            datareadMi = dataread / 1024 ** 2
            print
            'Bytes read = %s' % datareadMi

        elif buffersize >= two_size and buffersize < three_size:
            # buffersize must be whole fraction of the dimensions
            assert (dim4 * dim3 * dim1) % (buffersize / 8.) == 0, ValueError(
                'Buffersize must be whole fraction of dimensions 3 and 4')
            # then iterate over the remainder of dim3
            # calculate number of whole buffers in dim3
            num_buff = int(np.floor(dim4 * dim3 * dim1 / (buffersize / 8.)))
            buff_el = buffersize / (dim4 * dim1) / 8
            rem_from_dim = dim3 - num_buff * buff_el

            print
            'Filling by iterating over dims 2,3 with %s*%s buffers of size %sx%sx%s elements, and a remainder of %sx%sx%s elements' % (
            num_buff, dim2, buff_el, dim1, dim4, rem_from_dim, dim1, dim4)

            dataread = 0
            for i2 in range(dim2):
                for i3 in range(dim3):
                    data = var[:, i2, int(i3 * buff_el):int((i3 + 1) * buff_el), :]
                    dataread += reduce(lambda x, y: x * y, data.shape) * 8
            datareadMi = dataread / 1024 ** 2
            print
            'Bytes read = %s' % datareadMi



        elif buffersize >= three_size and buffersize < four_size:
            # buffersize must be whole fraction of the dimensions
            assert (dim1 * dim2 * dim3 * dim4) % (buffersize / 8.) == 0, ValueError(
                'Buffersize must be whole fraction of dimension 4')
            # then iterate over the remainder of dim3
            # calculate number of whole buffers in dim3
            num_buff = int(np.floor(dim1 * dim2 * dim3 * dim4 / (buffersize / 8.)))
            buff_el = buffersize / (dim1 * dim3 * dim4) / 8
            rem_from_dim = dim2 - num_buff * buff_el

            print
            'Filling by iterating over dim 2 with %s buffers of size %sx%sx%sx%s elements, and a remainder of %sx%sx%sx%s elements' % (
            num_buff, buff_el, dim1, dim3, dim4, rem_from_dim, dim1, dim3, dim4)

            dataread = 0
            for i2 in range(dim2):
                data = var[:, int(i2 * buff_el):int((i2 + 1) * buff_el), :, :]
                dataread += reduce(lambda x, y: x * y, data.shape) * 8
            datareadMi = dataread / 1024 ** 2
            print
            'Bytes read = %s' % datareadMi

        else:
            raise ValueError('error read size')

    else:
        raise ValueError('Only sequential (s) and striding (h) reads supported')
    cpu_time = clock() - cpu_time
    wall_time = time() - start
    rate = dataread / wall_time


    return '%s,%s,%s,%s,%s,%s,%s,%s' \
           % (
           'na', var.shape[0] * var.shape[1] * var.shape[2] * 8, dataread, buffersize, pattern, cpu_time, wall_time, rate / 1000 ** 2)


if __name__ == '__main__':
    fid = sys.argv[1]
    readmode = sys.argv[2]
    readsize = float(sys.argv[3])
    if len(sys.argv) == 5:
        rand_num = int(sys.argv[4])
    else:
        rand_num = None

    readfile_1d(mpirank, fid, readmode, readsize, rand_num)
