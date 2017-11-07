#!/usr/bin/env python2.7

import numpy as np
import os
import sys


sys.path.append(os.path.expanduser("~/s3netcdf/S3-netcdf-python/"))

def ispower2(n): 

    if n == 2:
        return True


    temp = 2

    while (temp <= n):
        if temp == n:
            return True
        temp *= 2

    return False

def create_netcdf(size, path):
    # Calculate dim size
    
    fid = path+'test_2d_chunkC.nc'
    f = Dataset(fid,'w',format='NETCDF4')
    dim1 = f.createDimension('dim1',size[0])
    dim2 = f.createDimension('dim2',size[1])
    var = f.createVariable('var','f8',('dim1','dim2'))#,chunksizes=(430,430))#,chunksizes=(11584,46336))

    for i in range(size[0]):
        var[i,:] = np.random.random(size[1])

    f.close()

def create_netcdf_4d(size, path, fname, buffersize, mpirank, pattern='s', var='var', stor='filesystem',objsize=-1,chunking=0):
    # I think things will work better if file size is base 2
    assert ispower2(size), 'Please provide a file size as base 2'
    #assert ispower2(buffersize), 'Pleaseprovide a buffer size as base 2'

    # check that path has a '/' at the end
    if not path[-1] == '/':
        path = path+'/'
    
    # Calculate dim size
    dsize = int(np.ceil((size/8)**0.25))

    # parse chunking option
    

    print dsize
    if stor == 'filesystem':
        from netCDF4 import Dataset
    elif stor == 's3':
        from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
    else:
        raise ValueError("Only stor='filesystem'|'s3' supported")
    
    fid = path+fname+str(mpirank)+'.nc'

    if stor == 's3':
        f = Dataset(fid,'w',format='CFA4')
    else:
        f = Dataset(fid,'w',format='NETCDF4')

    dim1 = f.createDimension('dim1',dsize)
    dim1d = f.createVariable('dim1','i4',('dim1',))
    dim1d[:] = range(dsize)
    dim2 = f.createDimension('dim2',dsize)
    dim2d = f.createVariable('dim2','i4',('dim2',))
    dim2d[:] = range(dsize)
    dim3 = f.createDimension('dim3',dsize)
    dim3d = f.createVariable('dim3','i4',('dim3',))
    dim3d[:] = range(dsize)
    dim4 = f.createDimension('dim4',dsize)
    dim4d = f.createVariable('dim4','i4',('dim4',))
    dim4d[:] = range(dsize)
    #var = f.createVariable('var','f8',('dim1','dim2','dim3','dim4'),chunksizes=(430,1,1,430))#,chunksizes=(430,1,430,430)
    if chunking == 0:
        var = f.createVariable(var,'f8',('dim1','dim2','dim3','dim4'),contiguous=True)
    else:
        split_chunk_arg = chunking.split('x')
        c1 = int(split_chunk_arg[0])
        c2 = int(split_chunk_arg[1])
        c3 = int(split_chunk_arg[2])
        c4 = int(split_chunk_arg[3])
        var = f.createVariable(var,'f8',('dim1','dim2','dim3','dim4'),contiguous=False, chunksizes=(c1,c2,c3,c4))
    print var

    # calculate the size of each dimension in bytes to compare to buffer size
    one_size = 8*dsize # size of 1 dim
    two_size = 8*dsize**2 # size of 2 dim, etc
    three_size = 8*dsize**3
    four_size = 8*dsize**4

    # note dimension order numbered (1,2,3,4)
    if pattern == 's': # serial write
        # if the buffer is larger than one dim, but smaller than two
        if buffersize >= one_size and buffersize < two_size:
            # buffersize must be whole fraction of the dimensions 
            assert (dsize**2)%(buffersize/8.) == 0, ValueError('Buffersize must be whole fraction of dimensions 3 and 4')
            # then iterate over the remainder of dim3
            # calculate number of whole buffers in dim3
            num_buff = int(np.floor(dsize**2/(buffersize/8.)))
            buff_el = buffersize/dsize/8
            rem_from_dim = dsize - num_buff*buff_el
            
            print 'Filling by iterating over dims 1,2,3 with %s buffers of size %sx%s elements, and a remainder of %sx%s elements' % (num_buff, buff_el,dsize,rem_from_dim, dsize)
            
            # if remainder is close to buffersize then continue, if a small remainder then recalculate buffer size
            # start with keeping buffer if rem is greater than 75% of buffer
            if (buffersize-rem_from_dim*8)/buffersize > 0.75:
                if not rem_from_dim == 0:
                    num_buff -= 1
                if not rem_from_dim == 0:
                    fin_buff = (((buffersize-rem_from_dim*8)/buffersize)*buffersize)
                    print 'Final buffer size in dim3 will be %s bytes' % fin_buff
                
                for i1 in range(dsize):
                    for i2 in range(dsize):
                        for i3 in range(num_buff):
                            var[i1,i2,int(i3*buff_el):int((i3+1)*buff_el),:] = np.random.random((int(buff_el),dsize))
                        if not rem_from_dim == 0:
                            
                            var[i1,i2,int((num_buff+1)*buff_el):int(((num_buff+1)*buff_el)+rem_from_dim),:] = np.random.random((int(rem_from_dim),dsize))
            else:
                # this section should no longer be needed due to assertion on buffer size
                '''# Else resize the buffer so that it fits better into dim3
                new_buffer = dsize**2*8/num_buff
                buff_el_new = new_buffer/dsize/8.
                print "Resized buffer to %s (%sx%s elements) so that it doesn't leave a small remainder" % (new_buffer, buff_el_new, dsize)
                for i1 in range(dsize):
                    for i2 in range(dsize):
                        for i3 in range(num_buff):
                            #print '\nfinal buffer index %s:%s\n' % (int(i3*new_buffer/dsize/8),int((i3+1)*new_buffer/dsize/8))
                            var[i1,i2,int(i3*buff_el_new):int((i3+1)*buff_el_new),:] = np.random.random((int(buff_el_new),dsize))
                '''
        elif buffersize >= two_size and buffersize < three_size:
            # buffersize must be whole fraction of the dimensions 
            assert (dsize**3)%(buffersize/8.) == 0, ValueError('Buffersize must be whole fraction of dimensions 3 and 4')
            # then iterate over the remainder of dim3
            # calculate number of whole buffers in dim3
            num_buff = int(np.floor(dsize**3/(buffersize/8.)))
            buff_el = buffersize/dsize**2/8
            rem_from_dim = dsize - num_buff*buff_el
            
            print 'Filling by iterating over dims 1,2 with %s buffers of size %sx%sx%s elements, and a remainder of %sx%sx%s elements' % (num_buff, buff_el,dsize,dsize,rem_from_dim, dsize,dsize)
            
            # if remainder is close to buffersize then continue, if a small remainder then recalculate buffer size
            # start with keeping buffer if rem is greater than 75% of buffer
            if (buffersize-rem_from_dim*8)/buffersize > 0.75:
                if not rem_from_dim == 0:
                    num_buff -= 1
                if not rem_from_dim == 0:
                    fin_buff = (((buffersize-rem_from_dim*8)/buffersize)*buffersize)
                    print 'Final buffer size in dim3 will be %s bytes' % fin_buff
                
                for i1 in range(dsize):
                    for i2 in range(dsize):
                    
                        var[i1,int(i2*buff_el):int((i2+1)*buff_el),:,:] = np.random.random((int(buff_el),dsize,dsize))
                    
            else:
                # this section should no longer be needed due to assertion on buffer size
                '''# Else resize the buffer so that it fits better into dim3
                new_buffer = dsize**2*8/num_buff
                buff_el_new = new_buffer/dsize/8.
                print "Resized buffer to %s (%sx%s elements) so that it doesn't leave a small remainder" % (new_buffer, buff_el_new, dsize)
                for i1 in range(dsize):
                    for i2 in range(dsize):
                        for i3 in range(num_buff):
                            #print '\nfinal buffer index %s:%s\n' % (int(i3*new_buffer/dsize/8),int((i3+1)*new_buffer/dsize/8))
                            var[i1,i2,int(i3*buff_el_new):int((i3+1)*buff_el_new),:] = np.random.random((int(buff_el_new),dsize))
                '''

        elif buffersize >= three_size and buffersize < four_size:
            # buffersize must be whole fraction of the dimensions 
            assert (dsize**4)%(buffersize/8.) == 0, ValueError('Buffersize must be whole fraction of dimension 4')
            # then iterate over the remainder of dim3
            # calculate number of whole buffers in dim3
            num_buff = int(np.floor(dsize**4/(buffersize/8.)))
            buff_el = buffersize/dsize**3/8
            rem_from_dim = dsize - num_buff*buff_el
            
            print 'Filling by iterating over dim 1 with %s buffers of size %sx%sx%sx%s elements, and a remainder of %sx%sx%sx%s elements' % (num_buff, buff_el,dsize,dsize,dsize,rem_from_dim, dsize,dsize,dsize)
            
            # if remainder is close to buffersize then continue, if a small remainder then recalculate buffer size
            # start with keeping buffer if rem is greater than 75% of buffer
            if (buffersize-rem_from_dim*8)/buffersize > 0.75:
                if not rem_from_dim == 0:
                    num_buff -= 1
                if not rem_from_dim == 0:
                    fin_buff = (((buffersize-rem_from_dim*8)/buffersize)*buffersize)
                    print 'Final buffer size in dim3 will be %s bytes' % fin_buff
                
                for i1 in range(dsize):
                    var[int(i1*buff_el):int((i1+1)*buff_el),:,:,:] = np.random.random((int(buff_el),dsize,dsize,dsize))
                    
            else:
                # this section should no longer be needed due to assertion on buffer size
                '''# Else resize the buffer so that it fits better into dim3
                new_buffer = dsize**2*8/num_buff
                buff_el_new = new_buffer/dsize/8.
                print "Resized buffer to %s (%sx%s elements) so that it doesn't leave a small remainder" % (new_buffer, buff_el_new, dsize)
                for i1 in range(dsize):
                    for i2 in range(dsize):
                        for i3 in range(num_buff):
                            #print '\nfinal buffer index %s:%s\n' % (int(i3*new_buffer/dsize/8),int((i3+1)*new_buffer/dsize/8))
                            var[i1,i2,int(i3*buff_el_new):int((i3+1)*buff_el_new),:] = np.random.random((int(buff_el_new),dsize))
                '''
        else:
            raise ValueError('not impl')


    elif pattern == 'h':
        raise ValueError('Striding write not implemented')
    else:
        raise ValueError('Only serial writes currently implemented')


    
    if stor== 'filesystem':
        f.close()
    elif objsize==-1:
        f.close()
    else:
        f.close(objsize=objsize)
    
    try:
        return size, new_buffer
    except:
        return size, buffersize

def create_netcdf_1d(size, path, fname, buffersize, mpirank, stor='filesystem',objsize=-1):
    
    if stor == 'filesystem':
        from netCDF4 import Dataset
    elif stor == 's3':
        from S3netCDF4._s3netCDF4 import s3Dataset as Dataset
    else:
        raise ValueError("Only stor='filesystem'|'s3' supported")
    # Calculate dim size
    dimsize = long(size/8.)
    
    bsize = long(buffersize/8.)
    fid = path+fname+str(mpirank)+'.nc'

    if stor == 's3':
        f = Dataset(fid,'w',format='CFA3')
    else:
        f = Dataset(fid,'w',format='NETCDF4')



    dim1 = f.createDimension('dim1',dimsize)
    # the file will be double the size expected because
    var = f.createVariable('var','f8',('dim1')) 
    dim1d = f.createVariable('dim1','f8',('dim1'))

    for i in xrange(0,dimsize,bsize):
        slice_ind0 = i
        slice_ind1 = i+bsize
        rdata = np.random.random(bsize)
        var[slice_ind0:slice_ind1] = rdata
        dim1d[slice_ind0:slice_ind1] = rdata

    if stor== 'filesystem':
        f.close()
    elif objsize==-1:
        f.close()
    else:
        f.close(objsize=objsize)
    return size 

def create_hdf5_1d(size, path, fname, buffersize, mpirank, stor='filesystem'):
    assert stor == 'filesystem', 'Only posix style filesystem currently supported with hdf5 creation'
    import h5py

    dimsize = long(size/8.)
    bsize = long(buffersize/8.)
    fid = path+fname+str(mpirank)+'.hdf5'
    f = h5py.File(fid,'w')
    dset = f.create_dataset('data',(dimsize,),'f8')

    for i in xrange(0,dimsize,bsize):
        slice_ind0 = i
        slice_ind1 = i+bsize
        dset[slice_ind0:slice_ind1] = np.random.random(bsize)



    f.close()


if __name__ == '__main__':
    create_netcdf_4d(1*1024**3, '/group_workspaces/jasmin/hiresgw/vol1/mj07/','test4d',1*108**3*8,'',stor='filesystem')
    #create_netcdf_4d(1000000000, 's3://s3testqwer/s3testqwer1/','testS3',10000000,0,stor='s3',objsize=1000000)
    #create_hdf5_1d(2560000000, '/group_workspaces/jasmin/hiresgw/vol1/mj07/','createtest',10000000,0)
    '''size = long(sys.argv[3]) # in bytes
    path = sys.argv[1]
    fname = sys.argv[2]
    create_netcdf_1d(size, path, fname, buffersize)
    #create_netcdf((184900,184900), path)
    #create_netcdf_4d((430,430,430,430), path)
    print 'Finished'
    '''
