#!/usr/bin/env python2.7

# rough setup for parallel read test script, need to take args from script and pass them through to the run file

import os
from mpi4py import MPI
import sys
import netcdf_creation
#import ctypes
import subprocess
import numpy as np
from time import time, clock

# initially just store the options in a dict
# defaults are set

def check_files(setup, mpisize, mpirank):
    ''' Checks whether the files required already exist, if so returns True, else False
    '''
    #TODO
    # not implemented
    #os.path.isfile('')
    try:
        if setup['stor'] == 's3':
            if setup['s3fileoverwrite']:
                return False
            else:
                return True
        else:
            if setup['filetype'] == 'nc':
                fpath = setup['floc']+setup['fname']+str(mpirank)+'.nc'
            elif setup['filetype'] == 'bin':
                fpath = setup['floc']+setup['fname']+str(mpirank)
            statinfo = os.stat(fpath)
            size = statinfo.st_size
            if abs(setup['filesize']-size) > 100000: # allow 100KB size difference
                return False
                
            else:
                print 'File exists, skipping creation.'
                return True
            
    except:
        return False

def cleanup(setup, mpisize, mpirank):
    ''' Remove files if nececery. 
    '''
    if setup['filetype'] == 'nc':
            fpath = setup['floc']+setup['fname']+str(mpirank)+'.nc'
    elif setup['filetype'] == 'bin':
            fpath = setup['floc']+setup['fname']+str(mpirank)
    os.remove(fpath)

def create_files(setup, mpisize, mpirank):
    ''' Creates the required files
    '''
    # cycle the file creation to avoid buffering as much as possible
    if mpirank == 0:
        print 'mpi rank %s creating file %s' % (mpirank, setup['floc']+setup['fname']+str(mpisize-1)+'.nc')
        if setup['filetype'] == 'nc':
            netcdf_creation.create_netcdf_1d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpisize-1, setup['stor'])
        elif setup['filetype'] == 'bin':
            fpath = setup['floc']+setup['fname']+str(mpisize-1)
            with open(fpath, 'wb') as fout:
                fout.write(os.urandom(setup['filesize']))
        
    else:
        print 'mpi rank %s creating file %s' % (mpirank, setup['floc']+setup['fname']+str(mpirank-1)+'.nc')
        if setup['filetype'] == 'nc':
            netcdf_creation.create_netcdf_1d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpirank-1, setup['stor'])
        elif setup['filetype'] == 'bin':
            fpath = setup['floc']+setup['fname']+str(mpirank-1)
            with open(fpath, 'wb') as fout:
                fout.write(os.urandom(setup['filesize']))
        

def get_setup(setup_config_file):
    ''' parse the config file and store in the dict
    '''
    # TODO needs changing to retrieve from input file
    setup = {}
    for line in open(setup_config_file):
        try:
            setup[line.split('=')[0]] = long(line.split('=')[1])
        except:
            setup[line.split('=')[0]] = line.split('=')[1].strip()

    return setup


def main():
    cwd = os.path.dirname(os.path.realpath(sys.argv[0]))
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    mpisize = comm.Get_size()
    #rank = 0
    #mpisize = 1
    
    setup_config_file = sys.argv[1]
    setup = get_setup(setup_config_file)
    writetime=time()
    writeclock=clock()
    # create files
    if setup['sharedfile']:
        if rank == 0:
            #create file if doesn't exist
            if not check_files(setup, mpisize, rank):
                create_files(setup, mpisize, rank)
                print 'write,%s,%s,%s,%s' % (rank,setup['filesize'],time()-writetime,clock()-writeclock,setup['filesize']/1000000/(time()-writetime))
    
    else:
        if not check_files(setup, mpisize, rank):
                create_files(setup, mpisize, rank)
                print 'write,%s,%s,%s,%s,%s' % (rank,setup['filesize'],time()-writetime,clock()-writeclock,setup['filesize']/1000000/(time()-writetime))
    comm.barrier()
    

    fid = setup['floc']+setup['fname']
    # use options to decide which test script to run
    if setup['language'] == 'Python' and setup['filetype'] == 'nc' and setup['stor'] == 's3':
        from readfile_s3 import readfile_1d as readfile
        
        results = 'read,'+readfile(rank, fid+str(rank)+'.nc', setup['readpattern'], setup['buffersize'], setup['randcount'])

    elif setup['language'] == 'Python' and setup['filetype'] == 'nc':
        from readfile_nc import readfile_1d as readfile
        
        results = 'read,'+readfile(rank, fid+str(rank)+'.nc', setup['readpattern'], setup['buffersize'], setup['randcount'])

    elif  setup['language'] == 'Python' and setup['filetype'] == 'bin':
        from readfile_bin import main as readfile
        
        results = 'read,'+readfile(rank, fid+str(rank), setup['readpattern'], setup['buffersize'], setup['randcount'])

    elif  setup['language'] == 'C' and setup['filetype'] == 'nc':
        #readfile = ctypes.CDLL('./readfile_ncc.so')
        #results = readfile.main(3, "%s %s %s" % (fid+'.nc', setup['readpattern'], setup['buffersize']))
        output = subprocess.check_output(cwd+'/readfile_nc %s %s %s' % (fid+str(rank)+'.nc', setup['buffersize'], setup['readpattern']),shell=True)
        results =  'read,'+str(rank)+','+output.split('\n')[4]

    elif  setup['language'] == 'C' and setup['filetype'] == 'bin':
        output = subprocess.check_output(cwd+'/readfile_bin %s %s %s' % (fid+str(rank), setup['buffersize'], setup['readpattern']),shell=True)
        results =  'read,'+str(rank)+','+output.split('\n')[3]

    else: 
        raise ValueError('Combination of language and filetype not supported')

    print results

    if setup['cleanup']:
        cleanup(setup,mpisize,rank)
    

if __name__ == '__main__':
    main()
