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
import boto3
import shutil
from glob import glob
from random import shuffle

# initially just store the options in a dict
# defaults are set

def check_files(setup, mpisize, mpirank):
    ''' Checks whether the files required already exist, if so returns True, else False
    '''

    #os.path.isfile('')
    try:
        if setup['stor'] == 's3':
            if setup['s3fileoverwrite']:
                return False
            else:
                return True
        elif setup['forcewrite']:
            return False
        else:
            if setup['filetype'] == 'nc':
                fpath = setup['floc']+setup['fname']+str(mpirank)+'.nc'
            elif setup['filetype'] == 'bin':
                fpath = setup['floc']+setup['fname']+str(mpirank)
            elif setup['filetype'] == 'hdf':
                fpath = setup['floc']+setup['fname']+str(mpirank)+'.hdf5'
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
    
    if setup['stor'] == 's3' and setup['floc'][0:2]=='s3':
        s3 = boto3.resource('s3',endpoint_url=setup['s3_endpoint'], aws_access_key_id=setup['s3_access_key'], aws_secret_access_key=setup['s3_secret_key'])
        print setup['fname']+str(mpirank)+'/'+setup['fname']+str(mpirank)+'_'+setup['var']
        obs = s3.Bucket(setup['floc'].split('/')[-2]).objects.filter(Prefix=setup['fname']+str(mpirank)+'/'+setup['fname']+str(mpirank)+'_'+setup['var'])
        obs.delete()
        '''ob_to_del = s3.meta.client.list_objects(Bucket=setup['floc'].split('/')[-2],Prefix=setup['floc'].split('/')[-2]+'/'+setup['fname']+'/')
        delete_keys = {'Objects':[]}
        delete_keys['Objects']=[{'Key' : k} for k in [obj['Key'] for obj in ob_to_del.get('Contents', [])]]
        print delete_keys
        s3.meta.client.delete_objects(Bucket=setup['floc'].split('/')[-2], Delete=delete_keys)'''
        s3.meta.client.delete_object(Bucket=setup['floc'].split('/')[-2], Key=setup['fname']+str(mpirank)+'.nc')
    elif setup['stor'] == 's3':
        fpath = setup['floc']+setup['fname']+str(mpirank)+'.nc'
        os.remove(fpath)
        shutil.rmtree(setup['floc']+setup['fname']+str(mpirank),ignore_errors=True)
        try:
            os.rmdir(setup['floc']+setup['fname']+str(mpirank))
        except:
            print 'Directory not removed'
    elif setup['filetype'] == 'nc':
        fpath = setup['floc']+setup['fname']+str(mpirank)+'.nc'
        os.remove(fpath)
    elif setup['filetype'] == 'bin':
        fpath = setup['floc']+setup['fname']+str(mpirank)
        os.remove(fpath)
    
    elif setup['filetype'] == 'CFA':
        fpath = setup['floc']+setup['fname']+str(mpirank)+'.nc'
    else:
        print 'WARNING: Unable to remove files'
    
    

def create_files(setup, mpisize, mpirank):
    ''' Creates the required files
    '''
    # cycle the file creation to avoid buffering as much as possible
    if mpirank == 0:
        print 'mpi rank %s creating file %s' % (mpirank, setup['floc']+setup['fname']+str(mpisize-1)+'.nc')
        if setup['stor'] == 's3':
            size,buffersize = netcdf_creation.create_netcdf_4d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpisize-1, stor=setup['stor'],objsize=int(setup['objsize']),chunking=setup['chunking'],pattern=setup['readpattern'])
        elif setup['filetype'] == 'nc' and setup['dim'] == '4d':
            size,buffersize = netcdf_creation.create_netcdf_4d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpisize-1, stor=setup['stor'],chunking=setup['chunking'])
        elif setup['filetype'] == 'nc':
            size = netcdf_creation.create_netcdf_1d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpisize-1, stor=setup['stor'])
        elif setup['filetype'] == 'hdf' and setup['dim'] == '4d':
            size,buffersize = netcdf_creation.create_netcdf_4d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpisize-1, stor=setup['stor'],chunking=setup['chunking'])
        elif setup['filetype'] == 'hdf':
            size = netcdf_creation.create_netcdf_1d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpisize-1, stor=setup['stor'])
            #size = setup['filesize']
        elif setup['filetype'] == 'bin':
            size = setup['filesize']
            fpath = setup['floc']+setup['fname']+str(mpisize-1)
            with open(fpath, 'wb') as fout:
                fout.write(os.urandom(setup['filesize']))

    else:
        print 'mpi rank %s creating file %s' % (mpirank, setup['floc']+setup['fname']+str(mpirank-1)+'.nc')
        if setup['stor'] == 's3':
            size,buffersize = netcdf_creation.create_netcdf_4d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpirank-1, stor=setup['stor'], objsize=int(setup['objsize']),chunking=setup['chunking'])
        elif setup['filetype'] == 'nc' and setup['dim'] == '4d':
            size,buffersize = netcdf_creation.create_netcdf_4d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpirank-1, stor=setup['stor'],chunking=setup['chunking'])
        elif setup['filetype'] == 'nc':
            size = netcdf_creation.create_netcdf_1d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpirank-1, stor=setup['stor'])
        elif setup['filetype'] == 'hdf' and setup['dim'] == '4d':
            #size = setup['filesize']
            size,buffersize = netcdf_creation.create_netcdf_4d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpirank-1, stor=setup['stor'],chunking=setup['chunking'])
        elif setup['filetype'] == 'hdf':
            #size = setup['filesize']
            size = netcdf_creation.create_netcdf_1d(setup['filesize'], setup['floc'], setup['fname'], setup['buffersize'], mpirank-1, stor=setup['stor'])
        elif setup['filetype'] == 'bin':
            size = setup['filesize']
            fpath = setup['floc']+setup['fname']+str(mpirank-1)
            with open(fpath, 'wb') as fout:
                fout.write(os.urandom(setup['filesize']))
    return size, buffersize


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
            if line.split('=')[0] == 'buffersize' and 'x' in line.split('=')[1]:
                setup[line.split('=')[0]] = reduce(lambda x, y: x*y, [long(x) for x in line.split('=')[1].strip().split('x')])*8

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
    os.system ('touch %s' % setup['results'])

    if setup['objsize'] == 0:
        setup['objsize']=sys.argv[2]

    print setup

    if setup['test'] == 'w' or setup['test'] == 'wr' or setup['test'] == 'rw':
        writetime=time()
        writeclock=clock()
        # create files
        if setup['sharedfile']:
            if rank == 0:
                #create file if doesn't exist
                if not check_files(setup, mpisize, rank):
                    size,buffersize = create_files(setup, mpisize, rank)
                    if setup['stor'] == 's3':
                        writeres =  'write,%s,objsize=%s,%s,%s,%s,%s' % (rank,setup['objsize'],size,buffersize,time()-writetime,clock()-writeclock,size/1000000/(time()-writetime))
                    else:
                        writeres =  'write,%s,%s,%s,%s,%s' % (rank,size,buffersize,time()-writetime,clock()-writeclock,size/1000000/(time()-writetime))

        else:
            if not check_files(setup, mpisize, rank):
                size,buffersize = create_files(setup, mpisize, rank)
                if setup['stor'] == 's3':
                    writeres =  'write,%s,objsize=%s,%s,%s,%s,%s' % (rank,setup['objsize'],size,time()-writetime,clock()-writeclock,size/1000000/(time()-writetime))
                else:
                    writeres =  'write,%s,%s,%s,%s,%s' % (rank,size,time()-writetime,clock()-writeclock,size/1000000/(time()-writetime))
        print >> open(setup['results'],'a'), writeres
        comm.barrier()

    if setup['stor'] == 'OPENDAP':
        assert setup['test'] == 'r', 'Only writes are used with OpenDAP tests'
        fids = glob('/group_workspaces/jasmin2/atsrrepro/mjones07/test*.nc')
        shuffle(fids)
        fid = setup['floc']+fids[rank].split('/')[-1]
        from readfile_nc import readfile_4d as readfile
        results = 'read,' + 'objsize=' + str(setup['stor']) + ',' + readfile(rank,fid,setup['readpattern'], setup['buffersize'],setup['var'],0)

    elif setup['test'] == 'r' or setup['test'] == 'wr' or setup['test'] == 'rw':

        fid = setup['floc']+setup['fname']
        # use options to decide which test script to run
        if setup['language'] == 'Python' and setup['filetype'] == 'nc' and setup['stor'] == 's3':

                from readfile_s3 import readfile_4d as readfile

                results = 'read,'+'objsize='+str(setup['stor'])+','+readfile(rank, fid+str(rank)+'.nc', setup['readpattern'], setup['buffersize'], setup['var'], setup['randcount'])


        elif setup['language'] == 'Python' and setup['filetype'] == 'nc':
            if setup['dim']=='1d':
                from readfile_nc import readfile_1d as readfile

                results = 'read,'+readfile(rank, fid+str(rank)+'.nc', setup['readpattern'], setup['buffersize'], setup['randcount'])
            elif setup['dim']=='4d':
                from readfile_nc import readfile_4d as readfile
                if fid[-1] == 'c':
                    results = 'read,'+readfile(rank, fid, setup['readpattern'], setup['buffersize'], setup['var'], setup['randcount'])
                else:
                    results = 'read,'+readfile(rank, fid+str(rank)+'.nc', setup['readpattern'], setup['buffersize'], setup['var'], setup['randcount'])
            else:
                raise ValueError('Only 1d and 4d reads supported')

        elif setup['language'] == 'Python' and setup['filetype'] == 'hdf':
            if setup['dim']=='1d':
                from readfile_hdf import readfile_1d as readfile
                if fid[-1] == 'c':
                    results = 'read,'+readfile(rank, fid, setup['readpattern'], setup['buffersize'], setup['var'],setup['randcount'])
                else:
                    results = 'read,'+readfile(rank, fid+str(rank)+'.nc', setup['readpattern'], setup['buffersize'], setup['var'],setup['randcount'])
            elif setup['dim']=='4d':
                from readfile_nc import readfile_4d as readfile
                if fid[-1] == 'c':
                    results = 'read,'+readfile(rank, fid, setup['readpattern'], setup['buffersize'], setup['var'], setup['randcount'])
                else:
                    results = 'read,'+readfile(rank, fid+str(rank)+'.nc', setup['readpattern'], setup['buffersize'], setup['var'], setup['randcount'])
            else:
                raise ValueError('Only 1d and 4d reads supported')

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
    with open(setup['results'],'a') as resfile:
        resfile.write(results+'\n')
    if setup['test'] == 'w' or setup['test'] == 'wr' or setup['test'] == 'rw':
        if setup['cleanup']:
            cleanup(setup,mpisize,rank)


if __name__ == '__main__':
    main()
