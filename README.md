
#userbased\_nc\_bench code



###Use


`mpirun userbased_nc_bench.py configfile`


###Config file

####Arguments

- language=Python|C

    Language to use to read the data. Note, Python is always used to do the writing.

- filetype=nc|bin|hdf

    File type to use, either NetCDF4, plain binary, or HDF5. Note, currently hdf read nc files using h5py library, and writes NetCDF4 files...

- floc= /data/file/location/

- fname=filename(.nc)

    Name of the data file. .nc is only required when reading from existing 4d files.

- output=/log/output/file/loc/

- rundir=/run/directory/

- readpattern=s|h

    Read write pattern. Either sequential (s) or striding (h). In 1d tests the striding pattern is read one, skip three. In 4d tests the striding pattern reads the slowest varying dimension first, then iterates next along the fastest.

- randcount=0

    Not used in current implementation.

- buffersize=

    Size of the read/write buffer. Constraint that the buffer size must fit a whole number of times into the dimension for the 4d test. Can either be a number of bytes, or numbers with 'x' in between, e.g. '1024x240' showing the dimension size, which is then multiplied into bytes internally.

- filesize=

    Size of the file to create in Bytes

- sharedfile=1|0

    Not used in current implementation.

- mpiio=1|0

    Not used in current implementation

- cleanup=1|0

    Determines whether created files are removed after read tests. Not used with ``stor=S3``

- dim=1d|4d

    Either one-dimensional tests, or four-dimensional tests. Must match the dimensions of the file in read only tests.

- test=r|w|rw

    Which tests to run. Read only, write only, or write then read.
   
- var=ncfilevar

    Name of the variable in the NetCDF/HDF5 file

- results=/dir/to/output/results.csv

- objsize=-1

    Size of the object in the S3 object store

- stor=filesystem|s3

    Either POSIX filesystem or object store with S3 interface using S3-netcdf4-python

- chunking=0

    Either 0 (no chunking) or 4d chunk shape e.g. 10x24x21x30

- forcewrite=1

    Either 1 or 0. 1 will force the overwrite on write tests, 0 will check if the file exists already.


####Example

Read from a specified existing data file using h5pyd


```
language=Python
filetype=hdf
floc=/group_workspaces/jasmin/hiresgw/vol1/mj07/IO_testing_files/
fname=comp_test_uxy_c0.nc
output=/home/users/mjones07/userbased-bench/
rundir=/home/users/mjones07/userbased-bench/
readpattern=s
randcount=0
buffersize=1132462080
filesize=256000000000
sharedfile=0
mpiio=0
cleanup=0
dim=4d
test=r
var=u
results=/home/users/mjones07/userbased-bench/4dresults
objsize=-1
stor=filesystem
chunking=0
forcewrite=1
```

Non-enabled features
--------------------
- C - NetCDF4 file read

- Shared file

- MPIIO

- Random reads

- Proper HDF5 implementation
