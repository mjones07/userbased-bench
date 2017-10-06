#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/stat.h> /* Only present on POSIX systems I think */
#include <sys/time.h>
#include <netcdf.h>

#define ERRCODE 2
#define ERR(e) {printf("Error: %s\n", nc_strerror(e)); exit(ERRCODE);}

double rand2();

int main(int argc, char *argv[])
{
    char *file_name;       /* Name of the file to read */
    int ncid;              /* Id of netcdf file */
    int dimid;             /* Id of dimension "dim1" */
    size_t dimlen;         /* Size of the dimension (number of elements in data variable) */
    int varid;             /* Id of data variable to be read from file */
    int retval;            /* Return code used to check status of netcdf function calls */
     
    char random;                         /* if we're reading from random positions in the file, 0 otherwise */
    unsigned long long random_read_count;               /* The number of buffers requested in a random read */
    size_t *offsets;                     /* If reading randomly, we'll precalculate an array of random offsets */
    int buffer_size;                     /* Number of bytes to read in a single operation */
    double *buffer;                      /* Buffer to hold data read from the file */
    unsigned long long buffer_len;                      /* Number of elements in buffer (= buffer_size/8) */
    unsigned long long num_buffers;      /* The number of buffers to be read in */
    unsigned long long total_bytes_read; /* The total number of bytes read */
    unsigned long long pos;                             /* Position of data to be read */    
    size_t start[1];                     /* Start location for each netCDF read */
    size_t count[1];                     /* Size of each netCDF read */

    int i;                 /* Loop control variables */
    
    clock_t cpu_time_start;   /* Used to get CPU time spent reading the file */
    clock_t cpu_time_end;
    double cpu_time_spent;
    
    struct timeval wall_time_start; /* Used to measure wall-clock time spent reading the file */
    struct timeval wall_time_end;
    double wall_time_spent;
    
    double read_rate;    /* Average data read rate in megabytes per wall clock seconds */
    
    random = 's'; /* Assume we're reading sequentially unless we find otherwise */
    srand(time(NULL)); /* Seed the random number generator */
    
    /* Check there are enough command-line arguments */
    if (argc < 3 || argc > 5) {
        fprintf(stderr, "Usage: readfile <filename> <buffer size in bytes> [r|s|h] [count if random]\n");
        exit(EXIT_FAILURE);
    }
    
    /* Read the command-line arguments */
    file_name = argv[1];
    if (sscanf(argv[2], "%d", &buffer_size) <= 0) {
        fprintf(stderr, "Invalid buffer size %s\n", argv[2]);
        exit(EXIT_FAILURE);
    }
    if (argc > 3) {
        /* Third argument is a character indicating if reads should be random, sequential or hopping */
    	sscanf(argv[3], "%c", &random);
    	if (random != 'r' && random != 's' && random != 'h') {
    	    fprintf(stderr, "Must be \"r\", \"s\" or \"h\", found %s\n", argv[3]);
    	    exit(EXIT_FAILURE);
    	}
    }
    if (random == 'r') {
        /* We need a count */
        if (argc < 5) {
            fprintf(stderr, "Must input a count when performing random reads\n");
            exit(EXIT_FAILURE);
        }
        if(sscanf(argv[4], "%d", &random_read_count) <= 0) {
            fprintf(stderr, "Invalid read count %s\n", argv[4]);
            exit(EXIT_FAILURE);
        }
    }
    
    /* Create buffer of the right size */
    printf("Buffer size = %llu bytes\n", buffer_size);
    buffer_len = buffer_size/8;                        /* use this variable for netcdf reads which are in elements */
    buffer = (double*)malloc(sizeof(double)*buffer_len); 
    if (buffer == NULL) {
        fprintf(stderr, "Error allocating buffer of size %d bytes", buffer_size);
        exit(EXIT_FAILURE);
    }
          
    /* Open the file and get the data variable id - assuming this is called "var" */
    if ((retval = nc_open(file_name, NC_NOWRITE, &ncid)))   
       ERR(retval);
    if ((retval = nc_inq_varid(ncid, "var", &varid)))
       ERR(retval);
    
    /* Find the dimension of the data variable - assuming this is called "dim1" */
    if ((retval = nc_inq_dimid(ncid, "dim1", &dimid)))
       ERR(retval);      
    if ((retval = nc_inq_dimlen(ncid, dimid, &dimlen)))
       ERR(retval); 
    printf("Data size: %llu bytes\n", dimlen*8);
  
    if (random == 'r') {
        /* Precalculate the array of offsets, so we don't cost CPU time during the read operation
           (I'm not sure this really makes a difference, we could probably compute these on the fly) */
        offsets = (size_t*) malloc(sizeof(size_t) * random_read_count);
        if (offsets == NULL) {
            fprintf(stderr, "Error allocating space for %d precalculated random offsets\n", random_read_count);
            exit(EXIT_FAILURE);
        }
        for (i = 0; i < random_read_count; i++) {
            offsets[i] = rand2() * (dimlen-buffer_len);
            /* Uncomment the line below to simulate sequential read as a test */
            //offsets[i] = i * (off_t)buffer_size;
        }
    }

    /* Initialise position and count variable */
    pos = 0; 
    count[0] = buffer_len;     

    /* Make a note of the time */
    gettimeofday(&wall_time_start, NULL);
    cpu_time_start = clock();
    
    /* Now read the data from the file */
    if (random == 's') {
        /* Read sequentially from the start until the end of the file */
        printf("Reading whole file sequentially...\n");
        num_buffers = dimlen / buffer_len; 
        printf("Reading %d buffers\n", num_buffers); 
        for (i = 0; i < num_buffers; i++){    
           start[0] = pos;
           if ((retval = nc_get_vara_double(ncid, varid, start, count, &buffer[0])))
              ERR(retval);
           pos = pos + buffer_len;
        }
        total_bytes_read = num_buffers * buffer_size;    
     
    }  else if (random == 'r') {
        /* Read the specified number of times from random positions within the file */
        printf("Reading data from file randomly...\n");
        printf("Reading %d buffers\n", random_read_count); 
        for (i = 0; i < random_read_count; i++) {
	  start[0] = offsets[i]; 
          if ((retval = nc_get_vara_double(ncid, varid, start, count, &buffer[0])))
             ERR(retval);
        } 
        total_bytes_read = random_read_count * buffer_size;           

    } else {
        /* Read in a "hopping" pattern reading a block and then skipping three blocks */
        printf("Reading file in \"hopping\" pattern...\n");
        num_buffers = (dimlen / buffer_len) / 4; 
        printf("Reading %d buffers\n", num_buffers); 
        for (i = 0; i < num_buffers; i++){    
           start[0] = pos;
           if ((retval = nc_get_vara_double(ncid, varid, start, count, &buffer[0])))
              ERR(retval);
           pos = pos + buffer_len*4;
        }
        total_bytes_read = num_buffers * buffer_size;    
    }
 
    /* Note the time again. */
    cpu_time_end = clock();
    gettimeofday(&wall_time_end, NULL);
    
    /* Calculate the wall-clock and CPU time spent */
    cpu_time_spent = (double) (cpu_time_end - cpu_time_start) / CLOCKS_PER_SEC;
    wall_time_spent = (double)(wall_time_end.tv_usec - wall_time_start.tv_usec) / 1e6 +
                      (double)(wall_time_end.tv_sec  - wall_time_start.tv_sec);
    
    /* Calculate the read rate in MB per sec (10^6 bytes per wall clock second) */
    read_rate = total_bytes_read / (wall_time_spent * 1e6);
    
    /* Output statistics as comma separated values */
    /* File size (bytes) | Bytes read | Buffer size (bytes) | No. buffers | Seq or random | CPU time (s) | Wall time (s) | Read rate (MB/s) */
    printf ("%llu,%llu,%d,%llu,%s,%f,%f,%f\n",
        dimlen*8,
        total_bytes_read,
        buffer_size,
        num_buffers,
        random == 'r' ? "random" : (random == 's' ? "sequential" : "hopping"),
        cpu_time_spent,
        wall_time_spent,
        read_rate)
    ;
    
    /* Close the file */
    if ((retval = nc_close(ncid)))
      ERR(retval);
    free(buffer);
    if (random == 'r') {
        free(offsets);
    }
    return 0;
}

/* Return a random number between 0.0 and 1.0 inclusive.
   Must seed the random number generator before this is called. */
double rand2()
{
	return (double)rand() / (double)RAND_MAX;
}
