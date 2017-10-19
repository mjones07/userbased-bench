#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/stat.h> /* Only present on POSIX systems I think */
#include <sys/time.h>

double rand2();

int main(int argc, char *argv[])
{
    char *file_name;       /* Name of the file to read */
    FILE *fin;             /* File pointer for the input file */
    off_t file_size;       /* Size of the input file */
    struct stat stbuf;     /* Will hold stats about the input file */
    
    char random;                         /* if we're reading from random positions in the file, 0 otherwise */
    int random_read_count;               /* The number of buffers requested in a random read */
    off_t *offsets;                      /* If reading randomly, we'll precalculate an array of random offsets */
    int buffer_size;                     /* Number of bytes to read in a single operation */
    char *buffer;                        /* Buffer to hold bytes read from the file */
    unsigned long long buffers_read;     /* The number of buffers actually read */
    size_t bytes_read;                   /* The bytes read in a single read operation */
    unsigned long long total_bytes_read; /* The total number of bytes read */
    
    int i;                 /* Loop control variables */
    int done;
    
    clock_t cpu_time_start;   /* Used to get CPU time spent reading the file */
    clock_t cpu_time_end;
    double cpu_time_spent;
    
    struct timeval wall_time_start; /* Used to measure wall-clock time spent reading the file */
    struct timeval wall_time_end;
    double wall_time_spent;
    
    double read_rate;    /* Average data read rate in megabytes per wall clock seconds */
    
    random = 's'; /* Assume we're reading sequentially unless we find otherwise */
    //srand(time(NULL)); /* Seed the random number generator */
    srand(1);
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
    printf("Buffer size = %d bytes\n", buffer_size);
    buffer = (char*) malloc(sizeof(char) * buffer_size);
    if (buffer == NULL) {
        fprintf(stderr, "Error allocating buffer of size %d bytes", buffer_size);
        exit(EXIT_FAILURE);
    }
    
    /* Find the size of the file, using code from
       https://www.securecoding.cert.org/confluence/display/c/FIO19-C.+Do+not+use+fseek%28%29+and+ftell%28%29+to+compute+the+size+of+a+regular+file */
    if ((stat(file_name, &stbuf) != 0) || (!S_ISREG(stbuf.st_mode))) {
        fprintf(stderr, "Error getting size of file %s\n", file_name);
        exit(EXIT_FAILURE);
    }
    file_size = stbuf.st_size;
    printf("File size: %llu bytes\n", file_size);
    
    if (random == 'r') {
        /* Precalculate the array of offsets, so we don't cost CPU time during the read operation
           (I'm not sure this really makes a difference, we could probably compute these on the fly) */
        offsets = (off_t*) malloc(sizeof(off_t) * random_read_count);
        if (offsets == NULL) {
            fprintf(stderr, "Error allocating space for %d precalculated random offsets\n", random_read_count);
            exit(EXIT_FAILURE);
        }
        for (i = 0; i < random_read_count; i++) {
            offsets[i] = rand2() * file_size;
            /* Uncomment the line below to simulate sequential read as a test */
            //offsets[i] = i * (off_t)buffer_size;
        }
    }
    
    /* Open the file */
    fin = fopen(file_name, "r");
    if (fin == NULL) {
        fprintf(stderr, "Error opening file %s\n", file_name);
        exit(EXIT_FAILURE); 
    }
    
    /* Turn off underlying buffering used by fread()
       (Although I don't think it makes much difference) */
    setvbuf(fin, NULL, _IONBF, 0);
    
    /* Initialise count variables */
    buffers_read = 0;
    bytes_read = 0;
    total_bytes_read = 0;
    
    /* Make a note of the time */
    gettimeofday(&wall_time_start, NULL);
    cpu_time_start = clock();
    
    /* Now read the data from the file */
    if (random == 's') {
        /* Read sequentially from the start until the end of the file */
        printf("Reading whole file sequentially...\n");
        while (!feof(fin)) {
            bytes_read = fread(buffer, sizeof(char), buffer_size, fin);
            buffers_read++;
            total_bytes_read += bytes_read;
        }
    } else if (random == 'r') {
        /* Read the specified number of times from random positions within the file */
        printf("Reading data from file randomly...\n");
        for (i = 0; i < random_read_count; i++) {
            //printf("Offsets[%d] = %llu\n", i, offsets[i]);
            /* Seek to the precomputed offset */
            if (fseeko(fin, offsets[i], SEEK_SET) < 0) {
                fprintf(stderr, "Error seeking to %llu, aborting due to internal error\n", offsets[i]);
                exit(EXIT_FAILURE);
            }
            /* Now read a buffer from this location */
            bytes_read = fread(buffer, sizeof(char), buffer_size, fin);
            buffers_read++;
            total_bytes_read += bytes_read;
        }
    } else {
        /* Read in a "hopping" pattern reading a block and then skipping three blocks */
        printf("Reading file in \"hopping\" pattern...\n");
        done = 0;
        while (!done) {
            /* Read a buffer */
            bytes_read = fread(buffer, sizeof(char), buffer_size, fin);
            buffers_read++;
            total_bytes_read += bytes_read;
            /* Skip three buffers' worth of data */
        	if (fseeko(fin, buffer_size * 3, SEEK_CUR) < 0) {
        	    fprintf(stderr, "Error in seek, aborting due to internal error\n");
        	    exit(EXIT_FAILURE);
        	}
        	if (bytes_read < buffer_size) {
        	    done = 1;
        	}
        }
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
        file_size,
        total_bytes_read,
        buffer_size,
        buffers_read,
        random == 'r' ? "random" : (random == 's' ? "sequential" : "hopping"),
        cpu_time_spent,
        wall_time_spent,
        read_rate
    );
    
    /* Close the file */
    fclose(fin);
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
