all: readfile_nc.c
	gcc -I/usr/include -Wall -g -o readfile_nc readfile_nc.c -L/usr/lib64 -lnetcdf
	gcc -Wall -o readfile_bin readfile_bin.c

clean:
	$(RM) readfile_nc
	$(RM) readfile_bin
