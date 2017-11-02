from distutils.core import setup, Extension
from distutils.command.build import build
import os
import subprocess
import fnmatch

def find_package_data_files(directory):
    for root, dirs, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, '*'):
                filename = os.path.join(root, basename)
#                if filename.find('/.svn') > -1:
#                    continue
                yield filename.replace('userbased_nc_bench/', '', 1)



package_data = [f for f in find_package_data_files('userbased_nc_bench')]


class build_C(build):

    def run(self):
        # Run original build code
        build.run(self)
        print 'Running build_C'
        build_dir = os.path.join(os.path.abspath(self.build_lib), 'userbased_nc_bench/')

        cmd = ['make', '-C', build_dir]

        rc = subprocess.call(cmd)

        def compile():

            rc = subprocess.call(cmd)
            print '\n','*'*80
            if not rc:
                print 'SUCCESSFULLY BUILT '
            else:
                print 'WARNING: Errors during build will cause C tests to not run'
        print '-' * 80, '\n'

        print '*' * 80
        print "build successful"

        self.execute(compile, [], 'compiling')
#--- End: class


setup(name='userbased-nc-bench',
	version='0.2.0',
	description='Tests read parallel read rates from NetCDF4 files using Python and C. Limited functional at current.',
	url='https://bitbucket.org/m_jones3/userbased-bench',
	author='Matthew Jones',
	author_email='m.jones3@pgr.reading.ac.uk',
	license='MIT',
	packages=['userbased_nc_bench'],
	zip_safe=False,
	install_requires=[
			'netCDF4',
			'minio',
			'h5py',
			'h5pyd'
			],
    package_data = {'userbased_nc_bench': package_data},
	dependency_links=['https://github.com/cedadev/S3-netcdf-python'],
      cmdclass     = {'build': build_C}

)
