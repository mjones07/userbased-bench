from setuptools import setup

setup(name='userbased-nc-bench',
	version='0.1.2',
	description='Tests read parallel read rates from NetCDF4 files using Python and C. Limited functional at current.',
	url='https://bitbucket.org/m_jones3/userbased-bench',
	author='Matthew Jones',
	author_email='m.jones3@pgr.reading.ac.uk',
	license='MIT',
	packages=['userbased_nc_bench'],
	zip_safe=False
,	install_requires=[
			'netCDF4',
			'mpi4py',
			'numpy',
			]
)
