import pandas
import sys
from glob import glob

# get test type

test = sys.argv[1]

results_files = glob('{}*'.format(test))

#print(results)

for r in results_files:
    data = pandas.read_csv(r, names = ["job_id",2,3,4,5,6,7,8,9,10,11])
    ids = set(data['job_id'])
    print('{}: {}'.format(r,len(ids)))
    print('')
