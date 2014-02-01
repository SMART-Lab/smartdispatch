import os, sys


f = open('jobCommands.sh', 'w')
f.write('#!/bin/bash\n')
f.write('#PBS -q qfat256@mp2\n')
f.write('#PBS -l nodes=1:ppn=1\n')
f.write('#--Exporting environment variables from the submission shell to the job shell\n')
f.write('#PBS -V\n')
f.write('#PBS -l walltime=2:12:00:00\n')
f.write('#module load cuda\n')
f.write('#SRC=/home/laulysta/exp/smart_lab_experiments/Stan/nips13/multiLangLearning_fr_en/translateAutoEnc\n')
f.write('\n\n\n')



f.write('#cd $SRC ; python -u trainAutoEnc2.py 10 80 sigmoid 0.1 vocKL_sarath_german True True > trainAutoEnc2.py-10-80-sigmoid-0.1-vocKL_sarath_german-True-True &\n')








f.write('\n\n\n')
f.write('#wait\n')
f.close()
