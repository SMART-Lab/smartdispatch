import os, sys
import argparse
import datetime
import numpy as np

def writeQjobFile(list_jobs_str, list_jobsOutput_folderName, qjobFile, subPathLogs):
    # Creating the file that will be launch by QJOB
    f = open(qjobFile, 'w')
    f.write('#!/bin/bash\n')
    f.write('#PBS -q qfat256@mp2\n')
    f.write('#PBS -l nodes=1:ppn=1\n')
    f.write('#--Exporting environment variables from the submission shell to the job shell\n')
    f.write('#PBS -V\n')
    f.write('#PBS -l walltime=2:12:00:00\n')
    f.write('#module load cuda\n')
    f.write('#SRC=/home/laulysta/exp/smart_lab_experiments/Stan/nips13/multiLangLearning_fr_en/translateAutoEnc\n')
    f.write('\n\n\n')

    for job, folderName in zip(list_jobs_str, list_jobsOutput_folderName):
        f.write(job + ' > ' + os.path.join(subPathLogs, folderName) + ' &\n')

    f.write('\n\n\n')
    f.write('#wait\n')
    f.close()

    


parser = argparse.ArgumentParser()
parser.add_argument('-q', '--queue', required=False, default='qwork@mp2', help='Queue used (ex: qwork@mp2, qfat256@mp2, qfat512@mp2)')
#parser.add_argument('command', nargs='?' , default='python', help='Program used (ex: python)')
args = parser.parse_args()

if args.queue == 'qfat256@mp2' or  args.queue == 'qfat512@mp2':
    nbCoreByNode = 48
else:
    nbCoreByNode = 24


list_commandAndOptions = [['python'], ['-u'], ['trainAutoEnc2.py'], ['10'], ['40','80'], ['sigmoid','tanh'], ['0.1','0.05','0.01'], ['vocKL_sarath_german'], ['True'], ['True']]

# Creating the LOGS folder
currentDir = os.getcwd()
pathLogs = os.path.join(currentDir, 'LOGS_QJOB')
if not os.path.exists(pathLogs):
    os.makedirs(pathLogs)


# Creating the folder in 'LOGS_QJOB' where the info will be saved
nameFolderSavingLogs = ''
for argument in list_commandAndOptions:
    str_tmp = argument[0][-30:] + ('' if len(argument) == 1 else ('-' + argument[-1][-30:]))
    nameFolderSavingLogs += str_tmp if nameFolderSavingLogs == '' else ('__' + str_tmp)
current_time = datetime.datetime.now()
nameFolderSavingLogs = current_time.isoformat() + '___' + nameFolderSavingLogs
subPathLogs = os.path.join(pathLogs, nameFolderSavingLogs)
if not os.path.exists(subPathLogs):
    os.makedirs(subPathLogs)


# Creating the folder where the QJOB files will be saved
pathQjobFolder = os.path.join(subPathLogs, 'QJOB_commands')
if not os.path.exists(pathQjobFolder):
    os.makedirs(pathQjobFolder)





## Example of a line for one job for QJOB
#f.write('#cd $SRC ; python -u trainAutoEnc2.py 10 80 sigmoid 0.1 vocKL_sarath_german True True > trainAutoEnc2.py-10-80-sigmoid-0.1-vocKL_sarath_german-True-True &\n')

list_jobs_str = ['#cd $SRC ;']
list_jobsOutput_folderName = ['']
for argument in list_commandAndOptions:
    list_jobs_tmp = []
    list_folderName_tmp = []
    for valueForArg in argument:
        for job_str, folderName in zip(list_jobs_str, list_jobsOutput_folderName):
            list_jobs_tmp += [job_str + ' ' + valueForArg]
            list_folderName_tmp += [valueForArg[-30:]] if folderName == '' else [folderName + '-' + valueForArg[-30:]]
    list_jobs_str = list_jobs_tmp
    list_jobsOutput_folderName = list_folderName_tmp

nbJobsTotal = len(list_jobs_str)
nbQjobFiles = int(np.ceil(nbJobsTotal/float(nbCoreByNode)))
nbJobPerFile =  int(np.ceil(nbJobsTotal/float(nbQjobFiles)))

list_qjobFiles = []
for i in range(nbQjobFiles):
    start = i*nbJobPerFile
    end = (i+1)*nbJobPerFile
    if end > nbJobsTotal:
        end = nbJobsTotal
    qjobFile = os.path.join(pathQjobFolder, 'jobCommands_' + str(i) + '.sh')
    writeQjobFile(list_jobs_str[start:end], list_jobsOutput_folderName[start:end], qjobFile, subPathLogs)
    list_qjobFiles += [qjobFile]
