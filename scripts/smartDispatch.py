#!/usr/bin/env python
import os
import argparse
import datetime
import math
from subprocess import check_output


AVAILABLE_QUEUES = \
{
    # Parallel Mammouth
    'qtest@mp2':    {'coresPerNode': 24,  'maxWalltime': '00:01:00:00'},
    'qwork@mp2':    {'coresPerNode': 24,  'maxWalltime': '05:00:00:00'},
    'qfbb@mp2':     {'coresPerNode': 288, 'maxWalltime': '05:00:00:00'},
    'qfat256@mp2':  {'coresPerNode': 48,  'maxWalltime': '05:00:00:00'},
    'qfat512@mp2':  {'coresPerNode': 48,  'maxWalltime': '02:00:00:00'},

    # Serial Mammouth
    'qtest@ms': {'coresPerNode': 8, 'maxWalltime': '00:01:00:00'},
    'qwork@ms': {'coresPerNode': 8, 'maxWalltime': '05:00:00:00'},
    'qlong@ms': {'coresPerNode': 8, 'maxWalltime': '41:16:00:00'},

    # GPU Mammouth
    # 'qwork@brume' : {'coresPerNode' : 0, 'maxWalltime' : '05:00:00:00'} # coresPerNode is variable and not relevant for this queue
}


def main():
    args = parse_arguments()

    if args.file is not None:
        # Jobs are listed in a file.
        jobname = args.file.name
        commands, logs_name = read_commands(args.file)
    else:
        # Jobs need to be parsed and unfold.
        arguments = []
        for opt in args.commandAndOptions:
            opt_split = opt.split()
            for i, split in enumerate(opt_split):
                opt_split[i] = os.path.normpath(split)  # If the arg value is a path, remove the final '/' if there is one at the end.
            arguments += [opt_split]

        jobname = generate_name(arguments)
        commands, logs_name = unfold_commands(arguments)

    job_dir, qsub_dir = create_job_folders(jobname)

    # Distribute equally the jobs among the QSUB files and generate those files
    nb_commands = len(commands)
    nb_jobs = int(math.ceil(nb_commands / float(args.jobsPerNode)))
    nb_commands_per_file = int(math.ceil(nb_commands / float(nb_jobs)))

    qsub_filenames = []
    for i in range(nb_jobs):
        start = i * nb_commands_per_file
        end = (i + 1) * nb_commands_per_file
        
        if end > nb_commands:
            end = nb_commands

        qsub_filename = os.path.join(qsub_dir, 'jobCommands_' + str(i) + '.sh')
        write_qsub_file(commands[start:end], logs_name[start:end], qsub_filename, job_dir, args.queueName, args.walltime, os.getcwd(), args.cuda)
        qsub_filenames.append(qsub_filename)

    # Launch the jobs with QSUB
    if not args.doNotLaunchJobs:
        for qsub_filename in qsub_filenames:
            qsub_output = check_output('qsub ' + qsub_filename, shell=True)
            print qsub_output,


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--queueName', required=True, help='Queue used (ex: qwork@mp2, qfat256@mp2, qfat512@mp2)')
    parser.add_argument('-t', '--walltime', required=False, help='Set the estimated running time of your jobs using the DD:HH:MM:SS format. Note that they will be killed when this time limit is reached.')
    parser.add_argument('-n', '--jobsPerNode', type=int, required=False, help='Set the number of jobs per nodes.')
    parser.add_argument('-c', '--cuda', action='store_true', help='Load CUDA before executing your code.')
    parser.add_argument('-x', '--doNotLaunchJobs', action='store_true', help='Creates the QSUB files without launching them.')
    parser.add_argument('-f', '--file', type=file, required=False, help='File containing commands to launch. Each command must be on a seperate line. (Replaces commandAndOptions)')
    parser.add_argument("commandAndOptions", help="Options for the command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    # Check for invalid arguments
    if args.file is None and len(args.commandAndOptions) < 1:
        parser.error("You need to specify a command to launch.")
    if args.queueName not in AVAILABLE_QUEUES and (args.jobsPerNode is None or args.walltime is None):
        parser.error("Unknown queue, --jobsPerNode and --walltime must be set.")
    
    # Set queue defaults for non specified params
    if args.jobsPerNode is None:
        args.jobsPerNode = AVAILABLE_QUEUES[args.queueName]['coresPerNode']
    if args.walltime is None:
        args.walltime = AVAILABLE_QUEUES[args.queueName]['maxWalltime']
    return args


def read_commands(fileobj):
    commands = fileobj.read().split('\n')
    logs_name = ['{0}_command_{1}.log'.format(fileobj.name, i) for i in range(len(commands))]
    return commands, logs_name


def unfold_commands(arguments):
    commands = ['']
    logs_name = ['']

    for argument in arguments:
        commands_tmp = []
        logs_name_tmp = []
        for valueForArg in argument:
            for job_str, folderName in zip(commands, logs_name):
                commands_tmp += [job_str + valueForArg + ' ']
                valueForArg_tmp = valueForArg[-30:].split('/')[-1]  # Deal with path as parameter
                logs_name_tmp += [valueForArg_tmp] if folderName == '' else [folderName + '-' + valueForArg_tmp]
        commands = commands_tmp
        logs_name = logs_name_tmp
    return commands, logs_name


def generate_name(arguments, max_length=255):
    # Creating the folder in 'LOGS_QSUB' where the results will be saved
    name = ''
    for argument in arguments:
        str_tmp = argument[0][-30:] + ('' if len(argument) == 1 else ('-' + argument[-1][-30:]))
        str_tmp = str_tmp.split('/')[-1]  # Deal with path as parameter
        name += str_tmp if name == '' else ('__' + str_tmp)

    current_time = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    name = current_time + ' ' + name[:max_length-len(current_time)-1]  # No more than 256 character
    return name

def create_job_folders(jobname):
    """Creates the folders where the logs and QSUB files will be saved."""
    path_logs = os.path.join(os.getcwd(), 'LOGS_QSUB')
    path_job_logs = os.path.join(path_logs, jobname)
    path_job_commands = os.path.join(path_job_logs, 'QSUB_commands')
    
    if not os.path.exists(path_job_commands):
        os.makedirs(path_job_commands)
    
    return path_job_logs, path_job_commands

    # subPathLogs = os.path.join(pathLogs, log_folder)
    # if not os.path.exists(subPathLogs):
    #     os.makedirs(subPathLogs)
    #
    # Creating the folder where the QSUB files will be saved
    # qsub_folder_path = os.path.join(subPathLogs, 'QSUB_commands')
    # if not os.path.exists(qsub_folder_path):
    #     os.makedirs(qsub_folder_path)
    # return subPathLogs, qsub_folder_path


def write_qsub_file(commands, logs_name, qsub_filename, job_dir, queue, walltime, current_dir, use_cuda=False):
    """
    Example of a line for one job for QSUB:
        cd $SRC ; python -u trainAutoEnc2.py 10 80 sigmoid 0.1 vocKL_sarath_german True True > trainAutoEnc2.py-10-80-sigmoid-0.1-vocKL_sarath_german-True-True &
    """
    # Creating the file that will be launch by QSUB
    with open(qsub_filename, 'w') as qsub_file:
        qsub_file.write('#!/bin/bash\n')
        qsub_file.write('#PBS -q ' + queue + '\n')
        qsub_file.write('#PBS -l nodes=1:ppn=1\n')
        qsub_file.write('#PBS -V\n')
        qsub_file.write('#PBS -l walltime=' + walltime + '\n\n')

        if use_cuda:
            qsub_file.write('module load cuda\n')

        qsub_file.write('SRC_DIR_SMART_LAUNCHER=' + current_dir + '\n\n')

        command_template = "cd $SRC_DIR_SMART_LAUNCHER; {0} &> {1} &\n"
        for command, log_name in zip(commands, logs_name):
            qsub_file.write(command_template.format(command, os.path.join(job_dir, log_name)))

        qsub_file.write('\nwait\n')


if __name__ == "__main__":
    main()
