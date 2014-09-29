import os


class PBSGenerator:
    ''' Generates the header content of a PBS file used by the command qsub.

    Parameters
    ----------
    queue : str
        name of the queue on which commands will be excuted
    walltime : str
        maximum time allocated to execute every commands (DD:HH:MM:SS)
    nodes : int
        number of nodes needed (default: 1)
    ppn: int
        number of cpus needed (default: 1)
    gpus : int
        number of gpus needed (default: 0)
    modules : list of str
        list of modules to load prior executing commands (default: [])
    cwd : str
            current working directory where commands will be executed (default: os.getcwd())
    '''
    def __init__(self, queue, walltime, nodes=1, ppn=1, gpus=0, modules=[], cwd=os.getcwd()):
        self.queue = queue
        self.walltime = walltime
        self.nodes = nodes
        self.ppn = ppn
        self.gpus = gpus
        self.modules = modules
        self.cwd = cwd
        self.commands = []

    def add_commands(self, *commands):
        self.commands += commands

    def clear_commands(self):
        self.commands = []

    def save(self, filename):
        with open(filename, 'w') as pbs_file:
            pbs_file.write(str(self))

    def get_header(self):
        pbs = []
        pbs += ["#!/bin/bash"]
        pbs += ["#PBS -q " + self.queue]
        pbs += ["#PBS -V"]

        # if "account_name" in kwargs:
        #     pbs += ["#PBS -A " + kwargs['account_name']]

        pbs += ["#PBS -l walltime=" + self.walltime]
        pbs += ["#PBS -l nodes={nodes}:ppn={ppn}".format(nodes=self.nodes, ppn=self.ppn)]

        if self.gpus > 0:
            pbs[-1] += ":gpus=" + str(self.gpus)

        return "\n".join(pbs)

    def get_modules_section(self):
        pbs = ["\n# Modules #"]
        for module in self.modules:
            pbs += ["module load " + module]

        return "\n".join(pbs)

    def get_commands_section(self):
        pbs = ["\n# Commands #"]

        command_template = 'cd ' + self.cwd + '; {command} &'
        pbs += [command_template.format(command=command) for command in self.commands]

        return "\n".join(pbs)

    def __str__(self):
        pbs = ""
        pbs += self.get_header() + "\n"
        pbs += self.get_modules_section() + "\n"
        pbs += self.get_commands_section() + "\n"
        pbs += "\nwait"
        return pbs
