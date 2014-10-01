import os
import math
import smartdispatch
from smartdispatch import utils


def pbs_generator_factory():
    cluster_name = utils.detect_cluster()
    if cluster_name == "mammouth":
        return PBSGeneratorMammouth
    elif cluster_name == "guillimin":
        return PBSGeneratorGuillimin

    print "Unknown cluster, using generic PBD generator."
    return PBSGenerator


class PBSGenerator:
    ''' Generates the header content of a PBS file used by the command qsub.

    Parameters
    ----------
    queue : str
        name of the queue on which commands will be excuted
    walltime : str
        maximum time allocated to execute every commands (DD:HH:MM:SS)
    cores : int
        number of cores available per node
    gpus : int
        number of gpus available per node
    modules : list of str
        list of modules to load prior executing commands (default: [])
    cwd : str
            current working directory where commands will be executed (default: os.getcwd())
    '''
    def __init__(self, commands, nb_cores_per_command, queue, walltime, cores, gpus, modules, cwd=os.getcwd()):
        if walltime is None and cores is None:
            raise ValueError("Walltime and cores must be set.")

        self.queue = queue
        self.walltime = walltime
        self.cores = cores
        self.gpus = gpus
        self.modules = modules
        self.cwd = cwd
        self.commands = commands
        self.nb_cores_per_command = nb_cores_per_command

        self.nb_commands_per_node = min(len(self.commands)*self.nb_cores_per_command, self.cores//self.nb_cores_per_command)
        self.nb_nodes = int(math.ceil(len(self.commands) / float(self.nb_commands_per_node)))

    def _load_config(self, configname):
        smartdispatch_dir, _ = os.path.split(smartdispatch.__file__)
        config_dir = os.path.join(smartdispatch_dir, 'config')
        return utils.load_dict_from_json_file(os.path.join(config_dir, configname))

    def save_to_files(self, pbs_dir):
        pbs_filenames = []

        # Distribute equally the jobs among the PBS files and generate those files
        for i, commands in enumerate(utils.chunks(self.commands, n=self.nb_commands_per_node)):
            pbs_filename = os.path.join(pbs_dir, 'job_commands_' + str(i) + '.sh')
            with open(pbs_filename, 'w') as pbs_file:

                pbs_content = self.generate_pbs(commands)
                pbs_file.write(pbs_content)
                pbs_filenames.append(pbs_filename)

        return pbs_filenames

    def get_header(self, commands):
        pbs = []
        pbs += ["#!/bin/bash"]
        pbs += ["#PBS -q " + self.queue]
        pbs += ["#PBS -V"]

        pbs += ["#PBS -l walltime=" + self.walltime]

        ppn = len(commands) * self.nb_cores_per_command
        pbs += ["#PBS -l nodes=1:ppn={ppn}".format(ppn=ppn)]

        if self.gpus > 0:
            pbs[-1] += ":gpus=" + str(ppn)

        return "\n".join(pbs)

    def get_modules_section(self):
        pbs = ["\n# Modules #"]
        if self.modules is not None:
            for module in self.modules:
                pbs += ["module load " + module]

        return "\n".join(pbs)

    def get_commands_section(self, commands):
        pbs = ["\n# Commands #"]

        command_template = 'cd ' + self.cwd + '; {command} &'
        pbs += [command_template.format(command=command) for command in commands]

        return "\n".join(pbs)

    def generate_pbs(self, commands):
        pbs = ""
        pbs += self.get_header(commands) + "\n"
        pbs += self.get_modules_section() + "\n"
        pbs += self.get_commands_section(commands) + "\n"
        pbs += "\nwait"
        return pbs


class PBSGeneratorMammouth(PBSGenerator):
    configname = "mammouth.json"

    def __init__(self, commands, nb_cores_per_command, queue, walltime=None, cores=None, gpus=None, modules=None, cwd=os.getcwd()):
        infos = self._load_config(self.configname)

        # Gather information from config file when possible
        if queue in infos:
            if walltime is None:
                walltime = infos['max_walltime']

            if cores is None:
                cores = infos['cores']

            if gpus is None:
                gpus = infos['gpus']

            if modules is None:
                modules = infos['modules']

        PBSGenerator.__init__(self, commands, nb_cores_per_command, queue, walltime, cores, gpus, modules, cwd)


class PBSGeneratorGuillimin(PBSGenerator):
    configname = "guillimin.json"

    def __init__(self, commands, queue, nb_cores_per_command, walltime=None, cores=None, gpus=None, modules=None, cwd=os.getcwd()):
        infos = self._load_config(self.configname)

        # Gather information from config file when possible
        if queue in infos:
            if walltime is None:
                walltime = infos['max_walltime']

            if cores is None:
                cores = infos['cores']

            if gpus is None:
                gpus = infos['gpus']

            if modules is None:
                modules = infos['modules']

        PBSGenerator.__init__(self, commands, nb_cores_per_command, queue, walltime, cores, gpus, modules, cwd)

        self.account_name = os.path.split(os.getenv('HOME_GROUP'))[-1]

    def get_header(self):
        pbs = PBSGenerator.get_header(self)
        pbs += "#PBS -A " + self.account_name
        return pbs
