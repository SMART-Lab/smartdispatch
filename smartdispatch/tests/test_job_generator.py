from nose.tools import assert_true, assert_false, assert_equal, assert_raises

import os
import tempfile
import shutil
from smartdispatch.queue import Queue
from smartdispatch.job_generator import JobGenerator, job_generator_factory
from smartdispatch.job_generator import HeliosJobGenerator, HadesJobGenerator
from smartdispatch.job_generator import GuilliminJobGenerator, MammouthJobGenerator


class TestJobGenerator(object):
    pbs_flags = ['-lfeature=k80', '-lwalltime=42:42', '-lnodes=6:gpus=66', '-m', '-A123-asd-11', '-t10,20,30']

    def setUp(self):
        self.testing_dir = tempfile.mkdtemp()
        self.cluster_name = "skynet"
        self.name = "9000@hal"
        self.walltime = "10:00"
        self.cores = 42
        self.gpus = 42
        self.mem_per_node = 32
        self.modules = ["cuda", "python"]

        self.queue = Queue(self.name, self.cluster_name, self.walltime, self.cores, 0, self.mem_per_node, self.modules)
        self.queue_gpu = Queue(self.name, self.cluster_name, self.walltime, self.cores, self.gpus, self.mem_per_node, self.modules)

        self.commands = ["echo 1", "echo 2", "echo 3", "echo 4"]
        self.prolog = ["echo prolog"]
        self.epilog = ["echo epilog"]

    def tearDown(self):
        shutil.rmtree(self.testing_dir)

    def test_generate_pbs(self):
        job_generator = JobGenerator(self.queue, self.commands, prolog=self.prolog, epilog=self.epilog)

        # Test nb_cores_per_command argument
        # Should needs one PBS file
        assert_equal(len(job_generator.pbs_list), 1)
        assert_equal(job_generator.pbs_list[0].commands, self.commands)
        assert_equal(job_generator.pbs_list[0].prolog, self.prolog)
        assert_equal(job_generator.pbs_list[0].epilog, self.epilog)

    def test_generate_pbs2_cpu(self):
        # Should needs two PBS file
        command_params = {'nb_cores_per_command': self.cores // 2}
        job_generator = JobGenerator(self.queue, self.commands, command_params=command_params)
        assert_equal(len(job_generator.pbs_list), 2)
        assert_equal(job_generator.pbs_list[0].commands, self.commands[:2])
        assert_equal(job_generator.pbs_list[1].commands, self.commands[2:])

    def test_generate_pbs4_cpu(self):
        # Should needs four PBS file
        command_params = {'nb_cores_per_command': self.cores}
        job_generator = JobGenerator(self.queue, self.commands, command_params=command_params)
        assert_equal(len(job_generator.pbs_list), 4)
        assert_equal([pbs.commands[0] for pbs in job_generator.pbs_list], self.commands)

        # Since queue has no gpus it should not be specified in PBS resource `nodes`
        assert_true('gpus' not in job_generator.pbs_list[0].resources['nodes'])

        # Test modules to load
        # Check if needed modules for this queue are included in the PBS file
        assert_equal(job_generator.pbs_list[0].modules, self.modules)

    def test_generate_pbs2_gpu(self):
        # Test nb_gpus_per_command argument
        # Should needs two PBS file
        command_params = {'nb_gpus_per_command': self.gpus // 2}
        job_generator = JobGenerator(self.queue_gpu, self.commands, command_params=command_params)
        assert_equal(len(job_generator.pbs_list), 2)
        assert_equal(job_generator.pbs_list[0].commands, self.commands[:2])
        assert_equal(job_generator.pbs_list[1].commands, self.commands[2:])

    def test_generate_pbs4_gpu(self):
        # Should needs four PBS files
        command_params = {'nb_gpus_per_command': self.gpus}
        job_generator = JobGenerator(self.queue_gpu, self.commands, command_params=command_params)
        assert_equal(len(job_generator.pbs_list), 4)
        assert_equal([pbs.commands[0] for pbs in job_generator.pbs_list], self.commands)

        # Since queue has gpus it should be specified in PBS resource `nodes`
        assert_true('gpus' in job_generator.pbs_list[0].resources['nodes'])

        # Test modules to load
        # Check if needed modules for this queue are included in the PBS file
        assert_equal(job_generator.pbs_list[0].modules, self.modules)

    def test_write_pbs_files(self):
        commands = ["echo 1", "echo 2", "echo 3", "echo 4"]
        command_params = {'nb_cores_per_command': self.cores}
        job_generator = JobGenerator(self.queue, commands, command_params=command_params)
        filenames = job_generator.write_pbs_files(self.testing_dir)
        assert_equal(len(filenames), 4)

    def _test_add_pbs_flags(self, flags):
        job_generator = JobGenerator(self.queue, self.commands)
        job_generator.add_pbs_flags(flags)

        resources = []
        options = []

        for flag in flags:
            if flag.startswith('-l'):
                resources += [flag[:2] + ' ' + flag[2:]]
            elif flag.startswith('-'):
                options += [(flag[:2] + ' ' + flag[2:]).strip()]

        for pbs in job_generator.pbs_list:
            pbs_str = pbs.__str__()
            for flag in resources:
                assert_equal(pbs_str.count(flag), 1)
                assert_equal(pbs_str.count(flag[:flag.find('=')]), 1)
            for flag in options:
                assert_equal(pbs_str.count(flag), 1)

    def test_add_pbs_flags(self):
        for flag in self.pbs_flags:
            yield self._test_add_pbs_flags, [flag]

        yield self._test_add_pbs_flags, self.pbs_flags

    def test_add_pbs_flags_invalid(self):
        assert_raises(ValueError, self._test_add_pbs_flags, 'weeee')

    def test_add_pbs_flags_invalid_resource(self):
        assert_raises(ValueError, self._test_add_pbs_flags, '-l weeee')


class TestGuilliminQueue(object):

    def setUp(self):
        self.commands = ["echo 1", "echo 2", "echo 3", "echo 4"]
        self.queue = Queue("test", "guillimin", "00:01", 1, 1, 1)

        self.bak_env_home_group = os.environ.get('HOME_GROUP')
        if self.bak_env_home_group is not None:
            del os.environ['HOME_GROUP']

    def tearDown(self):
        if self.bak_env_home_group is not None:
            os.environ['HOME_GROUP'] = self.bak_env_home_group

    def test_generate_pbs_no_home(self):
        assert_raises(ValueError, GuilliminJobGenerator, self.queue, self.commands)

    def test_generate_pbs(self):
        os.environ['HOME_GROUP'] = "/path/to/group"
        job_generator = GuilliminJobGenerator(self.queue, self.commands)
        pbs = job_generator.pbs_list[0]
        assert_true("-A" in pbs.options)
        assert_true(pbs.options["-A"] == 'group')


class TestMammouthQueue(object):

    def setUp(self):
        self.commands = ["echo 1", "echo 2", "echo 3", "echo 4"]
        self.queue = Queue("qtest@mp2", "mammouth")

    def test_generate_pbs(self):
        job_generator = MammouthJobGenerator(self.queue, self.commands)

        assert_true("ppn=1" in str(job_generator.pbs_list[0]))


class TestHeliosQueue(object):

    def setUp(self):
        self.commands = ["echo 1", "echo 2", "echo 3", "echo 4"]
        self.queue = Queue("gpu_8", "helios")

        self._home_backup = os.environ['HOME']
        os.environ['HOME'] = tempfile.mkdtemp()

        self.rap_filename = os.path.join(os.environ['HOME'], ".default_rap")
        if os.path.isfile(self.rap_filename):
            raise Exception("Test fail: {} should not be there.".format(self.rap_filename))
        else:
            self.rapid = 'asd-123-ab'
            with open(self.rap_filename, 'w+') as rap_file:
                rap_file.write(self.rapid)

        self.job_generator = HeliosJobGenerator(self.queue, self.commands)

    def tearDown(self):
        shutil.rmtree(os.environ['HOME'])
        os.environ['HOME'] = self._home_backup

    def test_generate_pbs_invalid_group(self):
        os.remove(self.rap_filename)

        assert_raises(ValueError, HeliosJobGenerator, self.queue, self.commands)

    def test_generate_pbs_valid_group(self):
        pbs = self.job_generator.pbs_list[0]

        assert_equal(pbs.options['-A'], self.rapid)

    def test_generate_pbs_ppn_is_absent(self):
        assert_false("ppn=" in str(self.job_generator.pbs_list[0]))

    def test_generate_pbs_even_nb_commands(self):
        assert_true("gpus=4" in str(self.job_generator.pbs_list[0]))

    def test_generate_pbs_odd_nb_commands(self):
        commands = ["echo 1", "echo 2", "echo 3", "echo 4", "echo 5"]
        job_generator = HeliosJobGenerator(self.queue, commands)

        assert_true("gpus=5" in str(job_generator.pbs_list[0]))


class TestHadesQueue(object):

    def setUp(self):
        self.queue = Queue("@hades", "hades")

        self.commands4 = ["echo 1", "echo 2", "echo 3", "echo 4"]
        job_generator = HadesJobGenerator(self.queue, self.commands4)
        self.pbs4 = job_generator.pbs_list

        # 8 commands chosen because there is 8 cores but still should be split because there is 6 gpu
        self.commands8 = ["echo 1", "echo 2", "echo 3", "echo 4", "echo 5", "echo 6", "echo 7", "echo 8"]
        job_generator = HadesJobGenerator(self.queue, self.commands8)
        self.pbs8 = job_generator.pbs_list

    def test_generate_pbs_ppn(self):
        assert_true("ppn={}".format(len(self.commands4)) in str(self.pbs4[0]))

    def test_generate_pbs_no_gpus_used(self):
        # Hades use ppn instead og the gpus flag and breaks if gpus is there
        assert_false("gpus=" in str(self.pbs4[0]))

    def test_pbs_split_1_job(self):
        assert_equal(len(self.pbs4), 1)

    def test_pbs_split_2_job(self):
        assert_equal(len(self.pbs8), 2)

    def test_pbs_split_2_job_nb_commands(self):
        assert_true("ppn=6" in str(self.pbs8[0]))
        assert_true("ppn=2" in str(self.pbs8[1]))


class TestJobGeneratorFactory(object):

    def setUp(self):
        self._home_backup = os.environ['HOME']
        os.environ['HOME'] = tempfile.mkdtemp()

        self.rap_filename = os.path.join(os.environ['HOME'], ".default_rap")
        if os.path.isfile(self.rap_filename):
            raise Exception("Test fail: {} should not be there.".format(self.rap_filename))
        else:
            self.rapid = 'asd-123-ab'
            with open(self.rap_filename, 'w+') as rap_file:
                rap_file.write(self.rapid)

    def tearDown(self):
        shutil.rmtree(os.environ['HOME'])
        os.environ['HOME'] = self._home_backup

    def _test_job_generator_factory(self, cluster_name, job_generator_class):
        q = Queue("test", cluster_name, 1, 1, 1, 1)
        job_generator = job_generator_factory(q, [], cluster_name=cluster_name)
        assert_true(isinstance(job_generator, job_generator_class))
        assert_true(type(job_generator) is job_generator_class)

    def test_job_generator_factory(self):
        clusters = [("guillimin", GuilliminJobGenerator),
                    ("mammouth", MammouthJobGenerator),
                    ("helios", HeliosJobGenerator),
                    ("hades", HadesJobGenerator),
                    (None, JobGenerator)]

        for cluster_name, job_generator_class in clusters:
            yield self._test_job_generator_factory, cluster_name, job_generator_class
