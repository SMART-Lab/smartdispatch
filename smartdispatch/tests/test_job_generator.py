from nose.tools import assert_true, assert_false, assert_equal, assert_raises

import os
import unittest
import tempfile
import shutil
from smartdispatch.queue import Queue
from smartdispatch.job_generator import JobGenerator, job_generator_factory
from smartdispatch.job_generator import HeliosJobGenerator, HadesJobGenerator
from smartdispatch.job_generator import GuilliminJobGenerator, MammouthJobGenerator


class TestJobGenerator(unittest.TestCase):

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

    def tearDown(self):
        shutil.rmtree(self.testing_dir)

    def test_generate_pbs(self):
        commands = ["echo 1", "echo 2", "echo 3", "echo 4"]

        job_generator = JobGenerator(self.queue, commands)

        # Test nb_cores_per_command argument
        # Should needs one PBS file
        pbs_list = job_generator.generate_pbs()
        assert_equal(len(pbs_list), 1)
        assert_equal(pbs_list[0].commands, commands)

        # Should needs two PBS file
        command_params = {'nb_cores_per_command': self.cores // 2}
        job_generator = JobGenerator(self.queue, commands, command_params)
        pbs_list = job_generator.generate_pbs()
        assert_equal(len(pbs_list), 2)
        assert_equal(pbs_list[0].commands, commands[:2])
        assert_equal(pbs_list[1].commands, commands[2:])

        # Should needs four PBS file
        command_params = {'nb_cores_per_command': self.cores}
        job_generator = JobGenerator(self.queue, commands, command_params)
        pbs_list = job_generator.generate_pbs()
        assert_equal(len(pbs_list), 4)
        assert_equal([pbs.commands[0] for pbs in pbs_list], commands)

        # Since queue has no gpus it should not be specified in PBS resource `nodes`
        assert_true('gpus' not in pbs_list[0].resources['nodes'])

        # Test modules to load
        # Check if needed modules for this queue are included in the PBS file
        assert_equal(pbs_list[0].modules, self.modules)

        # Test nb_gpus_per_command argument
        # Should needs two PBS file
        command_params = {'nb_gpus_per_command': self.gpus // 2}
        job_generator = JobGenerator(self.queue_gpu, commands, command_params)
        pbs_list = job_generator.generate_pbs()
        assert_equal(len(pbs_list), 2)
        assert_equal(pbs_list[0].commands, commands[:2])
        assert_equal(pbs_list[1].commands, commands[2:])

        # Should needs four PBS files
        command_params = {'nb_gpus_per_command': self.gpus}
        job_generator = JobGenerator(self.queue_gpu, commands, command_params)
        pbs_list = job_generator.generate_pbs()
        assert_equal(len(pbs_list), 4)
        assert_equal([pbs.commands[0] for pbs in pbs_list], commands)

        # Since queue has gpus it should be specified in PBS resource `nodes`
        assert_true('gpus' in pbs_list[0].resources['nodes'])

        # Test modules to load
        # Check if needed modules for this queue are included in the PBS file
        assert_equal(pbs_list[0].modules, self.modules)

        # Test creating a simple job generator
        queue = {"queue_name": "qtest"}
        job_generator = JobGenerator(queue, commands=[])

    def test_write_pbs_files(self):
        commands = ["echo 1", "echo 2", "echo 3", "echo 4"]
        command_params = {'nb_cores_per_command': self.cores}
        job_generator = JobGenerator(self.queue, commands, command_params)
        filenames = job_generator.write_pbs_files(self.testing_dir)
        assert_equal(len(filenames), 4)


class TestGuilliminQueue(TestJobGenerator):

    def test_generate_pbs(self):
        commands = ["echo 1", "echo 2", "echo 3", "echo 4"]
        job_generator = GuilliminJobGenerator(self.queue, commands)

        bak_env_home_group = os.environ.get('HOME_GROUP')
        if bak_env_home_group is not None:
            del os.environ['HOME_GROUP']

        assert_raises(ValueError, job_generator.generate_pbs)

        os.environ['HOME_GROUP'] = "/path/to/group"
        pbs = job_generator.generate_pbs()[0]
        assert_true("-A" in pbs.options)

        if bak_env_home_group is not None:
            os.environ['HOME_GROUP'] = bak_env_home_group


class TestMammouthQueue(unittest.TestCase):

    def setUp(self):
        self.commands = ["echo 1", "echo 2", "echo 3", "echo 4"]
        self.queue = Queue("qtest@mp2", "mammouth")

    def test_generate_pbs(self):
        job_generator = MammouthJobGenerator(self.queue, self.commands)

        assert_true("ppn=1" in job_generator.generate_pbs()[0].__str__())


class TestHeliosQueue(unittest.TestCase):

    def setUp(self):
        self.commands = ["echo 1", "echo 2", "echo 3", "echo 4"]
        self.queue = Queue("gpu_8", "helios")

        self.env_val = 'RAP'

        self.bak_env_home_group = os.environ.get(self.env_val)
        if self.bak_env_home_group is not None:
            del os.environ[self.env_val]
        os.environ[self.env_val] = "/rap/group/"

        self.job_generator = HeliosJobGenerator(self.queue, self.commands)

    def tearDown(self):
        if self.bak_env_home_group is not None:
            os.environ[self.env_val] = self.bak_env_home_group

    def test_generate_pbs_invalid_group(self):
        del os.environ[self.env_val]

        assert_raises(ValueError, self.job_generator.generate_pbs)

    def test_generate_pbs_valid_group(self):
        pbs = self.job_generator.generate_pbs()[0]

        assert_equal(pbs.options['-A'], "group")

    def test_generate_pbs_ppn_is_absent(self):
        assert_false("ppn=" in self.job_generator.generate_pbs()[0].__str__())

    def test_generate_pbs_even_nb_commands(self):
        assert_true("gpus=4" in self.job_generator.generate_pbs()[0].__str__())

    def test_generate_pbs_odd_nb_commands(self):
        commands = ["echo 1", "echo 2", "echo 3", "echo 4", "echo 5"]
        job_generator = HeliosJobGenerator(self.queue, commands)

        assert_true("gpus=6" in job_generator.generate_pbs()[0].__str__())


class TestHadesQueue(unittest.TestCase):

    def setUp(self):
        self.queue = Queue("@hades", "hades")

        self.commands4 = ["echo 1", "echo 2", "echo 3", "echo 4"]
        self.pbs4 = HadesJobGenerator(self.queue, self.commands4).generate_pbs()

        # 8 commands chosen because there is 8 cores but still should be split because there is 6 gpu
        self.commands8 = ["echo 1", "echo 2", "echo 3", "echo 4", "echo 5", "echo 6", "echo 7", "echo 8"]
        self.pbs8 = HadesJobGenerator(self.queue, self.commands8).generate_pbs()

    def test_generate_pbs_ppn(self):
        assert_true("ppn={}".format(len(self.commands4)) in self.pbs4[0].__str__())

    def test_generate_pbs_no_gpus_used(self):
        # Hades use ppn instead og the gpus flag and breaks if gpus is there
        assert_false("gpus=" in self.pbs4[0].__str__())

    def test_pbs_split_1_job(self):
        assert_equal(len(self.pbs4), 1)

    def test_pbs_split_2_job(self):
        assert_equal(len(self.pbs8), 2)

    def test_pbs_split_2_job_nb_commands(self):
        assert_true("ppn=6" in self.pbs8[0].__str__())
        assert_true("ppn=2" in self.pbs8[1].__str__())


def test_job_generator_factory():
    queue = {"queue_name": "qtest"}
    commands = []
    job_generator = job_generator_factory(queue, commands, cluster_name="guillimin")
    assert_true(isinstance(job_generator, GuilliminJobGenerator))

    job_generator = job_generator_factory(queue, commands, cluster_name="mammouth")
    assert_true(isinstance(job_generator, MammouthJobGenerator))

    job_generator = job_generator_factory(queue, commands, cluster_name="helios")
    assert_true(isinstance(job_generator, HeliosJobGenerator))

    job_generator = job_generator_factory(queue, commands, cluster_name="hades")
    assert_true(isinstance(job_generator, HadesJobGenerator))

    job_generator = job_generator_factory(queue, commands, cluster_name=None)
    assert_true(isinstance(job_generator, JobGenerator))
    assert_true(not isinstance(job_generator, GuilliminJobGenerator))
    assert_true(not isinstance(job_generator, MammouthJobGenerator))
    assert_true(not isinstance(job_generator, HeliosJobGenerator))
    assert_true(not isinstance(job_generator, HadesJobGenerator))
