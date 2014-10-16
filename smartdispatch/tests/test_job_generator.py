from nose.tools import assert_true, assert_equal, assert_raises

import os
from smartdispatch.job_generator import JobGenerator, GuilliminJobGenerator, MammouthJobGenerator
from smartdispatch.job_generator import job_generator_factory
import unittest
import tempfile
import shutil


class TestJobGenerator(unittest.TestCase):
    def setUp(self):
        self.testing_dir = tempfile.mkdtemp()
        self.name = "qtest@mp2"
        self.walltime = "10:00"
        self.cores = 42
        self.modules = ["cuda", "python"]

        self.queue = {'queue_name': self.name,
                      'walltime': self.walltime,
                      'nb_cores_per_node': self.cores,
                      #'nb_gpus_per_node': None,
                      #'mem_per_node': None,
                      'modules': self.modules
                      }

        self.gpus = 42
        self.queue_gpu = {'queue_name': self.name,
                          'walltime': self.walltime,
                          'nb_cores_per_node': self.cores,
                          'nb_gpus_per_node': self.gpus,
                          #'mem_per_node': None,
                          'modules': self.modules
                          }

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


def test_job_generator_factory():
    queue = {"queue_name": "qtest"}
    commands = []
    job_generator = job_generator_factory(queue, commands, cluster_name="mammouth")
    assert_true(isinstance(job_generator, MammouthJobGenerator))

    job_generator = job_generator_factory(queue, commands, cluster_name="guillimin")
    assert_true(isinstance(job_generator, GuilliminJobGenerator))

    job_generator = job_generator_factory(queue, commands, cluster_name=None)
    assert_true(isinstance(job_generator, JobGenerator))
    assert_true(not isinstance(job_generator, GuilliminJobGenerator))
    assert_true(not isinstance(job_generator, MammouthJobGenerator))
