[![Build Status](https://travis-ci.org/SMART-Lab/smartdispatch.png)](https://travis-ci.org/SMART-Lab/smartdispatch)
[![Coverage Status](https://coveralls.io/repos/SMART-Lab/smartdispatch/badge.png)](https://coveralls.io/r/SMART-Lab/smartdispatch)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/SMART-Lab/smartdispatch/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/SMART-Lab/smartdispatch/?branch=master)
# Smart Dispatch
A batch launcher for supercomputers using qsub (Torque).

## Installing
`pip install git+https://github.com/SMART-Lab/smartdispatch`

## Usage
See `smart_dispatch.py --help`

## Examples
###Basic
A batch composed of four variations of a simple command.

`smart_dispatch.py -q qtest@mp2 launch python my_script.py "1 2" 80 "tanh sigmoid" 0.1`

This will generate 4 different commands and launch them on the queue qtest@mp2:
```
python my_script.py 1 80 sigmoid 0.1
python my_script.py 1 80 tanh 0.1
python my_script.py 2 80 sigmoid 0.1
python my_script.py 2 80 tanh 0.1
```
The output/error logs in are saved in the folder `./SMART_DISPATCH_LOGS/{job_id}/logs/`.


###Using a pool of workers
`smart_dispatch.py -q qtest@mp2 -p 2 launch python my_script.py "1 2" 80 "tanh sigmoid" 0.1`

This will behave exactly the same way as the basic example above but the number of commands launched on the supercomputer will be 2 instead of 4 and each job will be in charge of running 2 commands each.


###Resuming a job (if launched using pool of workers)
Given the `job_id` (i.e. folder's name in `SMART_DISPATCH_LOGS/{job_id}/`) one can relaunch jobs that did not finished executing(maybe because of exeeded walltime).

`smart_dispatch.py -q qtest@mp2 -p 2 resume job_id`
