[![Build Status](https://travis-ci.org/SMART-Lab/smartdispatch.png)](https://travis-ci.org/SMART-Lab/smartdispatch)
[![Coverage Status](https://coveralls.io/repos/SMART-Lab/smartdispatch/badge.png)](https://coveralls.io/r/SMART-Lab/smartdispatch)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/SMART-Lab/smartdispatch/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/SMART-Lab/smartdispatch/?branch=master)
# Smart Dispatch
A batch job launcher for the Mammouth supercomputer.

## Installing
`pip install git+https://github.com/SMART-Lab/smartdispatch`

## Usage
See `smart_dispatch.py --help`

### Example
*Basic*
To launch a job composed of four variations of a simple command:
`smart_dispatch.py -q qtest@mp2 launch python my_script.py "1 2" 80 "tanh sigmoid" 0.1`

Will generate 4 different commands, launch them on the queue qtest@mp2 and save output/error logs in a folder `./SMART_DISPATCH_LOGS/{job_id}/logs/`.

```
python my_script.py 1 80 sigmoid 0.1
python my_script.py 1 80 tanh 0.1
python my_script.py 2 80 sigmoid 0.1
python my_script.py 2 80 tanh 0.1
```

*Using a pool of workers*
Building upon previous example, one could prefer using a pool of workers to achieve the execution of the commands :
`smart_dispatch.py -q qtest@mp2 -p 2 launch python my_script.py "1 2" 80 "tanh sigmoid" 0.1`

Will still generate four different commands but, instead of launching them, two worker commands will be launched on qtest@mp2 to execute all generated commands.


*Resuming a job (if launched using pool of workers)*
Given the `job_id` (i.e. folder's name in `SMART_DISPATCH_LOGS/{job_id}/`) one can resume a job that was launched using the pool of workers option.

`smart_dispatch.py -q qtest@mp2 -p 4 resume job_id`

Will launch four worker commands to resume the execution of generated commands. Note that if a command was not finished, it will be reexecuted thus one has to make sure commands are re-runnable.
