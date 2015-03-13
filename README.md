[![Build Status](https://travis-ci.org/SMART-Lab/smartdispatch.png)](https://travis-ci.org/SMART-Lab/smartdispatch)
[![Coverage Status](https://coveralls.io/repos/SMART-Lab/smartdispatch/badge.png)](https://coveralls.io/r/SMART-Lab/smartdispatch)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/SMART-Lab/smartdispatch/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/SMART-Lab/smartdispatch/?branch=master)
# Smart Dispatch
An easy to use job launcher for supercomputers with PBS compatible job manager.


## Features
- Launch multiple jobs with a single line.
- Automactically generate combinations of arguments. *(see examples)*
- Automatic ressource management. Determine for you the optimal fit for your commands on nodes.
- Resume batch of commands.
- Easily manage logs.
- Advanced mode for complete control.


## Installing
`pip install git+https://github.com/SMART-Lab/smartdispatch`


## Usage
See `smart_dispatch.py --help`
Output and error logs in are saved in : `./SMART_DISPATCH_LOGS/{batch_id}/logs/`.


## Examples
###Launch Job
`smart_dispatch.py -q qtest@mp2 launch python my_script.py 2 80 tanh 0.1`

This will launch `python my_script.py 2 80 tanh 0.1` on the queue qtest@mp2.

###Launch Jobs Batch
Automactically generate commands from combinations of arguments.

`smart_dispatch.py -q qtest@mp2 launch python my_script.py [1 2] 80 [tanh sigmoid] 0.1`

This will generate 4 different commands and launch them on the queue qtest@mp2:
```
python my_script.py 1 80 sigmoid 0.1
python my_script.py 1 80 tanh 0.1
python my_script.py 2 80 sigmoid 0.1
python my_script.py 2 80 tanh 0.1
```

###Resuming Job
Given the `batch_id` (i.e. folder's name in `SMART_DISPATCH_LOGS/{batch_id}/`) one can relaunch jobs that did not finished executing(maybe because of exeeded walltime).

`smart_dispatch.py -q qtest@mp2 resume {batch_id}`
