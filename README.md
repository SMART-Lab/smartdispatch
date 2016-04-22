[![Build Status](https://travis-ci.org/SMART-Lab/smartdispatch.png)](https://travis-ci.org/SMART-Lab/smartdispatch)
[![Scrutinizer Code Quality](https://scrutinizer-ci.com/g/SMART-Lab/smartdispatch/badges/quality-score.png?b=master)](https://scrutinizer-ci.com/g/SMART-Lab/smartdispatch/?branch=master)
[![Coverage Status](https://coveralls.io/repos/SMART-Lab/smartdispatch/badge.png)](https://coveralls.io/r/SMART-Lab/smartdispatch)
# Smart Dispatch
An easy to use job launcher for supercomputers with PBS compatible job manager.


## Features
- Launch multiple jobs with a single line.
- Automatically generate combinations of arguments. *(see examples)*
- Automatic resources management. Determine for you the optimal fit for your commands on nodes.
- Resume batch of commands.
- Easily manage logs.
- Advanced mode for complete control.


## Installing
`pip install git+https://github.com/SMART-Lab/smartdispatch`


## Usage
See `smart-dispatch --help`

Output and error logs in are saved in : *`./SMART_DISPATCH_LOGS/{batch_id}/logs/`*.


## Examples
### Launch job
`smart-dispatch -q qtest@mp2 launch python my_script.py 2 80 tanh 0.1`

Will launch *`python my_script.py 2 80 tanh 0.1`* on the queue qtest@mp2.

### Launch batch of jobs
Automatically generate commands from combinations of arguments.

`smart-dispatch -q qtest@mp2 launch python my_script.py [1 2] 80 [tanh sigmoid] 0.1`

Will generate 4 different commands and launch them on the queue qtest@mp2:
```
python my_script.py 1 80 sigmoid 0.1
python my_script.py 1 80 tanh 0.1
python my_script.py 2 80 sigmoid 0.1
python my_script.py 2 80 tanh 0.1
```

Another possiblility is to generate argument from a range.

`smart-dispatch -q qtest@mp2 launch python my_script.py [1:4]`

Will generate:
```
python my_script.py 1
python my_script.py 2
python my_script.py 3
```

You can also add a step size to the range as the 3rd argument.

`smart-dispatch -q qtest@mp2 launch python my_script.py [1:10:2]`

Will generate:
```
python my_script.py 1
python my_script.py 3
python my_script.py 5
python my_script.py 7
python my_script.py 9

```

### Resuming job/batch
`smart-dispatch -q qtest@mp2 resume {batch_id}`

Jobs that did not terminate properly, for example, it exceeded the walltime, can be resumed using the {batch_id} given to you upon launch. Of course, all this assuming your script is resumable.

*Note: Jobs are always in a batch, even if it's a batch of one.*
