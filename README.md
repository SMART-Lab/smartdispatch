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
`smart_dispatch.py -q qtest@mp2 launch python trainAutoEnc2.py "1 2 3 4" 80 sigmoid 0.1`

Will Generate 4 different commands and launch them on the queue qtest@mp2.


```
python trainAutoEnc2.py 1 80 sigmoid 0.1
python trainAutoEnc2.py 2 80 sigmoid 0.1
python trainAutoEnc2.py 3 80 sigmoid 0.1
python trainAutoEnc2.py 4 80 sigmoid 0.1
```
