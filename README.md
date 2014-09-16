[![Build Status](https://travis-ci.org/SMART-Lab/smartdispatch.png)](https://travis-ci.org/SMART-Lab/smartdispatch)
[![Coverage Status](https://coveralls.io/repos/SMART-Lab/smartdispatch/badge.png)](https://coveralls.io/r/SMART-Lab/smartdispatch)
# Smart Dispatch
A batch job launcher for the Mammouth supercomputer.

## Installing
`pip install git+https://github.com/SMART-Lab/smartdispatch`

## Usage
See `smart_dispatch.py --help`

### Example
`smart_dispatch.py -q qtest@mp2 python trainAutoEnc2.py "1 2 3 4" 80 sigmoid 0.1`

Will Generate 4 different commands and launch them on the queue qtest@mp2.


```
python trainAutoEnc2.py 1 80 sigmoid 0.1
python trainAutoEnc2.py 2 80 sigmoid 0.1
python trainAutoEnc2.py 3 80 sigmoid 0.1
python trainAutoEnc2.py 4 80 sigmoid 0.1
```
