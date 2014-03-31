# Smart Dispatch
A batch job launcher for the Mammouth supercomputer.

## Installing
`pip install git+https://github.com/SMART-Lab/smartDispatch`

## Usage
See `smartDispatch.py --help`

### Example
`smartDispatch.py -q qtest@mp2 python trainAutoEnc2.py "1 2 3 4" 80 sigmoid 0.1`

Will Generate 4 different commands and launch them on the queue qtest@mp2.


```
python trainAutoEnc2.py 1 80 sigmoid 0.1
python trainAutoEnc2.py 2 80 sigmoid 0.1
python trainAutoEnc2.py 3 80 sigmoid 0.1
python trainAutoEnc2.py 4 80 sigmoid 0.1
```
