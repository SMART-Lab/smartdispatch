from distutils.core import setup

setup(
    name='Smart-Dispatch',
    version='0.0.1',
    author='Stanislas Lauly',
    packages=['smartDispatch'],
    scripts=['scripts/smartDispatch.py'],
    url='https://github.com/SMART-Lab/smartDispatch',
    license='LICENSE.txt',
    description='A batch job launcher for the Mammouth supercomputer.',
    long_description=open('README.txt').read(),
    install_requires=[]
)
