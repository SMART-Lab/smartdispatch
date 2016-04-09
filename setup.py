# -*- coding: utf-8 -*-
from distutils.core import setup

setup(
    name='Smart-Dispatch',
    version='1.2.1',
    author='Stanislas Lauly, Marc-Alexandre Côté, Mathieu Germain',
    author_email='smart-udes-dev@googlegroups.com',
    packages=['smartdispatch'],
    scripts=['scripts/smart_dispatch.py', 'scripts/smart_worker.py'],
    url='https://github.com/SMART-Lab/smartdispatch',
    license='LICENSE.txt',
    description='A batch job launcher for the Mammouth supercomputer.',
    long_description=open('README.txt').read(),
    install_requires=['psutil>=1'],
    package_data={'smartdispatch': ['config/*.json']}
)
