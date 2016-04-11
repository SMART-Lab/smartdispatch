# -*- coding: utf-8 -*-
from distutils.core import setup

setup(
    name='smart-dispatch',
    version='1.3.0',
    author='Stanislas Lauly, Marc-Alexandre Côté, Mathieu Germain',
    author_email='smart-udes-dev@googlegroups.com',
    packages=['smartdispatch',
              'smartdispatch/workers'],
    scripts=['scripts/smart-dispatch'],
    url='https://github.com/SMART-Lab/smartdispatch',
    license='LICENSE.txt',
    description='A batch job launcher for computer clusters.',
    long_description=open('README.txt').read(),
    install_requires=['psutil>=1', 'numpy>=1.7'],
    package_data={'smartdispatch': ['config/*.json']}
)
