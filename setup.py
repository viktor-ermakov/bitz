from setuptools import setup, find_packages

setup(
    name='bitz',
    version='0.0.1',
    description='Classes and functions defined for Data Science at Bitz',
    author='Viktor Ermakov',
    author_email='victor.ermakov@gmail.com',
    packages=find_packages(include=['bitz', 'bitz.*']),  # same as name
    install_requires=[],  # external packages as dependencies
)
