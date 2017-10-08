#! /usr/bin/env python3

from setuptools import find_packages
from setuptools import setup


def parse_requirements(filename):
    """Given a filename, strip empty lines and those beginning with #."""
    with open(filename) as rfd:
        output = []
        for line in rfd:
            line = line.strip()
            if line != '' and not line.startswith('#'):
                output.append(line)
        return output


setup(
    name='FollowerAnalyzer',
    version='0.1',
    author='Sravan Bhamidipati',
    packages=find_packages(),
    install_requires=parse_requirements('requirements.txt'),
    description="Application to analyze a Twitter user's followers.",
    long_description='\n' + open('README.md').read(),
)
