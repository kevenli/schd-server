#!/usr/bin/env python3
from setuptools import setup, find_packages

def read_requirements():
    with open('requirements.txt', 'r', encoding='utf8') as f:
        return f.readlines()

setup(
    name="schd-server",
    version="0.0.4",
    url="https://github.com/kevenli/schd-server",
    packages=find_packages(exclude=('tests', 'tests.*')),
    package_data={
        'schds': [
            'migrations/*',
            'migrations/versions/*',
            'web_templates/*.html',
        ]
    },
    include_package_data=True,
    install_requires=read_requirements(),
    license="ApacheV2",
    license_files="LICENSE",
)
