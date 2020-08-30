import os
from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as fh:
    requires = fh.read().splitlines()

setup(
    name='stow',
    install_requires=requires,
    version="0.1.1",
    description="",

    author="Kieran Bacon",
    author_email="kieran.bacon@outlook.com",

    packages=[package for package in find_packages() if not package.startswith("tests")],

    entry_points={
        'stow_managers': [
            'fs=stow.managers:FS',
            'lfs=stow.managers:FS',
            'aws=stow.managers:Amazon',
            's3=stow.managers:Amazon'
        ]
    }
)