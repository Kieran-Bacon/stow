import os
from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as fh:
    requires = fh.read().splitlines()

setup(
    name='pywarehouse',
    install_requires=requires,
    version="0.0.8",
    description="",

    author="Kieran Bacon",
    author_email="kieran.bacon@outlook.com",

    packages=list(filter(lambda x: not x.startswith("tests"), find_packages())),

    entry_points={
        'console_scripts': [
            'backup = warehouse.backup:BackupManager.main',
        ],
        'warehouse_managers': [
            'fs=warehouse.managers:FS',
            'lfs=warehouse.managers:FS',
            'aws=warehouse.managers:Amazon',
            's3=warehouse.managers:Amazon'
        ]
    }
)