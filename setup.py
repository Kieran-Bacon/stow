import os
from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as fh:
    requires = fh.read().splitlines()

setup(
    name='storage',
    install_requires=requires,
    version="0.0.10",
    description="",

    author="Kieran Bacon",
    author_email="kieran.bacon@outlook.com",

    packages=[package for package in find_packages() if not package.startswith("tests")],

    entry_points={
        'console_scripts': [
            'backup = storage.backup:BackupManager.main',
        ],
        'storage_managers': [
            'fs=storage.managers:FS',
            'lfs=storage.managers:FS',
            'aws=storage.managers:Amazon',
            's3=storage.managers:Amazon'
        ]
    }
)