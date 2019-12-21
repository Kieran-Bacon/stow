import os
from setuptools import setup, find_packages

with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as fh:
    requires = fh.read().splitlines()

setup(
    name='storage',
    install_requires=requires,
    version="0.0.1",
    description="",

    author="Kieran Bacon",
    author_email="kieran.bacon@outlook.com",

    packages=find_packages(),

    entry_points={
        'storage_managers': [
            'FS=storage.managers:FS',
            'LFS=storage.managers:FS',
            'Locals=storage.managers:Locals'
        ]
    }
)