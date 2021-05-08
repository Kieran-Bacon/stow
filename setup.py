import os
from setuptools import setup, find_packages

# Read the index.md
with open(os.path.join(os.path.dirname(__file__), 'docs', 'index.md')) as fh:
    description = fh.read()

# Read in package requirements
with open(os.path.join(os.path.dirname(__file__), 'requirements.txt')) as fh:
    requires = fh.read().splitlines()

# Read in package test requirements
with open(os.path.join(os.path.dirname(__file__), 'test-requirements.txt')) as fh:
    testRequires = fh.read().splitlines()

def packageVersion(initpath: str) -> str:
    """ Get from the init of the source code the version string

    Params:
        initpath (str): path to the init file of the python package relative to the setup file

    Returns:
        str: The version string in the form 0.0.1
    """

    path = os.path.join(os.path.dirname(__file__), initpath)

    with open(path, "r") as handle:
        for line in handle.read().splitlines():
            if line.startswith("__version__"):
                return line.split("=")[1].strip().strip("\"'")

setup(
    name='stow',
    version=packageVersion("stow/__init__.py"),
    description="stow artefacts anywhere, with ease",
    long_description=description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],

    url="https://github.com/Kieran-Bacon/stow",
    project_urls={
        "Documentation": "https://stow.readthedocs.io/en/latest/",
    },

    install_requires=requires,
    tests_require=testRequires,

    author="Kieran Bacon",
    author_email="kieran.bacon@outlook.com",

    packages=[package for package in find_packages() if not package.startswith("tests")],

    entry_points={
        'stow_managers': [
            'fs=stow.managers:FS',
            'lfs=stow.managers:FS',
            'aws=stow.managers:Amazon',
            's3=stow.managers:Amazon',
            'ssh=stow.managers:SSH',
        ]
    }
)