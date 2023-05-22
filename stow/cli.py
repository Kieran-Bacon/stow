import logging
from typing import Tuple
import click

from .manager import Manager
from .artefacts import HashingAlgorithm

log = logging.getLogger(__name__)

manager = Manager()

@click.group()
@click.option('--debug/--no-debug', default=False)
def cli(debug: bool):

    if debug:
        logging.basicConfig(level=logging.DEBUG)
        log.info('Debugging enabled')

@cli.command()
@click.argument('artefact')
def exists(artefact: str):
    print(manager.exists(artefact))

@cli.command()
@click.argument('path')
def touch(path: str):
    print(manager.touch(path))

@cli.command()
@click.argument('source')
@click.argument('destination')
def put(source: str, destination: str):
    print(source)

@cli.command()
@click.argument('path')
@click.option('-i', '--ignore-exists', default=True, is_flag=True)
@click.option('-o', '--overwrite', default=False, is_flag=True)
def mkdir(path: str, ignore_exists: bool, overwrite: bool):
    print(manager.mkdir(path, ignoreExists=ignore_exists, overwrite=overwrite))

@cli.command()
@click.argument('artefact')
@click.argument('link')
@click.option('--soft/--hard', 'soft', default=True)
def mklink(link: str, target: str, soft: bool):
    manager.mklink(link, target)

@cli.command()
@click.argument('artefact', default='.')
@click.option('--recursive', default=False, is_flag=True)
def ls(artefact: str, recursive: bool):
    for artefact in manager.iterls(artefact, recursive=recursive, ignore_missing=True):
        print(artefact)

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.argument('--overwrite/--no-overwrite', default=False)
def get(source: str, destination: str, overwrite: bool):
    print(manager.get(source=source, destination=destination, overwrite=overwrite))

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.argument('--overwrite/--no-overwrite', default=False)
def put(source: str, destination: str, overwrite: bool):
    print(manager.put(source=source, destination=destination, overwrite=overwrite))

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.argument('--overwrite/--no-overwrite', default=False)
def cp(source: str, destination: str, overwrite: bool):
    print(manager.cp(source=source, destination=destination, overwrite=overwrite))

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.argument('--overwrite/--no-overwrite', default=False)
def mv(source: str, destination: str, overwrite: bool):
    print(manager.mv(source=source, destination=destination, overwrite=overwrite))

@cli.command()
@click.argument('artefact')
@click.argument('algorithm', type=click.Choice(HashingAlgorithm.__members__, case_sensitive=False), default='MD5')
def digest(artefact: str, algorithm: str):
    print(manager.digest(artefact, getattr(HashingAlgorithm, algorithm)))

@cli.command()
@click.argument('source')
@click.argument('destination')
def sync(source: str, destination: str):
    manager.sync(source, destination)

@cli.command()
@click.argument('artefacts', nargs=-1)
@click.option('-r', '--recursive', default=False, is_flag=True)
def rm(artefacts: Tuple[str], recursive: bool):
    for artefact in artefacts:
        manager.rm(artefact, recursive=recursive)