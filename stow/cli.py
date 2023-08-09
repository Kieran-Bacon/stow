import click
from click_option_group import optgroup

import logging
import pkg_resources
from typing import Tuple, Optional

from .manager import Manager
from .artefacts import HashingAlgorithm, File, Directory
from .callbacks import DefaultCallback, ProgressCallback

log = logging.getLogger(__name__)

# Build the initial stow cli options from loaded managers
managerNames = []
managerOptions = []
for entry_point in pkg_resources.iter_entry_points('stow_managers'):

    try:
        entryManager = entry_point.load()
        if not hasattr(entryManager, 'cli_arguments'):
            continue

    except:
        # The Manager is not installed or cannot be loaded
        log.warning('Manager %s could not be loaded', entry_point.name)
        pass

    managerNames.append(entry_point.name)
    managerOptions.append(
        optgroup.group(
            f'{entryManager.__name__} configuration',
            help=f'Options for the {entryManager.__name__} manager'
        )
    )
    for args, kwargs in entryManager.cli_arguments():
        managerOptions.append(optgroup.option(*args, **kwargs))


cli_decorators = [
    click.group(),
    click.option('-m', '--manager', type=click.Choice(managerNames, case_sensitive=False), default='fs', help='Select the manager you are connecting to - by default it will be the local filesystem or inferred from the protocol'),
    *managerOptions,
    click.option('--debug/--no-debug', default=False),
    click.pass_context
]


def cli(ctx: click.Context, debug: bool, manager: str, **kwargs):
    """Stow anything anywhere.

    Python based utility for the management of local and remote artefacts (files/directories) through a standard, expansive interface.

    """

    if debug:
        logging.basicConfig(level=logging.DEBUG)
        for logger in ['stow.callbacks']:
            logging.getLogger(logger).propagate = False
        log.info('Debugging enabled')

    if manager == 'fs':
        managerObj = Manager()

    elif manager == 's3':

        import boto3
        session = boto3.Session(
            aws_access_key_id=kwargs['access_key'],
            aws_secret_access_key=kwargs['secret_key'],
            aws_session_token=kwargs['token'],
            region_name=kwargs['region_name'],
            profile_name=kwargs['profile']
        )

        if not kwargs.get('bucket'):
            s3 = session.client('s3')
            response = s3.list_buckets()

            # Output the bucket names
            print('Bucket (-b, --bucket) is required - Existing buckets:')
            print()
            print('Name'.ljust(80)+' Creation Date')
            for bucket in response['Buckets']:
                print(f"{bucket['Name'].ljust(80)} {bucket['CreationDate'].isoformat()}")

            exit()

        from stow.managers.amazon import Amazon
        managerObj = Amazon(bucket=kwargs['bucket'], aws_session=session)

    # Attach callback to manager object for reference below
    managerObj.callback = ProgressCallback() # if debug else DefaultCallback()

    ctx.obj = managerObj

for decorator in cli_decorators[::-1]:
    cli = decorator(cli)

@cli.command()
@click.argument('artefact')
@click.pass_obj
def cat(manager: Manager, artefact: str):
    with manager.open(artefact) as handle:
        print(handle.read())

@cli.command()
@click.argument('artefact')
@click.pass_obj
def exists(manager: Manager, artefact: str):
    print(manager.exists(artefact))

@cli.command()
@click.argument('path')
@click.pass_obj
def touch(manager: Manager, path: str):
    print(manager.touch(path))

@cli.command()
@click.argument('path')
@click.option('-i', '--ignore-exists', default=True, is_flag=True)
@click.option('-o', '--overwrite', default=False, is_flag=True)
@click.pass_obj
def mkdir(manager: Manager, path: str, ignore_exists: bool, overwrite: bool):
    print(manager.mkdir(path, ignoreExists=ignore_exists, overwrite=overwrite))

@cli.command()
@click.argument('artefact')
@click.argument('link')
@click.option('--soft/--hard', 'soft', default=True)
@click.pass_obj
def mklink(manager: Manager, link: str, target: str, soft: bool):
    manager.mklink(link, target)

@cli.command()
@click.argument('artefact', default=None, required=False)
@click.option('--recursive', default=False, is_flag=True)
@click.pass_obj
def ls(manager: Manager, artefact: str, recursive: bool):
    for artefact in manager.iterls(artefact, recursive=recursive, ignore_missing=True):
        print(artefact)

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.argument('--overwrite/--no-overwrite', default=False)
@click.pass_obj
def get(manager: Manager, source: str, destination: str, overwrite: bool):
    print(manager.get(source=source, destination=destination, overwrite=overwrite))

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.argument('--overwrite/--no-overwrite', default=False)
@click.pass_obj
def put(manager: Manager, source: str, destination: str, overwrite: bool):
    print(manager.put(source=source, destination=destination, overwrite=overwrite))

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.option('-m', '--merge/--no-merge', default=False, help='make copy behaviour be merge - merge behaviour governed by merge-strat')
@click.option('-ms', '--merge-strat', type=click.Choice(['replace', 'rename']), default='replace', help='Set the behaviour of a merge operation. Either replace conflicts with source artefact or rename source name during copy to have a suffix of "-COPY"')
@click.option('-f', '--overwrite/--no-overwrite', default=False, help='A force flag for directories, to ensure you mean to overwrite the destination.')
@click.pass_obj
def cp(manager: Manager, source: str, destination: str, merge: str, merge_strat: str, overwrite: bool):
    """ Copy a source artefact into the destination """

    if merge:
        if merge_strat == "replace":
            for artefact in manager.iterls(source, recursive=True):
                if isinstance(artefact, File):
                    copy_path = manager.join(destination, manager.relpath(artefact, source))
                    manager.cp(artefact, copy_path, callback=manager.callback)

        else:
            for artefact in manager.iterls(source, recursive=True):
                if isinstance(artefact, File):

                    copy_path = manager.join(destination, manager.relpath(artefact, source))
                    while manager.exists(copy_path):
                        copy_path = manager.join(
                            manager.dirname(copy_path),
                            manager.name(copy_path) + "-COPY." + manager.extension(copy_path)
                        )

                    manager.cp(artefact, copy_path, callback=manager.callback)

    else:
        manager.cp(source=source, destination=destination, overwrite=overwrite, callback=manager.callback)

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.argument('--overwrite/--no-overwrite', default=False)
@click.pass_obj
def mv(manager: Manager, source: str, destination: str, overwrite: bool):
    print(manager.mv(source=source, destination=destination, overwrite=overwrite))

@cli.command()
@click.option('--algo', type=click.Choice(HashingAlgorithm.__members__, case_sensitive=False), default='MD5')
@click.argument('artefact')
@click.pass_obj
def digest(manager: Manager, artefact: str, algo: str):
    print(manager.digest(artefact, getattr(HashingAlgorithm, algo)))

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.pass_obj
def sync(manager: Manager, source: str, destination: str):
    manager.sync(source, destination)

@cli.command()
@click.argument('artefacts', nargs=-1)
@click.option('-r', '--recursive', default=False, is_flag=True)
@click.pass_obj
def rm(manager: Manager, artefacts: Tuple[str], recursive: bool):
    for artefact in artefacts:
        manager.rm(artefact, recursive=recursive)