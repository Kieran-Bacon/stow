import click
from click_option_group import optgroup

import shutil
import logging
import pkg_resources
import functools
import datetime
from typing import Tuple, Optional, List, Any

from .manager import Manager
from .artefacts import Artefact, File, Directory
from .types import HashingAlgorithm
from .callbacks import DefaultCallback, ProgressCallback
from .exceptions import ArtefactTypeError

log = logging.getLogger(__name__)
type_builtin = type

# Build the initial stow cli options from loaded managers
managerConfigs = {}
managerOptions = []
for entry_point in pkg_resources.iter_entry_points('stow_managers'):

    try:
        entryManager = entry_point.load()
        if not hasattr(entryManager, 'CommandLineConfig'):
            continue
        config = entryManager.CommandLineConfig(entryManager)

    except:
        # The Manager is not installed or cannot be loaded
        log.debug('Manager %s could not be loaded', entry_point.name)
        continue

    managerConfigs[entry_point.name] = config
    arguments = config.arguments()
    if arguments:
        managerOptions.append(
            optgroup.group(
                f'{entryManager.__name__} configuration',
                help=f'Options for the {entryManager.__name__} manager'
            )
        )
        for args, kwargs in arguments:
            managerOptions.append(optgroup.option(*args, **kwargs))

cli_decorators = [
    click.option(
        '-m', '--manager',
        type=click.Choice(['auto', *managerConfigs.keys()], case_sensitive=False),
        default='auto',
        help='Select the manager you are connecting to - by default the manager will be guessed from the protocol'
    ),
    *managerOptions,
    click.option('--debug/--no-debug', default=False),
    click.pass_context
]

def dynamicDecorators(cli_decorators):
    def wraps(func):
        for decorator in cli_decorators[::-1]:
            func = decorator(func)
        return func
    return wraps

@click.group()
@dynamicDecorators(cli_decorators)
def cli(ctx: click.Context, debug: bool, manager: str, **kwargs):
    """Stow anything anywhere.

    Python based utility for the management of local and remote artefacts (files/directories) through a standard, expansive interface.

    \b
    Examples:
        >>> stow get s3://my-bucket/my-cool-file.txt local-file.txt
        <stow.File: c:\\Users\\kieran\\Projects\\personal\\stow\\local-file.txt modified(2023-09-14 09:14:36+00:00) size(3645479 bytes)>

    """

    if debug:
        stow_logger = logging.getLogger('stow')
        stow_handler = logging.StreamHandler()
        stow_handler.setFormatter(logging.Formatter("%(name)s::%(levelname)s::%(message)s"))
        stow_handler.setLevel(logging.DEBUG)
        stow_logger.setLevel(logging.DEBUG)
        stow_logger.addHandler(stow_handler)

    if manager == 'auto':
        managerObj = Manager()

    else:
        config = managerConfigs[manager]
        try:
            managerObj = config.initialise(kwargs)
        except Exception as e:
            log.error("Failed to initialise manager [%s] with reason: %s", manager, e)
            exit()

    # Update all default callbacks to
    DefaultCallback.become(ProgressCallback())

    ctx.obj = managerObj

@cli.command()
@click.argument('artefacts', nargs=-1)
@click.pass_obj
def cat(manager: Manager, artefacts: List[str]):
    """ Concatinate file contents with stuff """
    for artefact in artefacts:
        with manager.open(artefact) as handle:
            print(handle.read())

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

        sourceArtefact = manager.artefact(source)
        if isinstance(sourceArtefact, File):
            print('Cannot perform merge action on a File artefact type:', sourceArtefact)
            exit(1)

        if merge_strat == "replace":
            for artefact in manager.iterls(source, recursive=True):
                copy_path = manager.join(destination, manager.relpath(artefact, source))
                if isinstance(artefact, File):
                    manager.cp(artefact, copy_path)
                else:
                    manager.mkdir(copy_path)

        else:
            for artefact in manager.iterls(source, recursive=True):
                copy_path = manager.join(destination, manager.relpath(artefact, source))
                if isinstance(artefact, File):
                    while manager.exists(copy_path):
                        copy_path = manager.join(
                            manager.dirname(copy_path),
                            manager.name(copy_path) + "-COPY." + manager.extension(copy_path)
                        )

                    manager.cp(artefact, copy_path)

                else:
                    manager.mkdir(copy_path)

    else:
        manager.cp(source=source, destination=destination, overwrite=overwrite)

@cli.command()
@click.argument('artefacts', nargs=-1)
@click.pass_obj
def exists(manager: Manager, artefacts: List[str]):
    """ Check if artefact exists """
    for artefact in artefacts:
        print(artefact.ljust(40), manager.exists(artefact))

@cli.command()
@click.argument('path')
@click.argument('modified-time', required=False, type=float)
@click.argument('accessed-time', required=False, type=float)
@click.pass_obj
def touch(manager: Manager, path: str, modified_time: Optional[float], accessed_time: Optional[float]):
    """ Perform the linux command touch, create a file/update file timestamps."""
    manager.touch(path, modified_time, accessed_time)

@cli.command()
@click.argument('path')
@click.option('--overwrite/--no-overwrite', default=False, help="Should this operation replace a directory if it exists at the location specified - DEFAULT no overwrite")
@click.pass_obj
def mkdir(manager: Manager, path: str, overwrite: bool):
    """ Create a directory at the path specified """
    print(manager.mkdir(path, overwrite=overwrite))

@cli.command()
@click.argument('artefact')
@click.argument('link')
@click.option('--soft/--hard', 'soft', default=True, help="DEFAULT soft")
@click.pass_obj
def mklink(manager: Manager, artefact: str, link: str, soft: bool):
    """ Create a symbolic link between to an artefact

    \b
    Arguments:
        ARTEFACT: should be the target path, the thing being linked too
        LINK: is the location of the link

    \b
    Links can be of two types:
    - Soft: A symbolic path indicating the abstract location of another file.
    - Hard: A reference to a specific location of physical data on disk.

    A soft link can be thought of as an alias, the path to this link effectively gets replaced in resolution by the path
    of the artefact targeted by this link. As such is it possible to have a link that points to an non-existent artefact.

    A hard link is an ordinary artefact object, and just like any other artefact it points to a location on disk of where its data exists.
    The difference being that this phisical location is shared with the target of the link. As such, if the data changes
    both are updated, however, the data is not deleted if one of the artefacts is removed. Like a reference counter on the
    data on disk, the physical location will remain allocated until all hard links to it have been deleted.


    """
    manager.mklink(artefact, link, soft)

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.option('--overwrite/--no-overwrite', default=False)
@click.pass_obj
def get(manager: Manager, source: str, destination: str, overwrite: bool):
    """ Get (fetch|pull) an artefact and write to local destination """
    print(manager.get(source=source, destination=destination, overwrite=overwrite))

@cli.command()
@click.argument('artefact', default=None, required=False)
@click.option('-r', '--recursive', default=False, is_flag=True, help='List artefacts recursively')
@click.option('-t', '--type', default=None, type=click.Choice(('File', 'Directory')), help='Filter artefacts to type given')
@click.option('--count', default=False, is_flag=True, help='Return the count of artefacts not the artefact paths')
@click.option('-c', '--columns', default='type,createdTime', type=str, help="Comma separated list of artefact metadata to tabulate e.g. modifiedTime,size")
@click.option('-o', '--order', default='type', type=str, help='Comma separated list of fields to order the files by e.g type,createdTime')
@click.pass_obj
def ls(manager: Manager, artefact: str, recursive: bool, type: Optional[str], count: bool = False, columns: str = 'type,createdTime', order: str = 'type'):
    """ List artefacts in a directory """

    if type is not None:
        type_ = File if type == 'File' else Directory
    else:
        type_ = None

    if count:
        artefactObj = manager.artefact(artefact, type=Directory)
        print(f"{artefactObj.path} - Count: {len(manager.ls(artefactObj, recursive=recursive, ignore_missing=True))}")

    else:
        try:
            source = manager.artefact(artefact)
        except Exception as e:
            print(str(e))
            exit(1)

        if isinstance(source, File):
            raise ArtefactTypeError(f'Cannot perform ls commad on a file: {source}')

        # Extract the list of fields to be used
        columns: List[str] = list(dict.fromkeys(columns.split(',')))
        column_types: List[Any] = []
        rows: List[List[Any]] = []

        include_type = "type" in columns
        if include_type:
            columns.remove('type')

        # Iterate over the artefacts in the location and build the row data
        for artefactIndex, subArtefact in enumerate(manager.iterls(source, recursive=recursive, ignore_missing=True)):
            if type_ is not None and not isinstance(subArtefact, type_):
                continue

            row = [source.relpath(subArtefact)]

            if include_type:
                row.append(subArtefact.__class__.__name__)

            for column_index, column in enumerate(columns):
                try:
                    value = getattr(subArtefact, column)
                except:
                    if manager.SUPPORTS_METADATA and isinstance(subArtefact, File):
                        value = subArtefact.metadata.get(column)
                    else:
                        value = None

                row.append(value)
                value_type = type_builtin(value)

                if not artefactIndex:
                    # First time creating the column type
                    column_types.append(value_type)

                elif value is not None:
                    column_type = column_types[column_index]
                    if issubclass(column_type, type_builtin(None)):
                        column_types[column_index] = value_type
                    else:
                        assert column_type == value_type, (columns[column_index], column_type, column_type is None, value_type)

            rows.append(row)

        # Add the special fields to the columns list
        columns.insert(0, 'path')
        column_types.insert(0, str)
        if include_type:
            columns.insert(1, 'type')
            column_types.insert(1, Artefact)

        # Extract the order and apply
        descending = '-' == order[0]
        if descending: order = order[1:]
        order_indexes = [columns.index(x) for x in order.split(',') if x in columns]
        if order_indexes:
            rows = sorted(rows, key=lambda r: tuple(r[i] for i in order_indexes), reverse=descending)



        # Convert data into a render able table

        # Fetch the dimensions of the display
        gui_dimensions = shutil.get_terminal_size()

        # Create the initial strings of display rows/data
        rows_rendered = [[str(v) if v is not None else '' for v in row] for row in rows]

        # Iteratively improve the row display until a solution has been found
        while True:

            # Reduce the row data down to a single list of the length of columns whose with the length of the value
            max_column_widths = functools.reduce(
                lambda counts, row: [max(len(r), c) for r, c in zip(row, counts)],
                [columns] + rows_rendered,  # Include the column names
                [0]*len(columns) # Default initial values
            )

            # Find out the required max length to display all information including the spaces between the columns
            sum_data_width = sum(max_column_widths)
            required_data_width = sum_data_width + len(columns) - 1


            if required_data_width < gui_dimensions.columns:
                # The display data is less than the display region - we can break out

                white_space_per_column = min((gui_dimensions.columns - sum(max_column_widths) - 10)//len(columns), 4)
                max_width = sum(max_column_widths) + (len(columns) - 1) * white_space_per_column

                break

            else:
                # The lines are too large, we must find ways to reduce the data

                # Identify the largest columns for reduction first
                resize_target_columns = [i for i, s in enumerate(max_column_widths) if s == max(max_column_widths)]

                # Iteratively reduce those columns
                for column_index in resize_target_columns:

                    column_type = column_types[column_index]
                    print(column_type, column_index)

                    if issubclass(column_type, str):
                        # Half the string length and use elipses

                        new_max_length = (max_column_widths[column_index]//2) - 3
                        print(max_column_widths[column_index], new_max_length)

                        for row_index in range(len(rows)):
                            value_render = rows_rendered[row_index][column_index]

                            if len(value_render) > new_max_length:
                                rows_rendered[row_index][column_index] = '...' + value_render[-new_max_length:]

                    elif issubclass(column_type, datetime.datetime):
                        # The datetime will get its precision dropped

                        datetime.datetime.now(datetime.timezone.utc)

                        for row_index in range(len(rows)):


                            value = rows[row_index][column_index]
                            value_render = rows_rendered[row_index][column_index]

                            # print(value, value_render)

                            if value_render == '...':
                                break

                            # Formats
                            value_renders = [
                                value.strftime(f) for f in (
                                    '%Y-%m-%d %H:%M:%S.%f',
                                    '%Y-%m-%d %H:%M:%S',
                                    '%Y-%m-%d',
                                    '%y-%m-%d',
                                )
                            ]
                            value_renders.insert(0, str(value))



                            # Create a mapping from current to next style
                            next_style_mapper = {cs: ns for cs, ns in zip(value_renders, value_renders[1:])}

                            print(next_style_mapper)

                            print(value_render, value_render in next_style_mapper)

                            if value_render in next_style_mapper:
                                # We have a next style to use instead
                                value_render = next_style_mapper[value_render]
                            else:
                                value_render = '...'

                            rows_rendered[row_index][column_index] = value_render

                        else:
                            # We didn't break meaning that values were updated
                            continue

                        # Delete column
                        del columns[column_index]
                        for row_index in range(len(rows)):
                            del rows[row_index][column_index]
                            del rows_rendered[row_index][column_index]

                    else:
                        raise ValueError(f'Unhandled row type: {value_type}')

        print() # Create buffer line in output
        print('Directory:', source.abspath)
        print(
            "|".join(
                (
                    column.ljust(width + white_space_per_column)
                    for column, width in zip(columns, max_column_widths)
                )
            )
        )
        print("="*(max_width + len(columns)))
        for row in rows_rendered:
            print(
                "|".join(
                    (
                        r.ljust(width + white_space_per_column)
                        for r, width in zip(row, max_column_widths)
                    )
                )
            )

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.option('--overwrite/--no-overwrite', default=False, help="Whether the destination should be overwritten")
@click.pass_obj
def mv(manager: Manager, source: str, destination: str, overwrite: bool):
    """Move an artefact to the destination location"""
    print(manager.mv(source=source, destination=destination, overwrite=overwrite))

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.option('--overwrite/--no-overwrite', default=False, help='Whether the destination should be overwritten')
@click.pass_obj
def put(manager: Manager, source: str, destination: str, overwrite: bool):
    """Put (push) a local artefact to the destination"""
    print(manager.put(source=source, destination=destination, overwrite=overwrite))

@cli.command()
@click.argument('artefact')
@click.option('--algo', type=click.Choice(list(HashingAlgorithm.__members__.keys()), case_sensitive=False), default='MD5', help='Checksum algorithm - default MD5')
@click.pass_obj
def digest(manager: Manager, artefact: str, algo: str):
    """ Get artefact checksum using digest algorithm"""
    print(manager.digest(artefact, getattr(HashingAlgorithm, algo)))

@cli.command()
@click.argument('source')
@click.argument('destination')
@click.option('--delete/--no-delete', default=False, help="Delete artefacts in destination that are not present in source")
@click.option('--check-modified/--ignore-modified', default=True, help="Whether to check modified time of files - if false a comparason function must be provided")
@click.option('--comparator', default=None, help="Path to python function capable of comparing artefacts in source and destination")
#  help="Delete artefacts in destination that are not present in source"
@click.pass_obj
def sync(manager: Manager, source: str, destination: str, delete: bool, check_modified: bool, comparator: Optional[str]):
    """ Syncronise source directory with destination directory

    An example s3 comparator would be: stow.managers.amazon.etagComparator
    """
    if check_modified is False and comparator is None:
        print('No comparison criteria provided, must either select comparator function or use modified time')
        exit(1)

    comparator_fn = None
    if comparator is not None:
        import importlib
        p, m = comparator.rsplit('.', 1)
        module = importlib.import_module(p)
        comparator_fn = getattr(module, m)

    manager.sync(
        source,
        destination,
        delete=delete,
        check_modified_times=check_modified,
        artefact_comparator=comparator_fn,
    )

@cli.command()
@click.argument('artefacts', nargs=-1)
@click.option('-r', '--recursive', default=False, is_flag=True)
@click.pass_obj
def rm(manager: Manager, artefacts: Tuple[str], recursive: bool):
    """ Remove artefact """
    for artefact in artefacts:
        manager.rm(artefact, recursive=recursive)