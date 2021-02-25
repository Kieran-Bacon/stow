
import os
import io
import typing
import urllib
import hashlib

from ..artefacts import Artefact, File, Directory


class ClassMethodManager:
    """ Class method namespace for the Manager
    """

    @staticmethod
    def _splitArtefactUnionForm(artefact: typing.Union[Artefact, str]) -> typing.Tuple[typing.Union[Artefact, None], str]:
        """ Take an artefact or a string and return in a strict format the object and string representation. This allows
        methods to accept both and resolve and ensure.

        Only the path is guaranteed, the artefact object will be None if it is not passed

        Args:
            artefact: Type unknown, artefact object or path

        Returns:
            artefact: An artefact object or None
            path: the path passed or pull from the artefact object

        """
        if isinstance(artefact, Artefact):
            return artefact, artefact.path

        return None, artefact

    @classmethod
    def abspath(cls, artefact: typing.Union[Artefact, str]) -> str:
        """ Return a normalized absolute version of the path or artefact given.

        Args:
            artefact: The path or object whose path is to be made absolute and returned

        Returns:
            str: the absolute path of the artefact provided

        Raises:
            ValueError: Cannot make a remote artefact object's path absolute
        """
        _, path = cls._splitArtefactUnionForm(artefact)
        return os.path.abspath(path)

    @classmethod
    def basename(cls, artefact: typing.Union[Artefact, str]) -> str:
        """ Return the base name of an artefact or path. This is the second element of the pair returned by passing path
        to the function `split()`.

        Args:
            artefact: The path or object whose path is to have its base name extracted

        Returns:
            str: the basename
        """
        _, path = cls._splitArtefactUnionForm(artefact)
        return os.path.basename(path)

    @classmethod
    def commonpath(cls, paths: typing.Iterable[typing.Union[Artefact, str]]) -> str:
        """ Return the longest common sub-path of each pathname in the sequence paths

        Examples:
            commonpath(["/foo/bar", "/foo/ban/pip"]) == "/foo"

        Args:
            paths: Artefact/paths who will have their paths comparied to find a common path

        Returns:
            str: A valid owning directory path that is the shared owning directory for all paths

        Raises:
            ValueError: If there is no crossover at all
        """
        return os.path.commonpath([cls._splitArtefactUnionForm(path)[1] for path in paths])

    @classmethod
    def commonprefix(cls, paths: typing.Iterable[typing.Union[Artefact, str]]) -> str:
        """ Return the longest common string literal for a collection of path/artefacts

        Examples:
            commonpath(["/foo/bar", "/foo/ban/pip"]) == "/foo/ba"

        Args:
            paths: Artefact/paths who will have their paths comparied to find a common path

        Returns:
            str: A string that all paths startwith (may be empty string)
        """
        return os.path.commonprefix([cls._splitArtefactUnionForm(path)[1] for path in paths])

    @classmethod
    def dirname(cls, artefact: typing.Union[Artefact, str]) -> str:
        """ Return the directory name of path or artefact. Preserve the protocol of the path if a protocol is given

        Args:
            artefact: The artefact or path whose directory path is to be returned

        Returns:
            str: The directory path for the holding directory of the artefact
        """
        obj, path = cls._splitArtefactUnionForm(artefact)

        if obj is not None or path.find(":") == -1:
            # Obj path or path within no protocol and therefore no need to parse
            return os.path.dirname(path)

        else:
            # Preserve protocol (if there is one) - dirname the path
            result = urllib.parse.urlparse(artefact)
            return urllib.parse.ParseResult(
                result.scheme,
                result.netloc,
                os.path.dirname(result.path),
                result.params,
                result.query,
                result.fragment
            ).geturl()

    @staticmethod
    def expanduser(path: str) -> str:
        """ On Unix and Windows, return the argument with an initial component of ~ or ~user replaced by that user’s
        home directory.

        On Unix, an initial ~ is replaced by the environment variable HOME if it is set; otherwise the current user’s
        home directory is looked up in the password directory through the built-in module pwd. An initial ~user is
        looked up directly in the password directory.

        On Windows, USERPROFILE will be used if set, otherwise a combination of HOMEPATH and HOMEDRIVE will be used.
        An initial ~user is handled by stripping the last directory component from the created user path derived above.

        If the expansion fails or if the path does not begin with a tilde, the path is returned unchanged.

        Args:
            path: the path which may contain a home variable indicator to be expanded

        Returns:
            str: A path with the home path factored in - if applicable
        """
        return os.path.expanduser(path)

    @staticmethod
    def expandvars(path: str):
        """ Return the argument with environment variables expanded. Substrings of the form $name or ${name} are
        replaced by the value of environment variable name. Malformed variable names and references to non-existing
        variables are left unchanged.

        On Windows, %name% expansions are supported in addition to $name and ${name}.

        Args:
            path: A path which might contain variables to be expanded

        Returns:
            str: A string with any environment variables added
        """
        return os.path.expandvars(path)

    @staticmethod
    def isabs(path: str) -> bool:
        """ Return True if path is an absolute pathname.
        On Unix, that means it begins with a slash,
        on Windows that it begins with a (back)slash after chopping off a potential drive letter.

        Args:
            path: the path to be checked for being absolute
        """
        return os.path.isabs(path)

    @classmethod
    def join(cls, *paths: typing.Iterable[str], separator=os.sep, joinAbsolutes: bool = False) -> str:
        """ Join one or more path components intelligently. The return value is the concatenation of path and any
        members of *paths with exactly one directory separator following each non-empty part except the last,
        meaning that the result will only end in a separator if the last part is empty. If a component is an absolute
        path, all previous components are thrown away and joining continues from the absolute path component.

        Protocols/drive letters are perserved in the event that an absolute is passed in.

        Args:
            *paths: segments of a path to be joined together
            separator: The character to be used to join the path segments
            joinAbsolutes: Whether to stick to normal behaviour continue from absolute paths or join them in series

        Returns:
            str: A joined path
        """
        if not paths:
            return ""

        parsedResult = None  # Store the network information while path is joined
        joined = ""  # Constructed path

        for segment in paths:
            if isinstance(segment, Artefact):
                # Convert artefacts to paths
                segment = segment.path

            # Identify and record the last full
            presult = urllib.parse.urlparse(segment)
            if presult.scheme:
                parsedResult = presult
                segment = presult.path

            if joined:
                # A path is in the midst of being created
                if cls.isabs(segment):
                    # The segment we are adding is an absolute path and as such we have to adjust
                    if joinAbsolutes:
                        # We are joining absolute paths - remove the absolute beginning character
                        segment = segment[1:]

                    else:
                        # Remove current constructed path
                        joined = segment
                        continue

                # Append the next path item into the joined path
                if joined[-1] == separator:
                    joined += segment

                else:
                    joined += separator + segment

            else:
                joined = segment

        # Add back in the protocol if given
        if parsedResult:
            return urllib.parse.ParseResult(
                parsedResult.scheme,
                parsedResult.netloc,
                joined,
                parsedResult.params,
                parsedResult.query,
                parsedResult.fragment
            ).geturl()

        return joined

    @staticmethod
    def normcase(path: str) -> str:
        """ Normalize the case of a pathname. On Windows, convert all characters in the pathname to lowercase, and also
        convert forward slashes to backward slashes. On other operating systems, return the path unchanged.

        Args:
            path: path to normalise

        Returns:
            str: the path normalised
        """
        return os.path.normcase(path)

    @staticmethod
    def normpath(path: str) -> str:
        """ Normalize a pathname by collapsing redundant separators and up-level references so that A//B, A/B/, A/./B
        and A/foo/../B all become A/B.

        Args:
            path: the path whose to be

        Returns:
            str: The path transformed
        """
        # Check that the url is for a remote manager
        url = urllib.parse.urlparse(path)
        if url.scheme and url.netloc:
            # URL with protocol
            return urllib.parse.ParseResult(
                url.scheme,
                url.netloc,
                os.path.normpath(url.path).replace("\\", "/"),
                url.params,
                url.query,
                url.fragment
            ).geturl()

        # Apply the normal path - method to the path
        return os.path.normpath(path)

    @classmethod
    def realpath(cls, path: str) -> str:
        """ Return the canonical path of the specified filename, eliminating any symbolic links encountered in the path
        (if they are supported by the operating system).

        Args:
            path: the path to have symbolic links corrected

        Returns:
            str: the path with the symbolic links corrected
        """
        return os.path.realpath(path)

    @classmethod
    def relpath(cls, path: str, start=os.curdir) -> str:
        """ Return a relative filepath to path either from the current directory or from an optional start directory

        Args:
            path: the path to be made relative
            start: the location to become relative to
        """
        return os.path.relpath(path, start)

    @classmethod
    def samefile(cls, artefact1: typing.Union[Artefact, str], artefact2: typing.Union[Artefact, str]) -> bool:
        """ Check if provided artefacts are represent the same data on disk

        Args:
            artefact1: An artefact object or path
            artefact2: An artefact object or path

        Returns:
            bool: True if the artefacts are the same
        """
        obj1, path1 = cls._splitArtefactUnionForm(artefact1)
        obj2, path2 = cls._splitArtefactUnionForm(artefact2)

        if obj1 is None or obj2 is None:
            return os.path.samefile(path1, path2)

        else:
            return obj1 is obj2

    @staticmethod
    def sameopenfile(handle1: io.IOBase, handle2: io.IOBase) -> bool:
        """ Return True if the file descriptors fp1 and fp2 refer to the same file.
        """
        return os.path.sameopenfile(handle1, handle2)

    @classmethod
    def samestat(cls, artefact1: typing.Union[Artefact, str], artefact2: typing.Union[Artefact, str]) -> bool:
        """ Check if provided artefacts are represent the same data on disk

        Args:
            artefact1: An artefact object or path
            artefact2: An artefact object or path

        Returns:
            bool: True if the artefacts are the same
        """
        _, path1 = cls._splitArtefactUnionForm(artefact1)
        _, path2 = cls._splitArtefactUnionForm(artefact2)

        return os.path.samestat(path1, path2)

    @classmethod
    def split(cls, artefact: typing.Union[Artefact, str]) -> typing.Tuple[str, str]:
        """ Split the pathname path into a pair, (head, tail) where tail is the last pathname component and head is
        everything leading up to that.

        Args:
            artefact: the artefact to be split

        Returns:
            (dirname, basename): the split parts of the artefact
        """

        _, path = cls._splitArtefactUnionForm(artefact)
        return (cls.dirname(path), cls.basename(path))

    @classmethod
    def splitdrive(cls, path: str) -> typing.Tuple[str, str]:
        """ Split the pathname path into a pair (drive, tail) where drive is either a mount point or the empty string.

        Args:
            path: the path whose mount point/drive is to be removed

        Returns:
            (drive, path): tuple with drive string separated from the path
        """

        return os.path.splitdrive(path)

    @classmethod
    def splitext(cls, artefact: typing.Union[Artefact, str]) -> typing.Tuple[str, str]:
        """ Split the pathname path into a pair (root, ext) such that root + ext == path, and ext is empty or begins
        with a period and contains at most one period.

        Args:
            artefact: the artefact to have the extension extracted

        Returns:
            (root, ext): The root path without the extension and the extension
        """
        _, path = cls._splitArtefactUnionForm(artefact)
        return os.path.splitext(path)

    @staticmethod
    def md5(path):
        """ TODO """
        hash_md5 = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)

        return hash_md5.hexdigest()