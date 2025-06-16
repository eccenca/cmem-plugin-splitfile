#!/usr/bin/env python3
"""based on:
Author: rjayapalan
Created: March 05, 2022
"""

import csv
import logging
import ntpath
import time
from collections.abc import Callable
from io import BytesIO
from pathlib import Path

log = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 1000000  # 1 MB
SPLIT_DELIMITER = "_"
ZERO_FILL = 4
MIN_ZERO_FILL = 1
MAX_ZERO_FILL = 10
MANIFEST_FILE_NAME = "manifest"


class ZeroFillOutOfRange(Exception):
    pass


class SplitGroupedPrefix:
    def __init__(self, inputfile: str, outputdir: str) -> None:
        """Constructor

        Args:
            inputfile (str): Path to the original file
            outputdir (str): Output directory path to write the file splits

        """
        log.info("Starting file split process")
        if not Path(inputfile).exists():
            raise FileNotFoundError(f'Given input file path "{inputfile}" does not exist.')
        if not Path(outputdir).is_dir():
            raise NotADirectoryError(
                f'Given output directory path "{outputdir}" is not a valid directory.'
            )
        self._terminate = False
        self._inputfile = inputfile
        self._outputdir = outputdir
        self._splitdelimiter = SPLIT_DELIMITER
        self._splitzerofill = ZERO_FILL
        self._manfilename = MANIFEST_FILE_NAME
        self._starttime = time.time()

    @property
    def terminate(self) -> bool:
        """Returns terminate flag value

        Returns:
            bool: Terminate flag value

        """
        return self._terminate

    @property
    def inputfile(self) -> str:
        """Returns input file path

        Returns:
            str: Input file path

        """
        return self._inputfile

    @property
    def outputdir(self) -> str:
        """Returns output dir path

        Returns:
            str: Output dir path

        """
        return self._outputdir

    @property
    def splitdelimiter(self) -> str:
        """Returns split file suffix char

        Returns:
            str: Split file suffix char

        """
        return self._splitdelimiter

    @property
    def splitzerofill(self) -> int:
        """Returns split file's number of zero fill digits

        Returns:
            int: Split file's number of zero fill digits

        """
        return self._splitzerofill

    @property
    def manfilename(self) -> str:
        """Returns manifest filename

        Returns:
            str: Manifest filename

        """
        return self._manfilename

    @terminate.setter
    def terminate(self, value: bool) -> None:
        """Sets terminate flag. Once flag is set
        the running process will safely terminate.

        Args:
            value (bool): True/False

        """
        self._terminate = value

    @splitdelimiter.setter
    def splitdelimiter(self, value: str) -> None:
        """Sets split file suffix char

        Args:
            value (str): Any character

        """
        self._splitdelimiter = value

    @splitzerofill.setter
    def splitzerofill(self, value: int) -> None:
        """Sets split file's number of zero fill digits

        Args:
            value (int): Any whole number

        """
        if not MIN_ZERO_FILL <= value <= MAX_ZERO_FILL:
            raise ZeroFillOutOfRange(
                f"Zero fill must be between {MIN_ZERO_FILL} and {MAX_ZERO_FILL}."
            )
        self._splitzerofill = value

    @manfilename.setter
    def manfilename(self, value: str) -> None:
        """Sets manifest filename

        Args:
            value (str): Manifest filename

        """
        self._manfilename = value

    @staticmethod
    def _getreadbuffersize(splitsize: int) -> int:
        """Returns buffer size to be used with the file reader

        Args:
            splitsize (int): Split size

        Returns:
            int: Buffer size

        """
        defaultchunksize = DEFAULT_CHUNK_SIZE
        if splitsize < defaultchunksize:
            return splitsize
        return defaultchunksize

    def _getnextsplit(self, splitnum: int) -> str:
        """Returns next split filename

        Args:
            splitnum (int): Next split number

        Returns:
            str: Split filename

        """
        filename = ntpath.split(self.inputfile)[1]
        fname, ext = ntpath.splitext(filename)
        zsplitnum = format(splitnum, "0" + str(self.splitzerofill))
        splitfilename = f"{fname}{self.splitdelimiter}{zsplitnum}{ext}"
        return splitfilename

    def _getmanifestpath(self) -> str:
        """Returns manifest filepath

        Returns:
            str: Manifest filepath

        """
        return str(Path(self.outputdir) / self.manfilename)

    def _endprocess(self):
        """Runs statements that marks the completion of the process"""
        endtime = time.time()
        runtime = int((endtime - self._starttime) / 60)
        log.info(f"Process completed in {runtime} min(s)")

    def bygroupedprefix(self, maxsize: int, callback: Callable = None) -> None:  # noqa: RUF013
        """Split file by groups of lines that start with the same first word (prefix), ensuring
        each group stays in a single file and total split size doesn't exceed maxsize.

        Args:
            maxsize (int): Maximum file size in bytes for each split
            callback (Callable, optional): Callback after each split with file path and size
        Raises:
            ValueError: If a single group of lines exceeds the allowed maxsize

        """
        with Path(self.inputfile).open(mode="rb") as reader:
            manifest_path = self._getmanifestpath()
            with Path(manifest_path).open(mode="w+", encoding="utf8", newline="") as writer:
                fieldnames = ["filename", "filesize", "header"]
                manifest = csv.DictWriter(writer, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                manifest.writeheader()

                splitnum = 1
                current_chunk = BytesIO()
                current_size = 0
                current_file_path = ""

                def write_current_chunk() -> None:
                    nonlocal splitnum, current_chunk, current_size, current_file_path
                    if current_chunk.tell() == 0:
                        return  # Nothing to write
                    current_file_path = Path(self.outputdir) / self._getnextsplit(splitnum)
                    with Path(current_file_path).open("wb") as out:
                        out.write(current_chunk.getbuffer())
                    splitsize = Path(current_file_path).stat().st_size
                    manifest.writerow(
                        {
                            "filename": Path(current_file_path).name,
                            "filesize": splitsize,
                            "header": False,
                        }
                    )
                    if callback:
                        callback(current_file_path, splitsize)
                    splitnum += 1
                    current_chunk = BytesIO()
                    current_size = 0

                prefix_buffer = BytesIO()
                current_prefix = None

                while True:
                    if self.terminate:
                        log.info("Term flag has been set by the user.")
                        log.info("Terminating the process.")
                        break

                    line = reader.readline()
                    if not line:
                        # Flush the last group
                        if prefix_buffer.tell():
                            group_size = prefix_buffer.tell()
                            if group_size > maxsize:
                                raise ValueError("Group exceeds max split file size limit.")
                            if current_size + group_size > maxsize:
                                write_current_chunk()
                            current_chunk.write(prefix_buffer.getbuffer())
                        write_current_chunk()
                        break

                    first_word = line.split(maxsplit=1)[0] if line.strip() else b""
                    if current_prefix is None:
                        current_prefix = first_word
                        prefix_buffer.write(line)
                    elif first_word == current_prefix:
                        prefix_buffer.write(line)
                    else:
                        group_data = prefix_buffer.getvalue()
                        group_size = len(group_data)
                        if group_size > maxsize:
                            raise ValueError(
                                f"Group with prefix '{current_prefix.decode(errors='ignore')}' "
                                f"exceeds max file size."
                            )
                        if current_size + group_size > maxsize:
                            write_current_chunk()
                        current_chunk.write(group_data)
                        current_size += group_size
                        prefix_buffer = BytesIO()
                        prefix_buffer.write(line)
                        current_prefix = first_word

        self._endprocess()
