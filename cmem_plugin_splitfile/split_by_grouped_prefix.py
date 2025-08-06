"""based on: https://pypi.org/project/filesplit/4.1.0/"""

import csv
import logging
import ntpath
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 1000000  # 1 MB
SPLIT_DELIMITER = "_"
ZERO_FILL = 4
MIN_ZERO_FILL = 1
MAX_ZERO_FILL = 10
MANIFEST_FILE_NAME = "manifest"


class ZeroFillOutOfRange(Exception):  # noqa: N818
    """Zero-fill out of range exception"""


class SplitGroupedPrefix:
    """Split ordered file and group lines with the same prefix (first word)"""

    def __init__(self, inputfile: str, outputdir: str) -> None:
        """Construct

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

    @terminate.setter
    def terminate(self, value: bool) -> None:
        """Set terminate flag. Once flag is set the running process will safely terminate.

        Args:
            value (bool): True/False

        """
        self._terminate = value

    @property
    def inputfile(self) -> str:
        """Returns input file path

        Returns:
            str: Input file path

        """
        return self._inputfile

    @property
    def outputdir(self) -> str:
        """Return output dir path

        Returns:
            str: Output dir path

        """
        return self._outputdir

    @property
    def splitdelimiter(self) -> str:
        """Return split file suffix char

        Returns:
            str: Split file suffix char

        """
        return self._splitdelimiter

    @splitdelimiter.setter
    def splitdelimiter(self, value: str) -> None:
        """Set split file suffix char

        Args:
            value (str): Any character

        """
        self._splitdelimiter = value

    @property
    def splitzerofill(self) -> int:
        """Return split file's number of zero fill digits

        Returns:
            int: Split file's number of zero fill digits

        """
        return self._splitzerofill

    @splitzerofill.setter
    def splitzerofill(self, value: int) -> None:
        """Set split file's number of zero fill digits

        Args:
            value (int): Any whole number

        """
        if not MIN_ZERO_FILL <= value <= MAX_ZERO_FILL:
            raise ZeroFillOutOfRange(
                f"Zero fill must be between {MIN_ZERO_FILL} and {MAX_ZERO_FILL}."
            )
        self._splitzerofill = value

    @property
    def manfilename(self) -> str:
        """Return manifest filename

        Returns:
            str: Manifest filename

        """
        return self._manfilename

    @manfilename.setter
    def manfilename(self, value: str) -> None:
        """Set manifest filename

        Args:
            value (str): Manifest filename

        """
        self._manfilename = value

    @staticmethod
    def _getreadbuffersize(splitsize: int) -> int:
        """Return buffer size to be used with the file reader

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
        """Return next split filename

        Args:
            splitnum (int): Next split number

        Returns:
            str: Split filename

        """
        filename = ntpath.split(self.inputfile)[1]
        fname, ext = ntpath.splitext(filename)
        zsplitnum = format(splitnum, "0" + str(self.splitzerofill))
        splitfilename = f"{fname}{self.splitdelimiter}{zsplitnum}{ext}"
        return splitfilename  # noqa: RET504

    def _getmanifestpath(self) -> str:
        """Return manifest filepath

        Returns:
            str: Manifest filepath

        """
        return str(Path(self.outputdir) / self.manfilename)

    def _endprocess(self) -> None:
        """Run statements that marks the completion of the process"""
        endtime = time.time()
        runtime = int((endtime - self._starttime) / 60)
        log.info(f"Process completed in {runtime} min(s)")

    def bygroupedprefix(  # noqa: C901 PLR0915
        self, maxsize: int, callback: Callable[[str, int], Any] | None = None
    ) -> None:
        """Split file by groups of lines that start with the same first word (prefix)

        Ensures each group stays in a single file and total split size doesn't exceed maxsize.
        Uses constant memory by storing only offsets and streaming chunks directly from disk.
        """
        splitnum = 1

        def write_chunk(start: int, end: int) -> None:
            nonlocal splitnum
            chunk_path = Path(self.outputdir) / self._getnextsplit(splitnum)
            with Path(self.inputfile).open("rb") as src, Path(chunk_path).open("wb") as dst:
                src.seek(start)
                remaining = end - start
                while remaining > 0:
                    buf = src.read(min(DEFAULT_CHUNK_SIZE, remaining))
                    if not buf:
                        break
                    dst.write(buf)
                    remaining -= len(buf)
            size = Path(chunk_path).stat().st_size
            manifest.writerow({"filename": chunk_path.name, "filesize": size, "header": False})
            if callback:
                callback(str(chunk_path), size)
            splitnum += 1

        with Path(self.inputfile).open("rb") as reader:
            manifest_path = self._getmanifestpath()
            with Path(manifest_path).open("w+", encoding="utf8", newline="") as writer:
                fieldnames = ["filename", "filesize", "header"]
                manifest = csv.DictWriter(writer, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
                manifest.writeheader()

                splitnum = 1
                chunk_start_offset = 0
                last_prefix_offset = 0
                current_prefix = None

                while True:
                    if self.terminate:
                        log.info("Term flag has been set by the user.")
                        log.info("Terminating the process.")
                        break

                    line_offset = reader.tell()
                    line = reader.readline()
                    if not line:
                        # End of file: write remaining chunk
                        if line_offset > chunk_start_offset:
                            write_chunk(chunk_start_offset, line_offset)
                        break

                    first_word = line.split(maxsplit=1)[0] if line.strip() else b""

                    if current_prefix is None:
                        current_prefix = first_word
                        last_prefix_offset = line_offset
                        continue

                    if first_word != current_prefix:
                        group_size = line_offset - last_prefix_offset
                        if group_size > maxsize:
                            raise ValueError(
                                f'Group with prefix "{current_prefix.decode(errors="ignore")}" '
                                f"exceeds max file size ({group_size} > {maxsize})."
                            )

                        chunk_size = line_offset - chunk_start_offset
                        if chunk_size + len(line) > maxsize:
                            write_chunk(chunk_start_offset, last_prefix_offset)
                            chunk_start_offset = last_prefix_offset

                        current_prefix = first_word
                        last_prefix_offset = line_offset

        self._endprocess()
