"""Split grouped prefix"""

import logging
import ntpath
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 1_000_000  # 1 MB
SPLIT_DELIMITER = "_"
ZERO_FILL = 4
MIN_ZERO_FILL = 1
MAX_ZERO_FILL = 10


class ZeroFillOutOfRangeError(Exception):
    """Zero-fill out of range exception"""


class SplitGroupedPrefix:
    """Split ordered file and group lines with the same prefix (first word)"""

    def __init__(self, inputfile: str, outputdir: str) -> None:
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
        self._starttime = time.time()

    @property
    def terminate(self) -> bool:
        """Terminate flag"""
        return self._terminate

    @terminate.setter
    def terminate(self, value: bool) -> None:
        self._terminate = value

    @property
    def inputfile(self) -> str:
        """Return input file path"""
        return self._inputfile

    @property
    def outputdir(self) -> str:
        """Return output directory path"""
        return self._outputdir

    @property
    def splitdelimiter(self) -> str:
        """Return delimiter"""
        return self._splitdelimiter

    @splitdelimiter.setter
    def splitdelimiter(self, value: str) -> None:
        self._splitdelimiter = value

    @property
    def splitzerofill(self) -> int:
        """Return zero fill"""
        return self._splitzerofill

    @splitzerofill.setter
    def splitzerofill(self, value: int) -> None:
        if not MIN_ZERO_FILL <= value <= MAX_ZERO_FILL:
            raise ZeroFillOutOfRangeError(
                f"Zero fill must be between {MIN_ZERO_FILL} and {MAX_ZERO_FILL}."
            )
        self._splitzerofill = value

    def _getnextsplit(self, splitnum: int) -> str:
        filename = ntpath.split(self.inputfile)[1]
        fname, ext = ntpath.splitext(filename)
        zsplitnum = format(splitnum, "0" + str(self.splitzerofill))
        return f"{fname}{self.splitdelimiter}{zsplitnum}{ext}"

    def _endprocess(self) -> None:
        endtime = time.time()
        runtime = int((endtime - self._starttime) / 60)
        log.info(f"Process completed in {runtime} min(s)")

    def bygroupedprefix(  # noqa: C901 PLR0915
        self, maxsize: int, callback: Callable[[str, int], Any] | None = None
    ) -> None:
        """Streaming split, keeping groups intact, packing multiple groups per file if possible"""
        splitnum = 1
        current_prefix = None
        current_size = 0
        group_size = 0
        outfile = None
        outfile_path = None

        def close_outfile() -> None:
            nonlocal outfile, outfile_path, current_size
            if outfile:
                outfile.close()
                size = Path(outfile_path).stat().st_size
                if callback:
                    callback(str(outfile_path), size)
                outfile = None
                current_size = 0

        with Path(self.inputfile).open("rb") as infile:
            for line in infile:
                if self.terminate:
                    log.info("Terminate flag set. Stopping split.")
                    break

                if not line.strip():
                    continue

                prefix = line.split(None, 1)[0]

                # First line
                if current_prefix is None:
                    current_prefix = prefix
                    group_size = 0
                    if outfile is None:
                        outfile_path = Path(self.outputdir) / self._getnextsplit(splitnum)
                        outfile = outfile_path.open("wb")

                # New prefix -> check group size & possible file change
                if prefix != current_prefix:
                    if group_size > maxsize:
                        close_outfile()
                        raise ValueError(
                            f'Group with prefix "{current_prefix.decode(errors="ignore")}" '
                            f"exceeds max file size."
                        )

                    # If adding this group would exceed file size, start new file
                    if current_size + group_size > maxsize:
                        close_outfile()
                        splitnum += 1
                        outfile_path = Path(self.outputdir) / self._getnextsplit(splitnum)
                        outfile = outfile_path.open("wb")

                    current_prefix = prefix
                    group_size = 0

                # Accumulate size for the current group
                group_size += len(line)

                # If writing current line would overflow the file, split within same group
                if current_size + len(line) > maxsize:
                    close_outfile()
                    splitnum += 1
                    outfile_path = Path(self.outputdir) / self._getnextsplit(splitnum)
                    outfile = outfile_path.open("wb")

                outfile.write(line)  # type: ignore[union-attr]
                current_size += len(line)

            # Final group size check
            if group_size > maxsize:
                close_outfile()
                raise ValueError(
                    f'Group with prefix "{current_prefix.decode(errors="ignore")}" '  # type: ignore[union-attr]
                    f"exceeds max file size."
                )

        close_outfile()
        self._endprocess()
