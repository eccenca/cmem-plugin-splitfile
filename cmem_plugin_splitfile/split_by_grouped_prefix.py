import csv
import logging
import ntpath
import time
from pathlib import Path
from typing import Any, Callable

log = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = 1_000_000  # 1 MB
SPLIT_DELIMITER = "_"
ZERO_FILL = 4
MIN_ZERO_FILL = 1
MAX_ZERO_FILL = 10
MANIFEST_FILE_NAME = "manifest"


class ZeroFillOutOfRange(Exception):
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
        self._manfilename = MANIFEST_FILE_NAME
        self._starttime = time.time()

    @property
    def terminate(self) -> bool:
        return self._terminate

    @terminate.setter
    def terminate(self, value: bool) -> None:
        self._terminate = value

    @property
    def inputfile(self) -> str:
        return self._inputfile

    @property
    def outputdir(self) -> str:
        return self._outputdir

    @property
    def splitdelimiter(self) -> str:
        return self._splitdelimiter

    @splitdelimiter.setter
    def splitdelimiter(self, value: str) -> None:
        self._splitdelimiter = value

    @property
    def splitzerofill(self) -> int:
        return self._splitzerofill

    @splitzerofill.setter
    def splitzerofill(self, value: int) -> None:
        if not MIN_ZERO_FILL <= value <= MAX_ZERO_FILL:
            raise ZeroFillOutOfRange(
                f"Zero fill must be between {MIN_ZERO_FILL} and {MAX_ZERO_FILL}."
            )
        self._splitzerofill = value

    @property
    def manfilename(self) -> str:
        return self._manfilename

    @manfilename.setter
    def manfilename(self, value: str) -> None:
        self._manfilename = value

    def _getnextsplit(self, splitnum: int) -> str:
        filename = ntpath.split(self.inputfile)[1]
        fname, ext = ntpath.splitext(filename)
        zsplitnum = format(splitnum, "0" + str(self.splitzerofill))
        return f"{fname}{self.splitdelimiter}{zsplitnum}{ext}"

    def _getmanifestpath(self) -> str:
        return str(Path(self.outputdir) / self.manfilename)

    def _endprocess(self) -> None:
        endtime = time.time()
        runtime = int((endtime - self._starttime) / 60)
        log.info(f"Process completed in {runtime} min(s)")

    def bygroupedprefix(
            self, maxsize: int, callback: Callable[[str, int], Any] | None = None
    ) -> None:
        """Streaming split, keeping groups intact, packing multiple groups per file if possible"""
        manifest_path = self._getmanifestpath()
        with Path(manifest_path).open("w+", encoding="utf8", newline="") as writer:
            fieldnames = ["filename", "filesize", "header"]
            manifest = csv.DictWriter(writer, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            manifest.writeheader()

            splitnum = 1
            current_prefix = None
            current_size = 0
            group_size = 0
            outfile = None
            outfile_path = None

            def close_outfile():
                nonlocal outfile, outfile_path, current_size
                if outfile:
                    outfile.close()
                    size = Path(outfile_path).stat().st_size
                    manifest.writerow(
                        {"filename": outfile_path.name, "filesize": size, "header": False}
                    )
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

                    outfile.write(line)
                    current_size += len(line)

                # Final group size check
                if group_size > maxsize:
                    close_outfile()
                    raise ValueError(
                        f'Group with prefix "{current_prefix.decode(errors="ignore")}" '
                        f"exceeds max file size."
                    )

            close_outfile()

        self._endprocess()
