"""Plugin tests."""

from collections.abc import Generator
from contextlib import suppress
from filecmp import cmp
from io import BytesIO
from pathlib import Path
from secrets import token_hex
from shutil import copy, rmtree
from typing import Any

import pytest
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource, get_resource
from requests import HTTPError

from cmem_plugin_splitfile.plugin_splitfile import SplitFilePlugin
from tests.utils import TestExecutionContext

from . import __path__

UUID4 = "fc26980a17144b20ad8138d2493f0c2b"
PROJECT_ID = f"project_{UUID4}"
TEST_FILENAME = f"{UUID4}.nt"


@pytest.fixture
def setup() -> Generator[None, Any, None]:
    """Set up Validate test"""
    with suppress(Exception):
        delete_project(PROJECT_ID)
    make_new_project(PROJECT_ID)

    (Path(__path__[0]) / PROJECT_ID / "resources").mkdir(parents=True, exist_ok=True)
    copy(
        Path(__path__[0]) / "test_files" / TEST_FILENAME,
        Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME,
    )
    (Path(__path__[0]) / PROJECT_ID / "resources" / f"empty_{TEST_FILENAME}").open("w").close()

    with (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).open("rb") as f:
        create_resource(
            project_name=PROJECT_ID,
            resource_name=TEST_FILENAME,
            file_resource=BytesIO(f.read()),
            replace=True,
        )
    create_resource(
        project_name=PROJECT_ID,
        resource_name=f"empty_{TEST_FILENAME}",
        file_resource=BytesIO(b""),
        replace=True,
    )

    yield

    rmtree(Path(__path__[0]) / PROJECT_ID)
    delete_project(PROJECT_ID)


@pytest.mark.usefixtures("setup")
def test_filesystem_size() -> None:
    """Test split by size using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n + 1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n + 1}.nt",
        )

    if not (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).is_file():
        raise OSError("Input file deleted.")


@pytest.mark.usefixtures("setup")
def test_filesystem_size_header() -> None:
    """Test split by size with header using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=6,
        size_unit="KB",
        include_header=True,
        projects_path=__path__[0],
        use_directory=True,
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n + 1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_size_header_00000000{n + 1}.nt",
        )

    if not (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).is_file():
        raise OSError("Input file deleted.")


@pytest.mark.usefixtures("setup")
def test_api_size() -> None:
    """Test split by size using API"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        f = get_resource(project_name=PROJECT_ID, resource_name=f"{UUID4}_00000000{n + 1}.nt")
        assert (
            f
            == (Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n + 1}.nt")
            .open("rb")
            .read()
        )

    get_resource(project_name=PROJECT_ID, resource_name=TEST_FILENAME)


@pytest.mark.usefixtures("setup")
def test_filesystem_size_delete() -> None:
    """Test split by size using file system and delete input file"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
        delete_file=True,
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n + 1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n + 1}.nt",
        )

    if (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).is_file():
        raise OSError("Input file not deleted.")


@pytest.mark.usefixtures("setup")
def test_api_size_delete() -> None:
    """Test split by size using API and delete input file"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
        delete_file=True,
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        f = get_resource(project_name=PROJECT_ID, resource_name=f"{UUID4}_00000000{n + 1}.nt")
        assert (
            f
            == (Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n + 1}.nt")
            .open("rb")
            .read()
        )

    with pytest.raises(HTTPError, match="404 Client Error: Not Found for url:"):
        get_resource(project_name=PROJECT_ID, resource_name=TEST_FILENAME)


@pytest.mark.usefixtures("setup")
def test_filesystem_lines() -> None:
    """Test split by lines using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=40,
        size_unit="Lines",
        projects_path=__path__[0],
        use_directory=True,
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n + 1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_lines_00000000{n + 1}.nt",
        )


@pytest.mark.usefixtures("setup")
def test_filesystem_lines_header() -> None:
    """Test split by lines with header using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=40,
        size_unit="Lines",
        include_header=True,
        projects_path=__path__[0],
        use_directory=True,
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n + 1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_lines_header_00000000{n + 1}.nt",
        )


@pytest.mark.usefixtures("setup")
def test_api_empty_file() -> None:
    """Test split by size using API"""
    plugin = SplitFilePlugin(
        input_filename=f"empty_{TEST_FILENAME}",
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
    )
    with pytest.raises(OSError, match="Input file is empty."):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))
    get_resource(project_name=PROJECT_ID, resource_name=TEST_FILENAME)


@pytest.mark.usefixtures("setup")
def test_filesystem_empty_file() -> None:
    """Test empty input file using file system"""
    plugin = SplitFilePlugin(
        input_filename=f"empty_{TEST_FILENAME}",
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
    )
    with pytest.raises(OSError, match="Input file is empty."):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))
    if not (Path(__path__[0]) / PROJECT_ID / "resources" / f"empty_{TEST_FILENAME}").is_file():
        raise OSError("Input file deleted.")


@pytest.mark.usefixtures("setup")
def test_api_empty_file_delete() -> None:
    """Test empty input file using API, delete input file"""
    plugin = SplitFilePlugin(
        input_filename=f"empty_{TEST_FILENAME}",
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
        delete_file=True,
    )
    with pytest.raises(OSError, match="Input file is empty."):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))
    with pytest.raises(HTTPError, match="404 Client Error: Not Found for url:"):
        get_resource(project_name=PROJECT_ID, resource_name=f"empty_{TEST_FILENAME}")


@pytest.mark.usefixtures("setup")
def test_filesystem_empty_file_delete() -> None:
    """Test empty input file using file system, delete input file"""
    plugin = SplitFilePlugin(
        input_filename=f"empty_{TEST_FILENAME}",
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
        delete_file=True,
    )
    with pytest.raises(OSError, match="Input file is empty."):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))
    if (Path(__path__[0]) / PROJECT_ID / "resources" / f"empty_{TEST_FILENAME}").is_file():
        raise OSError("Input file not deleted.")


def test_parameter_validation() -> None:
    """Test parameter validation"""
    with pytest.raises(ValueError, match="Invalid filename for parameter"):
        SplitFilePlugin(input_filename="", chunk_size=6)

    with pytest.raises(ValueError, match="Invalid size unit"):
        SplitFilePlugin(input_filename="file", chunk_size=6, size_unit="")

    SplitFilePlugin(input_filename="file", chunk_size=1, size_unit="lines")
    with pytest.raises(ValueError, match="Invalid chunk size for lines"):
        SplitFilePlugin(input_filename="file", chunk_size=1.5, size_unit="lines")

    with pytest.raises(ValueError, match="Invalid chunk size for lines"):
        SplitFilePlugin(input_filename="file", chunk_size=-1, size_unit="lines")

    with pytest.raises(ValueError, match="Minimum chunk size is 1024 bytes"):
        SplitFilePlugin(input_filename="file", size_unit="KB", chunk_size=0.5)

    SplitFilePlugin(input_filename="file", size_unit="MB", chunk_size=0.001)
    with pytest.raises(ValueError, match="Minimum chunk size is 1024 bytes"):
        SplitFilePlugin(input_filename="file", size_unit="MB", chunk_size=0.0004)

    SplitFilePlugin(input_filename="file", size_unit="GB", chunk_size=0.000001)
    with pytest.raises(ValueError, match="Minimum chunk size is 1024 bytes"):
        SplitFilePlugin(input_filename="file", size_unit="GB", chunk_size=0.0000005)

    with pytest.raises(ValueError, match="Invalid path for parameter"):
        SplitFilePlugin(input_filename="file", chunk_size=6, use_directory=True, projects_path="?")

    projects_path = token_hex(8)
    with pytest.raises(ValueError, match=f"Directory {projects_path} does not exist"):
        SplitFilePlugin(
            input_filename="file", chunk_size=6, use_directory=True, projects_path=projects_path
        )


# @pytest.mark.usefixtures("setup")
# def test_filesystem_size() -> None:
#     """Test split by size using file system"""
#     target_path = "testoutput"
#     SplitFilePlugin(
#         input_filename=TEST_FILENAME,
#         chunk_size=6,
#         size_unit="KB",
#         projects_path=__path__[0],
#         target_path=target_path,
#         use_directory=True,
#     ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))
#
#     for n in range(3):
#         assert cmp(
#             Path(target_path) / f"{UUID4}_00000000{n + 1}.nt",
#             Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n + 1}.nt",
#         )
#
#     if not (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).is_file():
#         raise OSError("Input file deleted.")
