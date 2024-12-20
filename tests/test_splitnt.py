"""Plugin tests."""

from contextlib import suppress
from filecmp import cmp
from io import BytesIO
from pathlib import Path
from shutil import copy, rmtree

import pytest
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources.resource import create_resource, get_resource
from requests.exceptions import HTTPError

from cmem_plugin_splitfile.plugin_splitfile import SplitFilePlugin
from tests.utils import TestExecutionContext, needs_cmem

from . import __path__

UUID4 = "fc26980a17144b20ad8138d2493f0c2b"
PROJECT_ID = f"project_{UUID4}"
TEST_FILENAME = f"{UUID4}.nt"


@pytest.fixture
def setup(request: pytest.FixtureRequest) -> None:
    """Set up Validate test"""
    with suppress(Exception):
        delete_project(PROJECT_ID)
    make_new_project(PROJECT_ID)

    (Path(__path__[0]) / PROJECT_ID / "resources").mkdir(parents=True, exist_ok=True)
    copy(
        Path(__path__[0]) / "test_files" / TEST_FILENAME,
        Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME,
    )

    with (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).open("rb") as f:
        buf = BytesIO(f.read())
        create_resource(
            project_name=PROJECT_ID,
            resource_name=TEST_FILENAME,
            file_resource=buf,
            replace=True,
        )

    request.addfinalizer(lambda: rmtree(Path(__path__[0]) / PROJECT_ID))
    request.addfinalizer(lambda: delete_project(PROJECT_ID))  # noqa: PT021


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_filesystem_size() -> None:
    """Test split by size using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n+1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n+1}.nt",
        )

    if not (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).is_file():
        raise OSError("Input file deleted.")


@needs_cmem
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
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n+1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_size_header_00000000{n+1}.nt",
        )

    if not (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).is_file():
        raise OSError("Input file deleted.")


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_api_size() -> None:
    """Test split by size using API"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        f = get_resource(project_name=PROJECT_ID, resource_name=f"{UUID4}_00000000{n+1}.nt")
        assert (
            f
            == (Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n+1}.nt")
            .open("rb")
            .read()
        )

    get_resource(project_name=PROJECT_ID, resource_name=TEST_FILENAME)


@needs_cmem
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
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n+1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n+1}.nt",
        )

    if (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).is_file():
        raise OSError("Input file not deleted.")


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_api_size_delete() -> None:
    """Test split by size using API and delete input file"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
        delete_file=True,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        f = get_resource(project_name=PROJECT_ID, resource_name=f"{UUID4}_00000000{n+1}.nt")
        assert (
            f
            == (Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n+1}.nt")
            .open("rb")
            .read()
        )

    try:
        get_resource(project_name=PROJECT_ID, resource_name=TEST_FILENAME)
    except Exception as exc:
        if type(exc) is HTTPError and exc.response.status_code == 404:  # noqa: PLR2004
            pass
        else:
            raise


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_filesystem_lines() -> None:
    """Test split by lines using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=40,
        size_unit="Lines",
        projects_path=__path__[0],
        use_directory=True,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n+1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_lines_00000000{n+1}.nt",
        )


@needs_cmem
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
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n+1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_lines_header_00000000{n+1}.nt",
        )
