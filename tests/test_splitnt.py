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

UID = "fc26980a17144b20ad8138d2493f0c2b"
PROJECT_ID = f"project_{UID}"


@pytest.fixture
def setup(request: pytest.FixtureRequest) -> None:
    """Set up Validate test"""
    with suppress(Exception):
        delete_project(PROJECT_ID)
    make_new_project(PROJECT_ID)

    (Path(__path__[0]) / PROJECT_ID / "resources").mkdir(parents=True, exist_ok=True)
    copy(Path(__path__[0]) / "test.nt", Path(__path__[0]) / PROJECT_ID / "resources" / "test.nt")

    with (Path(__path__[0]) / PROJECT_ID / "resources" / "test.nt").open("rb") as f:
        buf = BytesIO(f.read())
        create_resource(
            project_name=PROJECT_ID,
            resource_name="test.nt",
            file_resource=buf,
            replace=True,
        )

    request.addfinalizer(lambda: rmtree(Path(__path__[0]) / PROJECT_ID))
    request.addfinalizer(lambda: delete_project(PROJECT_ID))  # noqa: PT021


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_filesystem_size() -> None:
    """Test split by size using file system"""
    error = None
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        try:
            assert cmp(
                Path(__path__[0]) / PROJECT_ID / "resources" / f"test_00000000{n+1}.nt",
                Path(__path__[0]) / "test_files" / f"test_size_00000000{n+1}.nt",
            )
        except AssertionError:
            error = "compare"
            break

    if error:
        raise AssertionError("Error comparing files")
    if not (Path(__path__[0]) / PROJECT_ID / "resources" / "test.nt").is_file():
        raise OSError("Input file deleted.")


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_filesystem_size_header() -> None:
    """Test split by size with header using file system"""
    error = None
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=6,
        size_unit="KB",
        include_header=True,
        projects_path=__path__[0],
        use_directory=True,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        try:
            assert cmp(
                Path(__path__[0]) / PROJECT_ID / "resources" / f"test_00000000{n+1}.nt",
                Path(__path__[0]) / "test_files" / f"test_size_header_00000000{n+1}.nt",
            )
        except AssertionError:
            error = "compare"
            break

    if error:
        raise AssertionError("Error comparing files")
    if not (Path(__path__[0]) / PROJECT_ID / "resources" / "test.nt").is_file():
        raise OSError("Input file deleted.")


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_api_size() -> None:
    """Test split by size using API"""
    error = None
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        try:
            f = get_resource(project_name=PROJECT_ID, resource_name=f"test_00000000{n + 1}.nt")
            assert (
                f
                == (Path(__path__[0]) / "test_files" / f"test_size_00000000{n+1}.nt")
                .open("rb")
                .read()
            )
        except AssertionError:
            error = "compare"
            break

    if error:
        raise AssertionError("Error comparing files")

    get_resource(project_name=PROJECT_ID, resource_name="test.nt")


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_api_size_header() -> None:
    """Test split by size with header using API"""
    error = None
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=6,
        size_unit="KB",
        include_header=True,
        projects_path=__path__[0],
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        try:
            f = get_resource(project_name=PROJECT_ID, resource_name=f"test_00000000{n + 1}.nt")
            assert (
                f
                == (Path(__path__[0]) / "test_files" / f"test_size_header_00000000{n+1}.nt")
                .open("rb")
                .read()
            )
        except AssertionError:
            error = "compare"
            break

    if error:
        raise AssertionError("Error comparing files")

    get_resource(project_name=PROJECT_ID, resource_name="test.nt")


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_filesystem_size_delete() -> None:
    """Test split by size using file system and delete input file"""
    error = None
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
        delete_file=True,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        try:
            assert cmp(
                Path(__path__[0]) / PROJECT_ID / "resources" / f"test_00000000{n+1}.nt",
                Path(__path__[0]) / "test_files" / f"test_size_00000000{n+1}.nt",
            )
        except AssertionError:
            error = "compare"
            break

    if error:
        raise AssertionError("Error comparing files")
    if (Path(__path__[0]) / PROJECT_ID / "resources" / "test.nt").is_file():
        raise OSError("Input file not deleted.")


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_api_size_delete() -> None:
    """Test split by size using API and delete input file"""
    error = None
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=6,
        size_unit="KB",
        projects_path=__path__[0],
        delete_file=True,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        try:
            f = get_resource(project_name=PROJECT_ID, resource_name=f"test_00000000{n + 1}.nt")
            assert (
                f
                == (Path(__path__[0]) / "test_files" / f"test_size_00000000{n+1}.nt")
                .open("rb")
                .read()
            )
        except AssertionError:
            error = "compare"
            break

    if error:
        raise AssertionError("Error comparing files")

    try:
        get_resource(project_name=PROJECT_ID, resource_name="test.nt")
    except Exception as exc:
        if type(exc) is HTTPError and exc.response.status_code == 404:  # noqa: PLR2004
            pass
        else:
            raise exc  # noqa: TRY201


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_filesystem_lines() -> None:
    """Test split by lines using file system"""
    error = None
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=40,
        size_unit="Lines",
        projects_path=__path__[0],
        use_directory=True,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        try:
            assert cmp(
                Path(__path__[0]) / PROJECT_ID / "resources" / f"test_00000000{n+1}.nt",
                Path(__path__[0]) / "test_files" / f"test_lines_00000000{n+1}.nt",
            )
        except AssertionError:
            error = "compare"
            break

    if error:
        raise AssertionError("Error comparing files")


@needs_cmem
@pytest.mark.usefixtures("setup")
def test_filesystem_lines_header() -> None:
    """Test split by lines with header using file system"""
    error = None
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=40,
        size_unit="Lines",
        include_header=True,
        projects_path=__path__[0],
        use_directory=True,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        try:
            assert cmp(
                Path(__path__[0]) / PROJECT_ID / "resources" / f"test_00000000{n+1}.nt",
                Path(__path__[0]) / "test_files" / f"test_lines_header_00000000{n+1}.nt",
            )
        except AssertionError:
            error = "compare"
            break

    if error:
        raise AssertionError("Error comparing files")
