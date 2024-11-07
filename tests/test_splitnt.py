"""Plugin tests."""

from contextlib import suppress
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
def _setup(request: pytest.FixtureRequest) -> None:
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
def tests_api(_setup: None) -> None:
    """Tests using API without deleting inut file"""
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=600,
        size_unit="KB",
        use_directory=False,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        get_resource(project_name=PROJECT_ID, resource_name=f"test_00000000{n+1}.nt")

    get_resource(project_name=PROJECT_ID, resource_name="test.nt")


@needs_cmem
def tests_api_delete(_setup: None) -> None:
    """Tests using API with deleting inut file"""
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=600,
        size_unit="KB",
        delete_file=True,
        use_directory=False,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        get_resource(project_name=PROJECT_ID, resource_name=f"test_00000000{n+1}.nt")

    try:
        get_resource(project_name=PROJECT_ID, resource_name="test.nt")
        raise OSError("Input file not deleted")  # noqa: TRY301
    except Exception as e:
        if type(e) is HTTPError and e.response.status_code == 404:  # noqa: PLR2004
            pass
        else:
            raise e  # noqa: TRY201


@needs_cmem
def tests_filesystem(_setup: None) -> None:
    """Tests using API without deleting inut file"""
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=600,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        if not (Path(__path__[0]) / PROJECT_ID / "resources" / f"test_00000000{n+1}.nt").is_file():
            raise OSError(f"Output file {n+1} not found.")


@needs_cmem
def tests_filesystem_delete(_setup: None) -> None:
    """Tests using API without deleting inut file"""
    SplitFilePlugin(
        input_filename="test.nt",
        chunk_size=600,
        size_unit="KB",
        delete_file=True,
        projects_path=__path__[0],
        use_directory=True,
    ).execute(None, context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        if not (Path(__path__[0]) / PROJECT_ID / "resources" / f"test_00000000{n+1}.nt").is_file():
            raise OSError(f"Output file {n+1} not found.")

    if (Path(__path__[0]) / PROJECT_ID / "resources" / "test.nt").is_file():
        raise OSError("Input file not deleted.")
