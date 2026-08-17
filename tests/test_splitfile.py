"""Plugin tests."""

from filecmp import cmp
from pathlib import Path
from shutil import copy, rmtree

import pytest
from cmem_client.client import Client
from cmem_client.exceptions import FilesNotFoundError
from cmem_client.models.project import Project
from cmem_client.repositories.protocols.import_item import ImportConflictPolicy
from cmem_plugin_base.testing import TestExecutionContext

from cmem_plugin_splitfile.plugin_splitfile import SplitFilePlugin

from . import __path__

UUID4 = "fc26980a17144b20ad8138d2493f0c2b"
PROJECT_ID = f"project_{UUID4}"
TEST_FILENAME = f"{UUID4}.nt"


def get_client() -> Client:
    """Get a client for the test project"""
    return Client.from_context(context=TestExecutionContext(PROJECT_ID))


def read_resource(resource_name: str) -> bytes:
    """Read a project resource of the test project"""
    return bytes(get_client().files.read(f"{PROJECT_ID}:{resource_name}"))


@pytest.fixture
def setup(request: pytest.FixtureRequest) -> None:
    """Set up Validate test"""
    client = get_client()
    client.projects.delete_item(PROJECT_ID, skip_if_missing=True)
    client.projects.create_item(Project(name=PROJECT_ID))

    resources_path = Path(__path__[0]) / PROJECT_ID / "resources"
    resources_path.mkdir(parents=True, exist_ok=True)
    copy(Path(__path__[0]) / "test_files" / TEST_FILENAME, resources_path / TEST_FILENAME)
    (resources_path / f"empty_{TEST_FILENAME}").open("w").close()

    for resource_name in (TEST_FILENAME, f"empty_{TEST_FILENAME}"):
        client.files.import_item(
            path=resources_path / resource_name,
            key=f"{PROJECT_ID}:{resource_name}",
            on_conflict=ImportConflictPolicy.REPLACE,
        )

    request.addfinalizer(lambda: rmtree(Path(__path__[0]) / PROJECT_ID))
    request.addfinalizer(lambda: client.projects.delete_item(PROJECT_ID))  # noqa: PT021


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
        f = read_resource(f"{UUID4}_00000000{n + 1}.nt")
        assert (
            f
            == (Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n + 1}.nt")
            .open("rb")
            .read()
        )

    read_resource(TEST_FILENAME)


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
        f = read_resource(f"{UUID4}_00000000{n + 1}.nt")
        assert (
            f
            == (Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n + 1}.nt")
            .open("rb")
            .read()
        )

    with pytest.raises(FilesNotFoundError, match=r"not found in project"):
        read_resource(TEST_FILENAME)


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
    with pytest.raises(OSError, match=r"Input file is empty."):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))
    read_resource(TEST_FILENAME)


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
    with pytest.raises(OSError, match=r"Input file is empty."):
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
    with pytest.raises(OSError, match=r"Input file is empty."):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))
    with pytest.raises(FilesNotFoundError, match=r"not found in project"):
        read_resource(f"empty_{TEST_FILENAME}")


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
    with pytest.raises(OSError, match=r"Input file is empty."):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))
    if (Path(__path__[0]) / PROJECT_ID / "resources" / f"empty_{TEST_FILENAME}").is_file():
        raise OSError("Input file not deleted.")
