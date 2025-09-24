"""Plugin tests."""

from collections.abc import Generator
from contextlib import suppress
from filecmp import cmp
from io import BytesIO
from pathlib import Path
from shutil import copy, rmtree
from typing import Any

import pytest
from cmem.cmempy.workspace.projects.project import delete_project, make_new_project
from cmem.cmempy.workspace.projects.resources import get_resources
from cmem.cmempy.workspace.projects.resources.resource import create_resource, get_resource
from cmem_plugin_base.testing import TestExecutionContext
from requests import HTTPError

from cmem_plugin_splitfile.plugin_splitfile import SPLIT_ZERO_FILL, SplitFilePlugin

from . import __path__

UUID4 = "fc26980a17144b20ad8138d2493f0c2b"
PROJECT_ID = f"project_{UUID4}"
TEST_FILENAME = f"{UUID4}.nt"
CUSTOM_DIR = Path(__path__[0]) / PROJECT_ID / "custom"


@pytest.fixture
def setup_filesystem() -> Generator[None, Any, None]:
    """Set up Validate test"""
    with suppress(Exception):
        delete_project(PROJECT_ID)
    with suppress(Exception):
        rmtree(Path(__path__[0]) / PROJECT_ID)

    make_new_project(PROJECT_ID)

    (Path(__path__[0]) / PROJECT_ID / "resources").mkdir(parents=True, exist_ok=True)
    copy(
        Path(__path__[0]) / "test_files" / TEST_FILENAME,
        Path(__path__[0]) / PROJECT_ID / "resources",
    )
    (Path(__path__[0]) / PROJECT_ID / "resources" / f"empty_{TEST_FILENAME}").open("w").close()

    CUSTOM_DIR.mkdir(exist_ok=True)
    for n in range(2):
        filename = f"{UUID4}_{'0' * (SPLIT_ZERO_FILL - 1)}{n + 1}.nt"
        (Path(__path__[0]) / PROJECT_ID / "resources" / filename).open("w").close()
        (CUSTOM_DIR / filename).open("w").close()

    yield

    rmtree(Path(__path__[0]) / PROJECT_ID)
    delete_project(PROJECT_ID)


@pytest.fixture
def setup_api() -> Generator[None, Any, None]:
    """Set up Validate test"""
    with suppress(Exception):
        delete_project(PROJECT_ID)
    with suppress(Exception):
        rmtree(Path(__path__[0]) / PROJECT_ID)

    make_new_project(PROJECT_ID)

    with (Path(__path__[0]) / "test_files" / TEST_FILENAME).open("rb") as f:
        create_resource(
            project_name=PROJECT_ID,
            resource_name=TEST_FILENAME,
            file_resource=f,
            replace=True,
        )
    create_resource(
        project_name=PROJECT_ID,
        resource_name=f"empty_{TEST_FILENAME}",
        file_resource=BytesIO(),
        replace=True,
    )

    CUSTOM_DIR.mkdir(parents=True, exist_ok=True)
    for n in range(2):
        filename = f"{UUID4}_{'0' * (SPLIT_ZERO_FILL - 1)}{n + 1}.nt"
        create_resource(
            project_name=PROJECT_ID,
            resource_name=filename,
            file_resource=BytesIO(),
            replace=True,
        )
        (CUSTOM_DIR / filename).open("w").close()

    yield

    rmtree(Path(__path__[0]) / PROJECT_ID)
    delete_project(PROJECT_ID)


@pytest.fixture
def setup_no_file() -> Generator[None, Any, None]:
    """Set up Validate test"""
    with suppress(Exception):
        delete_project(PROJECT_ID)
    make_new_project(PROJECT_ID)

    yield

    delete_project(PROJECT_ID)


@pytest.mark.usefixtures("setup_filesystem")
def test_filesystem_size() -> None:
    """Test split by size using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
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


@pytest.mark.usefixtures("setup_filesystem")
def test_filesystem_size_custom_target() -> None:
    """Test split by size using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
        custom_target_directory=str(CUSTOM_DIR),
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            CUSTOM_DIR / f"{UUID4}_00000000{n + 1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n + 1}.nt",
        )

    if not (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).is_file():
        raise OSError("Input file deleted.")


@pytest.mark.usefixtures("setup_api")
def test_api_size_custom_target() -> None:
    """Test split by size using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=False,
        custom_target_directory=str(CUSTOM_DIR),
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            CUSTOM_DIR / f"{UUID4}_00000000{n + 1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n + 1}.nt",
        )

    get_resource(project_name=PROJECT_ID, resource_name=TEST_FILENAME)


@pytest.mark.usefixtures("setup_filesystem")
def test_filesystem_size_header() -> None:
    """Test split by size with header using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
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


@pytest.mark.usefixtures("setup_api")
def test_api_size() -> None:
    """Test split by size using API"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=False,
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


@pytest.mark.usefixtures("setup_filesystem")
def test_filesystem_size_delete() -> None:
    """Test split by size using file system and delete input file"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
        delete_input_file=True,
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n + 1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_size_00000000{n + 1}.nt",
        )

    if (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).exists():
        raise FileExistsError("Input file not deleted.")


@pytest.mark.usefixtures("setup_api")
def test_api_size_delete() -> None:
    """Test split by size using API and delete input file"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        delete_input_file=True,
        use_directory=False,
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


@pytest.mark.usefixtures("setup_filesystem")
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


@pytest.mark.usefixtures("setup_filesystem")
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


@pytest.mark.usefixtures("setup_filesystem")
def test_group_prefix_filesystem_delete_previous() -> None:
    """Test split by size using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
        group_prefix=True,
        delete_previous_result=True,
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n + 1}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_group_00000000{n + 1}.nt",
        )

    if not (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).is_file():
        raise OSError("Input file deleted.")


@pytest.mark.usefixtures("setup_filesystem")
def test_group_prefix_filesystem_increment() -> None:
    """Test split by size using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
        group_prefix=True,
        delete_previous_result=False,
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        assert cmp(
            Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n + 3}.nt",
            Path(__path__[0]) / "test_files" / f"{UUID4}_group_00000000{n + 1}.nt",
        )

    if not (Path(__path__[0]) / PROJECT_ID / "resources" / TEST_FILENAME).is_file():
        raise OSError("Input file deleted.")


@pytest.mark.usefixtures("setup_api")
def test_group_prefix_api_delete_previous() -> None:
    """Test split by size using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=False,
        group_prefix=True,
        delete_previous_result=True,
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        f = get_resource(project_name=PROJECT_ID, resource_name=f"{UUID4}_00000000{n + 1}.nt")
        assert (
            f
            == (Path(__path__[0]) / "test_files" / f"{UUID4}_group_00000000{n + 1}.nt")
            .open("rb")
            .read()
        )

    get_resource(project_name=PROJECT_ID, resource_name=TEST_FILENAME)


@pytest.mark.usefixtures("setup_api")
def test_group_prefix_api_increment() -> None:
    """Test split by size using file system"""
    SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=False,
        group_prefix=True,
        delete_previous_result=False,
    ).execute(inputs=[], context=TestExecutionContext(PROJECT_ID))

    for n in range(3):
        f = get_resource(project_name=PROJECT_ID, resource_name=f"{UUID4}_00000000{n + 3}.nt")
        assert (
            f
            == (Path(__path__[0]) / "test_files" / f"{UUID4}_group_00000000{n + 1}.nt")
            .open("rb")
            .read()
        )

    get_resource(project_name=PROJECT_ID, resource_name=TEST_FILENAME)


@pytest.mark.usefixtures("setup_api")
def test_api_empty_file() -> None:
    """Test split by size using API"""
    input_file = f"empty_{TEST_FILENAME}"
    plugin = SplitFilePlugin(
        input_filename=input_file,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=False,
    )
    with pytest.raises(OSError, match=f'Input file "{input_file}" is empty.'):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))
    get_resource(project_name=PROJECT_ID, resource_name=TEST_FILENAME)


@pytest.mark.usefixtures("setup_filesystem")
def test_filesystem_empty_file() -> None:
    """Test empty input file using file system"""
    input_file = f"empty_{TEST_FILENAME}"
    plugin = SplitFilePlugin(
        input_filename=input_file,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
    )
    with pytest.raises(OSError, match=f'Input file "{input_file}" is empty.'):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))
    if not (Path(__path__[0]) / PROJECT_ID / "resources" / f"empty_{TEST_FILENAME}").is_file():
        raise OSError("Input file deleted.")


@pytest.mark.usefixtures("setup_filesystem")
def test_delete_previous_files_filesystem() -> None:
    """Test delete previous result using file system"""
    resources_path = Path(__path__[0]) / PROJECT_ID / "resources"
    plugin = SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
        delete_previous_result=True,
    )
    plugin.context = TestExecutionContext(PROJECT_ID)
    plugin.delete_previous_results(resources_path)

    for n in range(2):
        if (Path(__path__[0]) / PROJECT_ID / "resources" / f"{UUID4}_00000000{n + 1}.nt").is_file():
            raise OSError("File not deleted.")


@pytest.mark.usefixtures("setup_filesystem")
def test_delete_previous_files_custom_target_filesystem() -> None:
    """Test delete previous result using file system"""
    resources_path = Path(__path__[0]) / PROJECT_ID / "resources"
    plugin = SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
        delete_previous_result=True,
        custom_target_directory=str(CUSTOM_DIR),
    )
    plugin.context = TestExecutionContext(PROJECT_ID)
    plugin.delete_previous_results(resources_path)

    for n in range(2):
        if (CUSTOM_DIR / f"{UUID4}_00000000{n + 1}.nt").is_file():
            raise OSError("File not deleted.")


@pytest.mark.usefixtures("setup_api")
def test_delete_previous_files_custom_target_api() -> None:
    """Test delete previous result using API"""
    resources_path = Path(__path__[0]) / PROJECT_ID / "resources"
    plugin = SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=False,
        delete_previous_result=True,
        custom_target_directory=str(CUSTOM_DIR),
    )
    plugin.context = TestExecutionContext(PROJECT_ID)
    plugin.delete_previous_results(resources_path)

    for n in range(2):
        if (CUSTOM_DIR / f"{UUID4}_00000000{n + 1}.nt").is_file():
            raise OSError("File not deleted.")


@pytest.mark.usefixtures("setup_api")
def test_delete_previous_files_api() -> None:
    """Test delete previous result using API"""
    resources_path = Path(__path__[0]) / PROJECT_ID / "resources"
    plugin = SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=False,
        delete_previous_result=True,
    )
    plugin.context = TestExecutionContext(PROJECT_ID)
    plugin.delete_previous_results(resources_path)
    resources = [r["name"] for r in get_resources(PROJECT_ID)]
    for n in range(2):
        if f"{UUID4}_00000000{n + 1}.nt" in resources:
            raise OSError("File not deleted.")


def test_parameter_validation() -> None:
    """Test parameter validation"""
    with pytest.raises(ValueError, match="Invalid filename for parameter"):
        SplitFilePlugin(input_filename="", chunk_size=3, projects_path=__path__[0])

    with pytest.raises(ValueError, match="Invalid size unit"):
        SplitFilePlugin(
            input_filename="file", chunk_size=3, size_unit="", projects_path=__path__[0]
        )

    SplitFilePlugin(
        input_filename="file", chunk_size=1, size_unit="lines", projects_path=__path__[0]
    )
    with pytest.raises(ValueError, match="Invalid chunk size for lines"):
        SplitFilePlugin(
            input_filename="file", chunk_size=1.5, size_unit="lines", projects_path=__path__[0]
        )

    with pytest.raises(ValueError, match="Invalid chunk size for lines"):
        SplitFilePlugin(
            input_filename="file", chunk_size=-1, size_unit="lines", projects_path=__path__[0]
        )

    with pytest.raises(ValueError, match="Minimum chunk size is 1024 bytes"):
        SplitFilePlugin(
            input_filename="file", size_unit="KB", chunk_size=0.5, projects_path=__path__[0]
        )

    SplitFilePlugin(
        input_filename="file", size_unit="MB", chunk_size=0.001, projects_path=__path__[0]
    )
    with pytest.raises(ValueError, match="Minimum chunk size is 1024 bytes"):
        SplitFilePlugin(
            input_filename="file", size_unit="MB", chunk_size=0.0004, projects_path=__path__[0]
        )

    SplitFilePlugin(
        input_filename="file", size_unit="GB", chunk_size=0.000001, projects_path=__path__[0]
    )
    with pytest.raises(ValueError, match="Minimum chunk size is 1024 bytes"):
        SplitFilePlugin(
            input_filename="file", size_unit="GB", chunk_size=0.0000005, projects_path=__path__[0]
        )

    with pytest.raises(ValueError, match="Invalid path for parameter"):
        SplitFilePlugin(input_filename="file", chunk_size=3, use_directory=True, projects_path="?")

    projects_path = f"/{UUID4}"
    with pytest.raises(ValueError, match=f"Directory {projects_path} does not exist"):
        SplitFilePlugin(
            input_filename="file", chunk_size=3, use_directory=True, projects_path=projects_path
        )

    with pytest.raises(
        ValueError, match='Grouping lines with the same prefix does not support size unit "lines".'
    ):
        SplitFilePlugin(
            input_filename="file",
            chunk_size=3,
            size_unit="lines",
            use_directory=True,
            projects_path=projects_path,
            group_prefix=True,
        )

    with pytest.raises(
        ValueError, match="Grouping lines with the same prefix does not support 'headers'."
    ):
        SplitFilePlugin(
            input_filename="file",
            chunk_size=3,
            include_header=True,
            use_directory=True,
            projects_path=projects_path,
            group_prefix=True,
        )


@pytest.mark.usefixtures("setup_filesystem")
def test_group_prefix_size_error() -> None:
    """Test split by size using file system"""
    plugin = SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=1,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
        group_prefix=True,
    )

    with pytest.raises(
        ValueError, match='Group with prefix "<http://example.org/subject1>" exceeds max file size.'
    ):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))


@pytest.mark.usefixtures("setup_no_file")
def test_api_no_file() -> None:
    """Test empty input file using file system"""
    plugin = SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=False,
    )
    with pytest.raises(HTTPError, match="404 Client Error: Not Found for url:"):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))


@pytest.mark.usefixtures("setup_no_file")
def test_filesystem_no_file() -> None:
    """Test empty input file using file system"""
    plugin = SplitFilePlugin(
        input_filename=TEST_FILENAME,
        chunk_size=3,
        size_unit="KB",
        projects_path=__path__[0],
        use_directory=True,
    )
    with pytest.raises(FileNotFoundError, match=f'Input file "{TEST_FILENAME}" not found.'):
        plugin.execute(inputs=[], context=TestExecutionContext(PROJECT_ID))
