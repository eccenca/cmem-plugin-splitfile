"""A task splitting a text file into multiple parts with a specified size"""

import re
from collections import OrderedDict
from collections.abc import Callable, Sequence
from io import BytesIO
from pathlib import Path
from shutil import move
from tempfile import TemporaryDirectory

import requests
from cmem.cmempy.api import config, get_access_token
from cmem.cmempy.workspace.projects.resources import get_resources
from cmem.cmempy.workspace.projects.resources.resource import (
    create_resource,
    delete_resource,
    get_resource_uri,
)
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import Icon, Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
from cmem_plugin_base.dataintegration.parameter.resource import ResourceParameterType
from cmem_plugin_base.dataintegration.plugins import WorkflowPlugin
from cmem_plugin_base.dataintegration.ports import FixedNumberOfInputs
from cmem_plugin_base.dataintegration.types import (
    BoolParameterType,
    FloatParameterType,
    StringParameterType,
)
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access
from filesplit.split import Split
from pathvalidate import is_valid_filepath

from cmem_plugin_splitfile.doc import SPLITFILE_DOC
from cmem_plugin_splitfile.split_by_grouped_prefix import SplitGroupedPrefix

DEFAULT_PROJECT_DIR = "/data/datalake"
SIZE_UNIT_KB = "kb"
SIZE_UNIT_MB = "mb"
SIZE_UNIT_GB = "gb"
SIZE_UNIT_LINES = "lines"
SIZE_UNIT_PARAMETER_CHOICES = OrderedDict(
    {
        SIZE_UNIT_KB: "KB",
        SIZE_UNIT_MB: "MB",
        SIZE_UNIT_GB: "GB",
        SIZE_UNIT_LINES: "Lines",
    }
)

SPLIT_ZERO_FILL = 9


@Plugin(
    label="Split file",
    description="Split a file into multiple parts with a specified size.",
    documentation=SPLITFILE_DOC,
    icon=Icon(package=__package__, file_name="splitfile.svg"),
    parameters=[
        PluginParameter(
            param_type=ResourceParameterType(),
            name="input_filename",
            label="Input filename",
            description="The input file to be split.",
        ),
        PluginParameter(
            param_type=FloatParameterType(),
            name="chunk_size",
            label="Chunk size",
            description="The maximum size of the chunk files.",
        ),
        PluginParameter(
            param_type=ChoiceParameterType(SIZE_UNIT_PARAMETER_CHOICES),
            name="size_unit",
            label="Size unit",
            description="""The unit of the size value: kilobyte (KB), megabyte (MB), gigabyte (GB),
            or number of lines (Lines).""",
        ),
        PluginParameter(
            param_type=BoolParameterType(),
            name="include_header",
            label="Include header",
            description="""Include the header in each split. The first line of the input file is
            treated as the header.""",
        ),
        PluginParameter(
            param_type=BoolParameterType(),
            name="delete_input_file",
            label="Delete input file",
            description="Delete the input file after splitting.",
        ),
        PluginParameter(
            param_type=BoolParameterType(),
            name="group_prefix",
            label="Group lines with the same prefix in one file.",
            description="""Group lines with the same prefix in one file. This assumes a
            sorted input file, and does not support headers or a number of lines for the split
            file size. The prefix is the first item when a line is split at whitespaces.""",
        ),
        PluginParameter(
            param_type=BoolParameterType(),
            name="use_directory",
            label="Use internal projects directory",
            description="""Use the internal projects directory of DataIntegration to fetch and store
          files, instead of using the API. If enabled, the "Internal projects directory" parameter
        has to be set. The split files will be stored in a subdirectory with the name of the
        project identifier.""",
            advanced=True,
        ),
        PluginParameter(
            param_type=StringParameterType(),
            name="projects_path",
            label="Internal projects directory",
            description="""The path to the internal projects directory. If "Use internal projects
            directory" is disabled, this parameter has no effect.""",
            advanced=True,
        ),
        PluginParameter(
            param_type=StringParameterType(),
            name="custom_target_directory",
            label="Custom target directory",
            description="""If enabled the output files are written to this directory instead of the
            project path. The directory needs to be accessible from DataIntegration.""",
            advanced=True,
        ),
        PluginParameter(
            param_type=BoolParameterType(),
            name="delete_previous_result",
            label="""Delete previous result.""",
            description="""Delete the previous result from splitting a file with the input filename
            from the target directory. If disabled, the output file numbering increments from the
            last chunk (only works with "Group lines with the same prefix in one file").""",
        ),
    ],
)
class SplitFilePlugin(WorkflowPlugin):
    """Split File Workflow Plugin"""

    def __init__(  # noqa: C901, PLR0912, PLR0913
        self,
        input_filename: str,
        chunk_size: float,
        size_unit: str = SIZE_UNIT_MB,
        group_prefix: bool = False,
        include_header: bool = False,
        delete_input_file: bool = False,
        use_directory: bool = True,
        projects_path: str = DEFAULT_PROJECT_DIR,
        custom_target_directory: str = "",
        delete_previous_result: bool = False,
    ) -> None:
        errors = ""
        if not is_valid_filepath(input_filename):
            errors += 'Invalid filename for parameter "Input filename". '

        self.lines = False
        size_unit = size_unit.lower()
        if size_unit == SIZE_UNIT_KB:
            chunk_size *= 1024
        elif size_unit == SIZE_UNIT_MB:
            chunk_size *= 1024**2
        elif size_unit == SIZE_UNIT_GB:
            chunk_size *= 1024**3
        elif size_unit == SIZE_UNIT_LINES:
            self.lines = True
        else:
            errors += "Invalid size unit. "

        if self.lines:
            if int(chunk_size) != chunk_size or chunk_size < 1:
                errors += "Invalid chunk size for lines. "
            if group_prefix:
                errors += 'Grouping lines with the same prefix does not support size unit "lines".'
        elif chunk_size < 1024:  # noqa: PLR2004
            errors += "Minimum chunk size is 1024 bytes. "

        if group_prefix and include_header:
            errors += "Grouping lines with the same prefix does not support 'headers'."

        if use_directory:
            test_path = projects_path.removeprefix("/")
            if not is_valid_filepath(test_path):
                errors += 'Invalid path for parameter "Internal projects directory". '
            elif not Path(projects_path).is_dir():
                errors += f"Directory {projects_path} does not exist. "
            if custom_target_directory:
                test_path = custom_target_directory.removeprefix("/")
                if not is_valid_filepath(test_path):
                    raise ValueError('Invalid path for parameter "Custom target directory"')

        if errors:
            raise ValueError(errors[:-1])

        self.input_filename = input_filename
        self.size = int(chunk_size)
        self.group_prefix = group_prefix
        self.projects_path = Path(projects_path)
        self.custom_target_directory = custom_target_directory
        self.include_header = include_header
        self.delete_input_file = delete_input_file
        self.use_directory = use_directory
        self.delete_previous_result = delete_previous_result
        self.input_ports = FixedNumberOfInputs([])
        self.output_port = None
        self.moved_files = 0
        self.split_filenames: list[str] = []
        self.last_file = 0

    def cancel_workflow(self) -> bool:
        """Cancel workflow"""
        if hasattr(self.context, "workflow") and self.context.workflow.status() != "Running":
            self.log.info("End task (Cancelled Workflow).")
            return True
        return False

    def split_file(self, input_file_path: Path) -> None:
        """Split file"""
        if not self.group_prefix:
            self.split = Split(inputfile=str(input_file_path), outputdir=self.temp)
            self.split.splitzerofill = SPLIT_ZERO_FILL
            if self.lines:
                self.split.bylinecount(
                    linecount=self.size,
                    includeheader=self.include_header,
                    callback=self.split_callback,
                )
            else:
                self.split.bysize(
                    size=self.size,
                    newline=True,
                    includeheader=self.include_header,
                    callback=self.split_callback,
                )
        else:
            self.split = SplitGroupedPrefix(inputfile=str(input_file_path), outputdir=self.temp)
            self.split.splitzerofill = SPLIT_ZERO_FILL
            self.split.bygroupedprefix(
                maxsize=self.size, splitnum=self.last_file + 1, callback=self.split_callback
            )

    def split_callback(self, file_path: str, file_size: int) -> None:
        """Add split files to list"""
        if self.cancel_workflow():
            self.split.terminate = True
        self.log.info(f"File {Path(file_path).name} generated ({file_size} bytes)")
        self.split_filenames.append(file_path)

    def execute_split(self) -> bool:
        """Execute plugin using file system"""
        if not self.use_directory:
            self.get_file_api()
        resources_path = self.projects_path / self.context.task.project_id() / "resources"
        input_file_path = self.get_input_path(resources_path)

        if not input_file_path.exists():
            raise FileNotFoundError(f'Input file "{self.input_filename}" not found.')

        if input_file_path.stat().st_size == 0:
            raise OSError(f'Input file "{self.input_filename}" is empty.')

        self.delete_previous_results(resources_path)

        self.split_file(input_file_path)

        for filename in self.split_filenames:
            if self.cancel_workflow():
                return False
            self.move_or_upload_output_file(filename, resources_path)
            self.moved_files += 1

        if self.delete_input_file:
            self.delete_file(input_file_path)

        return True

    def get_input_path(self, resources_path: Path) -> Path:
        """Get input file path"""
        if self.use_directory:
            return resources_path / self.input_filename
        return Path(self.temp) / Path(self.input_filename).name

    def delete_previous_results(self, resources_path: Path) -> None:  # noqa: C901
        """Delete previous results, or collect last file number."""
        self.log.info("Checking for previous results...")
        numbers = []
        input_path = Path(self.input_filename)

        # Regex anchored and escaped, with capturing group
        fname_pattern = rf"^{re.escape(input_path.stem)}_(\d{{{SPLIT_ZERO_FILL}}}){re.escape(input_path.suffix)}$"  # noqa: E501
        regex = re.compile(fname_pattern)

        def handle_match(name: str, delete_fn: Callable[[], None] | None = None) -> None:
            """Process a matching filename: delete or collect number."""
            m = regex.match(name)
            if not m:
                return
            if self.delete_previous_result and delete_fn:
                try:
                    delete_fn()
                    self.log.info(f"Deleted: {name}")
                except Exception as e:  # noqa: BLE001
                    self.log.error(f"Failed to delete {name}: {e}")  # noqa: TRY400
            elif self.group_prefix:
                numbers.append(int(m.group(1)))

        if self.use_directory or self.custom_target_directory:
            target_path = (
                Path(self.custom_target_directory)
                if self.custom_target_directory
                else resources_path
            )
            if not self.custom_target_directory and str(input_path.parent) != ".":
                target_path /= input_path.parent

            for f in target_path.iterdir():
                if f.is_file():
                    handle_match(f.name, delete_fn=lambda f=f: f.unlink(missing_ok=True))  # type: ignore[misc]

        else:
            setup_cmempy_user_access(self.context.user)
            project_id = self.context.task.project_id()

            for r in get_resources(project_id):
                handle_match(
                    r["name"],
                    delete_fn=lambda r=r: delete_resource(project_id, r["name"]),  # type: ignore[misc]
                )

        self.last_file = max(numbers) if numbers else 0

    def move_or_upload_output_file(self, filename: str, resources_path: Path) -> None:
        """Move or upload output file"""
        target = Path(filename).name
        if self.use_directory or self.custom_target_directory:
            target_path = (
                Path(self.custom_target_directory)
                if self.custom_target_directory
                else resources_path
            )
            input_parent = Path(self.input_filename).parent
            if not self.custom_target_directory and str(input_parent) != ".":
                target_path /= input_parent
                target_path.mkdir(exist_ok=True)
            move(Path(filename), target_path / target)
        else:
            with Path(filename).open("rb") as f:
                buf = BytesIO(f.read())
                setup_cmempy_user_access(self.context.user)
                create_resource(
                    project_name=self.context.task.project_id(),
                    resource_name=str(Path(self.input_filename).parent / target),
                    file_resource=buf,
                    replace=True,
                )

    def delete_file(self, input_file_path: Path) -> None:
        """Delete input file"""
        if self.use_directory:
            input_file_path.unlink()
        else:
            setup_cmempy_user_access(self.context.user)
            delete_resource(self.context.task.project_id(), self.input_filename)

    def get_file_api(self) -> None:
        """Stream resource to temp folder using the API"""
        file_path = Path(self.temp) / Path(self.input_filename).name
        resource_url = get_resource_uri(
            project_name=self.context.task.project_id(), resource_name=self.input_filename
        )
        setup_cmempy_user_access(self.context.user)
        headers = {
            "Authorization": f"Bearer {get_access_token()}",
            "User-Agent": config.get_cmem_user_agent(),
        }
        with requests.get(resource_url, headers=headers, stream=True) as r:  # noqa: S113
            r.raise_for_status()
            if r.text == "":
                raise OSError(f'Input file "{self.input_filename}" is empty.')
            with file_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=10485760):
                    f.write(chunk)

    def execute(self, inputs: Sequence[Entities], context: ExecutionContext) -> None:  # noqa: ARG002
        """Execute plugin with temporary directory"""
        if (
            self.use_directory
            and self.custom_target_directory
            and not Path(self.custom_target_directory).is_dir()
        ):
            raise ValueError(f"Directory {self.custom_target_directory} does not exist ")

        self.context = context
        context.report.update(ExecutionReport(entity_count=0, operation_desc="files generated"))

        if self.cancel_workflow():
            context.report.update(
                ExecutionReport(entity_count=0, operation_desc="files generated (cancelled")
            )
            return

        with TemporaryDirectory() as self.temp:
            finished = self.execute_split()

        operation_desc = "file generated" if self.moved_files == 1 else "files generated"
        if not finished:
            operation_desc += " (cancelled)"
        context.report.update(
            ExecutionReport(entity_count=self.moved_files, operation_desc=operation_desc)
        )
