"""A task splitting a text file into multiple parts with a specified size"""

import re
from collections import OrderedDict
from collections.abc import Sequence
from io import BytesIO
from pathlib import Path
from shutil import move
from tempfile import TemporaryDirectory

import requests
from cmem.cmempy.api import config, get_access_token
from cmem.cmempy.workspace.projects.resources.resource import (
    create_resource,
    delete_resource,
    get_resource_uri,
)
from cmem_plugin_base.dataintegration.context import ExecutionContext, ExecutionReport
from cmem_plugin_base.dataintegration.description import Icon, Plugin, PluginParameter
from cmem_plugin_base.dataintegration.entity import Entities
from cmem_plugin_base.dataintegration.parameter.choice import ChoiceParameterType
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
from cmem_plugin_splitfile.resource_parameter_type import ResourceParameterType

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
        # PluginParameter(
        #     param_type=BoolParameterType(),
        #     name="use_directory",
        #     label="Use internal projects directory",
        #   description="""Use the internal projects directory of DataIntegration to fetch and store
        #   files, instead of using the API. If enabled, the "Internal projects directory" parameter
        #     has to be set. The split files will be stored in a subdirectory with the name of the
        #     project identifier.""",
        #     advanced=True,
        # ),
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
            project path.""",
            advanced=True,
        ),
        PluginParameter(
            param_type=BoolParameterType(),
            name="delete_previous_result",
            label="""Delete previous result.""",
            description="""Delete previous result from splitting a file with the input filename from
            the custom target directory.""",
        ),
    ],
)
class SplitFilePlugin(WorkflowPlugin):
    """Split File Workflow Plugin"""

    def __init__(  # noqa: C901 PLR0912 PLR0913
        self,
        input_filename: str,
        chunk_size: float,
        size_unit: str = SIZE_UNIT_MB,
        include_header: bool = False,
        delete_input_file: bool = False,
        # use_directory: bool = True,
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
            chunk_size *= 1048576
        elif size_unit == SIZE_UNIT_GB:
            chunk_size *= 1073741824
        elif size_unit == SIZE_UNIT_LINES:
            self.lines = True
        else:
            errors += "Invalid size unit. "

        if self.lines:
            if int(chunk_size) != chunk_size or chunk_size < 1:
                errors += "Invalid chunk size for lines. "
        elif chunk_size < 1024:  # noqa: PLR2004
            errors += "Minimum chunk size is 1024 bytes. "

        use_directory = True
        if use_directory:
            test_path = projects_path.removeprefix("/")
            if not is_valid_filepath(test_path):
                errors += 'Invalid path for parameter "Internal projects directory". '
            elif not Path(projects_path).is_dir():
                errors += f"Directory {projects_path} does not exist. "
            if custom_target_directory:
                test_path = custom_target_directory.removeprefix("/")
                if not is_valid_filepath(test_path):
                    errors += 'Invalid path for parameter "Target directory". '
                elif not Path(custom_target_directory).is_dir():
                    errors += f"Directory {custom_target_directory} does not exist. "

        if errors:
            raise ValueError(errors[:-1])

        self.input_filename = input_filename
        self.size = int(chunk_size)
        self.projects_path = Path(projects_path)
        self.custom_target_directory = custom_target_directory
        self.include_header = include_header
        self.delete_input_file = delete_input_file
        self.use_directory = use_directory
        # self.delete_file_regex = re.compile(delete_file_regex)
        self.delete_previous_result = delete_previous_result
        self.input_ports = FixedNumberOfInputs([])
        self.output_port = None
        self.moved_files = 0
        self.split_filenames: list[str] = []

    def cancel_workflow(self) -> bool:
        """Cancel workflow"""
        if hasattr(self.context, "workflow") and self.context.workflow.status() != "Running":
            self.log.info("End task (Cancelled Workflow).")
            return True
        return False

    def split_file(self, input_file_path: Path) -> None:
        """Split file"""
        split = Split(inputfile=str(input_file_path), outputdir=self.temp)
        split.splitzerofill = SPLIT_ZERO_FILL
        if self.lines:
            split.bylinecount(
                linecount=self.size,
                includeheader=self.include_header,
                callback=self.split_callback,
            )
        else:
            split.bysize(
                size=self.size,
                newline=True,
                includeheader=self.include_header,
                callback=self.split_callback,
            )

    def split_callback(self, file_path: str, file_size: int) -> None:
        """Add split files to list"""
        self.log.info(f"File {Path(file_path).name} generated ({file_size} bytes)")
        self.split_filenames.append(file_path)

    # def get_file(self, file_path: Path) -> None:
    #     """Stream resource to temp folder"""
    #     resource_url = get_resource_uri(
    #         project_name=self.context.task.project_id(), resource_name=self.input_filename
    #     )
    #     setup_cmempy_user_access(self.context.user)
    #     headers = {
    #         "Authorization": f"Bearer {get_access_token()}",
    #         "User-Agent": config.get_cmem_user_agent(),
    #     }
    #     with requests.get(resource_url, headers=headers, stream=True) as r:  # noqa: S113
    #         r.raise_for_status()
    #         if r.text == "":
    #             raise OSError("Input file is empty.")
    #         with file_path.open("wb") as f:
    #             for chunk in r.iter_content(chunk_size=10485760):
    #                 f.write(chunk)
    #
    # def execute_api(self) -> bool:
    #     """Execute plugin using the API"""
    #     file_path = Path(self.temp) / Path(self.input_filename).name
    #     self.get_file(file_path)
    #     if self.cancel_workflow():
    #         return False
    #     self.split_file(file_path)
    #
    #     for filename in self.split_filenames:
    #         if self.cancel_workflow():
    #             return False
    #         with Path(filename).open("rb") as f:
    #             buf = BytesIO(f.read())
    #             setup_cmempy_user_access(self.context.user)
    #             create_resource(
    #                 project_name=self.context.task.project_id(),
    #                 resource_name=str(Path(self.input_filename).parent / Path(filename).name),
    #                 file_resource=buf,
    #                 replace=True,
    #             )
    #             self.moved_files += 1
    #
    #     if self.delete_input_file:
    #         setup_cmempy_user_access(self.context.user)
    #         delete_resource(self.context.task.project_id(), self.input_filename)
    #     return True

    def execute_filesystem(self) -> bool:
        """Execute plugin using file system"""
        resources_path = self.projects_path / self.context.task.project_id() / "resources"
        input_file_path = resources_path / self.input_filename
        if input_file_path.stat().st_size == 0:
            raise OSError("Input file is empty.")

        if self.custom_target_directory:
            target_path = Path(self.custom_target_directory)
        else:
            input_file_parent = Path(self.input_filename).parent
            target_path = resources_path
            if str(input_file_parent) != ".":
                target_path /= input_file_parent
                target_path.mkdir(exist_ok=True)

        if self.delete_previous_result:
            self.log.info("Removing files from previous result.")
            stem = Path(self.input_filename).stem
            suffix = Path(self.input_filename).suffix
            fname_pattern = rf"{stem}_[0-9]{{{SPLIT_ZERO_FILL}}}{suffix}"
            files = [f for f in target_path.iterdir() if re.match(fname_pattern, f.name)]
            for f in files:
                f.unlink(missing_ok=True)
                self.log.info(f"File {f} deleted.")

        self.split_file(input_file_path)

        for filename in self.split_filenames:
            if self.cancel_workflow():
                return False
            move(Path(filename), target_path / Path(filename).name)
            self.moved_files += 1

        if self.delete_input_file:
            input_file_path.unlink()
        return True

    def execute(self, inputs: Sequence[Entities], context: ExecutionContext) -> None:  # noqa: ARG002
        """Execute plugin with temporary directory"""
        self.context = context
        context.report.update(ExecutionReport(entity_count=0, operation_desc="files generated"))

        if self.cancel_workflow():
            context.report.update(
                ExecutionReport(entity_count=0, operation_desc="files generated (cancelled")
            )
            return

        with TemporaryDirectory() as self.temp:

            finished = self.execute_filesystem()  # if self.use_directory else self.execute_api()

        operation_desc = "file generated" if self.moved_files == 1 else "files generated"
        if not finished:
            operation_desc += " (cancelled)"
        context.report.update(
            ExecutionReport(entity_count=self.moved_files, operation_desc=operation_desc)
        )
