"""A task splitting a text file into multiple parts with a specified size"""

from collections import OrderedDict
from collections.abc import Sequence
from io import BytesIO
from pathlib import Path
from shutil import move
from tempfile import TemporaryDirectory
from warnings import simplefilter

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
from urllib3.exceptions import InsecureRequestWarning

from cmem_plugin_splitfile.doc import SPLITFILE_DOC
from cmem_plugin_splitfile.resource_parameter_type import ResourceParameterType

simplefilter("ignore", category=InsecureRequestWarning)


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
            param_type=ChoiceParameterType(
                OrderedDict({"KB": "KB", "MB": "MB", "GB": "GB", "lines": "Lines"})
            ),
            name="size_unit",
            label="Size unit",
            description="""The unit of the size value: kilobyte (KB), megabyte (MB), gigabyte (GB),
            or number of lines (Lines).""",
            default_value="MB",
        ),
        PluginParameter(
            param_type=BoolParameterType(),
            name="include_header",
            label="Include header",
            description="""Include the header in each split. The first line of the input file is
            treated as the header.""",
            default_value=False,
        ),
        PluginParameter(
            param_type=BoolParameterType(),
            name="delete_file",
            label="Delete input file",
            description="Delete the input file after splitting.",
            default_value=False,
        ),
        PluginParameter(
            param_type=BoolParameterType(),
            name="use_directory",
            label="Use internal projects directory",
            description="""Use the internal projects directory of DataIntegration to fetch and store
            files, instead of using the API. If enabled, the "Internal projects directory" parameter
            has to be set.""",
            default_value=False,
            advanced=True,
        ),
        PluginParameter(
            param_type=StringParameterType(),
            name="projects_path",
            label="Internal projects directory",
            description="""The path to the internal projects directory. If "Use internal projects
            directory" is disabled, this parameter has no effect.""",
            default_value="/data/datalake",
            advanced=True,
        ),
    ],
)
class SplitFilePlugin(WorkflowPlugin):
    """Split File Workflow Plugin"""

    def __init__(  # noqa: C901 PLR0913
        self,
        input_filename: str,
        chunk_size: float,
        size_unit: str = "MB",
        include_header: bool = False,
        delete_file: bool = False,
        use_directory: bool = False,
        projects_path: str = "/data/datalake",
    ) -> None:
        errors = ""
        if not is_valid_filepath(input_filename):
            errors += 'Invalid filename for parameter "Input filename". '

        self.lines = False
        match size_unit.lower():
            case "kb":
                chunk_size *= 1024
            case "mb":
                chunk_size *= 1048576
            case "gb":
                chunk_size *= 1073741824
            case "lines":
                self.lines = True

        if self.lines:
            if int(chunk_size) != chunk_size or chunk_size < 1:
                errors += "Invalid chunk size. "
        elif chunk_size < 1024:  # noqa: PLR2004
            errors += "Minimum chunk size is 1024 bytes. "

        if use_directory:
            test_path = projects_path.removeprefix("/")
            if not is_valid_filepath(test_path):
                errors += 'Invalid path for parameter "Internal projects directory". '
            elif not Path(projects_path).is_dir():
                errors += f"Directory {projects_path} does not exist. "

        if errors:
            raise ValueError(errors[:-1])

        self.input_filename = input_filename
        self.size = int(chunk_size)
        self.projects_path = Path(projects_path)
        self.include_header = include_header
        self.delete_file = delete_file
        self.use_directory = use_directory
        self.input_ports = FixedNumberOfInputs([])
        self.output_port = None
        self.moved_files = 0
        self.split_filenames: list[str] = []

    def cancel_workflow(self) -> bool:
        """Cancel workflow"""
        if self.context.workflow.status() != "Running":
            self.log.info("End task (Cancelled Workflow).")
            return True
        return False

    def split_file(self, input_file_path: Path) -> None:
        """Split file"""
        split = Split(inputfile=str(input_file_path), outputdir=self.temp)
        split.splitzerofill = 9
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

    def get_file(self, file_path: Path) -> None:
        """Stream resource to temp folder"""
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
                if self.delete_file:
                    setup_cmempy_user_access(self.context.user)
                    delete_resource(self.context.task.project_id(), self.input_filename)
                raise OSError("Input file is empty.")
            with file_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=10485760):
                    f.write(chunk)

    def execute_api(self) -> bool:
        """Execute plugin using the API"""
        file_path = Path(self.temp) / Path(self.input_filename).name
        self.get_file(file_path)
        if self.cancel_workflow():
            return False
        self.split_file(file_path)

        for filename in self.split_filenames:
            if self.cancel_workflow():
                return False
            with Path(filename).open("rb") as f:
                buf = BytesIO(f.read())
                setup_cmempy_user_access(self.context.user)
                create_resource(
                    project_name=self.context.task.project_id(),
                    resource_name=str(Path(self.input_filename).parent / Path(filename).name),
                    file_resource=buf,
                    replace=True,
                )
                self.moved_files += 1

        if self.delete_file:
            setup_cmempy_user_access(self.context.user)
            delete_resource(self.context.task.project_id(), self.input_filename)
        return True

    def execute_filesystem(self) -> bool:
        """Execute plugin using file system"""
        resources_path = self.projects_path / self.context.task.project_id() / "resources"
        input_file_path = resources_path / self.input_filename
        if input_file_path.stat().st_size == 0:
            if self.delete_file:
                input_file_path.unlink()
            raise OSError("Input file is empty.")

        self.split_file(input_file_path)
        input_file_parent = Path(self.input_filename).parent
        if str(input_file_parent) != ".":
            resources_path /= input_file_parent
            resources_path.mkdir(exist_ok=True)

        for filename in self.split_filenames:
            if self.cancel_workflow():
                return False
            move(Path(filename), resources_path / Path(filename).name)
            self.moved_files += 1

        if self.delete_file:
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
            finished = self.execute_filesystem() if self.use_directory else self.execute_api()

        operation_desc = "file generated" if self.moved_files == 1 else "files generated"
        if not finished:
            operation_desc += " (cancelled)"
        context.report.update(
            ExecutionReport(entity_count=self.moved_files, operation_desc=operation_desc)
        )
