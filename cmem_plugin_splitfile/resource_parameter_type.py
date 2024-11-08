"""parameter types"""

from typing import Any

from cmem.cmempy.workspace.projects.resources import get_resources
from cmem_plugin_base.dataintegration.context import PluginContext
from cmem_plugin_base.dataintegration.types import Autocompletion, StringParameterType
from cmem_plugin_base.dataintegration.utils import setup_cmempy_user_access


class ResourceParameterType(StringParameterType):
    """Resource parameter type."""

    allow_only_autocompleted_values: bool = False
    autocomplete_value_with_labels: bool = False

    def __init__(self):
        """Construct."""

    def autocomplete(
        self,
        query_terms: list[str],
        depend_on_parameter_values: list[Any],  # noqa: ARG002
        context: PluginContext,
    ) -> list[Autocompletion]:
        """Autocomplete"""
        setup_cmempy_user_access(context.user)
        resources = [i["fullPath"] for i in get_resources(context.project_id)]
        result = []
        for res in resources:
            str_match = True
            for term in query_terms:
                if term.lower() not in res.lower():
                    str_match = False
                    break
            if str_match:
                result.append(Autocompletion(value=res, label=""))
        result.sort(key=lambda x: x.value)
        return list(set(result))
