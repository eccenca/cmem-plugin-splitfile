"""cmem-plugin-splitfile"""

import re
from pathlib import Path

CONF = Path("/opt/cmem/eccenca-DataIntegration/dist/etc/dataintegration/conf/dataintegration.conf")

PROJECTFILEPATH = ""
if CONF.is_file():
    with CONF.open("r") as conf_file:
        conf_lines = conf_file.readlines()
    for line, text in enumerate(conf_lines):
        if text == "workspace.repository.projectFile = {\n":
            match = re.search(r'=\s*["\'](.*?)["\']', conf_lines[line + 1])
            if match:
                PROJECTFILEPATH = match.group(1)
            break
