# cmem-plugin-splitfile

Split a text file into parts with a specified size.

[![eccenca Corporate Memory][cmem-shield]][cmem-link]  
[![poetry][poetry-shield]][poetry-link] [![ruff][ruff-shield]][ruff-link] [![mypy][mypy-shield]][mypy-link] [![copier][copier-shield]][copier] 


## Options

### Input filename

The input file to be split.  
_Example:_ An input file with the name _input.nt_ will be split into files with the names _input\_0000000001.nt_,
_input\_0000000002.nt_,   _input\_0000000003.nt_, etc.  
⚠️ Existing files will be overwritten!

### Chunk size

The maximum size of the chunk files.

### Size unit

The unit of the size value: kilobyte (KB), megabyte (MB) or gigabyte (GB).

### Include header

Include the header in each split. The first line of the input file is treated as the header.

### Delete input file

Delete the input file after splitting.

### Use internal projects directory

Use the internal projects directory of DataIntegration to fetch and store files, instead of using the API.
If enabled, the "Internal projects directory" parameter has to be set.

### Internal projects directory

The path to the internal projects directory. If "Use API to fetch and store files" is enabled,
this parameter has no effect.


## Development

- Run [task](https://taskfile.dev/) to see all major development tasks.
- Use [pre-commit](https://pre-commit.com/) to avoid errors before commit.
- This repository was created with [this copier template](https://github.com/eccenca/cmem-plugin-template).

[cmem-link]: https://documentation.eccenca.com
[cmem-shield]: https://img.shields.io/endpoint?url=https://dev.documentation.eccenca.com/badge.json
[poetry-link]: https://python-poetry.org/
[poetry-shield]: https://img.shields.io/endpoint?url=https://python-poetry.org/badge/v0.json
[ruff-link]: https://docs.astral.sh/ruff/
[ruff-shield]: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json&label=Code%20Style
[mypy-link]: https://mypy-lang.org/
[mypy-shield]: https://www.mypy-lang.org/static/mypy_badge.svg
[copier]: https://copier.readthedocs.io/
[copier-shield]: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/copier-org/copier/master/img/badge/badge-grayscale-inverted-border-purple.json
