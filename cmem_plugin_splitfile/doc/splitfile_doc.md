A task splitting a text file into multiple parts with a specified size.

## Options

### Input filename

The input file to be split.  
_Example:_ An input file with the name _input.nt_ will be split into files with the names _input\_000000001.nt_,
_input\_000000002.nt_,   _input\_000000003.nt_, etc.  
⚠️ Existing files will be overwritten!

### Chunk size

The maximum size of the chunk files.

### Size unit

The unit of the size value: kilobyte (KB), megabyte (MB) or gigabyte (GB).

### Delete input file

Delete the input file after splitting.

### Include header

Include the header in each split. The first line of the input file is treated as the header.

### Use internal projects directory

Use the internal projects directory of DataItegration to fetch and store files, instead of using the API.
If enabled, the "Internal projects directory" parameter has to be set.

### Internal projects directory

The path to the internal projects directory. If "Use API to fetch and store files" is enabled,
this parameter has no effect.
