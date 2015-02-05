
# hindex - A Huge file INDEXer

`hindex` is a command line utility providing fast extraction of
sections of very large, line-oriented text files.  It works by
building a simple index on each file.  Examples of such large files
are: log files; large ASCII-text data sets in comma- or tab-delimited
form; mail box archive files, etc.  The files can be many hundreds of
millions of lines or gigabytes or even terabytes in size.

Once a file is indexed, `hindex` can quickly extract ranges of lines
from any section of the file by line number.  In the special case
where a leading portion of each line is ordered, *e.g.*, for
timestamped log files or sorted data files, `hindex` can extract based
on ranges of this beginning-of-line content.

`hindex` comes in two, functionally identical implementations, one in
"C" and one in pure Python.  They can be used interchangeably, as the
index file format and command syntax is exactly the same for each.  In
the examples below, `hindex.py` can be used where the `hindex` command
appears.

# Usage

The general format of the command is

```bash
hindex [<options>] <file> [<file> ...]
```

At least one data `<file>` must be supplied, and when searching, only
one `<file>` is allowed.  The various `<options>` each have a
single-letter and `--long-name` form.  The "C" version only supports
the single letter form.

## Options for mode of operation

The default mode of operation is to extract and print lines from
`<file>`, possibly building or rebuilding its index if needed, and
applying any search filters.

Other modes (default is "off" for all) are:

`-b`/`--build-only`  
Builds index(es) *only* on `<file>`(s).  No output is generated.  This
mode is useful for batch indexing of a set of files.  Other options
affect whether and how indexes are built, named and stored.  See
"Index build options" below.

`-l`/`--list`  
Only list info for `<file>`(s) and their related indexes.  More detail
is shown when `-v`/`--verbose` is given.

`-x`/`--delete`  
Delete the index file(s) for `<file>`(s) where they exist.  This is
useful for cleaning up indexes in bulk.  Note that the *same* values
of `-F`/`--fullname` and `-D`/`--index-dir` used at time of index
creation must be used here.

`-d`/`--dry-run`  
Where files are to be created, refreshed, or deleted only show what
would be done, don't actually do it.

`-h`/`--help`  
Prints a brief command summary and exits.

## Search options

The default mode when searching is not to apply any filters, and
simply copy the entire input to the output.

Options to filter a subset of lines are below.  The result output is
always a contiguous range of lines from the data `<file>`.  When
searching, only one `<file>` may be given.

`-S <lineno>`/`--start <lineno>`  
Line-number search: start at source line `<lineno>`. Line numbers
start at 1.  This is the "from" line number.  Default: `1`

`-E <lineno>`/`--end <lineno>`  
Line-number search: end at source line `<lineno>`. Line numbers start
at 1.  This is the "to" line number. Default: output to end of file.

`-G <minval>`/`--greater-than <minval>`  
Content search for lines >= `<minval>` in sorted file (see
`-P`/`--snaplen`).  This is the "from" value.  *The `<file>` must have
been indexed with `-P` to use this option.*

`-L <maxval>`/`--less-than <maxval>`  
Content search for lines <= `<maxval>` in sorted file (see
`-P`/`--snaplen`).  This is the "to" value.  *The `<file>` must have
been indexed with `-P` to use this option.*

`-N <lines>`/`--count <lines>`  
Limit output to at most `<lines>` lines.  Default: unlimited.

## Output options

By default, output lines are written to the standard output
(`stdout`), exactly as they occur in the input.  Errors and other
diagnostics are written to the standard error (`stderr`).  This
behavior can be modified with the following options:

`-o <file>`/`--output <file>`  
Output to `<file>` instead of default `stdout`.  As a special case, a
single dash (`-`) for `<file>` denotes standard output.  Default:
standard output.

`-n`/`--line-number`  
Prefix each output line with the line number in the original `<file>`.
Line numbers start at 1.  Default: no prefix.

`-q`/`--quiet`  
Limit messages to a minimum.  Default: do not suppress messages.

` -v`/`--verbose`  
More verbose output when indexing, listing or searching. Default: do
not be verbose.

## Index build options

In general indexes are built only on first use.  If the indexed file
content has changed, the index will be refreshed to include any
appended data, or entirely rebuilt as needed.  For large log files
that are continuously growing, each invocation of `hindex` will
freshen the index incrementally.

`-f`/`--force`  
Force rebuild of any existing index(es).  Normally, an index is not
rebuilt if it exists and the indexed file's size has not changed.
Default: do not do unnecessary rebuild.

`-P <bytes>`/`--snaplen <bytes>`  
Capture leading `<bytes>`-byte fragments of each line, for use in
content based search with `-G`/`--greater-than` and
`-L`/`--less-than`.  Default: do not capture.

`-C <bytes>`/`--chunk-size <bytes>`  
Create index entries for every `<bytes>` byte chunk of `<file>`.
Smaller values will make searching faster at the expense of larger
index size.  Default: 1,000,000

## Index file name and location options

By default, index file names are generated using a compact hash of the
full path, and stored in `/tmp` directory.  Generated index file names
*always* end with `.hindex`.  The following options allow for
alternate locations, e.g. storing the indexes alongside their related
data files in the same directory, or to use more readable names for
the index, based on the related data file.

An example of an index file name using default options is:
`/tmp/f_f9f4c7c7ec4f6c224790c21272ca754032a25957.hindex`

When indexing very many files, it is useful to put the indexes in the
same directory as their files, and to name them based on their related
files.  This is done by using `-F` option.

`-i <index>`/`--index-file <index>`  
Use explicitly named index file `<index>`.  Default: generate
filename.

`-D <dir>`/`--index-dir <dir>`  
Store indexes in `<dir>`.  A value of `.` (dot) for `<dir>` means to
store the index in the *same directory* as the file being indexed.
Note: this is *not* the current working directory of the command, but
the directory of the file, and can vary when multiple `<file>`s in
various directories are indexed.  Default: `/tmp/`

`-H`/`--hidden`  
Prefix index filenames with dot (.) to make them hidden.  Default: do
not prefix.

`-F`/`--fullname`  
Use the full base name of `<file>` plus `.hindex` for the index name,
instead of a hash.  Use of this option implies `-D .` / `--index-dir
.` (i.e. storing indexes in the same directory as the file).  Default:
generate the index file name as a hash.

# Examples

## Building an index (only)

Consider a sample file `/usr/local/data/sample.txt` with 200,000 lines
as follows:

```
Line 0000000001 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
Line 0000000002 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
Line 0000000003 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
...
Line 0000199997 xxxxxxxxxxxxxx
Line 0000199998 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
Line 0000199999 xxxxxxxxxxxxxxx
Line 0000200000 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

```

This command will (only) build the index, capturing the first 20
characters of each line for content-based search:  
`hindex -P 20 -b -F small.txt`

## Extracting a range of lines by line number

This command will extract lines 1,000 to 1,003:

```
> hindex -S 1000 -E 1003 -F /usr/local/data/sample.txt
Line 0000001000 xxxxxxxxxxxxxxxxxxxxx
Line 0000001001 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
Line 0000001002 xxxxxxxxx
Line 0000001003 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

Note that we needed to give `-F` to tell `hindex` indexes are in the
same directories as files.

## Extracting based on content (beginning portion of line)

This command will extract lines based on a range of leading substring:

```
> hindex -n -G 'Line 000000100' -L 'Line 000000101' -F /usr/local/data/sample.txt
1,000: Line 0000001000 xxxxxxxxxxxxxxxxxxxxx
1,001: Line 0000001001 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
1,002: Line 0000001002 xxxxxxxxx
1,003: Line 0000001003 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
[...output elided...]
1,016: Line 0000001016 xxxxxxxxxxxxxxxx
1,017: Line 0000001017 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
1,018: Line 0000001018 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
1,019: Line 0000001019 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

# Building the executable

The pure Python version requires no build, however it may be needed to
adjust the first line which assumes python in `/usr/bin/python3`

The "C" version may be built by editing the `Makefile` as needed and
running `make`.  The `Makefile` is specific to Linux and `gcc`, but
with adjustments should be able to be made to compile on other
platforms.

Once built, the executable may be installed where needed,
e.g. `/usr/local/bin/` is a common place.  Running `make install` will
copy the executables there.

# Index file format

Each data files has a corresponding index file which, unless
explicitly named otherwise, always ends with the suffix `.hindex`.

The index file format is plain text and line-oriented.  It begins with
a two-line *header* followed by one or more index *entries*.

The index file *header* is two lines:
```
<filename>
<file_mtime> <file_size> <file_lines> <chunk_size> <snaplen> <nentry>
```

where:

* `<filename>` is the name of the source data file indexed
* `<file_mtime>` is the Unix epoch modified time of <filename>
* `<file_size>` is the size in bytes of <filename> when indexed
* `<file_lines>` is the number lines in <filename> when indexed
* `<chunk_size>` is the minimum number of bytes per index entry
* `<snaplen>` is the number of leading bytes of lines snapped for searching.  *The file must be ordered by this leading substring*
* `<nentry>` is the number of index entries that follow

The format of each *index entry* line in the index file is:
```
<filepos> <line_number> [<content>]
```

where:

* `<filepos>` is the zero-origin file position of the start of `<line_number>`
* `<line_number>` is the *zero-origin* line number that begins at `<filepos>`.  Note that human-oriented, *one-origin* line numbers are used for command arguments and in output with `-n`/`--line-number`  
* `<content>` is optional and gives a leading substring of the line known to be less than or equal to the line at `<line_number>`.  The length of the substring is capped at `<snaplen>`

The final line in the index represents the conceptual line that starts
at "EOF" and so will have `<filepos>` equal to the file size,
`<line_number>` equal to the number of lines in the file, and
`<content>` usually absent.

The `<content>` field will be present if and only if `-P`/`--snaplen`
was given when building the index.  These leading line fragments
*must* be available when searching on ranges of line content.

## Example index file

Continuing with the 200,000-line sample file
`/usr/local/data/sample.txt` as an example, the index file
`sample.txt.hindex` produced with the command:

`hindex -P 20 -b -F small.txt`

will be:

```
/usr/local/data/sample.txt
1738355165.303481 12295652 200000 1000000 20 13
1000001 16270 Line 0000016270 xxxx
2000002 32582 Line 0000032582 xxxx
3000085 48844 Line 0000048844 xxxx
4000166 65222 Line 0000065222 xxxx
5000170 81576 Line 0000081576 xxxx
6000210 97774 Line 0000097774 xxxx
7000212 114016 Line 0000114016 xxxx
8000215 130272 Line 0000130272 xxxx
9000225 146573 Line 0000146573 xxxx
10000258 162764 Line 0000162764 xxxx
11000321 178955 Line 0000178955 xxxx
12000360 195210 Line 0000195210 xxxx
12295652 200000
```

The default chunk size of 1,000,000 was used.  Note that the final
line always has the byte size and line count of the file.

# Author, Copyright, License

Copyright 2015, 2025 by John K. Hinsdale `<hin@alma.com>` and freely
distributed under the MIT License.

