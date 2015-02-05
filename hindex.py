#!/usr/bin/python3
"""hindex.py -- a Huge file INDEXer

Utility to index huge line-oriented files for extraction of ranges of lines.

This utility indexes very large line-oriented files, many gigabytes in
size with millions of lines, and then allows quickly extracting ranges
of lines, by line number, or, when the data are sorted, by ranges of
content of the line, specifically, leading portion of the line (e.g
timestamps in logs).

The index file format is a two-line header followed by one or more
index entries.  The index file header is two lines:
    <filename>
    <file_mtime> <file_size> <file_lines> <chunk_size> <snaplen> <nentry>
where:
* <filename> is the name of the source data file indexed
* <file_mtime> is the Unix epoch modified time of <filename>
* <file_size> is the size in bytes of <filename> when indexed
* <file_lines> is the number lines in <filename> when indexed
* <chunk_size> is the minimum number of bytes per index entry
* <snaplen> is the number of leading bytes of lines snapped
  for searching.  The file must be ordered by this leading substring
* <nentry> is the number of index entries that follow

The format of each index entry line in the index file is:
    <filepos> <line_number> [<content>]
where:
* <filepos> is the zero-origin file position of the start of <line_number>
* <line_number> is the zero-origin line number that begins at <filepos>
* <content> is optional and gives a leading substring of the line
  known to be less than or equal to the line at <line_number>.  The
  length of the substring is capped at <snaplen>

The final line in the index represents the conceptual line that starts
at "EOF" and so will have <filepos> equal to the file size,
<line_number> equal to the number of lines in the file, and <content>
usually absent.

The <content> field will be present when -P/--snaplen was given when
building the index, and must be available when searching on ranges of
line content.
"""

import sys
import os
import argparse
import hashlib
import time
from dataclasses import dataclass, field
from typing import Optional

VERSION = '0.9'

# Defaults
DEFAULT_CHUNK_SIZE = 1 * 1000 * 1000

# Index dir and filenaming
DEFAULT_INDEX_DIR = '/tmp'
INDEX_HASH_PREFIX = 'f_'
INDEX_SUFFIX = '.hindex'

# Index states of existence, freshness, validity
INDEX_STATUS_ABSENT = 0
INDEX_STATUS_FRESH = 1
INDEX_STATUS_STALE = 2
INDEX_STATUS_INVALID = 3
INDEX_STATUS_NAME = {
    INDEX_STATUS_ABSENT: 'Not found',
    INDEX_STATUS_FRESH: 'Up to date',
    INDEX_STATUS_STALE: 'Out of date (needs refresh)',
    INDEX_STATUS_INVALID: 'Invalid (needs rebuild)',
}

INDEX_PROGRESS_INTERVAL = 100 * 1000 * 1000


@dataclass
class IndexInfo:  # pylint: disable=too-many-instance-attributes
    """All information about a file's index, returned by get_index_info."""
    status: int = INDEX_STATUS_ABSENT
    file_size: Optional[int] = None
    file_lines: Optional[int] = None
    last_file_size: Optional[int] = None
    last_file_lines: Optional[int] = None
    file_mtime: Optional[float] = None
    index_file_size: Optional[int] = None
    index_mtime: Optional[float] = None
    chunk_size: Optional[int] = None
    snaplen: Optional[int] = None
    entries: list = field(default_factory=list)


@dataclass
class IndexOptions:
    """User-supplied options controlling index creation."""
    chunk_size: int = DEFAULT_CHUNK_SIZE
    snaplen: Optional[int] = None
    quiet: bool = False
    verbose: bool = False
    force: bool = False
    dryrun: bool = False
    for_content_search: bool = False


@dataclass
class SearchOptions:  # pylint: disable=too-many-instance-attributes
    """User-supplied options controlling file search and output."""
    output_file: Optional[str] = None
    start: Optional[int] = None
    end: Optional[int] = None
    greater_than: Optional[str] = None
    less_than: Optional[str] = None
    count: Optional[int] = None
    line_number: bool = False
    verbose: bool = False


def index_file(filename, index_filename, opts):
    """
    Index a file if needed.  Checks to see if already fully built, and if not will build it.
    For dry run, just shows what would be built.
    """

    # Get current index info
    try:
        info = get_index_info(filename, index_filename)
    except ValueError as exc:
        return False, str(exc)
    exists = (info.status != INDEX_STATUS_ABSENT)

    # Report newly indexed file
    if not exists:
        if opts.for_content_search and not opts.snaplen:
            return False, 'Need to specify -P/--snaplen <snaplen> for new index when using -G/--greater-than or -L/--less-than'
        if not opts.quiet:
            action = 'Would create' if opts.dryrun else 'Creating'
            fmt = '{} new index "{}" on "{}" {:,d} bytes ... please wait ... (-q/--quiet to suppress)'
            _error(fmt.format(action, index_filename, filename, info.file_size))

    # Report status
    if opts.verbose:
        if info.status == INDEX_STATUS_ABSENT:
            fmt = 'Index "{}" on "{}" not found'
            _error(fmt.format(index_filename, filename))
        elif info.status == INDEX_STATUS_FRESH:
            fmt = 'Index "{}" on "{}" is up to date, {:,d} bytes / {:,d} lines'
            _error(fmt.format(index_filename, filename, info.file_size, info.file_lines))
        elif info.status == INDEX_STATUS_INVALID:
            fmt = 'Index "{}" on "{}" made on larger or older file, resetting'
            _error(fmt.format(index_filename, filename))
        elif info.status == INDEX_STATUS_STALE:
            fmt = 'Index "{}" on "{}" was made on older file {:,d} bytes / {:,d} lines < current size {:,d} bytes ... appending index ...'
            _error(fmt.format(index_filename, filename, info.last_file_size, info.last_file_lines, info.file_size))

        if exists and info.chunk_size and _changed(info.chunk_size, opts.chunk_size):
            fmt = 'Note: Chunk size for index "{}" on "{}" changed from {} to {}'
            _error(fmt.format(index_filename, filename, info.chunk_size, opts.chunk_size))

        if exists and info.snaplen and _changed(info.snaplen, opts.snaplen):
            fmt = 'Note: Snap len for index "{}" on "{}" changed from {} to {}'
            _error(fmt.format(index_filename, filename, info.snaplen, opts.snaplen))

    # Reset entries if force-rebuild
    if opts.force:
        info.entries = []
        if not opts.quiet:
            fmt = 'Option -f/--force given, forcing rebuild of index "{}" on "{}" {:,d} bytes (-q/--quiet to suppress)'
            _error(fmt.format(index_filename, filename, info.file_size))
    elif info.status == INDEX_STATUS_FRESH:
        # Nothing to do if index is up to date
        return True, (info.file_size, info.file_lines, info.file_mtime, info.entries)

    # Write new or appended entries
    chunk_bytes_read = 0
    with open(filename, 'rb') as src_fp:
        line_start = 0
        lineno = 0
        if not opts.force and info.entries:
            # Restore state from last indexing
            last_pos, lineno = info.entries[-1][:2]
            src_fp.seek(last_pos)
            line = src_fp.readline()
            linelen = len(line)
            line_start = last_pos + linelen
            chunk_bytes_read = linelen
            if linelen:
                lineno += 1
        # Scan file and enumerate entries
        tot_bytes_read = 0
        tot_bytes_to_read = info.file_size - line_start
        last_report_bytes = 0
        last_line = None
        while True:
            if chunk_bytes_read and (chunk_bytes_read >= opts.chunk_size):
                if opts.snaplen:
                    frag = line[:opts.snaplen].rstrip(b'\n')
                    info.entries.append((line_start, lineno, frag))
                else:
                    info.entries.append((line_start, lineno))
                chunk_bytes_read = 0
            line = src_fp.readline()
            if not line:
                break
            if opts.snaplen and last_line is not None:
                # If snapping content (leading portion of lines) for search, the data must be in order.
                # Check sort order of leading portion being snapped (OK if stuff beyond is out of order in a "tie")
                line_cmp = line[:opts.snaplen]
                last_line_cmp = last_line[:opts.snaplen]  # pylint: disable=unsubscriptable-object
                if line_cmp < last_line_cmp:
                    fmt = '-P/--snaplen = {} given and have unordered data in "{}"\nFirst {} chars of line {}:\n{}\nis less than that in previous line:\n{}'
                    return False, fmt.format(opts.snaplen, filename, opts.snaplen, lineno+1, line_cmp, last_line_cmp)
            last_line = line

            next_line_start = line_start + len(line)
            bytes_read = next_line_start - line_start
            if not bytes_read:
                break
            chunk_bytes_read += bytes_read
            tot_bytes_read += bytes_read
            last_report_bytes += bytes_read
            line_start = next_line_start
            lineno += 1
            if not opts.quiet and last_report_bytes >= INDEX_PROGRESS_INTERVAL:
                fmt = 'Indexed {:5.1f}% = {:,d} / {:,d} bytes of "{}" (-q/--quiet to suppress)'
                _error(fmt.format(100.0 * float(tot_bytes_read)/tot_bytes_to_read, tot_bytes_read, tot_bytes_to_read, filename))
                last_report_bytes = 0

    # Add terminating entry: file size and total line count
    # Don't write if we hit EOF exactly on a chunk boundary.  This will
    # be evident by chunk_bytes_read of zero.  As a special case,
    # always write an entry for empty file.
    if chunk_bytes_read or not info.file_size:
        info.entries.append((line_start, lineno))

    # Show what would be done w/ index
    if opts.dryrun:
        action = 'refresh' if exists else 'create'
        _error('Would {} index "{}" with {} entries\n'.format(action, index_filename, len(info.entries)))
        return True, (info.file_size, info.file_lines, info.file_mtime, info.entries)

    # Warn if file grew while we were indexing it
    if line_start < info.file_size:
        fmt = 'File "{}" was originally {:,d} bytes but shrank to {:,d} while indexing it'
        return False, fmt.format(filename, info.file_size, line_start)
    if line_start > info.file_size:
        if opts.verbose:
            fmt = 'Warning: File "{}" grew from {:,d} to at least {:,d} bytes while indexing it'
            _error(fmt.format(filename, info.file_size, line_start))
        # Store the updated amount of data indexed
        info.file_size = line_start

    # Write out file all at once
    with open(index_filename, 'wb') as index_fp:

        # Write two-line header: filename then (mtime, size, lines, chunk_size, snaplen, nentry)
        fmt = '{}\n{:.6f} {} {} {} {} {}\n'
        index_fp.write(_s2b(fmt.format(filename, info.file_mtime, info.file_size, lineno, opts.chunk_size, opts.snaplen or 0, len(info.entries))))
        # Write entries
        for flds in info.entries:
            pos, lno = flds[:2]
            index_fp.write(_s2b('{} {}'.format(pos, lno)))
            if len(flds) == 3:
                index_fp.write(b' ')
                index_fp.write(flds[2])
            index_fp.write(b'\n')
    if opts.verbose:
        action = 'updated' if info.status == INDEX_STATUS_STALE else 'created'
        fmt = 'Index "{}" {} on "{}" {:,d} bytes / {:,d} lines'
        _error(fmt.format(index_filename, action, filename, line_start, lineno))

    return True, (info.file_size, lineno, info.file_mtime, info.entries)


def get_index_info(filename, index_filename):
    """
    Get status info from index file.  Returns IndexInfo.
    Raises ValueError if the index file is malformed.
    """
    info = IndexInfo()

    # Get source file attributes
    info.file_size = os.path.getsize(filename)
    info.file_mtime = os.path.getmtime(filename)

    # Check nonexistent index
    if not os.path.exists(index_filename):
        return info

    # Index exists, get entries
    info.index_file_size = os.path.getsize(index_filename)
    info.index_mtime = os.path.getmtime(index_filename)
    with open(index_filename, 'rb') as index_fp:
        # Read header lines (2).  First is filename, then (mtime, size, lines, chunk_size, snaplen, nentry)
        h_filename = _b2s(index_fp.readline()).strip()
        if h_filename != filename:
            raise ValueError('Name mismatch: index "{}" has "{}" for file "{}"'.format(index_filename, h_filename, filename))
        h_mslcse_flds = _b2s(index_fp.readline()).strip().split()
        _h_file_mtime = float(h_mslcse_flds[0])
        _h_file_size, _h_file_lines, info.chunk_size, info.snaplen, h_nentry = [int(x) for x in h_mslcse_flds[1:]]

        # Read entries
        for line in index_fp:
            line = line.rstrip(b'\n')

            # Extract file pos
            idx = line.find(b' ')
            if idx <= 0:
                raise ValueError('No file pos in line "{}" in "{}"'.format(line, index_filename))
            filepos = int(line[:idx])
            line = line[idx+1:]

            # Extract line number
            frag = None
            idx = line.find(b' ')
            if idx < 0:
                lineno = int(line)
            else:
                lineno = int(line[:idx])
                frag = line[idx+1:]
            if frag is None:
                info.entries.append((filepos, lineno))
            else:
                if len(frag) > info.snaplen:
                    raise ValueError('Index "{}" header had snaplen of {} but see snap of length {}'.format(index_filename, info.snaplen, len(frag)))
                info.entries.append((filepos, lineno, frag))

        if len(info.entries) != h_nentry:
            raise ValueError('Index "{}" had {} entries in header but {} in index itself'.format(index_filename, h_nentry, len(info.entries)))

    # File exists, check if stale due to file replaced or grew
    if info.entries:
        info.last_file_size, info.last_file_lines = info.entries[-1][:2]
        if info.last_file_size == info.file_size:
            # File was fully indexed, at size given by last_file_size
            info.status = INDEX_STATUS_FRESH
            info.file_lines = info.last_file_lines
        elif info.file_size < info.last_file_size or info.file_mtime < info.index_mtime:
            # Current file size smaller than last indexed or index out of date - must have been replaced
            info.status = INDEX_STATUS_INVALID
            info.entries = []
        else:
            info.status = INDEX_STATUS_STALE
            # Lop off last entry w/ file size and total line count
            info.entries = info.entries[0:-1]

    return info


def main():
    """
    Main driver
    """

    parser = argparse.ArgumentParser(description='hindex - Huge file INDEXer, version {}'.format(VERSION))

    # Mode of operation options (default mode is line output)
    g_mode = parser.add_argument_group('Mode of operation (default is extract and print lines)')
    g_mode.add_argument('-b', '--build-only', action='store_true', help='Build index(es) only on FILE(s), no output [False]')
    g_mode.add_argument('-l', '--list', action='store_true', help='Just list info for FILE(s), more with -v/--verbose [False]')
    g_mode.add_argument('-x', '--delete', action='store_true', help='Delete index file if it exists [False]')
    g_mode.add_argument('-d', '--dry-run', action='store_true', help='Dry run: only show what would do [False]')

    # Search options
    g_search = parser.add_argument_group('Search options')
    g_search.add_argument('-S', '--start', type=int, metavar='LINENO', help='Line-number search: start at source line LINENO [1]')
    g_search.add_argument('-E', '--end', type=int, metavar='LINENO', help='Line-number search: end at source line LINENO [None]')
    g_search.add_argument('-G', '--greater-than', metavar='MINVAL', help='Content search for lines >= MINVAL in sorted file (see -P/--snaplen) [None]')
    g_search.add_argument('-L', '--less-than', metavar='MAXVAL', help='Content search for lines <= MAXVAL in sorted file (see -P/--snaplen) [None]')
    g_search.add_argument('-N', '--count', type=int, metavar='LINES', help='Limit output to at most LINES lines [None]')

    # Output options
    g_output = parser.add_argument_group('Output options')
    g_output.add_argument('-o', '--output', metavar='FILE', help='Output to FILE instead of default stdout [stdout]')
    g_output.add_argument('-n', '--line-number', action='store_true', help='Include original line number in output [False]')
    g_output.add_argument('-q', '--quiet', action='store_true', help='Limit messages to a minimum [False]')
    g_output.add_argument('-v', '--verbose', action='store_true', help='More verbose output when indexing, listing or searching [False]')

    # Index build options
    g_build = parser.add_argument_group('Index build options')
    g_build.add_argument('-f', '--force', action='store_true', help='Force (re-)build of index [False]')
    g_build.add_argument('-P', '--snaplen', metavar='BYTES', type=int, help='Capture leading BYTES bytes of each line for content search [None]')
    g_build.add_argument('-C', '--chunk-size', metavar='BYTES', type=int, default=DEFAULT_CHUNK_SIZE, help='Create index entries every BYTES bytes [{}]'.format(DEFAULT_CHUNK_SIZE))

    # Index file name and location options
    g_index = parser.add_argument_group('Index file name and location options')
    g_index.add_argument('-i', '--index-file', metavar='INDEX', help='Use explicit index file INDEX (else generate) [None]')
    g_index.add_argument('-D', '--index-dir', metavar='DIR', help='Store indexes in DIR, "." means use dir of file [{}]'.format(DEFAULT_INDEX_DIR))
    g_index.add_argument('-H', '--hidden', action='store_true', help='Prefix index files with dot (.) to make them hidden [False]')
    g_index.add_argument('-F', '--fullname', action='store_true', help='Use full name of file + "{}" for index name instead of hash (requires -D .) [False]'.format(INDEX_SUFFIX))

    # Input data
    parser.add_argument('files', nargs='+', metavar='FILE', help='Search/index lines in FILE(s) (required)')

    args = parser.parse_args()

    def usage_error(msg):
        "Show usage error and return False"
        sys.stderr.write('ERROR: {}\n'.format(msg))
        parser.print_help()
        return False

    ### VALIDATE OPTIONS
    nfile = len(args.files)
    if nfile > 1 and args.index_file:
        return usage_error('Can only specify explicit index file with -i/--index-file when indexing a single file, not {}'.format(nfile))

    search_opts = (('-S/--start', args.start), ('-E/--end', args.end),
                   ('-G/--greater-than', args.greater_than), ('-L/--less-than', args.less_than),
                   ('-N/--count', args.count))
    search_opt_given = None
    for opt, val in search_opts:
        if val is not None:
            search_opt_given = (opt, val)
            break

    # Check line range and count options
    if args.start is not None and args.start < 1:
        return usage_error('Value {} for -S/--start must be positive'.format(args.start))
    if args.end is not None and args.end < 1:
        return usage_error('Value {} for -E/--end must be positive'.format(args.end))
    if args.count is not None and args.count < 0:
        return usage_error('Value {} for -N/--count must be non-negative'.format(args.count))

    # Can't both search and list
    if args.list and search_opt_given:
        opt, val = search_opt_given
        return usage_error('Search option {} = {} not compatible with -l/--list'.format(opt, val))

    # Can't search, list or build with delete
    if args.delete:
        if search_opt_given:
            opt, val = search_opt_given
            return usage_error('Search option {} = {} not compatible with -x/--delete'.format(opt, val))
        if args.list or args.build_only:
            return usage_error('Cannot mix -x/--delete with -l/--list or -b/--build_only')

    # Imply build_only if multiple files and no search options given
    build_only = args.build_only
    if nfile > 1:
        if search_opt_given:
            opt, val = search_opt_given
            return usage_error('Search option {} = {} not compatible with multiple files ({})'.format(opt, val, nfile))
        if not build_only:
            if args.verbose:
                _error('[Building indexes only since multiple files ({}) given]'.format(nfile))
            build_only = True

    # Check content search options
    if (args.greater_than or args.less_than) and args.snaplen and args.verbose:
        if args.greater_than and len(args.greater_than) > args.snaplen:
            _error('-G/--greater-than value "{}" longer than snaplen of {}'.format(args.greater_than, args.snaplen))
        if args.less_than and len(args.less_than) > args.snaplen:
            _error('-L/--less-than value "{}" longer than snaplen of {}'.format(args.less_than, args.snaplen))

    # Warn if inconsistent range and max count
    if args.verbose and all(optval is not None for optval in (args.start, args.end, args.count)):
        start_end_range = args.end - args.start + 1
        if args.count > start_end_range:
            _error('Warning: line count implied by --start and --end ({}) will override max --count {}'.format(start_end_range, args.count))
        elif args.count < start_end_range:
            _error('Warning: max --count {} will override line count implied by --start and --end ({})'.format(args.count, start_end_range))

    # Check search by line number or content, but not both
    if (args.start is not None or args.end is not None) and (args.greater_than is not None or args.less_than is not None):
        return usage_error('Cannot mix line number search -S/--start -E/--end with content search -G/--greater-than -L/--less-than')

    # Check index build options
    index_dir = args.index_dir if args.index_dir else DEFAULT_INDEX_DIR
    if index_dir != '.':
        if not os.path.isdir(index_dir):
            return usage_error('Index directory "{}" not found or not a directory')
        if args.fullname and args.index_dir:
            return usage_error('Cannot store derived-named indexes with -F/--fullname in directory "{}" ... only -D/--index-dir of "." (same as file) may be used.'.format(index_dir))
    if args.fullname and not args.index_dir:
        index_dir = '.'
        if args.verbose:
            _error('-F/--full-name given, presuming -D/--index-dir . (indexes in same directory as files)')
    if args.chunk_size <= 0:
        return usage_error('Chunk size -C/--chunk-size {} not a positive integer'.format(args.chunk_size))
    if args.snaplen is not None and args.snaplen < 0:
        return usage_error('Snap len -P/--snaplen {} not a positive integer'.format(args.snaplen))
    if args.snaplen is not None and args.snaplen >= args.chunk_size:
        return usage_error('Snap len -P/--snaplen {} must be less than -C/--chunk-size {}'.format(args.snaplen, args.chunk_size))

    # Check search range options are sensible and warn if not
    if not args.quiet:
        if args.count == 0:
            _error('Warning: -N/--count of 0 given ... no lines will be output.  Use -q/--quiet to suppress this message')
        if args.end is not None and args.start is not None and args.end < args.start:
            fmt = 'Warning: -E/--end {} precedes -S/--start {} ... no lines will be output.  Use -q/--quiet to suppress this message'
            _error(fmt.format(args.end, args.start))
        if args.greater_than is not None and args.less_than is not None and args.greater_than > args.less_than:
            fmt = 'Warning: -L/--less-than "{}" precedes -G/--greater-than "{}" ... no lines will be output.  Use -q/--quiet to suppress this message'
            _error(fmt.format(args.less_than, args.greater_than))

    ### PROCESS FILES

    if args.dry_run and not args.quiet:
        _error('DRY RUN MODE ... will not touch any files')

    idx_opts = IndexOptions(
        chunk_size=args.chunk_size,
        snaplen=args.snaplen,
        quiet=args.quiet,
        verbose=args.verbose,
        force=args.force,
        dryrun=args.dry_run,
        for_content_search=args.greater_than is not None or args.less_than is not None,
    )
    srch_opts = SearchOptions(
        output_file=args.output,
        start=args.start,
        end=args.end,
        greater_than=args.greater_than,
        less_than=args.less_than,
        count=args.count,
        line_number=args.line_number,
        verbose=args.verbose,
    )

    success = True
    for filename in args.files:
        # Get the index file to delete, build, list ...
        filename_full = os.path.realpath(filename)
        if not os.path.isfile(filename_full):
            return _error('Not a file: "{}"'.format(filename_full))
        index_filename = args.index_file
        if not index_filename:
            index_filename = get_index_filename(filename_full, index_dir, args.hidden, args.fullname)
            if not index_filename:
                return _error('Problems indexing "{}"'.format(filename))

        # Check delete
        if args.delete:
            if os.path.exists(index_filename):
                if args.dry_run:
                    action = 'Would delete'
                else:
                    os.unlink(index_filename)
                    action = 'Deleted'
                if not args.quiet:
                    _error('{} index "{}" on "{}" (use -q/--quiet to suppress this message)'.format(action, index_filename, filename_full))
            elif args.verbose:
                _error('Warning: index "{}" on "{}" not found for delete'.format(index_filename, filename_full))
            continue

        # List info only
        if args.list:
            try:
                info = get_index_info(filename_full, index_filename)
            except ValueError as exc:
                _error('ERROR: ' + str(exc))
                success = False
                break
            print_index_info(filename_full, index_filename, info, args.verbose)
            continue

        # Check or create the index
        i_success, i_result = index_file(filename_full, index_filename, idx_opts)
        if not i_success:
            _error('ERROR: ' + i_result)
            break
        _, file_lines, _, entries = i_result

        # Nothing to do if just indexing or dry run
        if build_only or args.dry_run:
            continue

        # Search the file for lines
        success = search_file(filename_full, index_filename, file_lines, entries, srch_opts)
        if not success:
            break

    return success


def search_file(filename_full, index_filename, file_lines, entries, opts):
    """
    Search file for lines and output
    """

    # Handle zero count case
    if opts.count == 0:
        return True

    # Check non-overlapping ranges
    if opts.start is not None and opts.end is not None and opts.start > opts.end:
        return True
    if opts.greater_than is not None and opts.less_than is not None and opts.greater_than > opts.less_than:
        return True

    # Check if start is beyond the end of data
    if opts.start is not None and opts.start > file_lines and opts.verbose:
        _error('Start line {:,d} > {:,d} lines in file "{}" ... nothing will be output'.format(opts.start, file_lines, filename_full))

    # Starting offset and current line
    line_start = 0
    lineno = 0

    # Seek to offset of start line number
    if opts.start is not None:
        for flds in entries:
            offset, lno = flds[:2]
            if opts.start <= lno:
                break
            line_start = offset
            lineno = lno

    # Seek to offset of start of content range
    greater_than = bytes(opts.greater_than, 'utf-8') if opts.greater_than is not None else None
    if greater_than is not None:
        ngreater = len(greater_than)
        for i, flds in enumerate(entries):
            if i < (len(entries) - 1) and len(flds) < 3:
                return _error('ERROR: -G/--greater-than given, but "{}" does not appear to have been indexed with -P/--snaplen'.format(index_filename))
            offset, lno = flds[:2]
            ncmp = ngreater if len(flds) < 3 else min(ngreater, len(flds[2]))
            if len(flds) < 3 or greater_than[:ncmp] <= flds[2][:ncmp]:
                break
            line_start = offset
            lineno = lno

    # Open output file, or use stdout
    close_fp = False
    if opts.output_file and opts.output_file != '-':
        try:
            out_fp = open(opts.output_file, 'wb')
            close_fp = True
        except OSError as exc:
            return _error('Cannot write output "{}": {}'.format(opts.output_file, exc))
    else:
        out_fp = sys.stdout.buffer

    less_than = bytes(opts.less_than, 'utf-8') if opts.less_than is not None else None

    # Read lines from file and copy to output
    noutput = 0
    try:
        with open(filename_full, 'rb') as src_fp:

            # Go to initial position
            if line_start:
                src_fp.seek(line_start)

            # Copy out lines until limit reached
            while True:

                # Truncate by end line
                if opts.end is not None and lineno >= opts.end:
                    break

                # Truncate based on count
                if opts.count is not None and noutput >= opts.count:
                    break

                # Read and copy out line
                line = src_fp.readline()
                if not line:
                    break

                # Truncate based on max content filter
                if less_than is not None and line[:len(less_than)] > less_than:
                    break

                lineno += 1

                # Skip if not yet reached start line
                if opts.start is not None and lineno < opts.start:
                    continue

                # Skip if not yet reached the min content filter
                if greater_than is not None and line < greater_than:
                    continue

                # Output line
                if opts.line_number:
                    out_fp.write(_s2b('{:,}: '.format(lineno)))
                out_fp.write(line)
                noutput += 1
    finally:
        if close_fp:
            out_fp.close()
    return True


def print_index_info(filename_full, index_filename, info, verbose):
    """
    Output info for file and its index
    """
    def out_size(num):
        "Format an int with commas"
        return '{:15,d}'.format(num)
    def out_tm(epoch):
        "Format an epoch timestamp readably"
        return time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(epoch))
    def out_line(prompt, line):
        "Format a line with prompt and value"
        _out(_s2b('{:<17s}{}\n'.format(prompt + ':', line)))
    out_line('Data file', filename_full)
    out_line('File modified', out_tm(info.file_mtime))
    out_line('Index file', index_filename)
    if info.index_mtime is not None:
        out_line('Index modified', out_tm(info.index_mtime))
    out_line('Index status', INDEX_STATUS_NAME[info.status])
    out_line('File size', out_size(info.file_size))
    if info.index_file_size is not None:
        out_line('Index file size', out_size(info.index_file_size))
    if info.chunk_size is not None:
        out_line('Index chunk size', out_size(info.chunk_size))
    if info.snaplen:
        out_line('Index snap len', out_size(info.snaplen))
    if info.file_lines is not None:
        out_line('File lines', out_size(info.file_lines))
    if info.status != INDEX_STATUS_FRESH:
        if info.last_file_size is not None:
            out_line('Previous size', out_size(info.last_file_size))
        if info.last_file_lines is not None:
            out_line('Previous lines', out_size(info.last_file_lines))
    if info.status in (INDEX_STATUS_ABSENT, INDEX_STATUS_INVALID):
        return
    nentry = len(info.entries)
    out_line('No. entries', out_size(nentry))
    if verbose:
        output_header = False
        for i, flds in enumerate(info.entries):
            filepos, lineno = flds[:2]
            if len(flds) == 3:
                frag = flds[2]
            else:
                frag = None

            if not output_header:
                hdr1 = ' Entry   File position     Line number'
                hdr2 = '------  --------------  --------------'
                if frag:
                    hdr1 += '  Content'
                    hdr2 += '  ----------'
                print(hdr1)
                print(hdr2)
                output_header = True
                sys.stdout.flush()

            _out(_s2b('{:6,d} {} {}'.format(i+1, out_size(filepos), out_size(lineno+1))))
            if frag:
                _out(b'  ')
                _out(frag)
            _out(b'\n')


def _error(msg):
    sys.stderr.write(msg)
    sys.stderr.write('\n')
    return False

def _out(data):
    try:
        sys.stdout.buffer.write(data)
        sys.stdout.flush()
    except BrokenPipeError:
        os._exit(1) # pylint: disable=protected-access

def get_index_filename(filename_full, index_dir, hidden, fullname):
    """
    Generate index filename given full ("real") path of source file name.
    Index dir is either a directory or if "." is same as filename.
    """
    if index_dir == '.':
        index_dir = os.path.dirname(filename_full)
    if fullname:
        index_base = os.path.basename(filename_full)
    else:
        index_base = INDEX_HASH_PREFIX + get_filename_hash(filename_full)
    if hidden:
        index_base = '.' + index_base
    if not os.path.isdir(index_dir):
        _error('Not a directory: "{}"'.format(index_dir))
        return None
    result = os.path.join(index_dir, index_base) + INDEX_SUFFIX
    return result


def get_filename_hash(filename):
    """
    Get hash of file to be indexed given full (real) path.
    """
    sha = hashlib.sha256()
    sha.update(filename.encode('utf-8'))
    return sha.hexdigest()

def _b2s(barr):
    "Decode UTF-8 bytes to string"
    return barr.decode('utf-8')

def _s2b(stringval):
    "Encode string as UTF-8 bytes"
    return stringval.encode('utf-8')

def _changed(old, new):
    "Return if value changed, coalescing false values"
    return (old and not new) or (not old and new) or (old and new and old != new)

if __name__ == '__main__':
    try:
        MAIN_SUCCESS = main()
    except KeyboardInterrupt:
        _error('\nAborted.\n')
        MAIN_SUCCESS = False
    sys.exit(0 if MAIN_SUCCESS else 1)
