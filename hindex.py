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

def index_file(filename, index_filename, chunk_size, snaplen, quiet, verbose, force, dryrun, for_content_search):
    """
    Index a file if needed.  Checks to see if already fully built, and if not will build it,
    For dry run, just shows what would be built.
    """

    # Get current index info
    status, file_size, file_lines, last_file_size, last_file_lines, file_mtime, _index_file_size, _index_mtime, last_chunk_size, last_snaplen, entries = get_index_info(filename, index_filename)
    exists = (status != INDEX_STATUS_ABSENT)

    # Report newly indexed file
    if not exists:
        if for_content_search and not snaplen:
            return False, 'Need to specify -P/--snaplen <snaplen> for new index when using -G/--greater-than or -L/--less-than'
        if not quiet:
            action = 'Would create' if dryrun else 'Creating'
            fmt = '{} new index "{}" on "{}" {:,d} bytes ... please wait ... (-q/--quiet to suppress)'
            _error(fmt.format(action, index_filename, filename, file_size))

    # Report status
    if verbose:
        if status == INDEX_STATUS_ABSENT:
            fmt = 'Index "{}" on "{}" not found'
            _error(fmt.format(index_filename, filename))
        elif status == INDEX_STATUS_FRESH:
            fmt = 'Index "{}" on "{}" is up to date, {:,d} bytes / {:,d} lines'
            _error(fmt.format(index_filename, filename, file_size, file_lines))
        elif status == INDEX_STATUS_INVALID:
            fmt = 'Index "{}" on "{}" made on larger or older file, resetting'
            _error(fmt.format(index_filename, filename))
        elif status == INDEX_STATUS_STALE:
            fmt = 'Index "{}" on "{}" was made on older file {:,d} bytes / {:,d} lines < current size {:,d} bytes ... appending index ...'
            _error(fmt.format(index_filename, filename, last_file_size, last_file_lines, file_size))

        if exists and last_chunk_size and _changed(last_chunk_size, chunk_size):
            fmt = 'Note: Chunk size for index "{}" on "{}" changed from {} to {}'
            _error(fmt.format(index_filename, filename, last_chunk_size, chunk_size))

        if exists and last_snaplen and _changed(last_snaplen, snaplen):
            fmt = 'Note: Snap len for index "{}" on "{}" changed from {} to {}'
            _error(fmt.format(index_filename, filename, last_snaplen, snaplen))

    # Reset entries if force-rebuild
    if force:
        entries = []
        if not quiet:
            fmt = 'Option -f/--force given, forcing rebuild of index "{}" on "{}" {:,d} bytes (-q/--quiet to suppress)'
            _error(fmt.format(index_filename, filename, file_size))
    elif status == INDEX_STATUS_FRESH:
        # Nothing to do if index is up to date
        return True, (file_size, file_lines, file_mtime, entries)

    # Write new or appended entries
    line_start = 0
    chunk_bytes_read = 0
    with open(filename, 'rb') as src_fp:
        line_start = 0
        lineno = 0
        if not force and entries:
            # Restore state from last indexing
            last_pos, lineno = entries[-1][:2]
            src_fp.seek(last_pos)
            line = src_fp.readline()
            linelen = len(line)
            line_start = last_pos + linelen
            chunk_bytes_read = linelen
            if linelen:
                lineno += 1
        # Scan file and enumerate entries
        tot_bytes_read = 0
        tot_bytes_to_read = file_size - line_start
        last_report_bytes = 0
        last_line = None
        while src_fp:
            if chunk_bytes_read and (chunk_bytes_read >= chunk_size):
                if snaplen:
                    frag = line[:snaplen].rstrip(b'\n')
                    entries.append((line_start, lineno, frag))
                else:
                    entries.append((line_start, lineno))
                chunk_bytes_read = 0
            line = src_fp.readline()
            if not line:
                break
            if snaplen and last_line is not None:
                # If snapping content (leading portion of lines) for search, the data must be in order.
                # Check sort order of leading portion being snapped (OK of stuff beyond is out of order in a "tie")
                line_cmp = line[:snaplen]
                last_line_cmp = last_line[:snaplen] # pylint: disable=unsubscriptable-object
                if line_cmp < last_line_cmp:
                    fmt = '-P/--snaplen = {} given and have unordered data in "{}"\nFirst {} chars of line {}:\n{}\nis less than that in previous line:\n{}'
                    return False, fmt.format(snaplen, filename, snaplen, lineno+1, line_cmp, last_line_cmp)
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
            if not quiet and last_report_bytes >= INDEX_PROGRESS_INTERVAL:
                fmt = 'Indexed {:5.1f}% = {:,d} / {:,d} bytes of "{}" (-q/--quiet to suppress)'
                _error(fmt.format(100.0 * float(tot_bytes_read)/tot_bytes_to_read, tot_bytes_read, tot_bytes_to_read, filename))
                last_report_bytes = 0

    # Add terminating entry: file size and total line count
    # Don't write if we hit EOF exactly on a chunk boundary.  This will
    # be evident by chunk_bytes_read of zero.  As a special case,
    # always write an entry for empty file.
    if chunk_bytes_read or not file_size:
        entries.append((line_start, lineno))

    # Show what would be done w/ index
    if dryrun:
        action = 'refresh' if exists else 'create'
        _error('Would {} index "{}" with {} entries\n'.format(action, index_filename, len(entries)))
        return True, (file_size, file_lines, file_mtime, entries)

    # Warn if file grew while were indexing it
    if line_start < file_size:
        fmt = 'File "{}" was originally {:,d} bytes but shrank to {:,d} while indexing it'
        return False, fmt.format(filename, file_size, line_start)
    if line_start > file_size:
        if verbose:
            fmt = 'Warning: File "{}" grew from {:,d} to at least {:,d} bytes while indexing it'
            _error(fmt.format(filename, file_size, line_start))
        # Store the updated amount of data indexed
        file_size = line_start

    # Write out file all at once
    with open(index_filename, 'wb') as index_fp:

        # Write two-line header: filename then (mtime, size, lines, chunk_size, snaplen, nentry)
        fmt = '{}\n{:.6f} {} {} {} {} {}\n'
        index_fp.write(_s2b(fmt.format(filename, file_mtime, file_size, lineno, chunk_size, snaplen or 0, len(entries))))
        # Write entries
        for flds in entries:
            pos, lno = flds[:2]
            index_fp.write(_s2b('{} {}'.format(pos, lno)))
            if len(flds) == 3:
                index_fp.write(b' ')
                index_fp.write(flds[2])
            index_fp.write(b'\n')
    if verbose:
        action = 'updated' if status == INDEX_STATUS_STALE else 'created'
        fmt = 'Index "{}" {} on "{}" {:,d} bytes / {:,d} lines'
        _error(fmt.format(index_filename, action, filename, line_start, lineno))

    return True, (file_size, lineno, file_mtime, entries)


def get_index_info(filename, index_filename):
    """
    Get status info from index file.
    """
    # Result data
    defaults = INDEX_STATUS_ABSENT, None, None, None, None, None, None, None, None, None, []
    status, file_size, file_lines, last_file_size, last_file_lines, file_mtime, index_file_size, index_mtime, chunk_size, snaplen, entries = defaults

    # Get source file attributes
    file_size = os.path.getsize(filename)
    file_mtime = os.path.getmtime(filename)

    # Check nonexistent index
    if not os.path.exists(index_filename):
        return status, file_size, file_lines, last_file_size, last_file_lines, file_mtime, index_file_size, index_mtime, chunk_size, snaplen, entries

    # Index exists, get entries
    index_file_size = os.path.getsize(index_filename)
    index_mtime = os.path.getmtime(index_filename)
    with open(index_filename, 'rb') as index_fp:
        # Read header lines (2).  First is filename, then (mtime, size, lines, chunk_size, snaplen, nentry)
        h_filename = _b2s(index_fp.readline()).strip()
        assert h_filename == filename, 'Name mismatch: index "{}" has "{}" for file "{}"'.format(index_filename, h_filename, filename)
        h_mslcse_flds = _b2s(index_fp.readline()).strip().split()
        _h_file_mtime = float(h_mslcse_flds[0])
        _h_file_size, _h_file_lines, chunk_size, snaplen, h_nentry = [int(x) for x in h_mslcse_flds[1:]]

        # Read entries
        for line in index_fp:
            line = line.rstrip(b'\n')

            # Extract file pos
            idx = line.find(b' ')
            assert idx > 0, 'No file pos in line "{}" in "{}"'.format(line, index_filename)
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
                entries.append((filepos, lineno))
            else:
                assert len(frag) <= snaplen, 'Index "{}" header had snaplen of {} but see snap of length {}'.format(index_filename, snaplen, len(frag))
                entries.append((filepos, lineno, frag))

        assert len(entries) == h_nentry, 'Index "{}" had {} entries in header but {} in index itself'.format(index_filename, len(entries), h_nentry)

    # File exists, check if stale due to file replaced or grew
    if entries:
        last_file_size, last_file_lines = entries[-1][:2]
        if last_file_size == file_size:
            # File was fully indexed, at size given by last_file_size
            status = INDEX_STATUS_FRESH
            file_lines = last_file_lines
        elif file_size < last_file_size or file_mtime < index_mtime:
            # Current file size smaller than last indexed or index out of date - must have been replaced
            status = INDEX_STATUS_INVALID
            entries = []
        else:
            status = INDEX_STATUS_STALE
            # Lop off last entry w/ file size and total line count
            entries = entries[0:-1]

    return status, file_size, file_lines, last_file_size, last_file_lines, file_mtime, index_file_size, index_mtime, chunk_size, snaplen, entries


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
        return usage_error('Snap len -S/--snaplen {} not a positive integer'.format(args.snaplen))
    if args.snaplen is not None and args.snaplen >= args.chunk_size:
        return usage_error('Snap len -S/--snaplen {} must be less than -C/--chunk-size {}'.format(args.snaplen, args.chunk_size))

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
            status, file_size, file_lines, last_file_size, last_file_lines, file_mtime, index_file_size, index_mtime, chunk_size, snaplen, entries = get_index_info(filename_full, index_filename)
            print_index_info(filename_full, index_filename, status, file_size, file_lines, last_file_size, last_file_lines, file_mtime, index_file_size, index_mtime, chunk_size, snaplen, entries, args.verbose)
            continue

        # Check or create the index
        for_content_search = args.greater_than is not None or args.less_than is not None
        i_success, i_result = index_file(filename_full, index_filename, args.chunk_size, args.snaplen, args.quiet, args.verbose, args.force, args.dry_run, for_content_search)
        if not i_success:
            _error('ERROR: ' + i_result)
            break
        file_size, file_lines, file_mtime, entries = i_result

        # Nothing to do if just indexing or dry run
        if build_only or args.dry_run:
            continue

        # Search the file for lines
        success = search_file(filename_full, index_filename, args.output, file_lines, entries,
                              args.start, args.end, args.greater_than, args.less_than, args.count,
                              args.line_number, args.verbose)
        if not success:
            break

    return success


def search_file(filename_full, index_filename, output_file, file_lines, entries,
                start, end, greater_than, less_than, count,
                line_number, verbose):
    """
    Search file for lines and output
    """

    # Handle zero count case
    if count == 0:
        return True

    # Check non-overlapping ranges
    if start is not None and end is not None and start > end:
        return True
    if greater_than is not None and less_than is not None and greater_than > less_than:
        return True

    # Check if start is beyond the end of data
    if start is not None and start > file_lines and verbose:
        _error('Start line {:,d} > {:,d} lines in file "{}" ... nothing will be output'.format(start, file_lines, filename_full))

    # Starting offset and current line
    line_start = 0
    lineno = 0

    # Seek to offset of start line number
    if start is not None:
        for flds in entries:
            offset, lno = flds[:2]
            if start <= lno:
                break
            line_start = offset
            lineno = lno

    # Seek to offset of start of content range
    greater_than = bytes(greater_than, 'utf8') if greater_than is not None else None
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

    # Read lines from file
    noutput = 0
    out_fp = None
    if output_file and output_file != '-':
        try:
            out_fp = open(output_file, 'wb')
        except OSError as exc:
            return _error('Cannot write output "{}": {}'.format(output_file, exc))
    else:
        out_fp = sys.stdout.buffer
    with open(filename_full, 'rb') as src_fp:

        # Go to initial position
        if line_start:
            src_fp.seek(line_start)

        less_than = bytes(less_than, 'utf8') if less_than is not None else None

        # Copy out lines until limit reached
        while True:

            # Truncate by end line
            if end is not None and lineno >= end:
                break

            # Truncate based on count
            if count is not None and noutput >= count:
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
            if start is not None and lineno < start:
                continue

            # Skip if not yet reached the min content filter
            if greater_than is not None and line < greater_than:
                continue

            # Output line
            if line_number:
                out_fp.write(_s2b('{:,}: '.format(lineno)))
            out_fp.write(line)
            noutput += 1
    out_fp.close()
    return True


def print_index_info(filename_full, index_filename, status, file_size, file_lines, last_file_size, last_file_lines, file_mtime, index_file_size, index_mtime, chunk_size, snaplen, entries, verbose):
    """
    Ouptut info for file and its index
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
    out_line('File modified', out_tm(file_mtime))
    out_line('Index file', index_filename)
    if index_mtime is not None:
        out_line('Index modified', out_tm(index_mtime))
    out_line('Index status', INDEX_STATUS_NAME[status])
    out_line('File size', out_size(file_size))
    if index_file_size is not None:
        out_line('Index file size', out_size(index_file_size))
    if chunk_size is not None:
        out_line('Index chunk size', out_size(chunk_size))
    if snaplen:
        out_line('Index snap len', out_size(snaplen))
    if file_lines is not None:
        out_line('File lines', out_size(file_lines))
    if status != INDEX_STATUS_FRESH:
        if last_file_size is not None:
            out_line('Previous size', out_size(last_file_size))
        if last_file_lines is not None:
            out_line('Previous lines', out_size(last_file_lines))
    if status in (INDEX_STATUS_ABSENT, INDEX_STATUS_INVALID):
        return
    nentry = len(entries)
    out_line('No. entries', out_size(nentry))
    if verbose:
        output_header = False
        for i, flds in enumerate(entries):
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

            _out(bytes('{:6,d} {} {}'.format(i+1, out_size(filepos), out_size(lineno+1)), 'ascii'))
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
    Generate index filename give full ("real") path of source file name
    Index dir is either a directory or if "." is same as filename
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
        return  None
    result = os.path.join(index_dir, index_base) + INDEX_SUFFIX
    return result


def get_filename_hash(filename):
    """
    Get hash of file to be indexed given full (real) path.
    """
    sha = hashlib.sha1()
    sha.update(filename.encode('utf-8'))
    return sha.hexdigest()

def _b2s(barr):
    "Decode ASCII bytes to string"
    return barr.decode('ascii')

def _s2b(stringval):
    "Encode ASCII string as bytes"
    return bytes(stringval, 'ascii')

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
