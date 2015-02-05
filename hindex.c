/*

hindex.c - a Huge file INDEXer

*/

#include "hindex.h"

void _break() {}

int _min_of(int a, int b) {
  return a < b ? a : b;
}

/* Read line from a file, optionally capturing leading fragment.
   Return pointer to static buffer with full line (including newline).
   Store line length, including newline in *nread_p.

   If non-null, pointer arg frag is assumed to be allocated to
   accommodate up to snaplen bytes plus a terminating NUL, and will be
   populated with leading line fragment of up to snaplen bytes (never
   including the newline).

   FILE pointer fp must be "seekable" backward (i.e., cannot be stdin)
*/
static unsigned char * _full_buff = 0;
static int _full_bufflen = 512 * 1024;
unsigned char * _read_line(FILE * fp, long snaplen, unsigned char * frag, long * nread_p) {

  /* Init buffer */
  if ( ! _full_buff )
    _full_buff = malloc(_full_bufflen * sizeof *_full_buff);
  _full_buff[0] = '\0';

  /* Init output fragment */
  if ( frag )
    *frag = '\0';

  long nread = 0;
  while ( true ) {
    char * data_read = fgets(_full_buff, _full_bufflen, fp);

    /* EOF */
    if ( ! data_read ) {
      *nread_p = nread;
      _full_buff[nread] = '\0';
      return _full_buff;
    }

    nread = strlen(data_read);
    if ( nread < _full_bufflen - 1 )
      break;

    /* Grow buffer */
    _full_bufflen *= 2;
    _full_buff = realloc(_full_buff, _full_bufflen);

    /* Back up and re-read */
    fseek(fp, -nread, SEEK_CUR);
  }

  /* Capture leading fragment */
  if ( frag ) {
    int to_copy = _min_of(snaplen, nread);
    if ( _full_buff[to_copy-1] == '\n' )
      to_copy--;
    memcpy(frag, _full_buff, to_copy);
    frag[to_copy] = '\0';
  }

  *nread_p = nread;
  _full_buff[nread] = '\0';
  return _full_buff;
}

/* Output to sdtderr and return false */
bool _error(char * s) {
  fprintf(stderr, "%s\n", s);
  return false;
}

/* Output to stdout */
void _out(char * s) {
  printf("%s\n", s);
}

/* Strip NL from string */
void _strip_nl(char *s) {
  if ( !s  || !*s )
    return;
  int len = strlen(s);
  if ( len && s[len-1] == '\n' )
    s[len-1] = '\0';
}

/* Format long int w/ commas. Pad to given len or if 0, shrink to fit */
char * _out_size(long long n, int len) {
  static char result[BUFSIZE];
  char buf[BUFSIZE];

  sprintf(buf, "%lld", n);
  int ndig = strlen(buf);
  int ncommas = (ndig - 1)/3;
  int len_needed = ndig + ncommas;
  if (!len || len < len_needed)
    len = len_needed;

  int src = ndig-1, dst = len-1;
  int ncopy = 0;
  result[len] = '\0';
  while ( src >= 0 ) {
    if (ncopy > 0 && (ncopy % 3) == 0) {
      result[dst] = ',';
      dst--;
    }
    result[dst] = buf[src];
    src--;
    dst--;
    ncopy++;
  }
  while (dst >= 0) {
    result[dst] = ' ';
    dst--;
  }
  return result;
}

/* Format double epoch */
char * _out_tm(long double tm) {
  static char result[BUFSIZE];
  char tmpbuf[BUFSIZE];
  time_t secs = floorl(tm);
  struct tm * ltime = localtime(&secs);
  strftime(result, 80, "%Y-%m-%d %H:%M:%S %Z", ltime);
  /* Omit nanos for now */
  /*
  long nanos = (tm - secs) * 1000000000.0;
  sprintf(tmpbuf, ".%09ld", nanos);
  strcat(result, tmpbuf);
  strftime(tmpbuf, 80, " %Z", ltime);
  strcat(result, tmpbuf);
  */
  return result;
}
/* Put line to stdout */
void _out_line(char * prompt, char * line) {
  char p[BUFSIZE];
  strcpy(p, prompt);
  strcat(p, ":");
  printf("%-17s%s\n", p, line);
}

/* Get file size and mtime w/  nanos */
void _get_file_size_mtime(char *fn, long long * size, long double * mtime) {
  struct stat statinfo;
  if (stat(fn, &statinfo))
    return;
  if (size)
    *size = statinfo.st_size;
  if (mtime) {
    long double d = statinfo.st_mtime;
    d += (long double) statinfo.st_mtim.tv_nsec / 1000000000;
    *mtime = d;
  }
}

/* Convert string to long long */
long long _convert_ll(char * s, bool * valid) {
  char errc_c[1] = "";
  char * errc = errc_c;
  long long result = strtoll(s, &errc, 10);
  if (! *s || *errc || errno == EINVAL || errno == ERANGE)
    *valid = false;
  else
    *valid = true;
  return result;
}

/* Print usage w/ optional error and return false for exit(1) */
int usage_error(char * msg) {
  if (msg && *msg)
    fprintf(stderr, "%s\n", msg);
  fprintf(stderr, "%s", USAGE);
  return false;
}

/* Get a hash for a file name given full (real) path */
char * get_filename_hash(char * fn) {
  static char result[BUFSIZE];
  size_t len = strlen(fn);
  unsigned char hash[SHA_DIGEST_LENGTH];
  SHA1(fn, len, hash);
  int i;
  for (i=0; i < SHA_DIGEST_LENGTH; i++) {
    char hdig[3];
    sprintf(hdig, "%02x", hash[i]);
    result[i*2] = hdig[0];
    result[i*2 + 1] = hdig[1];
  }
  result[i*2] = '\0';
  return result;
}

/* Generate index filename give full ("real") path of source file name
   Index dir is either a directory or if "." is same as filename
 */
char * get_index_filename(char * filename_full, char * index_dir, bool hidden, bool fullname) {
  /* From the man page:
     Both dirname() and basename() may modify the contents of path, so it may be desirable to pass a copy when calling one of these functions.
  */
  char filename_full_copy_1[BUFSIZE], filename_full_copy_2[BUFSIZE], i_base[BUFSIZE], i_dir[BUFSIZE], result[BUFSIZE];
  struct stat statinfo;
  strcpy(filename_full_copy_1, filename_full);
  strcpy(filename_full_copy_2, filename_full);
  if (0 == strcmp(index_dir, "."))
    strcpy(i_dir, dirname(filename_full_copy_1));
  else
    strcpy(i_dir, index_dir);
  if (fullname)
    strcpy(i_base, basename(filename_full_copy_2));
  else
    sprintf(i_base, "%s%s", INDEX_HASH_PREFIX, get_filename_hash(filename_full));

  char * hidden_prefix = hidden ? "." : "";

  if (stat(i_dir, &statinfo) || !S_ISDIR(statinfo.st_mode)) {
    sprintf(result, "Not a directory: \"%s\"", index_dir);
    _error(result);
    return 0;
  }
  sprintf(result, "%s/%s%s%s", i_dir, hidden_prefix, i_base, INDEX_SUFFIX);
  return strdup(result);
}

/* Init a new entry */
void init_entry(struct entry * s) {
  s->filepos = -1;
  s->lineno = -1;
  s->frag = 0;
};

/* Reset a single entry */
void reset_entry(struct entry * s) {
  s->filepos = -1;
  s->lineno = -1;
  if ( s->frag )
    free(s->frag);
  s->frag = 0;
};

/* Clear out all entries */
void _reset_entries(struct hindex *idx) {
  int i;
  for ( i=0; i < idx->nentry; i++ )
    reset_entry(idx->entries + i);
  if ( idx->entries ) {
    free(idx->entries);
    idx->entries = 0;
  }
  idx->nentry = idx->maxentry = 0;
}

/* Append index entry, growing array */
void _append_index_entry(struct hindex * idx, long long filepos, long long lineno, unsigned char *frag) {
  /* Check have room */
  if ( ! idx->maxentry ) {
    /* Init first entry */
    idx->entries = malloc(DEFAULT_INDEX_ENTRY_ALLOC * sizeof(struct entry));
    idx->maxentry = DEFAULT_INDEX_ENTRY_ALLOC;
    int i;
    for ( i=0; i < idx->maxentry; i++ )
      init_entry(idx->entries + i);
  }
  else if ( idx->nentry == idx->maxentry ) {
    /* At limit, grow by double */
    idx->maxentry *= 2;
    struct entry * new_ents = malloc(idx->maxentry * sizeof(struct entry));
    int i;
    for ( i=0; i < idx->nentry; i++ )
      new_ents[i] = idx->entries[i];
    for ( i=idx->nentry; i < idx->maxentry; i++ )
      init_entry(new_ents + i);
    free(idx->entries);
    idx->entries = new_ents;
  }
  idx->entries[idx->nentry] = (struct entry) { filepos, lineno, frag };
  idx->nentry++;
}

/* Init index structure */
void init_hindex(struct hindex * s) {
  s->filename_full   = 0;
  s->index_filename  = 0;
  s->status          = INDEX_STATUS_ABSENT;
  s->file_size       = -1;
  s->file_lines      = -1;
  s->last_file_size  = -1;
  s->last_file_lines = -1;
  s->file_mtime      = 0;
  s->index_file_size = -1;
  s->index_mtime     = 0;
  s->chunk_size      = DEFAULT_CHUNK_SIZE;
  s->snaplen         = 0;
  s->nentry          = 0;
  s->maxentry        = 0;
  s->entries         = 0;
}

/* Load info from index file */
bool get_index_info(char * filename_full, char * index_filename, struct hindex * idx) {
  char line[BUFSIZE], buf[BUFSIZE];
  init_hindex(idx);
  idx->filename_full = filename_full;
  idx->index_filename = index_filename;
  _get_file_size_mtime(filename_full, &idx->file_size,  &idx->file_mtime);

  /* Check nonexistent index */
  if  (access(index_filename, F_OK) != 0)
    return true;

  /* Load index file info */
  _get_file_size_mtime(index_filename, &idx->index_file_size,  &idx->index_mtime);
  FILE * fp = fopen(index_filename, "r");
  if ( ! fp ) {
    sprintf(buf, "Cannot read index file \"%s\":", index_filename);
    _error(buf);
    return _error(strerror(errno));
  }

  /*
    Read header lines (2).  First is filename, then (mtime, size, lines, chunk_size, snaplen, nentry)
    /u/hin/dev/hindex/grow.txt
    1712854689.4860396 5330 84 100 20 39
    147 2 Line          2 xxxx
    247 4 Line          4 xxxx
  */

  /* Read filename from header */
  char * h_filename = fgets(line, BUFSIZE, fp);
  if ( ! h_filename ) {
    fclose(fp);
    sprintf(buf, "ERROR: Got EOF reading filename on index file \"%s\"", filename_full);
    return _error(buf);
  }
  _strip_nl(h_filename);
  if ( 0 != strcmp(filename_full, h_filename) ) {
    fclose(fp);
    sprintf(buf, "ERROR: Name mismatch: index \"%s\" has \"%s\" for file \"%s\"", index_filename, h_filename, filename_full);
    return _error(buf);
  }

  /* Read (mtime, size, lines, chunk_size, snaplen, nentry) from header */
  char * h_mslcse_flds = fgets(line, BUFSIZE, fp);
  if ( ! h_mslcse_flds ) {
    fclose(fp);
    sprintf(buf, "ERRROR: Got EOF reading stats on index file \"%s\"", filename_full);
    return _error(buf);
  }
  _strip_nl(h_mslcse_flds);

  int nentry_expected = 0;
  long long _hdr_file_size; /* Ignored */
  long long _hdr_file_lines; /* Ignored */
  long double _hdr_file_mtime; /* Ignored */
  int nparse = sscanf(h_mslcse_flds, "%Lf %lld %lld %ld %ld %d", &_hdr_file_mtime, &_hdr_file_size, &_hdr_file_lines, &idx->chunk_size, &idx->snaplen, &nentry_expected);
  if ( nparse != 6 ) {
    fclose(fp);
    sprintf(buf, "ERROR: Line not of form (mtime, size, lines, chunk_size, snaplen, nentry) in \"%s\":\n%s\n", index_filename, h_mslcse_flds);
    return _error(buf);
  }

  /* Read index entries */
  int nread = 0;
  while ( nread < nentry_expected ) {
    int i_bufsize = BUFSIZE + idx->snaplen;
    char ibuf[i_bufsize];
    char * i_line = fgets(ibuf, i_bufsize, fp);
    if ( ! i_line ) {
      fclose(fp);
      sprintf(buf, "ERROR: EOF after %d lines(s) in \"%s\"\n", nread, index_filename);
      return _error(buf);
    }
    _strip_nl(h_mslcse_flds);

    /* Get file offset and line no */
    long long e_filepos = -1, e_lineno = -1;
    char snapbuf[idx->snaplen + 1];
    nparse = sscanf(i_line, "%lld %lld", &e_filepos, &e_lineno);
    if ( nparse != 2 ) {
      fclose(fp);
      sprintf(buf, "ERROR: Index line %d not of form (offset, lineno, ...) in \"%s\":%s\n", nread+1, index_filename, i_line);
      return _error(buf);
    }
    /* Get line fragment */
    unsigned char * e_frag = 0;
    if ( idx->snaplen > 0 ) {
      unsigned char * frag = strchr(i_line, ' ');
      if ( frag ) {
        frag++;
        frag = strchr(frag, ' ');
      }
      if ( frag ) {
        frag++;
        _strip_nl(frag);
        e_frag = strdup(frag);
      }
    }

    /* Append entry */
    _append_index_entry(idx, e_filepos, e_lineno, e_frag);
    nread++;
  }
  fclose(fp);
  fp = 0;

  if ( idx->nentry != nentry_expected ) {
    sprintf(buf, "ERROR: Expected %d entries, read %d in \"%s\"\n", nentry_expected, idx->nentry, index_filename);
    return _error(buf);
  }

  /* File exists, check if stale due to file replaced or grew */
  if ( idx->nentry ) {
    struct entry last_ent = idx->entries[idx->nentry - 1];
    idx->last_file_size = last_ent.filepos;
    idx->last_file_lines = last_ent.lineno; 
    if ( last_ent.filepos == idx->file_size ) {
      /* File was fully indexed, at size given by last_ent.filepos */
      idx->status = INDEX_STATUS_FRESH;
      idx->file_lines = last_ent.lineno;
    }
    else if ( idx->file_size < last_ent.filepos || idx->file_mtime < idx->index_mtime ) {
      /* Current file size smaller than last indexed or index out of date - must have been replaced */
      idx->status = INDEX_STATUS_INVALID;
      _reset_entries(idx);
    }
    else {
      idx->status = INDEX_STATUS_STALE;
      /* Lop off last entry w/ file size and total line count */
      reset_entry(&last_ent);
      idx->nentry--;
    }
  }

  return true;
}

/* Check, build or freshen an index file */
bool index_file(struct hindex * idx, char * filename, char * index_filename, long chunk_size, long snaplen, bool quiet, bool verbose, bool force, bool dryrun, bool for_content_search) {
  char buf[BUFSIZE], bytes_disp[BUFSIZE], last_bytes_disp[BUFSIZE], lines_disp[BUFSIZE];

  /* Get current index info */
  bool success = get_index_info(filename, index_filename, idx);
  if ( !success )
    return false;
  bool exists = idx->status != INDEX_STATUS_ABSENT;

  /* Report newly indexed file */
  if ( !exists ) {
    if ( for_content_search  && ! snaplen )
      return _error("ERROR: Need to specify -P <snaplen> for new index when using -G or -L");
    if ( !quiet ) {
      char * action = dryrun ? "Would create" : "Creating";
      sprintf(buf, "%s new index \"%s\" on \"%s\" %s bytes .. please wait ... (-q to suppress)", action, index_filename, filename, _out_size(idx->file_size, 0));
      _error(buf);
    }
  }

  /* Report status */
  if (verbose) {
    if (idx->status == INDEX_STATUS_ABSENT) {
      sprintf(buf, "Index \"%s\" on \"%s\" not found", index_filename, filename);
      _error(buf);
    }
    else if (idx->status == INDEX_STATUS_FRESH) {
      strcpy(bytes_disp, _out_size(idx->file_size, 0));
      strcpy(lines_disp, _out_size(idx->file_lines, 0));
      sprintf(buf, "Index \"%s\" on \"%s\" is up to date, %s bytes / %s lines", index_filename, filename, bytes_disp, lines_disp);
      _error(buf);
    }
    else if (idx->status == INDEX_STATUS_INVALID) {
      sprintf(buf, "Index \"%s\" on \"%s\" made on larger or older file, resetting", index_filename, filename);
      _error(buf);
    }
    else if (idx->status == INDEX_STATUS_STALE) {
      strcpy(bytes_disp, _out_size(idx->file_size, 0));
      strcpy(last_bytes_disp, _out_size(idx->file_size, 0));
      strcpy(lines_disp, _out_size(idx->file_lines, 0));
      sprintf(buf, "Index \"%s\" on \"%s\" was made on older file %s bytes / %s lines < current size %s bytes ... appending index ...", index_filename, filename, last_bytes_disp, lines_disp, bytes_disp);
      _error(buf);
    }
    if (exists && idx->chunk_size && idx->chunk_size != chunk_size) {
      sprintf(buf, "Note: Chunk size for index \"%s\" on \"%s\" changed from %ld to %ld", index_filename, filename, idx->chunk_size, chunk_size);
      _error(buf);
    }
    if (exists && idx->snaplen && idx->snaplen != snaplen) {
      sprintf(buf, "Note: Snap len for index \"%s\" on \"%s\" changed from %ld to %ld", index_filename, filename, idx->snaplen, snaplen);
      _error(buf);
    }
  }

  idx->snaplen = snaplen;

  /* Reset entries if force-rebuild */
  if ( force ) {
    _reset_entries(idx);
    if ( ! quiet ) {
      sprintf(buf, "Option -f given, forcing rebuild of index \"%s\" on \"%s\" %s bytes (-q to suppress)", index_filename, filename, _out_size(idx->file_size, 0));
      _error(buf);
    }
  }
  else if ( idx->status == INDEX_STATUS_FRESH )
    /* Nothing to do if index is up to date */
    return true;

  /* Write new or appended entries */
  FILE * src_fp = fopen(filename, "rb");
  if ( ! src_fp ) {
    sprintf(buf, "ERROR: Cannot read data file \"%s\":", filename);
    _error(buf);
    return _error(strerror(errno));
  }

  long long line_start = 0;
  long long chunk_bytes_read = 0;
  long long lineno = 0;
  unsigned char * frag = snaplen ? malloc((snaplen + 1) * sizeof *frag) : 0;
  if ( ! force && idx->nentry ) {
    /* Restore state from last indexing */
    long long last_pos = idx->entries[idx->nentry-1].filepos;
    lineno = idx->entries[idx->nentry-1].lineno;
    int seek_error = fseek(src_fp, last_pos, SEEK_SET);
    if ( seek_error ) {
      sprintf(buf, "ERROR: Error seeking to position %lld of line %lld in file \"%s\":", last_pos, lineno, filename);
      _error(buf);
      _error(strerror(errno));
      fclose(src_fp);
      return false;
    }

    long linelen = 0;
    _read_line(src_fp, snaplen, frag, &linelen);
    line_start = last_pos + linelen;
    chunk_bytes_read = linelen;
    if ( linelen )
      lineno += 1;
  }

  /* Scan file and enumerate entries */
  long long tot_bytes_read = 0;
  long long tot_bytes_to_read = idx->file_size - line_start;
  long long last_report_bytes = 0;
  unsigned char * last_line = 0;

  while ( true ) {
    if ( chunk_bytes_read && (chunk_bytes_read >= chunk_size) ) {
      _append_index_entry(idx, line_start, lineno, frag);
      chunk_bytes_read = 0;
    }
    frag = snaplen ? malloc((snaplen + 1) * sizeof *frag) : 0;
    long nread = 0;
    _read_line(src_fp, snaplen, frag, &nread);
    if ( ! nread )
      break;
    if ( snaplen && last_line ) {
      /* If snapping content (leading portion of lines) for search, the data must be in order.
         Check sort order of leading portion being snapped (OK of stuff beyond is out of order in a "tie")
      */
      if ( strcmp(frag, last_line) < 0 ) {
        sprintf(buf, "ERROR: -P/--snaplen = %ld given and have unordered data in \"%s\"\nFirst %ld chars of line %lld:\n%s\nis less than that in previous line:\n%s\n",
                snaplen, filename, snaplen, lineno+1, frag, last_line);
        _error(buf);
        fclose(src_fp);
        return false;
      }
    }
    last_line = frag;

    long long next_line_start = line_start + nread;
    long long bytes_read = next_line_start - line_start;
    if ( bytes_read <= 0 )
      break;
    chunk_bytes_read += bytes_read;

    tot_bytes_read += bytes_read;
    last_report_bytes += bytes_read;
    line_start = next_line_start;
    lineno += 1;
    if ( ! quiet && last_report_bytes >= INDEX_PROGRESS_INTERVAL ) {
      strcpy(bytes_disp, _out_size(tot_bytes_read, 0));
      strcpy(last_bytes_disp, _out_size(tot_bytes_to_read, 0));
      sprintf(buf, "Indexed %5.1f%% = %s / %s bytes of \"%s\" (-q/--quiet to suppress)",
              100.0 * tot_bytes_read / tot_bytes_to_read, bytes_disp, last_bytes_disp, filename);
      _error(buf);
      last_report_bytes = 0;
    }
  }
  fclose(src_fp);
  src_fp = 0;

  /* Add terminating entry: file size and total line count.
     Don't write if we hit EOF exactly on a chunk boundary.  This will
     be evident by chunk_bytes_read of zero.  As a special case,
     always write an entry for empty file.
  */
  if ( chunk_bytes_read || ! idx->file_size )
    _append_index_entry(idx, line_start, lineno, 0);
  idx->file_lines = lineno;

  /* Show what would be done w/ index */
  if ( dryrun ) {
    char * action = exists ? "refresh" : "create";
    sprintf(buf, "Would %s index \"%s\" with %d entries\n", action, idx->index_filename, idx->nentry);
    return _error(buf);
  }

  /* Warn if file grew while were indexing it */
  if ( line_start < idx->file_size ) {
    strcpy(bytes_disp, _out_size(idx->file_size, 0));
    strcpy(last_bytes_disp, _out_size(line_start, 0));
    sprintf(buf, "ERROR: File \"%s\" was originally %s bytes but shrank to %s while indexing it", filename, bytes_disp, last_bytes_disp);
    return _error(buf);
  }
  if ( line_start > idx->file_size ) {
    if ( verbose ) {
      strcpy(bytes_disp, _out_size(idx->file_size, 0));
      strcpy(last_bytes_disp, _out_size(line_start, 0));
      sprintf(buf, "Warning: File \"%s\" grew from %s to at least %s bytes while indexing it", filename, bytes_disp, last_bytes_disp);
      _error(buf);
    }
  }
  /* Store the updated amount of data indexed */
  idx->file_size = line_start;

  /* Write out file all at once */
  FILE * idx_fp = fopen(index_filename, "wb");
  if ( ! idx_fp ) {
    sprintf(buf, "ERROR: Cannot write index file \"%s\":", index_filename);
    _error(buf);
    return _error(strerror(errno));
  }

  /* Write two-line header: filename then (mtime, size, lines, chunk_size, snaplen, nentry) */
  fprintf(idx_fp, "%s\n%.6Lf %lld %lld %ld %ld %d\n", filename, idx->file_mtime, idx->file_size, lineno, chunk_size, snaplen, idx->nentry);

  /* Write entries */
  int i;
  for ( i = 0; i < idx->nentry; i++ ) {
    struct entry ent = idx->entries[i];
    fprintf(idx_fp, "%lld %lld", ent.filepos, ent.lineno);
    if ( ent.frag )
      fprintf(idx_fp, " %s", ent.frag);
    fprintf(idx_fp, "\n");
  }

  if ( verbose ) {
    char * action = idx->status == INDEX_STATUS_STALE ? "updated" : "created";
    strcpy(bytes_disp, _out_size(line_start, 0));
    strcpy(lines_disp, _out_size(lineno, 0));
    sprintf(buf, "Index \"%s\" %s on \"%s\" %s bytes / %s lines", index_filename, action, filename, bytes_disp, lines_disp);
    _error(buf);
  }

  fclose(idx_fp);

  /* Update index fields fields */
  _get_file_size_mtime(index_filename, &idx->index_file_size,  &idx->index_mtime);
  idx->status = INDEX_STATUS_FRESH;

  return true;
}

/* Search the file for lines */
bool search_file(struct hindex * idx, char * output_file, long long start, long long end, unsigned char * greater_than, unsigned char * less_than, long long count, bool line_number, bool verbose) {

  char buf[BUFSIZE], buf2[BUFSIZE], buf3[BUFSIZE];

  /* Handle zero count case */
  if ( count == 0 )
    return true;

  /* Check non-overlapping ranges */
  if ( start > 0 && end > 0 && start > end )
    return true;
  if ( greater_than && less_than && strcmp(greater_than, less_than) > 0 )
    return true;

  /* Check if start is beyond the end of data */
  if ( start > 0 && start > idx->file_lines && verbose ) {
    strcpy(buf2, _out_size(start, 0));
    strcpy(buf3, _out_size(idx->file_lines, 0));
    sprintf(buf, "Start line %s > %s lines in file \"%s\" ... nothing will be output", buf2, buf3, idx->filename_full);
    _error(buf);
  }

  /* Starting offset and current line */
  long long line_start = 0;
  long long lineno = 0;

  /* Seek to offset of start line number */
  if ( start > 0 ) {
    int i;
    for ( i=0; i < idx->nentry; i++ ) {
      struct entry ent = idx->entries[i];
      if ( start <= ent.lineno )
        break;
      line_start = ent.filepos;
      lineno = ent.lineno;
    }
  }

  /* Seek to offset of start of content range */
  if ( greater_than ) {
    int ngreater = strlen(greater_than);
    int i;
    for ( i=0; i < idx->nentry; i++ ) {
      struct entry ent = idx->entries[i];
      if ( i < (idx->nentry - 1) && ! ent.frag ) {
        sprintf(buf, "ERROR: -G/--greater-than given, but \"%s\" does not appear to have been indexed with -P/--snaplen", idx->index_filename);
        return _error(buf);
      }
      int ncmp = ent.frag ? _min_of(ngreater, strlen(ent.frag)) : ngreater;
      if ( ! ent.frag || strncmp(greater_than, ent.frag, ncmp) <= 0 )
        break;
      line_start = ent.filepos;
      lineno = ent.lineno;
    }
  }

  /* Open source for read */
  FILE * src_fp = fopen(idx->filename_full, "rb");
  if ( ! src_fp ) {
    sprintf(buf, "Cannot read data file \"%s\":", idx->filename_full);
    _error(buf);
    return _error(strerror(errno));
  }

  /* Read lines from file */
  long long noutput = 0;
  FILE * out_fp = 0;
  if ( output_file && strcmp(output_file, "-") != 0 ) {
    out_fp = fopen(output_file, "wb");
    if ( ! out_fp ) {
      sprintf(buf, "Cannot write output file \"%s\":", output_file);
      _error(buf);
      return _error(strerror(errno));
    }
  }
  else
    out_fp = stdout;

  /* Go to initial position */
  if ( line_start ) {
    int seek_error = fseek(src_fp, line_start, SEEK_SET);
    if ( seek_error ) {
      sprintf(buf, "Error seeking to position %lld in file \"%s\":", line_start, idx->filename_full);
      _error(buf);
      fclose(src_fp);
      fclose(out_fp);
      return false;
    }
  }

  /* Copy out lines until limit reached */
  int nless_than = less_than ? strlen(less_than) : 0;
  while ( true ) {

    /* Truncate by end line */
    if ( end > 0 && lineno >= end )
      break;

    /* Truncate based on count */
    if ( count >= 0 && noutput >= count )
      break;

    /* Read and copy out line */
    long nread = 0;
    unsigned char * line = _read_line(src_fp, 0, 0, &nread);
    if ( ! line || ! nread )
      break;

    /* Truncate based on max content filter */
    if ( less_than && strncmp(line, less_than, nless_than) > 0 )
      break;

    lineno += 1;

    /* Skip if not yet reached start line */
    if ( start > 0 && lineno < start )
      continue;

    /* Skip if not yet reached the min content filter */
    if ( greater_than && strcmp(line, greater_than) < 0 )
      continue;

    /* Output line */
    if ( line_number )
      fprintf(out_fp, "%s: ", _out_size(lineno, 0));

    long nwrote = fwrite(line, 1, nread, out_fp);
    if ( nwrote != nread ) {
      sprintf(buf, "Error: wrote %ld != %ld bytes to output \"%s\":", nwrote, nread, output_file);
      _error(buf);
      fclose(src_fp);
      fclose(out_fp);
      return false;
    }

    noutput += 1;
  }

  fclose(src_fp);
  fclose(out_fp);
  return true;
}

/* Show index info */
void print_index_info(struct hindex * idx, bool verbose) {
  int LEN = 15;
  char pos_buf[BUFSIZE], lines_buf[BUFSIZE];
  _out_line("Data file", idx->filename_full);
  _out_line("File modified", _out_tm(idx->file_mtime));
  _out_line("Index file", idx->index_filename);
  if ( idx->index_mtime > 0 )
    _out_line("Index modified", _out_tm(idx->index_mtime));
  _out_line("Index status", INDEX_STATUS_NAME[idx->status]);
  _out_line("File size", _out_size(idx->file_size, LEN));
  if ( idx->index_file_size > 0 )
    _out_line("Index file size", _out_size(idx->index_file_size, LEN));
  if( idx->chunk_size > 0 )
    _out_line("Index chunk size", _out_size(idx->chunk_size, LEN));
  if( idx->snaplen > 0 )
    _out_line("Index snap len", _out_size(idx->snaplen, LEN));
  if( idx->file_lines >= 0 )
    _out_line("File lines", _out_size(idx->file_lines, LEN));

  if ( idx->status != INDEX_STATUS_FRESH ) {
    if ( idx->last_file_size >= 0 )
      _out_line("Previous size", _out_size(idx->last_file_size, LEN));
    if ( idx->last_file_lines >= 0 )
      _out_line("Previous lines", _out_size(idx->last_file_lines, LEN));
  }
  if ( idx->status == INDEX_STATUS_ABSENT || idx->status == INDEX_STATUS_INVALID )
        return;
  _out_line("No. entries", _out_size(idx->nentry, LEN));

  if ( verbose ) {
    bool output_header = false;
    int i;
    for ( i = 0; i < idx->nentry; i++ ) {
      struct entry ent = idx->entries[i];
      if ( ! output_header ) {
        char * content_col = ent.frag ? "  Content" : "";
        printf(" Entry   File position     Line number%s\n", content_col);
        content_col = ent.frag ? "  ----------" : "";
        printf("------  --------------  --------------%s\n", content_col);
        output_header = true;
      }
      strcpy(pos_buf, _out_size(ent.filepos, LEN));
      strcpy(lines_buf, _out_size(ent.lineno + 1, LEN));
      printf("%s %s %s", _out_size(i+1, 6), pos_buf, lines_buf);
      if ( ent.frag )
        printf("  %s", ent.frag);
      printf("\n");
    }
  }
}

/* Main driver */
int main2(int argc, char *argv[]) {

  static char buf[BUFSIZE];
  struct stat statinfo;

  /* Arg values */
  bool            arg_build_only   = false;
  bool            arg_list         = false;
  bool            arg_delete       = false;
  bool            arg_dry_run      = false;
  long long       arg_start        = 0;
  long long       arg_end          = 0;
  unsigned char * arg_greater_than = 0;
  unsigned char * arg_less_than    = 0;
  long long       arg_count        = -1;
  char *          arg_output       = 0;
  bool            arg_line_number  = false;
  bool            arg_quiet        = false;
  bool            arg_verbose      = false;
  bool            arg_force        = false;
  long            arg_snaplen      = 0;
  long            arg_chunk_size   = DEFAULT_CHUNK_SIZE;
  char *          arg_index_file   = 0;
  char *          arg_index_dir    = 0;
  bool            arg_hidden       = false;
  bool            arg_fullname     = false;

  /* Parse options */
  char * OPTS = "hblxdS:E:G:L:N:o:nqvfP:C:i:D:HF";
  int c = 0;
  bool valid = false;
  while ((c = getopt(argc, argv, OPTS)) != -1) {
    switch(c) {
    case '?':  /* Bad option char (will print message to stderr) */
      return usage_error(0);
    case 'h':  /* -h          Show help message and exit */
      return usage_error(0);
    case 'b':  /* -b          Build index(es) only on FILE(s), no output */
      arg_build_only = true;
      break;
    case 'l':  /* -l          Just list info for FILE(s), more with -v */
      arg_list = true;
      break;
    case 'x':  /* -x          Delete index file if it exists */
      arg_delete = true;
      break;
    case 'd':  /* -d          Dry run: only show what would do */
      arg_dry_run = true;
      break;
    case 'S':  /* -S LINENO   Line-number search: start at source line LINENO */
      arg_start = atoll(optarg);
      if ( arg_start < 1 ) {
        sprintf(buf, "Value %lld for -S (start line) must be positive", arg_start);
        return usage_error(buf);
      }
      break;
    case 'E':  /* -E LINENO   Line-number search: end at source line LINENO */
      arg_end = atoll(optarg);
      if ( arg_end < 1 ) {
        sprintf(buf, "Value %lld for -E (end line) must be positive", arg_end);
        return usage_error(buf);
      }
      break;
    case 'G':  /* -G MINVAL   Content search for lines >= MINVAL in sorted file (see -P) */
      arg_greater_than = strdup(optarg);
      break;
    case 'L':  /* -L MAXVAL   Content search for lines <= MAXVAL in sorted file (see -P */
      arg_less_than = strdup(optarg);
      break;
    case 'N':  /* -N LINES    Limit output to at most LINES lines */
      arg_count= _convert_ll(optarg, &valid);
      if (! valid) {
        sprintf(buf, "Invalid arg for -N (count): \"%s\" ... should be non-negative integer", optarg);
        return usage_error(buf);
      }
      if (arg_count < 0) {
        sprintf(buf, "Value %lld for -N (output line count) should be non-negative integer", arg_count);
        return usage_error(buf);
      }
      break;
    case 'o':  /* -o FILE     Output to FILE instead of default stdout [stdout] */
      arg_output = strdup(optarg);
      break;
    case 'n':  /* -n          Include original line number in output */
      arg_line_number = true;
      break;
    case 'q':  /* -q          Limit messages to a minium */
      arg_quiet = true;
      break;
    case 'v':  /* -v          More verbose output when indexing, listing or searching */
      arg_verbose = true;
      break;
    case 'f':  /* -f          Force (re-)build of index */
      arg_force = true;
      break;
    case 'P':  /* -P BYTES    Capture leading BYTES bytes of each line for content search */
      arg_snaplen = atol(optarg);
      if (arg_snaplen <= 0) {
        sprintf(buf, "Value %ld for -S (snap length) should be positive integer", arg_snaplen);
        return usage_error(buf);
      }
      break;
    case 'C':  /* -C BYTES    Create index entries every BYTES bytes */
      arg_chunk_size = atol(optarg);
      if (arg_chunk_size <= 0) {
        sprintf(buf, "Value %ld for -C (chunk size) should be positive integer", arg_chunk_size);
        return usage_error(buf);
      }
      break;
    case 'i':  /* -i INDEX    Use explicit index file INDEX (else generate) */
      arg_index_file = strdup(optarg);
      break;
    case 'D':  /* -D DIR      Store indexes in DIR, "." means use dir of file */
      arg_index_dir = strdup(optarg);
      break;
    case 'H':  /* -H          Prefix index files with dot (.) to make them hidden */
      arg_hidden = true;
      break;
    case 'F':  /* -F          Use base name of file + ".hindex" for index name instead of hash */
      arg_fullname = true;
      break;
    default:
      /* If here, we did not specify the getopt() string consistently */
      sprintf(buf, "INTERNAL ERROR: Unhandled option \"-%c\"", c);
      return usage_error(buf);
    }
  }

  int nfile = argc <= optind ? 0 : argc - optind;
  if (! nfile)
    return usage_error("Must supply at least one file name");
  if (nfile > 1 && arg_index_file) {
    sprintf(buf, "Can only specify explicit index file with -i when indexing a single file, not %d", nfile);
    return usage_error(buf);
  }

  /* Can't both search and list */
  bool search_opt_given = arg_start > 0 || arg_end > 0 || arg_greater_than || arg_less_than || arg_count >= 0;
  if ( arg_list && search_opt_given )
    return usage_error("Cannot list with -l and also use search option(s) -SEGLN");
  /* Can't search, list or build with delete */
  if ( arg_delete ) {
    if (search_opt_given)
      return usage_error("Cannot delete index with -x and also use search option(s) -SEGLN");
    if (arg_list || arg_build_only)
      return usage_error("Cannot mix -x (delete) with -l (list) or -b (build only)");
  }

  /* Imply build_only if multiple files and no search options given */
  bool build_only = arg_build_only;
  if (nfile > 1) {
    if (search_opt_given) {
      sprintf(buf, "Search options -SEGLN not compatible with multiple files (%d)", nfile);
      return usage_error(buf);
    }
    else if (! build_only) {
      /* Report that we implied -b */
      if (arg_verbose) {
        sprintf(buf, "[Building indexes only since multiple files (%d) given]", nfile);
        _error(buf);
      }
      build_only = true;
    }
  }

  /* Check content search options */
  if ((arg_greater_than || arg_less_than) && arg_snaplen > 0 && arg_verbose) {
    if (arg_greater_than && strlen(arg_greater_than) > arg_snaplen) {
      sprintf(buf, "-G (greater than) value \"%s\" longer than snaplen of %ld", arg_greater_than, arg_snaplen);
      _error(buf);
    }
    if (arg_less_than && strlen(arg_less_than) > arg_snaplen) {
      sprintf(buf, "-L (less than) value \"%s\" longer than snaplen of %ld", arg_less_than, arg_snaplen);
      _error(buf);
    }
  }

  /* Warn if inconsistent range and max count */
  if (arg_verbose && arg_start > 0 && arg_end > 0 && arg_count >= 0) {
    long long start_end_range = arg_end - arg_start + 1;
    if (arg_count > start_end_range) {
      sprintf(buf, "Warning: line count implied by -S (start) and -E (end) of %lld will override max -N %lld", start_end_range, arg_count);
      _error(buf);
    }
    else if (arg_count < start_end_range) {
      sprintf(buf, "Warning: max -N (count) %lld will override line count implied by -S (start) and -E (end) of %lld", arg_count, start_end_range);
      _error(buf);
    }
  }

  /* Check search by line number or content, but not both */
  if ((arg_start > 0 || arg_end > 0) && (arg_greater_than || arg_less_than))
    return usage_error("Cannot search by both line number (-SE) and content (-GL)");

  /* Check index build options */
  char * index_dir = arg_index_dir ? arg_index_dir : DEFAULT_INDEX_DIR;
  if ( 0 != strcmp(index_dir, ".")) {
    if (stat(index_dir, &statinfo) || !S_ISDIR(statinfo.st_mode)) {
      sprintf(buf, "Index directory \"%s\" not found or not a directory", index_dir);
      return usage_error(buf);
    }
    if ( arg_fullname && arg_index_dir ) {
      sprintf(buf, "Cannot store derived-named indexes with -F (full name) in directory \"%s\" ... only -D of \".\" (same as file) may be used.", arg_index_dir);
      return usage_error(buf);
    }
  }
  if ( arg_fullname && ! arg_index_dir ) {
    index_dir = ".";
    if ( arg_verbose )
      _error("-F/--full-name given, presuming -D/--index-dir . (indexes in same directory as files)");
  }
  if ( arg_snaplen > 0 && arg_snaplen >= arg_chunk_size ) {
    sprintf(buf, "Snap len given with -S %ld must be less than chunk size given with -C %ld", arg_snaplen, arg_chunk_size);
    return usage_error(buf);
  }

  /* Check search range options are sensible and warn if not */
  if (!arg_quiet) {
    if (arg_count == 0)
      _error("Warning: -N of 0 given ... no lines will be output.  Use -q (quiet) to suppress this message");
    if (arg_end > 0 && arg_start > 0 && arg_end < arg_start) {
      sprintf(buf, "Warning: -E (end) of %lld precedes -S (start) of %lld ... no lines will be output.  Use -q (quiet) to suppress this message", arg_end, arg_start);
      _error(buf);
    }
    if (arg_greater_than && arg_less_than && strcmp(arg_greater_than, arg_less_than) > 0) {
      sprintf(buf, "Warning: -L (less than) of \"%s\" precedes -G (greater than) of \"%s\" ... no lines will be output.  Use -q (quiet) to suppress this message", arg_less_than, arg_greater_than);
      _error(buf);
    }
  }

  /*** PROCESS FILES ***/

  if (arg_dry_run && !arg_quiet)
    _error("DRY RUN MODE ... will not touch any files");

  bool success = true;
  for( ; optind < argc ; optind++) {

    char * filename = argv[optind];
    char * filename_full = realpath(filename, 0);
    if (!filename_full) {
      sprintf(buf, "Cannot resolve full path of data file \"%s\": %s", filename, strerror(errno));
      return _error(buf);
    }
    if  (access(filename_full, R_OK) != 0) {
      sprintf(buf, "File \"%s\" not readable: %s", filename_full, strerror(errno));
      return _error(buf);
    }
    if (stat(filename_full, &statinfo) || !S_ISREG(statinfo.st_mode)) {
      sprintf(buf, "Not a regular file that can be indexed: \"%s\"", filename_full);
      return _error(buf);
    }

    /* Get the index file to delete, build, list ... */
    char * index_filename = arg_index_file;
    if (!index_filename) {
      index_filename = get_index_filename(filename_full, index_dir, arg_hidden, arg_fullname);
      if (!index_filename) {
        sprintf(buf, "Problems indexing \"%s\"", filename);
        return _error(buf);
      }
    }

    /* Check delete */
    if (arg_delete) {
      char * action = 0;
      if  (access(index_filename, F_OK) == 0) {
        /* File exists for delete */
        if (arg_dry_run)
          action = "Would delete";
        else {
          if (unlink(index_filename)) {
            sprintf(buf, "Could not delete \"%s\": %s", index_filename, strerror(errno));
            return _error(buf);
          }
          else
            action = "Deleted";
        }
        if (!arg_quiet) {
          sprintf(buf, "%s index \"%s\" on \"%s\" (Use -q to suppress this message)", action, index_filename, filename_full);
          _error(buf);
        }
      }
      else if (arg_verbose) {
        sprintf(buf, "Warning: index \"%s\" on \"%s\" not found for delete", index_filename, filename_full);
        _error(buf);
      }
      continue;
    }

    /* List info only */
    if (arg_list) {
      struct hindex idx;
      bool success = get_index_info(filename_full, index_filename, &idx);
      if ( !success )
        break;
      print_index_info(&idx, arg_verbose);
      continue;
    }

    /* Check or create the index */
    struct hindex idx;
    bool for_content_search = arg_greater_than || arg_less_than;
    bool success = index_file(&idx, filename_full, index_filename, arg_chunk_size, arg_snaplen, arg_quiet, arg_verbose, arg_force, arg_dry_run, for_content_search);
    if (!success)
      break;

    /* Nothing to do if just indexing or dry run */
    if ( build_only || arg_dry_run )
      continue;

    /* Search the file for lines */
    success = search_file(&idx, arg_output, arg_start, arg_end, arg_greater_than, arg_less_than, arg_count, arg_line_number, arg_verbose);
    if ( ! success )
      break;
  }

  return true;
}

int main(int argc, char *argv[]) {
  return main2(argc, argv) ? 0 : 1;
}

