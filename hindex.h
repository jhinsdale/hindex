/* hindex.h */

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>
#include <getopt.h>
#include <string.h>
#include <stdbool.h>
#include <libgen.h>
#include <openssl/sha.h>
#include <math.h>
#include <time.h>

/* Defaults */
/* Following value must agree with USAGE below */
#define DEFAULT_CHUNK_SIZE 1 * 1000 * 1000

/* Index dir and filenaming */
/* Following value must agree with USAGE below */
#define DEFAULT_INDEX_DIR "/tmp"
#define INDEX_HASH_PREFIX "f_"
#define INDEX_SUFFIX ".hindex"

/* Index states of existence, freshness, validity */
int INDEX_STATUS_ABSENT = 0;
int INDEX_STATUS_FRESH = 1;
int INDEX_STATUS_STALE = 2;
int INDEX_STATUS_INVALID = 3;
char * INDEX_STATUS_NAME[] = {
 "Not found",
 "Up to date",
 "Out of date (needs refresh)",
 "Invalid (needs rebuild)"
};

#define BUFSIZE 65536
long int INDEX_PROGRESS_INTERVAL = 100 * 1000 * 1000;
#define DEFAULT_INDEX_ENTRY_ALLOC 100

/* Index entry */
struct entry {
  long long       filepos;
  long long       lineno;
  unsigned char * frag;
};

struct hindex {
  char *         filename_full;
  char *         index_filename;
  int            status;
  long long      file_size;
  long long      file_lines;
  long long      last_file_size;
  long long      last_file_lines;
  long double    file_mtime;
  long long      index_file_size;
  long double    index_mtime;
  long           chunk_size;
  long           snaplen;
  int            nentry;
  int            maxentry;
  struct entry * entries;
};

/* Usage string, contains program version */
static char * USAGE =
"Usage: hindex [-h] [-b] [-l] [-x] [-d]\n"
"              [-S LINENO] [-E LINENO] [-G MINVAL] [-L MAXVAL] [-N LINES]\n"
"              [-n] [-q] [-v]\n"
"              [-f] [-P BYTES] [-C BYTES] [-i INDEX] [-D DIR] [-H] [-F]\n"
"              FILE [FILE ...]\n"
"\n"
"hindex - Huge file INDEXer, version 0.9\n"
"\n"
"Positional arguments:\n"
"  FILE        Search/index lines in FILE(s) (required)\n"
"\n"
"Mode of operation (default is extract and print lines):\n"
"  -b          Build index(es) only on FILE(s), no output [False]\n"
"  -l          Just list info for FILE(s), more with -v [False]\n"
"  -x          Delete index file if it exists [False]\n"
"  -d          Dry run: only show what would do [False]\n"
"  -h          Show this help message and exit [False]\n"
"\n"
"Search options:\n"
"  -S LINENO   Line-number search: start at source line LINENO [1]\n"
"  -E LINENO   Line-number search: end at source line LINENO [None]\n"
"  -G MINVAL   Content search for lines >= MINVAL in sorted file (see -P) [None]\n"
"  -L MAXVAL   Content search for lines <= MAXVAL in sorted file (see -P) [None]\n"
"  -N LINES    Limit output to at most LINES lines [None]\n"
"\n"
"Output options:\n"
"  -o FILE     Output to FILE instead of default stdout [stdout]\n"
"  -n          Include original line number in output [False]\n"
"  -q          Limit messages to a minimum [False]\n"
"  -v          More verbose output when indexing, listing or searching [False]\n"
"\n"
"Index build options:\n"
"  -f          Force (re-)build of index [False]\n"
"  -P BYTES    Capture leading BYTES bytes of each line for content search [None]\n"
"  -C BYTES    Create index entries every BYTES bytes [1000000]\n"
"\n"
"Index file name and location options:\n"
"  -i INDEX    Use explicit index file INDEX (else generate) [None]\n"
"  -D DIR      Store indexes in DIR, \".\" means use dir of file [/tmp]\n"
"  -H          Prefix index files with dot (.) to make them hidden [False]\n"
"  -F          Use full name of file + \".hindex\" for index name instead of hash [False]\n"
;
