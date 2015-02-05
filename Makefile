
# Install locations
INSTALL_BIN_DIR=/usr/local/bin

# -lcrypto depends on libssl-dev package and is needed to compute SHA-1 hashed filenames
LIBS=-lcrypto -lm

CFLAGS=-Wformat-overflow=0 -O3

hindex: hindex.c hindex.h Makefile
	gcc $(CFLAGS) -o hindex hindex.c $(LIBS)

install: hindex hindex.py
	cp -p $^ $(INSTALL_BIN_DIR)

# Requires pandoc installed
doc: README.html

# Convert .md to .html with pandoc
%.html: %.md
	pandoc -f markdown -t html $< -o $@
