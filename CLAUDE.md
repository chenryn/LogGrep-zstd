# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**LogGrep-zstd** is a research implementation from the EuroSys 2023 paper "LogGrep: Fast and Cheap Cloud Log Storage by Exploiting both Static and Runtime Patterns". It's a log compression and querying system that uses zstd as the underlying compression method.

## Architecture

The system has three main components:

1. **Compression Engine** (`/compression/`)
   - Processes raw log files into compressed archives
   - Uses pattern recognition and static analysis for optimal compression
   - Built with C++ using zstd for compression

2. **Query Engine** (`/query/`)
   - Searches compressed logs without full decompression
   - Provides command-line interface (`thulr_cmdline`)
   - Supports complex query expressions and variable substitution

3. **zstd Integration** (`/zstd-dev/`)
   - Facebook's zstd library integrated as the compression backend
   - Provides both static linking and shared library support

## Key Directories

- **`compression/`** - Log compression utilities (THULR binary)
- **`query/`** - Log querying system (thulr_cmdline binary)  
- **`example/`** - Sample log files for testing (Apache, HPC, Linux, etc.)
- **`example_zip/`** - Compressed output from quick tests
- **`lib_output_zip/`** - Output from library-based compression

## Build Commands

### Initial Setup
```bash
# Create required directories
mkdir ./output ./example_zip

# Build zstd library
cd ./zstd-dev/lib && make

# Build compression engine
cd ../../compression && make

# Build query engine
cd ../query && make
```

### Build Shared Library (for Python integration)
```bash
chmod +x build_shared_lib.sh
./build_shared_lib.sh
```

## Testing Commands

### Quick Test
```bash
# Compress sample logs
cd compression
python3 quickTest.py

# Query compressed logs
cd ../query
./thulr_cmdline ../example_zip/Apache "error and Invalid URI in request"
```

### Large Dataset Test
```bash
# Download LogHub dataset to ../LogHub_Seg/
cd compression
python3 largeTest.py ../LogHub_Seg/

# Query large dataset
cd ../query
./thulr_cmdline ../LogHub_Seg_zip/Hadoop "ERROR and RECEIVED SIGNAL 15"
```

### Python Integration
```bash
# Test shared library integration
python3 python_example.py
```

## Usage Patterns

### Compression
- **CLI**: `./THULR -I input.log -O output.zip`
- **Library**: Use `compress_from_memory()` via ctypes
- **Batch**: Use `quickTest.py` or custom scripts

### Querying
- **Basic**: `./thulr_cmdline [compressed_folder] "query_string"`
- **Complex**: Supports logical operators (AND, OR) and variable substitution
- **Examples**: See `query4quicktest.txt` and `query4largetest.txt`

## Development Notes

- **Language**: C++11 with Python utilities
- **Dependencies**: zstd, standard C++ libraries
- **Tested Platforms**: Red Hat 4.8.5, Ubuntu 11.3.0
- **Python**: 3.6+ required for utilities

## File Formats

- **Input**: Plain text log files (any format)
- **Output**: `.zip` files with custom compression format
- **Metadata**: `.meta`, `.templates`, `.variables` files accompany compressed logs