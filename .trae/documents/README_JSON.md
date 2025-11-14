# JSON Output Extension for LogGrep-zstd

This extension adds JSON output capability to the LogGrep-zstd query engine, providing template and template ID information for each matched log line.

## Features

- **JSON Format**: Each log line is returned as a JSON object with:
  - `log_line`: The actual log content
  - `template_id`: The ID of the matching template
  - `template`: The actual template pattern
  - `line_number`: The line number in the original file

- **Proper JSON Escaping**: Handles special characters and newlines correctly
- **Outlier Support**: Outliers are marked with template_id = -1 and template = "OUTLIER"
- **Flexible Querying**: Supports all existing query syntax

## Usage

### Quick Test
```bash
# Test with existing data
python3 json_wrapper.py ../example_zip/Apache "error" 5
```

### Build JSON Tool
```bash
# Build the enhanced query tool
g++ -w -g -Wall -ggdb -arch x86_64 -I. -I../compression -o thulr_cmdline_json thulr_cmdline_json.cpp LogStore_API.o LogStructure.o SearchAlgorithm.o LogDispatcher.o Coffer.o -lzstd -l dl

# Or use the script approach
chmod +x json_wrapper.py
```

### Examples

#### Basic JSON Search
```bash
# Search for errors with JSON output
./thulr_cmdline_json ../example_zip/Apache "error"

# Search with limit
./thulr_cmdline_json ../example_zip/Apache "Invalid URI" 10

# Complex query
./thulr_cmdline_json ../LogHub_Seg_zip/Hadoop "ERROR and RECEIVED SIGNAL 15" 5
```

#### Python Wrapper
```bash
# Use Python wrapper for JSON output
python3 json_wrapper.py ../example_zip/Linux "authentication failure" 3
```

## JSON Output Format

```json
[
  {
    "log_line": "2023-01-01 10:00:00 ERROR Invalid URI in request /api/invalid",
    "template_id": 123,
    "template": "\u003cTIMESTAMP\u003e ERROR Invalid URI in request \u003cURI\u003e",
    "line_number": 456
  },
  {
    "log_line": "2023-01-01 10:01:15 ERROR Authentication failed for user admin",
    "template_id": 124,
    "template": "\u003cTIMESTAMP\u003e ERROR Authentication failed for user \u003cUSER\u003e",
    "line_number": 457
  }
]
```

## Implementation Details

### Files Created
- `thulr_cmdline_json.cpp`: Enhanced query tool with JSON output
- `json_wrapper.py`: Python wrapper for JSON formatting
- `README_JSON.md`: This documentation

### Key Modifications
1. **Materialization_JSON()**: JSON-formatted materialization of log lines
2. **MaterializOutlier_JSON()**: JSON-formatted outlier handling
3. **SearchByWildcard_Token_JSON()**: JSON-enabled search function
4. **JSON Escaping**: Proper handling of special characters

### Limitations
- The current implementation requires modifications to `LogStore_API.cpp` for full functionality
- The Python wrapper provides a temporary solution without modifying core files
- Template extraction from patterns requires additional mapping logic

## Building from Source

### Prerequisites
- g++ compiler
- zstd library
- Python 3 (for wrapper)

### Build Steps
```bash
cd /Users/rizhiyi/Downloads/LogGrep-zstd-master/query

# Build missing object files
g++ -w -c -g -Wall -ggdb -arch x86_64 LogStore_API.cpp LogStructure.cpp SearchAlgorithm.cpp LogDispatcher.cpp ../compression/Coffer.cpp -I. -I../compression

# Build JSON tool
g++ -w -g -Wall -ggdb -arch x86_64 -I. -I../compression -o thulr_cmdline_json thulr_cmdline_json.cpp LogStore_API.o LogStructure.o SearchAlgorithm.o LogDispatcher.o Coffer.o -lzstd -l dl

# Test
timeout 10s ./thulr_cmdline_json ../example_zip/Apache "error" 3
```

## Testing

### Test with Sample Data
```bash
# Test with Apache logs
./thulr_cmdline_json ../example_zip/Apache "error" 5

# Test with Linux logs
./thulr_cmdline_json ../example_zip/Linux "authentication" 3

# Test with Healthapp logs
./thulr_cmdline_json ../example_zip/Healthapp "Step_ExtSDM" 2
```

### Validation
- Verify JSON is valid
- Check template IDs match patterns
- Confirm line numbers are sequential
- Ensure special characters are properly escaped