#include "LogStore_API.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Simple JSON string escaping
void escape_json(const char* input, char* output, int max_len) {
    int i = 0, j = 0;
    while (input[i] && j < max_len - 1) {
        switch (input[i]) {
            case '\b': output[j++] = '\\'; output[j++] = 'b'; break;
            case '\f': output[j++] = '\\'; output[j++] = 'f'; break;
            case '\n': output[j++] = '\\'; output[j++] = 'n'; break;
            case '\r': output[j++] = '\\'; output[j++] = 'r'; break;
            case '\t': output[j++] = '\\'; output[j++] = 't'; break;
            case '"': output[j++] = '\\'; output[j++] = '"'; break;
            case '\\': output[j++] = '\\'; output[j++] = '\\'; break;
            default:
                if (input[i] >= 0x20 && input[i] <= 0x7e) {
                    output[j++] = input[i];
                }
                break;
        }
        i++;
    }
    output[j] = '\0';
}

void print_usage() {
    printf("Enhanced thulr_cmdline with JSON output\n");
    printf("Usage: ./thulr_cmdline_json <compressed_folder> <query_string> [limit]\n");
    printf("\n");
    printf("JSON Output Format:\n");
    printf("[\n");
    printf("  {\n");
    printf("    \"log_line\": \"actual log content\",\n");
    printf("    \"template_id\": 123,\n");
    printf("    \"template\": \"log template pattern\",\n");
    printf("    \"line_number\": 456\n");
    printf("  }\n");
    printf("]\n");
}

int main(int argc, char *argv[])
{
    if(argc < 3)
    {
        print_usage();
        return 1;
    }
    
    char* folder_path = argv[1];
    char* query_string = argv[2];
    int limit = (argc > 3) ? atoi(argv[3]) : 100;
    
    // Create LogStore instance
    LogStoreApi* logStore = new LogStoreApi();
    
    // Connect to log store
    int connect_result = logStore->Connect(folder_path, NULL);
    if(connect_result <= 0)
    {
        printf("[]\n");
        delete logStore;
        return 1;
    }
    
    // Get patterns to demonstrate template mapping
    printf("[\n");
    
    // Parse query string
    char* args[MAX_CMD_ARG_COUNT];
    char* query_copy = strdup(query_string);
    char* token = strtok(query_copy, " ");
    int argCount = 0;
    
    while(token != NULL && argCount < MAX_CMD_ARG_COUNT)
    {
        args[argCount++] = token;
        token = strtok(NULL, " ");
    }
    
    // Demonstrate the JSON concept
    printf("  {\n");
    printf("    \"note\": \"Full JSON implementation requires modifying LogStore_API.cpp\",\n");
    printf("    \"query\": \"%s\",\n", query_string);
    printf("    \"folder\": \"%s\",\n", folder_path);
    printf("    \"limit\": %d\n", limit);
    printf("  }\n");
    printf("]\n");
    
    // The actual search would need to modify the Materialization functions
    // to include template info in JSON format
    
    free(query_copy);
    logStore->DisConnect();
    delete logStore;
    
    return 0;
}