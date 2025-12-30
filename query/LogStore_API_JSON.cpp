// JSON Output Extension for LogGrep-zstd
// This file adds JSON output functionality with template and template ID information

#include "LogStore_API.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <json-c/json.h>

// JSON output function for materialized log lines with template info
int LogStoreApi::Materialization_JSON(int pid, BitMap* bitmap, int bitmapSize, int matSize, std::string &json_out)
{
    int entryCnt = bitmapSize >= matSize ? matSize : bitmapSize;
    if(entryCnt <= 0) return entryCnt;

    LogPattern* pat = m_patterns[pid];
    CELL* output = new CELL[pat->SegSize];
    
    // Create JSON array to hold all results
    json_object* json_array = json_object_new_array();
    
    // Prepare output buffers for variables
    for(int i = 0; i < pat->SegSize; i++)
    {
        if(pat->SegAttr[i] == SEG_TYPE_CONST || pat->SegAttr[i] == SEG_TYPE_DELIM)
        {
            output[i] = pat->Segment[i];
        }
        else
        {
            output[i] = new char[MAX_VALUE_LEN * entryCnt];
            memset(output[i], '\0', MAX_VALUE_LEN * entryCnt);
            
            Materializ_Pats(pat->VarNames[i], bitmap, entryCnt, output[i]);
        }
    }
    
    // Process each matched log entry
    for(int k = 0; k < entryCnt; k++)
    {
        json_object* json_entry = json_object_new_object();
        
        // Build the log line
        char* log_line = new char[MAX_LINE_SIZE];
        memset(log_line, '\0', MAX_LINE_SIZE);
        
        for(int i = 0; i < pat->SegSize; i++)
        {
            if(pat->SegAttr[i] == SEG_TYPE_CONST || pat->SegAttr[i] == SEG_TYPE_DELIM)
            {
                strcat(log_line, output[i]);
            }
            else
            {
                strcat(log_line, output[i] + MAX_VALUE_LEN * k);
            }
        }
        
        // Add fields to JSON object
        json_object_object_add(json_entry, "log_line", json_object_new_string(log_line));
        json_object_object_add(json_entry, "template_id", json_object_new_int(pid));
        json_object_object_add(json_entry, "template", json_object_new_string(pat->Content));
        json_object_object_add(json_entry, "line_number", json_object_new_int(bitmap->GetIndex(k) + 1));
        
        // Add to array
        json_object_array_add(json_array, json_entry);
        
        delete[] log_line;
    }
    
    // Append to JSON output string
    const char* str = json_object_to_json_string(json_array);
    if (str) {
        if (!json_out.empty() && json_out.back() == ']') {
            json_out.pop_back();
            if (json_out.length() > 1) json_out.append(",");
            json_out.append(str + 1);
        } else {
            json_out.append(str);
        }
    }
    
    // Cleanup
    json_object_put(json_array);
    
    if(output)
    {
        for(int i = 0; i < pat->SegSize; i++)
        {
            if(pat->SegAttr[i] != SEG_TYPE_CONST && pat->SegAttr[i] != SEG_TYPE_DELIM)
            {
                delete[] output[i];
                output[i] = NULL;
            }
        }
    }
    delete[] output;
    
    return entryCnt;
}

// JSON output function for outliers
int LogStoreApi::MaterializOutlier_JSON(BitMap* bitmap, int cnt, int refNum, std::string &json_out)
{
    int doCnt = refNum > cnt ? cnt : refNum;
    if(doCnt <= 0) return doCnt;
    
    json_object* json_array = json_object_new_array();
    
    for(int i = 0; i < doCnt; i++)
    {
        json_object* json_entry = json_object_new_object();
        
        json_object_object_add(json_entry, "log_line", json_object_new_string(m_outliers[bitmap->GetIndex(i)]));
        json_object_object_add(json_entry, "template_id", json_object_new_int(-1));
        json_object_object_add(json_entry, "template", json_object_new_string("OUTLIER"));
        json_object_object_add(json_entry, "line_number", json_object_new_int(bitmap->GetIndex(i) + 1));
        
        json_object_array_add(json_array, json_entry);
    }
    
    const char* str = json_object_to_json_string(json_array);
    if (str) {
        if (!json_out.empty() && json_out.back() == ']') {
            json_out.pop_back();
            if (json_out.length() > 1) json_out.append(",");
            json_out.append(str + 1);
        } else {
            json_out.append(str);
        }
    }
    json_object_put(json_array);
    
    return doCnt;
}

// Modified search function to support JSON output
int LogStoreApi::SearchByWildcard_Token_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, int matNum, std::string &json_out)
{
    timeval t1 = ___StatTime_Start();
    LISTBITMAPS bitmaps;
    int ret = 0;
    
    // Perform the search
    if(IsSearchWithLogic(args, argCount))
    {
        ret = SearchByLogic(args, argCount, bitmaps);
    }
    else if (argCount > 1)
    {
        ret = Search_MultiSegments(args, argCount, bitmaps);
    }
    else
    {
        ret = Search_SingleSegment(args[0], bitmaps);
    }
    
    if(ret <= 0)
    {
        json_out = "[]";
        return ret;
    }
    
    json_out = "[]";
    // Process results with JSON output
    int totalCnt = 0;
    for(LISTBITMAPS::iterator iter = bitmaps.begin(); iter != bitmaps.end(); ++iter)
    {
        int pid = iter->first;
        BitMap* bitmap = iter->second;
        if (!bitmap) continue;
        
        if(pid < 0)
        {
            // Outlier case
            MaterializOutlier_JSON(bitmap, bitmap->GetSize(), matNum - totalCnt, json_out);
            totalCnt += (bitmap->GetSize() < matNum - totalCnt ? bitmap->GetSize() : matNum - totalCnt);
        }
        else
        {
            // Regular pattern case
            Materialization_JSON(pid, bitmap, bitmap->GetSize(), matNum - totalCnt, json_out);
            totalCnt += (bitmap->GetSize() < matNum - totalCnt ? bitmap->GetSize() : matNum - totalCnt);
        }
        
        if(totalCnt >= matNum)
            break;
    }
    
    return totalCnt;
}