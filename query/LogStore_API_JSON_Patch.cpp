// JSON Output Extension for LogStore_API.cpp
// This adds JSON output capability to the existing LogStore_API

// Add these functions to LogStore_API.cpp after the existing Materialization function

// JSON string escaping utility
static std::string escape_json(const std::string& input) {
    std::ostringstream ss;
    for (char c : input) {
        switch (c) {
            case '\b': ss << "\\b"; break;
            case '\f': ss << "\\f"; break;
            case '\n': ss << "\\n"; break;
            case '\r': ss << "\\r"; break;
            case '\t': ss << "\\t"; break;
            case '"': ss << "\\\""; break;
            case '\\': ss << "\\\\"; break;
            default:
                if (c >= 0x20 && c <= 0x7e) {
                    ss << c;
                } else {
                    ss << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)c;
                }
                break;
        }
    }
    return ss.str();
}

// JSON output for materialized log lines
int LogStoreApi::Materialization_JSON(int pid, BitMap* bitmap, int bitmapSize, int matSize)
{
    int entryCnt = bitmapSize >= matSize ? matSize : bitmapSize;
    if(entryCnt <= 0) return entryCnt;

    LogPattern* pat = m_patterns[pid];
    CELL* output = new CELL[pat->SegSize];
    for(int i=0;i< pat->SegSize;i++)
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
    
    printf("[\n");
    
    for(int k=0; k < entryCnt; k++)
    {
        // Build log line
        char* log_line = new char[MAX_LINE_SIZE];
        memset(log_line, '\0', MAX_LINE_SIZE);
        
        for(int i=0;i< pat->SegSize;i++)
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
        
        // Output JSON object
        std::string escaped_log = escape_json(std::string(log_line));
        std::string escaped_template = escape_json(std::string(pat->Content));
        
        printf("  {\n");
        printf("    \"log_line\": \"%s\",\n", escaped_log.c_str());
        printf("    \"template_id\": %d,\n", pid);
        printf("    \"template\": \"%s\",\n", escaped_template.c_str());
        printf("    \"line_number\": %d\n", bitmap->GetIndex(k) + 1);
        
        if(k < entryCnt - 1)
            printf("  },\n");
        else
            printf("  }\n");
        
        delete[] log_line;
    }
    
    printf("]\n");
    
    // Cleanup
    if(output)
    {
        for(int i=0;i< pat->SegSize;i++)
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

// JSON output for outliers
int LogStoreApi::MaterializOutlier_JSON(BitMap* bitmap, int cnt, int refNum)
{
    int doCnt = refNum > cnt ? cnt : refNum;
    if(doCnt <= 0) return doCnt;
    
    printf("[\n");
    
    for(int i=0; i < doCnt; i++)
    {
        std::string escaped_log = escape_json(std::string(m_outliers[bitmap->GetIndex(i)]));
        
        printf("  {\n");
        printf("    \"log_line\": \"%s\",\n", escaped_log.c_str());
        printf("    \"template_id\": -1,\n");
        printf("    \"template\": \"OUTLIER\",\n");
        printf("    \"line_number\": %d\n", bitmap->GetIndex(i) + 1);
        
        if(i < doCnt - 1)
            printf("  },\n");
        else
            printf("  }\n");
    }
    
    printf("]\n");
    
    return doCnt;
}

// JSON-enabled search function
int LogStoreApi::SearchByWildcard_Token_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, int matNum)
{
    LISTBITMAPS bitmaps;
    int ret = 0;
    
    if(IsSearchWithLogic(args, argCount))
    {
        ret = SearchByLogic(args, argCount, bitmaps);
    }
    else
    {
        ret = Search_SingleSegment(args[0], bitmaps);
    }
    
    if(ret <= 0)
    {
        printf("[]\n");
        return ret;
    }
    
    int totalCnt = 0;
    bool first_entry = true;
    
    printf("[\n");
    
    for(LISTBITMAPS::iterator iter = bitmaps.begin(); iter != bitmaps.end(); ++iter)
    {
        int pid = atoi(iter->first.c_str());
        BitMap* bitmap = iter->second;
        
        if(pid < 0)
        {
            // Outliers
            MaterializOutlier_JSON(bitmap, bitmap->GetSize(), matNum - totalCnt);
            totalCnt += std::min(bitmap->GetSize(), matNum - totalCnt);
        }
        else
        {
            // Regular patterns
            Materialization_JSON(pid, bitmap, bitmap->GetSize(), matNum - totalCnt);
            totalCnt += std::min(bitmap->GetSize(), matNum - totalCnt);
        }
        
        if(totalCnt >= matNum)
            break;
    }
    
    printf("]\n");
    
    return totalCnt;
}