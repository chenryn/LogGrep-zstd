#ifndef LOGSTORE_API_JSON_H
#define LOGSTORE_API_JSON_H

#include "LogStore_API.h"

class LogStoreApi_JSON : public LogStoreApi {
public:
    // JSON output functions
    int Materialization_JSON(int pid, BitMap* bitmap, int bitmapSize, int matSize);
    int MaterializOutlier_JSON(BitMap* bitmap, int cnt, int refNum);
    int SearchByWildcard_Token_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, int matNum);
};

#endif // LOGSTORE_API_JSON_H