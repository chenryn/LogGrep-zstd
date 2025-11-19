#ifndef CMD_LOGDISPATCH_H
#define CMD_LOGDISPATCH_H

#include <iostream>
#include <queue>
#include "LogStore_API.h"

class LogDispatcher
{
public:
	LogDispatcher();
	~LogDispatcher();

private:
	std::queue<char*> m_filequeue;
	LogStoreApi* m_logStores[MAX_FILE_CNT];
	int m_nServerHandle;
	int m_fileCnt;

	char** m_args;
	int m_argCount;
	int m_spid;//pid of each thread
	RunningStatus m_runt;

private:
	int CalRunningTime();
	int ResetRunningTime();
	int TraveDir(char* dirPath);
	void * SearchByWildcard_pthread_exe();

public:
    int Connect(char* dirPath);
    int IsConnect();
    void DisConnect();

    int SearchByWildcard(char *args[MAX_CMD_ARG_COUNT], int argCount);
    int SearchByWildcard_Seq(char *args[MAX_CMD_ARG_COUNT], int argCount);
    int SearchByWildcard_Thread(char *args[MAX_CMD_ARG_COUNT], int argCount);
    static void * SearchByWildcard_pthread(void *ptr);
    int GetRunningStatus(OUT RunningStatus& out);
    int SearchByWildcard_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, int matNum, std::string &json_out);
    int CountByWildcard(char *args[MAX_CMD_ARG_COUNT], int argCount);
    int Aggregate_Scalar(char *args[MAX_CMD_ARG_COUNT], int argCount, int opType, const std::string& alias, double& value_out);
    int Aggregate_Distinct(char *args[MAX_CMD_ARG_COUNT], int argCount, const std::string& alias, int& value_out);
    int Aggregate_TopK_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, const std::string& alias, int k, std::string& json_out);
    int Aggregate_Group_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, const std::string& groupAlias, int opType, const std::string& valueAlias, std::string& json_out);
};

#endif
