#ifndef STATISTICS_API_H
#define STATISTICS_API_H

#include "LogStore_API.h"
#include "../compression/Coffer.h"
#include "HLL.h"
#include <map>
#include <vector>
#include <string>
#include <utility>

// 前向声明
class LogStoreApi;

/**
 * 列式存储统计功能 API
 * 利用列式存储的优势，提供高效的统计分析功能
 * 
 * 注意：StatisticsAPI 需要访问 LogStoreApi 的私有方法 DeCompressCapsule
 * 建议将 StatisticsAPI 声明为 LogStoreApi 的友元类，或者将统计方法直接添加到 LogStoreApi
 */
class StatisticsAPI {
private:
    LogStoreApi* m_api;
    
    // 访问 LogStoreApi 的私有方法（需要友元声明）
    int DeCompressCapsule(int patName, Coffer* &coffer, int type = 0);
    
    // 辅助函数：判断是否为数值类型
    bool IsNumericType(int varType);
    
    // 辅助函数：从固定长度列式存储中读取单个值
    int ReadValue_Fixed(char* data, int index, int eleLen, char* buffer, int bufferLen);
    
    // 辅助函数：从变长列式存储中读取单个值
    int ReadValue_Diff(char* data, int sLen, int index, char* buffer, int bufferLen);
    
    // 辅助函数：解析数值
    double ParseNumeric(const char* value, int len);
    
    // 辅助函数：去除填充
    void RemovePadding(const char* padded, int len, char* result, int& resultLen);

public:
    StatisticsAPI(LogStoreApi* api) : m_api(api) {}
    
    /**
     * 数值类统计
     */
    
    // 计算平均值
    double GetVarAvg(int varname, BitMap* filter = NULL);
    
    // 获取最大值
    double GetVarMax(int varname, BitMap* filter = NULL);
    
    // 获取最小值
    double GetVarMin(int varname, BitMap* filter = NULL);
    
    // 计算总和
    double GetVarSum(int varname, BitMap* filter = NULL);
    
    // 获取计数（考虑过滤条件）
    int GetVarCount(int varname, BitMap* filter = NULL);
    
    // 计算中位数
    double GetVarMedian(int varname, BitMap* filter = NULL);
    
    // 计算标准差
    double GetVarStdDev(int varname, BitMap* filter = NULL);
    
    // 计算百分位数
    double GetVarPercentile(int varname, double percentile, BitMap* filter = NULL);
    
    /**
     * 文本类统计
     */
    
    // 获取唯一值数量
    int GetVarDistinctCount(int varname, BitMap* filter = NULL);
    void BuildHLL(int varname, BitMap* filter, HyperLogLog& h);
    
    // 获取频率分布（返回 Top-K）
    std::map<std::string, int> GetVarFrequency(int varname, int topK = 10, BitMap* filter = NULL);
    
    // 获取 Top-K 值及其频率
    std::vector<std::pair<std::string, int> > GetVarTopK(int varname, int k, BitMap* filter = NULL);
    
    /**
     * 组合统计
     */
    
    std::map<std::string, double> GetVarGroupByAvg(int groupVar, int valueVar, BitMap* filter = NULL);
    std::map<std::string, int> GetVarGroupByDistinctCount(int groupVar, int valueVar, BitMap* filter = NULL);
    
    // 按分组变量分组，计算数值变量的总和
    std::map<std::string, double> GetVarGroupBySum(int groupVar, int valueVar, BitMap* filter = NULL);
    
    // 按分组变量分组，计算计数
    std::map<std::string, int> GetVarGroupByCount(int groupVar, BitMap* filter = NULL);
};

#endif // STATISTICS_API_H

