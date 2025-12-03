#include "StatisticsAPI.h"
#include "SearchAlgorithm.h"
#include "../compression/util.h"
#include "HLL.h"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <sstream>
#include <iomanip>
#include <cstdlib>

bool StatisticsAPI::IsNumericType(int varType) {
    // NUM_TY = 1, 纯数字类型
    // 也可以包含十六进制 (AF_TY=2, af_TY=4)
    return (varType & 1) != 0 || (varType & 2) != 0 || (varType & 4) != 0;
}

int StatisticsAPI::ReadValue_Fixed(char* data, int index, int eleLen, char* buffer, int bufferLen) {
    if (data == NULL || buffer == NULL || eleLen <= 0) return 0;
    
    int offset = index * eleLen;
    int actualLen = 0;
    
    // 去除填充空格
    int i = 0;
    while (i < eleLen && data[offset + i] == ' ') {
        i++;
    }
    
    actualLen = eleLen - i;
    if (actualLen > bufferLen - 1) {
        actualLen = bufferLen - 1;
    }
    
    if (actualLen > 0) {
        memcpy(buffer, data + offset + i, actualLen);
    }
    buffer[actualLen] = '\0';
    
    return actualLen;
}

int StatisticsAPI::ReadValue_Diff(char* data, int sLen, int index, char* buffer, int bufferLen) {
    if (data == NULL || buffer == NULL) return 0;
    
    int lineIdx = 0;
    int offset = 0;
    char* p = data;
    
    while (*p && (p - data) < sLen && lineIdx < index) {
        if (*p == '\n') {
            lineIdx++;
        }
        p++;
    }
    
    if (lineIdx != index || !*p) {
        return 0;
    }
    
    while (*p && *p != '\n' && offset < bufferLen - 1) {
        buffer[offset++] = *p++;
    }
    buffer[offset] = '\0';
    
    return offset;
}

double StatisticsAPI::ParseNumeric(const char* value, int len) {
    if (value == NULL || len <= 0) return 0.0;
    
    // 尝试解析为整数或浮点数
    char* endPtr;
    double result = strtod(value, &endPtr);
    
    // 如果解析失败，返回 0
    if (endPtr == value) {
        return 0.0;
    }
    
    return result;
}

void StatisticsAPI::RemovePadding(const char* padded, int len, char* result, int& resultLen) {
    int i = 0;
    while (i < len && padded[i] == ' ') {
        i++;
    }
    
    resultLen = len - i;
    if (resultLen > 0) {
        memcpy(result, padded + i, resultLen);
    }
    result[resultLen] = '\0';
}

// 通过友元访问 LogStoreApi 的私有方法
int StatisticsAPI::DeCompressCapsule(int patName, Coffer* &coffer, int type) {
    if (m_api == NULL) return -1;
    return m_api->DeCompressCapsule(patName, coffer, type);
}

double StatisticsAPI::GetVarAvg(int varname, BitMap* filter) {
    Coffer* meta;
    int ret = DeCompressCapsule(varname, meta);
    if (ret <= 0 || meta == NULL || meta->data == NULL) {
        return 0.0;
    }
    
    int totalCount = meta->lines;
    if (totalCount <= 0) return 0.0;
    
    double sum = 0.0;
    int count = 0;
    char buffer[1024];
    
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    if (meta->eleLen > 0) {
        // 固定长度存储
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                sum += value;
                count++;
            }
        }
    } else {
        // 变长存储
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                sum += value;
                count++;
            }
        }
    }
    
    return (count > 0) ? (sum / count) : 0.0;
}

double StatisticsAPI::GetVarMax(int varname, BitMap* filter) {
    Coffer* meta;
    int ret = DeCompressCapsule(varname, meta);
    if (ret <= 0 || meta == NULL || meta->data == NULL) {
        return 0.0;
    }
    
    int totalCount = meta->lines;
    if (totalCount <= 0) return 0.0;
    
    double maxVal = -1e308;
    bool found = false;
    char buffer[1024];
    
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    if (meta->eleLen > 0) {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                if (!found || value > maxVal) {
                    maxVal = value;
                    found = true;
                }
            }
        }
    } else {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                if (!found || value > maxVal) {
                    maxVal = value;
                    found = true;
                }
            }
        }
    }
    
    return found ? maxVal : 0.0;
}

double StatisticsAPI::GetVarMin(int varname, BitMap* filter) {
    Coffer* meta;
    int ret = DeCompressCapsule(varname, meta);
    if (ret <= 0 || meta == NULL || meta->data == NULL) {
        return 0.0;
    }
    
    int totalCount = meta->lines;
    if (totalCount <= 0) return 0.0;
    
    double minVal = 1e308;
    bool found = false;
    char buffer[1024];
    
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    if (meta->eleLen > 0) {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                if (!found || value < minVal) {
                    minVal = value;
                    found = true;
                }
            }
        }
    } else {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                if (!found || value < minVal) {
                    minVal = value;
                    found = true;
                }
            }
        }
    }
    
    return found ? minVal : 0.0;
}

double StatisticsAPI::GetVarSum(int varname, BitMap* filter) {
    Coffer* meta;
    int ret = DeCompressCapsule(varname, meta);
    if (ret <= 0 || meta == NULL || meta->data == NULL) {
        return 0.0;
    }
    
    int totalCount = meta->lines;
    double sum = 0.0;
    char buffer[1024];
    
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    if (meta->eleLen > 0) {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer));
            if (len > 0) {
                sum += ParseNumeric(buffer, len);
            }
        }
    } else {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) {
                sum += ParseNumeric(buffer, len);
            }
        }
    }
    
    return sum;
}

int StatisticsAPI::GetVarCount(int varname, BitMap* filter) {
    Coffer* meta;
    int ret = DeCompressCapsule(varname, meta);
    if (ret <= 0 || meta == NULL) {
        return 0;
    }
    
    int totalCount = meta->lines;
    
    if (filter == NULL || filter->GetSize() == 0) {
        return totalCount;
    }
    
    // 统计过滤后的数量
    int count = 0;
    for (int i = 0; i < totalCount; i++) {
        if (filter->GetValue(i) == 1) {
            count++;
        }
    }
    
    return count;
}

int StatisticsAPI::GetVarDistinctCount(int varname, BitMap* filter) {
    Coffer* meta;
    int ret = DeCompressCapsule(varname, meta);
    if (ret <= 0 || meta == NULL || meta->data == NULL) {
        return 0;
    }
    
    int totalCount = meta->lines;
    std::map<std::string, int> distinctValues;
    char buffer[1024];
    
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    if (meta->eleLen > 0) {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer));
            if (len > 0) {
                std::string value(buffer, len);
                distinctValues[value] = 1;
            }
        }
    } else {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) {
                std::string value(buffer, len);
                distinctValues[value] = 1;
            }
        }
    }
    
    return distinctValues.size();
}

void StatisticsAPI::BuildHLL(int varname, BitMap* filter, HyperLogLog& h) {
    Coffer* meta;
    int ret = DeCompressCapsule(varname, meta);
    if (ret <= 0 || meta == NULL || meta->data == NULL) {
        return;
    }
    int totalCount = meta->lines;
    char buffer[1024];
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    if (meta->eleLen > 0) {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer));
            if (len > 0) h.add(buffer, (size_t)len);
        }
    } else {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) h.add(buffer, (size_t)len);
        }
    }
}

std::map<std::string, int> StatisticsAPI::GetVarFrequency(int varname, int topK, BitMap* filter) {
    std::map<std::string, int> frequency;
    Coffer* meta;
    int ret = DeCompressCapsule(varname, meta);
    if (ret <= 0 || meta == NULL || meta->data == NULL) {
        return frequency;
    }
    
    int totalCount = meta->lines;
    char buffer[1024];
    
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    // 统计频率
    if (meta->eleLen > 0) {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer));
            if (len > 0) {
                std::string value(buffer, len);
                frequency[value]++;
            }
        }
    } else {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) {
                std::string value(buffer, len);
                frequency[value]++;
            }
        }
    }
    
    // 如果指定了 topK，只返回前 K 个
    if (topK > 0 && topK < (int)frequency.size()) {
        std::vector<std::pair<std::string, int> > sorted;
        for (auto it = frequency.begin(); it != frequency.end(); ++it) {
            sorted.push_back(*it);
        }
        
        std::sort(sorted.begin(), sorted.end(), 
            [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {
                return a.second > b.second;
            });
        
        frequency.clear();
        for (int i = 0; i < topK && i < (int)sorted.size(); i++) {
            frequency[sorted[i].first] = sorted[i].second;
        }
    }
    
    return frequency;
}

std::vector<std::pair<std::string, int> > StatisticsAPI::GetVarTopK(int varname, int k, BitMap* filter) {
    std::map<std::string, int> frequency = GetVarFrequency(varname, 0, filter);
    std::vector<std::pair<std::string, int> > result;
    
    for (auto it = frequency.begin(); it != frequency.end(); ++it) {
        result.push_back(*it);
    }
    
    std::sort(result.begin(), result.end(), 
        [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b) {
            return a.second > b.second;
        });
    
    if (k > 0 && k < (int)result.size()) {
        result.resize(k);
    }
    
    return result;
}

double StatisticsAPI::GetVarMedian(int varname, BitMap* filter) {
    Coffer* meta;
    int ret = DeCompressCapsule(varname, meta);
    if (ret <= 0 || meta == NULL || meta->data == NULL) {
        return 0.0;
    }
    
    int totalCount = meta->lines;
    if (totalCount <= 0) return 0.0;
    
    std::vector<double> values;
    char buffer[1024];
    
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    // 收集所有值
    if (meta->eleLen > 0) {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                values.push_back(value);
            }
        }
    } else {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                values.push_back(value);
            }
        }
    }
    
    if (values.empty()) return 0.0;
    
    // 排序
    std::sort(values.begin(), values.end());
    
    // 计算中位数
    size_t n = values.size();
    if (n % 2 == 0) {
        return (values[n/2 - 1] + values[n/2]) / 2.0;
    } else {
        return values[n/2];
    }
}

double StatisticsAPI::GetVarStdDev(int varname, BitMap* filter) {
    double avg = GetVarAvg(varname, filter);
    
    Coffer* meta;
    int ret = DeCompressCapsule(varname, meta);
    if (ret <= 0 || meta == NULL || meta->data == NULL) {
        return 0.0;
    }
    
    int totalCount = meta->lines;
    if (totalCount <= 0) return 0.0;
    
    double sumSquaredDiff = 0.0;
    int count = 0;
    char buffer[1024];
    
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    // 计算方差
    if (meta->eleLen > 0) {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                double diff = value - avg;
                sumSquaredDiff += diff * diff;
                count++;
            }
        }
    } else {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                double diff = value - avg;
                sumSquaredDiff += diff * diff;
                count++;
            }
        }
    }
    
    if (count <= 1) return 0.0;
    
    // 计算标准差
    double variance = sumSquaredDiff / count;
    return sqrt(variance);
}

double StatisticsAPI::GetVarPercentile(int varname, double percentile, BitMap* filter) {
    if (percentile < 0.0 || percentile > 100.0) {
        return 0.0;
    }
    
    Coffer* meta;
    int ret = DeCompressCapsule(varname, meta);
    if (ret <= 0 || meta == NULL || meta->data == NULL) {
        return 0.0;
    }
    
    int totalCount = meta->lines;
    if (totalCount <= 0) return 0.0;
    
    std::vector<double> values;
    char buffer[1024];
    
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    // 收集所有值
    if (meta->eleLen > 0) {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                values.push_back(value);
            }
        }
    } else {
        for (int i = 0; i < totalCount; i++) {
            if (useFilter && filter->GetValue(i) == 0) {
                continue;
            }
            
            int len = ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) {
                double value = ParseNumeric(buffer, len);
                values.push_back(value);
            }
        }
    }
    
    if (values.empty()) return 0.0;
    
    // 排序
    std::sort(values.begin(), values.end());
    
    // 计算百分位数位置
    double position = (percentile / 100.0) * (values.size() - 1);
    size_t lower = (size_t)floor(position);
    size_t upper = (size_t)ceil(position);
    
    if (lower == upper) {
        return values[lower];
    }
    
    // 线性插值
    double weight = position - lower;
    return values[lower] * (1.0 - weight) + values[upper] * weight;
}

std::map<std::string, double> StatisticsAPI::GetVarGroupByAvg(int groupVar, int valueVar, BitMap* filter) {
    std::map<std::string, double> result;
    
    // 解压分组变量和数值变量
    Coffer* groupMeta;
    Coffer* valueMeta;
    int ret1 = DeCompressCapsule(groupVar, groupMeta);
    int ret2 = DeCompressCapsule(valueVar, valueMeta);
    
    if (ret1 <= 0 || ret2 <= 0 || groupMeta == NULL || valueMeta == NULL ||
        groupMeta->data == NULL || valueMeta->data == NULL) {
        return result;
    }
    
    int totalCount = groupMeta->lines;
    if (totalCount != valueMeta->lines || totalCount <= 0) {
        return result;
    }
    
    // 按分组变量分组，统计数值变量的总和和计数
    std::map<std::string, double> sumMap;
    std::map<std::string, int> countMap;
    
    char groupBuffer[1024];
    char valueBuffer[1024];
    
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    for (int i = 0; i < totalCount; i++) {
        if (useFilter && filter->GetValue(i) == 0) {
            continue;
        }
        
        // 读取分组变量值
        int groupLen = 0;
        if (groupMeta->eleLen > 0) {
            groupLen = ReadValue_Fixed(groupMeta->data, i, groupMeta->eleLen, groupBuffer, sizeof(groupBuffer));
        } else {
            groupLen = ReadValue_Diff(groupMeta->data, groupMeta->srcLen, i, groupBuffer, sizeof(groupBuffer));
        }
        
        if (groupLen <= 0) continue;
        
        // 读取数值变量值
        int valueLen = 0;
        if (valueMeta->eleLen > 0) {
            valueLen = ReadValue_Fixed(valueMeta->data, i, valueMeta->eleLen, valueBuffer, sizeof(valueBuffer));
        } else {
            valueLen = ReadValue_Diff(valueMeta->data, valueMeta->srcLen, i, valueBuffer, sizeof(valueBuffer));
        }
        
        if (valueLen <= 0) continue;
        
        std::string groupKey(groupBuffer, groupLen);
        double value = ParseNumeric(valueBuffer, valueLen);
        
        sumMap[groupKey] += value;
        countMap[groupKey]++;
    }
    
    // 计算平均值
    for (auto it = sumMap.begin(); it != sumMap.end(); ++it) {
        const std::string& key = it->first;
        if (countMap[key] > 0) {
            result[key] = it->second / countMap[key];
        }
    }
    
    return result;
}

std::map<std::string, double> StatisticsAPI::GetVarGroupBySum(int groupVar, int valueVar, BitMap* filter) {
    std::map<std::string, double> result;
    
    Coffer* groupMeta;
    Coffer* valueMeta;
    int ret1 = DeCompressCapsule(groupVar, groupMeta);
    int ret2 = DeCompressCapsule(valueVar, valueMeta);
    
    if (ret1 <= 0 || ret2 <= 0 || groupMeta == NULL || valueMeta == NULL ||
        groupMeta->data == NULL || valueMeta->data == NULL) {
        return result;
    }
    
    int totalCount = groupMeta->lines;
    if (totalCount != valueMeta->lines || totalCount <= 0) {
        return result;
    }
    
    char groupBuffer[1024];
    char valueBuffer[1024];
    
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    for (int i = 0; i < totalCount; i++) {
        if (useFilter && filter->GetValue(i) == 0) {
            continue;
        }
        
        int groupLen = 0;
        if (groupMeta->eleLen > 0) {
            groupLen = ReadValue_Fixed(groupMeta->data, i, groupMeta->eleLen, groupBuffer, sizeof(groupBuffer));
        } else {
            groupLen = ReadValue_Diff(groupMeta->data, groupMeta->srcLen, i, groupBuffer, sizeof(groupBuffer));
        }
        
        if (groupLen <= 0) continue;
        
        int valueLen = 0;
        if (valueMeta->eleLen > 0) {
            valueLen = ReadValue_Fixed(valueMeta->data, i, valueMeta->eleLen, valueBuffer, sizeof(valueBuffer));
        } else {
            valueLen = ReadValue_Diff(valueMeta->data, valueMeta->srcLen, i, valueBuffer, sizeof(valueBuffer));
        }
        
        if (valueLen <= 0) continue;
        
        std::string groupKey(groupBuffer, groupLen);
        double value = ParseNumeric(valueBuffer, valueLen);
        
        result[groupKey] += value;
    }
    
    return result;
}

std::map<std::string, int> StatisticsAPI::GetVarGroupByCount(int groupVar, BitMap* filter) {
    std::map<std::string, int> result;
    
    Coffer* meta;
    int ret = DeCompressCapsule(groupVar, meta);
    if (ret <= 0 || meta == NULL || meta->data == NULL) {
        return result;
    }
    
    int totalCount = meta->lines;
    if (totalCount <= 0) return result;
    
    char buffer[1024];
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    
    for (int i = 0; i < totalCount; i++) {
        if (useFilter && filter->GetValue(i) == 0) {
            continue;
        }
        
        int len = 0;
        if (meta->eleLen > 0) {
            len = ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer));
        } else {
            len = ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
        }
        
        if (len > 0) {
            std::string key(buffer, len);
            result[key]++;
        }
    }
    
    return result;
}

std::map<std::string, int> StatisticsAPI::GetVarGroupByDistinctCount(int groupVar, int valueVar, BitMap* filter) {
    std::map<std::string, int> result;
    Coffer* groupMeta;
    Coffer* valueMeta;
    int ret1 = DeCompressCapsule(groupVar, groupMeta);
    int ret2 = DeCompressCapsule(valueVar, valueMeta);
    if (ret1 <= 0 || ret2 <= 0 || groupMeta == NULL || valueMeta == NULL || groupMeta->data == NULL || valueMeta->data == NULL) {
        return result;
    }
    int totalCount = groupMeta->lines;
    if (totalCount != valueMeta->lines || totalCount <= 0) {
        return result;
    }
    char groupBuffer[1024];
    char valueBuffer[1024];
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    std::map<std::string, HyperLogLog> hllMap;
    for (int i = 0; i < totalCount; i++) {
        if (useFilter && filter->GetValue(i) == 0) {
            continue;
        }
        int groupLen = 0;
        if (groupMeta->eleLen > 0) {
            groupLen = ReadValue_Fixed(groupMeta->data, i, groupMeta->eleLen, groupBuffer, sizeof(groupBuffer));
        } else {
            groupLen = ReadValue_Diff(groupMeta->data, groupMeta->srcLen, i, groupBuffer, sizeof(groupBuffer));
        }
        if (groupLen <= 0) continue;
        int valueLen = 0;
        if (valueMeta->eleLen > 0) {
            valueLen = ReadValue_Fixed(valueMeta->data, i, valueMeta->eleLen, valueBuffer, sizeof(valueBuffer));
        } else {
            valueLen = ReadValue_Diff(valueMeta->data, valueMeta->srcLen, i, valueBuffer, sizeof(valueBuffer));
        }
        if (valueLen <= 0) continue;
        std::string groupKey(groupBuffer, groupLen);
        HyperLogLog& h = hllMap[groupKey];
        if (&h == NULL) { /* noop */ }
        h.add(valueBuffer, (size_t)valueLen);
    }
    for (auto it = hllMap.begin(); it != hllMap.end(); ++it) {
        result[it->first] = (int)std::llround(it->second.estimate());
    }
    return result;
}

