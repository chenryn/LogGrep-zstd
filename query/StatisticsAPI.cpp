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
#include <set>
#include <vector>
#include <map>
#include <string>

bool StatisticsAPI::IsNumericType(int varType) {
    return (varType & 1) != 0 || (varType & 2) != 0 || (varType & 4) != 0;
}

int StatisticsAPI::ReadValue_Fixed(char* data, int index, int eleLen, char* buffer, int bufferLen) {
    if (data == NULL || buffer == NULL || eleLen <= 0) return 0;
    int offset = index * eleLen;
    int i = 0;
    while (i < eleLen && data[offset + i] == ' ') i++;
    int actualLen = eleLen - i;
    if (actualLen > bufferLen - 1) actualLen = bufferLen - 1;
    if (actualLen > 0) memcpy(buffer, data + offset + i, actualLen);
    buffer[actualLen] = '\0';
    return actualLen;
}

int StatisticsAPI::ReadValue_Diff(char* data, int sLen, int index, char* buffer, int bufferLen) {
    if (data == NULL || buffer == NULL) return 0;
    int lineIdx = 0;
    char* p = data;
    while (*p && (p - data) < sLen && lineIdx < index) {
        if (*p == '\n') lineIdx++;
        p++;
    }
    if (lineIdx != index || !*p) return 0;
    int offset = 0;
    while (*p && *p != '\n' && offset < bufferLen - 1) buffer[offset++] = *p++;
    buffer[offset] = '\0';
    return offset;
}

double StatisticsAPI::ParseNumeric(const char* value, int len) {
    if (value == NULL || len <= 0) return 0.0;
    char* endPtr;
    double result = strtod(value, &endPtr);
    if (endPtr == value) return 0.0;
    return result;
}

void StatisticsAPI::RemovePadding(const char* padded, int len, char* result, int& resultLen) {
    int i = 0;
    while (i < len && padded[i] == ' ') i++;
    resultLen = len - i;
    if (resultLen > 0) memcpy(result, padded + i, resultLen);
    result[resultLen] = '\0';
}

int StatisticsAPI::DeCompressCapsule(int patName, Coffer* &coffer, int type) {
    if (m_api == NULL) return -1;
    return m_api->DeCompressCapsule(patName, coffer, type);
}

double StatisticsAPI::GetVarAvg(int varname, BitMap* filter) {
    int varType = m_api->GetVarType(varname);
    double sum = 0.0;
    int count = 0;
    char buffer[MAX_VALUE_LEN];
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    if (varType == VAR_TYPE_DIC) {
        int dicname = varname + VAR_TYPE_DIC, entryname = varname + VAR_TYPE_ENTRY;
        Coffer *entryMeta, *dicMeta;
        if (DeCompressCapsule(entryname, entryMeta, 1) <= 0 || !entryMeta || !entryMeta->data) return 0.0;
        if (DeCompressCapsule(dicname, dicMeta, 1) <= 0 || !dicMeta || !dicMeta->data) return 0.0;
        char* entryBuf = entryMeta->data; int entryLen = entryMeta->eleLen;
        for (int i = 0; i < entryMeta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) { entryBuf += entryLen; continue; }
            int dicIdx = atoi(entryBuf, entryLen); entryBuf += entryLen;
            if (dicIdx < dicMeta->lines) {
                int dicLen = 0; int offset = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[varname], dicIdx, dicLen);
                m_api->RemovePadding(dicMeta->data + offset, dicLen, buffer);
                sum += ParseNumeric(buffer, strlen(buffer)); count++;
            }
        }
    } else {
        int targetVar = varname + (varType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR);
        Coffer* meta;
        if (DeCompressCapsule(targetVar, meta) <= 0 || !meta || !meta->data) return 0.0;
        for (int i = 0; i < meta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = (meta->eleLen > 0) ? ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer)) : ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) { sum += ParseNumeric(buffer, len); count++; }
        }
    }
    return (count > 0) ? (sum / count) : 0.0;
}

double StatisticsAPI::GetVarMax(int varname, BitMap* filter) {
    int varType = m_api->GetVarType(varname);
    double maxVal = -1e308; bool found = false;
    char buffer[MAX_VALUE_LEN]; bool useFilter = (filter != NULL && filter->GetSize() > 0);
    if (varType == VAR_TYPE_DIC) {
        int dicname = varname + VAR_TYPE_DIC, entryname = varname + VAR_TYPE_ENTRY;
        Coffer *entryMeta, *dicMeta;
        if (DeCompressCapsule(entryname, entryMeta, 1) <= 0 || !entryMeta || !entryMeta->data) return 0.0;
        if (DeCompressCapsule(dicname, dicMeta, 1) <= 0 || !dicMeta || !dicMeta->data) return 0.0;
        char* entryBuf = entryMeta->data; int entryLen = entryMeta->eleLen;
        for (int i = 0; i < entryMeta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) { entryBuf += entryLen; continue; }
            int dicIdx = atoi(entryBuf, entryLen); entryBuf += entryLen;
            if (dicIdx < dicMeta->lines) {
                int dicLen = 0; int offset = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[varname], dicIdx, dicLen);
                m_api->RemovePadding(dicMeta->data + offset, dicLen, buffer);
                double v = ParseNumeric(buffer, strlen(buffer)); if (!found || v > maxVal) { maxVal = v; found = true; }
            }
        }
    } else {
        int targetVar = varname + (varType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR);
        Coffer* meta;
        if (DeCompressCapsule(targetVar, meta) <= 0 || !meta || !meta->data) return 0.0;
        for (int i = 0; i < meta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = (meta->eleLen > 0) ? ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer)) : ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) { double v = ParseNumeric(buffer, len); if (!found || v > maxVal) { maxVal = v; found = true; } }
        }
    }
    return found ? maxVal : 0.0;
}

double StatisticsAPI::GetVarMin(int varname, BitMap* filter) {
    int varType = m_api->GetVarType(varname);
    double minVal = 1e308; bool found = false;
    char buffer[MAX_VALUE_LEN]; bool useFilter = (filter != NULL && filter->GetSize() > 0);
    if (varType == VAR_TYPE_DIC) {
        int dicname = varname + VAR_TYPE_DIC, entryname = varname + VAR_TYPE_ENTRY;
        Coffer *entryMeta, *dicMeta;
        if (DeCompressCapsule(entryname, entryMeta, 1) <= 0 || !entryMeta || !entryMeta->data) return 0.0;
        if (DeCompressCapsule(dicname, dicMeta, 1) <= 0 || !dicMeta || !dicMeta->data) return 0.0;
        char* entryBuf = entryMeta->data; int entryLen = entryMeta->eleLen;
        for (int i = 0; i < entryMeta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) { entryBuf += entryLen; continue; }
            int dicIdx = atoi(entryBuf, entryLen); entryBuf += entryLen;
            if (dicIdx < dicMeta->lines) {
                int dicLen = 0; int offset = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[varname], dicIdx, dicLen);
                m_api->RemovePadding(dicMeta->data + offset, dicLen, buffer);
                double v = ParseNumeric(buffer, strlen(buffer)); if (!found || v < minVal) { minVal = v; found = true; }
            }
        }
    } else {
        int targetVar = varname + (varType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR);
        Coffer* meta;
        if (DeCompressCapsule(targetVar, meta) <= 0 || !meta || !meta->data) return 0.0;
        for (int i = 0; i < meta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = (meta->eleLen > 0) ? ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer)) : ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) { double v = ParseNumeric(buffer, len); if (!found || v < minVal) { minVal = v; found = true; } }
        }
    }
    return found ? minVal : 0.0;
}

double StatisticsAPI::GetVarSum(int varname, BitMap* filter) {
    int varType = m_api->GetVarType(varname);
    double sum = 0.0; char buffer[MAX_VALUE_LEN]; bool useFilter = (filter != NULL && filter->GetSize() > 0);
    if (varType == VAR_TYPE_DIC) {
        int dicname = varname + VAR_TYPE_DIC, entryname = varname + VAR_TYPE_ENTRY;
        Coffer *entryMeta, *dicMeta;
        if (DeCompressCapsule(entryname, entryMeta, 1) <= 0 || !entryMeta || !entryMeta->data) return 0.0;
        if (DeCompressCapsule(dicname, dicMeta, 1) <= 0 || !dicMeta || !dicMeta->data) return 0.0;
        char* entryBuf = entryMeta->data; int entryLen = entryMeta->eleLen;
        for (int i = 0; i < entryMeta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) { entryBuf += entryLen; continue; }
            int dicIdx = atoi(entryBuf, entryLen); entryBuf += entryLen;
            if (dicIdx < dicMeta->lines) {
                int dicLen = 0; int offset = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[varname], dicIdx, dicLen);
                m_api->RemovePadding(dicMeta->data + offset, dicLen, buffer);
                sum += ParseNumeric(buffer, strlen(buffer));
            }
        }
    } else {
        int targetVar = varname + (varType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR);
        Coffer* meta;
        if (DeCompressCapsule(targetVar, meta) <= 0 || !meta || !meta->data) return 0.0;
        for (int i = 0; i < meta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = (meta->eleLen > 0) ? ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer)) : ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) sum += ParseNumeric(buffer, len);
        }
    }
    return sum;
}

int StatisticsAPI::GetVarCount(int varname, BitMap* filter) {
    int varType = m_api->GetVarType(varname);
    int targetVar = (varType == VAR_TYPE_DIC) ? (varname + VAR_TYPE_ENTRY) : (varname + (varType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR));
    Coffer* meta;
    if (DeCompressCapsule(targetVar, meta, (varType == VAR_TYPE_DIC ? 1 : 0)) <= 0 || !meta) return 0;
    if (filter == NULL || filter->GetSize() == 0) return meta->lines;
    int count = 0;
    for (int i = 0; i < meta->lines; i++) if (filter->GetValue(i) == 1) count++;
    return count;
}

int StatisticsAPI::GetVarDistinctCount(int varname, BitMap* filter) {
    int varType = m_api->GetVarType(varname);
    std::set<std::string> distinctValues; char buffer[MAX_VALUE_LEN]; bool useFilter = (filter != NULL && filter->GetSize() > 0);
    if (varType == VAR_TYPE_DIC) {
        int dicname = varname + VAR_TYPE_DIC, entryname = varname + VAR_TYPE_ENTRY;
        Coffer *entryMeta, *dicMeta;
        if (DeCompressCapsule(entryname, entryMeta, 1) <= 0 || !entryMeta || !entryMeta->data) return 0;
        if (DeCompressCapsule(dicname, dicMeta, 1) <= 0 || !dicMeta || !dicMeta->data) return 0;
        char* entryBuf = entryMeta->data; int entryLen = entryMeta->eleLen;
        for (int i = 0; i < entryMeta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) { entryBuf += entryLen; continue; }
            int dicIdx = atoi(entryBuf, entryLen); entryBuf += entryLen;
            if (dicIdx < dicMeta->lines) {
                int dicLen = 0; int offset = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[varname], dicIdx, dicLen);
                m_api->RemovePadding(dicMeta->data + offset, dicLen, buffer);
                distinctValues.insert(buffer);
            }
        }
    } else {
        int targetVar = varname + (varType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR);
        Coffer* meta;
        if (DeCompressCapsule(targetVar, meta) <= 0 || !meta || !meta->data) return 0;
        for (int i = 0; i < meta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = (meta->eleLen > 0) ? ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer)) : ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) distinctValues.insert(std::string(buffer, len));
        }
    }
    return distinctValues.size();
}

void StatisticsAPI::BuildHLL(int varname, BitMap* filter, HyperLogLog& h) {
    int varType = m_api->GetVarType(varname);
    char buffer[MAX_VALUE_LEN]; bool useFilter = (filter != NULL && filter->GetSize() > 0);
    if (varType == VAR_TYPE_DIC) {
        int dicname = varname + VAR_TYPE_DIC, entryname = varname + VAR_TYPE_ENTRY;
        Coffer *entryMeta, *dicMeta;
        if (DeCompressCapsule(entryname, entryMeta, 1) <= 0 || !entryMeta || !entryMeta->data) return;
        if (DeCompressCapsule(dicname, dicMeta, 1) <= 0 || !dicMeta || !dicMeta->data) return;
        char* entryBuf = entryMeta->data; int entryLen = entryMeta->eleLen;
        for (int i = 0; i < entryMeta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) { entryBuf += entryLen; continue; }
            int dicIdx = atoi(entryBuf, entryLen); entryBuf += entryLen;
            if (dicIdx < dicMeta->lines) {
                int dicLen = 0; int offset = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[varname], dicIdx, dicLen);
                m_api->RemovePadding(dicMeta->data + offset, dicLen, buffer);
                h.add(buffer, strlen(buffer));
            }
        }
    } else {
        int targetVar = varname + (varType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR);
        Coffer* meta;
        if (DeCompressCapsule(targetVar, meta) <= 0 || !meta || !meta->data) return;
        for (int i = 0; i < meta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = (meta->eleLen > 0) ? ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer)) : ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) h.add(buffer, (size_t)len);
        }
    }
}

std::map<std::string, int> StatisticsAPI::GetVarFrequency(int varname, int topK, BitMap* filter) {
    std::map<std::string, int> frequency;
    int varType = m_api->GetVarType(varname);
    char buffer[MAX_VALUE_LEN]; bool useFilter = (filter != NULL && filter->GetSize() > 0);
    if (varType == VAR_TYPE_DIC) {
        int dicname = varname + VAR_TYPE_DIC, entryname = varname + VAR_TYPE_ENTRY;
        Coffer *entryMeta, *dicMeta;
        if (DeCompressCapsule(entryname, entryMeta, 1) <= 0 || !entryMeta || !entryMeta->data) return frequency;
        if (DeCompressCapsule(dicname, dicMeta, 1) <= 0 || !dicMeta || !dicMeta->data) return frequency;
        char* entryBuf = entryMeta->data; int entryLen = entryMeta->eleLen;
        for (int i = 0; i < entryMeta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) { entryBuf += entryLen; continue; }
            int dicIdx = atoi(entryBuf, entryLen); entryBuf += entryLen;
            if (dicIdx < dicMeta->lines) {
                int dicLen = 0; int offset = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[varname], dicIdx, dicLen);
                m_api->RemovePadding(dicMeta->data + offset, dicLen, buffer);
                frequency[buffer]++;
            }
        }
    } else {
        int targetVar = varname + (varType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR);
        Coffer* meta;
        if (DeCompressCapsule(targetVar, meta) <= 0 || !meta || !meta->data) return frequency;
        for (int i = 0; i < meta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = (meta->eleLen > 0) ? ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer)) : ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) frequency[std::string(buffer, len)]++;
        }
    }
    if (topK > 0 && topK < (int)frequency.size()) {
        std::vector<std::pair<std::string, int>> sorted(frequency.begin(), frequency.end());
        std::sort(sorted.begin(), sorted.end(), [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b){ return a.second > b.second; });
        frequency.clear();
        for (int i = 0; i < topK && i < (int)sorted.size(); i++) frequency[sorted[i].first] = sorted[i].second;
    }
    return frequency;
}

std::vector<std::pair<std::string, int>> StatisticsAPI::GetVarTopK(int varname, int k, BitMap* filter) {
    std::map<std::string, int> freq = GetVarFrequency(varname, 0, filter);
    std::vector<std::pair<std::string, int>> res(freq.begin(), freq.end());
    std::sort(res.begin(), res.end(), [](const std::pair<std::string, int>& a, const std::pair<std::string, int>& b){ return a.second > b.second; });
    if (k > 0 && k < (int)res.size()) res.resize(k);
    return res;
}

std::map<std::string, double> StatisticsAPI::GetVarGroupByAvg(int groupVar, int valueVar, BitMap* filter) {
    std::map<std::string, double> sums; std::map<std::string, int> counts;
    char groupBuf[MAX_VALUE_LEN], valueBuf[MAX_VALUE_LEN];
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    int gType = m_api->GetVarType(groupVar), vType = m_api->GetVarType(valueVar);
    
    Coffer *gMeta = NULL, *gDic = NULL, *vMeta = NULL, *vDic = NULL;
    if (gType == VAR_TYPE_DIC) {
        DeCompressCapsule(groupVar + VAR_TYPE_ENTRY, gMeta, 1);
        DeCompressCapsule(groupVar + VAR_TYPE_DIC, gDic, 1);
    } else {
        DeCompressCapsule(groupVar + (gType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR), gMeta);
    }
    if (vType == VAR_TYPE_DIC) {
        DeCompressCapsule(valueVar + VAR_TYPE_ENTRY, vMeta, 1);
        DeCompressCapsule(valueVar + VAR_TYPE_DIC, vDic, 1);
    } else {
        DeCompressCapsule(valueVar + (vType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR), vMeta);
    }

    if (!gMeta || !vMeta) return {};
    int lines = std::min(gMeta->lines, vMeta->lines);
    for (int i = 0; i < lines; i++) {
        if (useFilter && filter->GetValue(i) == 0) continue;
        int gl = 0;
        if (gDic) {
            int idx = atoi(gMeta->data + i * gMeta->eleLen, gMeta->eleLen);
            if (idx < gDic->lines) {
                int off = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[groupVar], idx, gl);
                m_api->RemovePadding(gDic->data + off, gl, groupBuf);
                gl = strlen(groupBuf);
            }
        } else {
            gl = (gMeta->eleLen > 0) ? ReadValue_Fixed(gMeta->data, i, gMeta->eleLen, groupBuf, sizeof(groupBuf)) : ReadValue_Diff(gMeta->data, gMeta->srcLen, i, groupBuf, sizeof(groupBuf));
        }
        int vl = 0;
        if (vDic) {
            int idx = atoi(vMeta->data + i * vMeta->eleLen, vMeta->eleLen);
            if (idx < vDic->lines) {
                int off = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[valueVar], idx, vl);
                m_api->RemovePadding(vDic->data + off, vl, valueBuf);
                vl = strlen(valueBuf);
            }
        } else {
            vl = (vMeta->eleLen > 0) ? ReadValue_Fixed(vMeta->data, i, vMeta->eleLen, valueBuf, sizeof(valueBuf)) : ReadValue_Diff(vMeta->data, vMeta->srcLen, i, valueBuf, sizeof(valueBuf));
        }
        if (gl > 0 && vl > 0) { std::string g(groupBuf, gl); sums[g] += ParseNumeric(valueBuf, vl); counts[g]++; }
    }
    std::map<std::string, double> res;
    for (auto& kv : sums) res[kv.first] = kv.second / counts[kv.first];
    return res;
}

std::map<std::string, double> StatisticsAPI::GetVarGroupBySum(int groupVar, int valueVar, BitMap* filter) {
    std::map<std::string, double> sums;
    char groupBuf[MAX_VALUE_LEN], valueBuf[MAX_VALUE_LEN];
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    int gType = m_api->GetVarType(groupVar), vType = m_api->GetVarType(valueVar);
    Coffer *gMeta = NULL, *gDic = NULL, *vMeta = NULL, *vDic = NULL;
    if (gType == VAR_TYPE_DIC) {
        DeCompressCapsule(groupVar + VAR_TYPE_ENTRY, gMeta, 1);
        DeCompressCapsule(groupVar + VAR_TYPE_DIC, gDic, 1);
    } else {
        DeCompressCapsule(groupVar + (gType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR), gMeta);
    }
    if (vType == VAR_TYPE_DIC) {
        DeCompressCapsule(valueVar + VAR_TYPE_ENTRY, vMeta, 1);
        DeCompressCapsule(valueVar + VAR_TYPE_DIC, vDic, 1);
    } else {
        DeCompressCapsule(valueVar + (vType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR), vMeta);
    }
    if (!gMeta || !vMeta) return {};
    int lines = std::min(gMeta->lines, vMeta->lines);
    for (int i = 0; i < lines; i++) {
        if (useFilter && filter->GetValue(i) == 0) continue;
        int gl = 0;
        if (gDic) {
            int idx = atoi(gMeta->data + i * gMeta->eleLen, gMeta->eleLen);
            if (idx < gDic->lines) {
                int off = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[groupVar], idx, gl);
                m_api->RemovePadding(gDic->data + off, gl, groupBuf);
                gl = strlen(groupBuf);
            }
        } else {
            gl = (gMeta->eleLen > 0) ? ReadValue_Fixed(gMeta->data, i, gMeta->eleLen, groupBuf, sizeof(groupBuf)) : ReadValue_Diff(gMeta->data, gMeta->srcLen, i, groupBuf, sizeof(groupBuf));
        }
        int vl = 0;
        if (vDic) {
            int idx = atoi(vMeta->data + i * vMeta->eleLen, vMeta->eleLen);
            if (idx < vDic->lines) {
                int off = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[valueVar], idx, vl);
                m_api->RemovePadding(vDic->data + off, vl, valueBuf);
                vl = strlen(valueBuf);
            }
        } else {
            vl = (vMeta->eleLen > 0) ? ReadValue_Fixed(vMeta->data, i, vMeta->eleLen, valueBuf, sizeof(valueBuf)) : ReadValue_Diff(vMeta->data, vMeta->srcLen, i, valueBuf, sizeof(valueBuf));
        }
        if (gl > 0 && vl > 0) sums[std::string(groupBuf, gl)] += ParseNumeric(valueBuf, vl);
    }
    return sums;
}

std::map<std::string, int> StatisticsAPI::GetVarGroupByCount(int groupVar, BitMap* filter) {
    std::map<std::string, int> counts;
    char groupBuf[MAX_VALUE_LEN]; bool useFilter = (filter != NULL && filter->GetSize() > 0);
    int gType = m_api->GetVarType(groupVar);
    Coffer *gMeta = NULL, *gDic = NULL;
    if (gType == VAR_TYPE_DIC) {
        DeCompressCapsule(groupVar + VAR_TYPE_ENTRY, gMeta, 1);
        DeCompressCapsule(groupVar + VAR_TYPE_DIC, gDic, 1);
    } else {
        DeCompressCapsule(groupVar + (gType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR), gMeta);
    }
    if (!gMeta) return {};
    for (int i = 0; i < gMeta->lines; i++) {
        if (useFilter && filter->GetValue(i) == 0) continue;
        int gl = 0;
        if (gDic) {
            int idx = atoi(gMeta->data + i * gMeta->eleLen, gMeta->eleLen);
            if (idx < gDic->lines) {
                int off = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[groupVar], idx, gl);
                m_api->RemovePadding(gDic->data + off, gl, groupBuf);
                gl = strlen(groupBuf);
            }
        } else {
            gl = (gMeta->eleLen > 0) ? ReadValue_Fixed(gMeta->data, i, gMeta->eleLen, groupBuf, sizeof(groupBuf)) : ReadValue_Diff(gMeta->data, gMeta->srcLen, i, groupBuf, sizeof(groupBuf));
        }
        if (gl > 0) counts[std::string(groupBuf, gl)]++;
    }
    return counts;
}

std::map<std::string, int> StatisticsAPI::GetVarGroupByDistinctCount(int groupVar, int valueVar, BitMap* filter) {
    std::map<std::string, std::set<std::string>> distincts;
    char groupBuf[MAX_VALUE_LEN], valueBuf[MAX_VALUE_LEN];
    bool useFilter = (filter != NULL && filter->GetSize() > 0);
    int gType = m_api->GetVarType(groupVar), vType = m_api->GetVarType(valueVar);
    Coffer *gMeta = NULL, *gDic = NULL, *vMeta = NULL, *vDic = NULL;
    if (gType == VAR_TYPE_DIC) {
        DeCompressCapsule(groupVar + VAR_TYPE_ENTRY, gMeta, 1);
        DeCompressCapsule(groupVar + VAR_TYPE_DIC, gDic, 1);
    } else {
        DeCompressCapsule(groupVar + (gType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR), gMeta);
    }
    if (vType == VAR_TYPE_DIC) {
        DeCompressCapsule(valueVar + VAR_TYPE_ENTRY, vMeta, 1);
        DeCompressCapsule(valueVar + VAR_TYPE_DIC, vDic, 1);
    } else {
        DeCompressCapsule(valueVar + (vType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR), vMeta);
    }
    if (!gMeta || !vMeta) return {};
    int lines = std::min(gMeta->lines, vMeta->lines);
    for (int i = 0; i < lines; i++) {
        if (useFilter && filter->GetValue(i) == 0) continue;
        int gl = 0;
        if (gDic) {
            int idx = atoi(gMeta->data + i * gMeta->eleLen, gMeta->eleLen);
            if (idx < gDic->lines) {
                int off = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[groupVar], idx, gl);
                m_api->RemovePadding(gDic->data + off, gl, groupBuf);
                gl = strlen(groupBuf);
            }
        } else {
            gl = (gMeta->eleLen > 0) ? ReadValue_Fixed(gMeta->data, i, gMeta->eleLen, groupBuf, sizeof(groupBuf)) : ReadValue_Diff(gMeta->data, gMeta->srcLen, i, groupBuf, sizeof(groupBuf));
        }
        int vl = 0;
        if (vDic) {
            int idx = atoi(vMeta->data + i * vMeta->eleLen, vMeta->eleLen);
            if (idx < vDic->lines) {
                int off = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[valueVar], idx, vl);
                m_api->RemovePadding(vDic->data + off, vl, valueBuf);
                vl = strlen(valueBuf);
            }
        } else {
            vl = (vMeta->eleLen > 0) ? ReadValue_Fixed(vMeta->data, i, vMeta->eleLen, valueBuf, sizeof(valueBuf)) : ReadValue_Diff(vMeta->data, vMeta->srcLen, i, valueBuf, sizeof(valueBuf));
        }
        if (gl > 0 && vl > 0) distincts[std::string(groupBuf, gl)].insert(std::string(valueBuf, vl));
    }
    std::map<std::string, int> res;
    for (auto& kv : distincts) res[kv.first] = kv.second.size();
    return res;
}

double StatisticsAPI::GetVarMedian(int varname, BitMap* filter) {
    std::vector<double> values; char buffer[MAX_VALUE_LEN]; bool useFilter = (filter != NULL && filter->GetSize() > 0);
    int varType = m_api->GetVarType(varname);
    if (varType == VAR_TYPE_DIC) {
        int dicname = varname + VAR_TYPE_DIC, entryname = varname + VAR_TYPE_ENTRY;
        Coffer *entryMeta, *dicMeta;
        if (DeCompressCapsule(entryname, entryMeta, 1) <= 0 || !entryMeta) return 0.0;
        if (DeCompressCapsule(dicname, dicMeta, 1) <= 0 || !dicMeta) return 0.0;
        for (int i = 0; i < entryMeta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int idx = atoi(entryMeta->data + i * entryMeta->eleLen, entryMeta->eleLen);
            if (idx < dicMeta->lines) {
                int len = 0; int off = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[varname], idx, len);
                m_api->RemovePadding(dicMeta->data + off, len, buffer);
                values.push_back(ParseNumeric(buffer, strlen(buffer)));
            }
        }
    } else {
        int targetVar = varname + (varType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR);
        Coffer* meta;
        if (DeCompressCapsule(targetVar, meta) <= 0 || !meta) return 0.0;
        for (int i = 0; i < meta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = (meta->eleLen > 0) ? ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer)) : ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) values.push_back(ParseNumeric(buffer, len));
        }
    }
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    size_t n = values.size();
    if (n % 2 == 0) return (values[n/2 - 1] + values[n/2]) / 2.0;
    return values[n/2];
}

double StatisticsAPI::GetVarStdDev(int varname, BitMap* filter) {
    std::vector<double> values; char buffer[MAX_VALUE_LEN]; bool useFilter = (filter != NULL && filter->GetSize() > 0);
    int varType = m_api->GetVarType(varname);
    if (varType == VAR_TYPE_DIC) {
        int dicname = varname + VAR_TYPE_DIC, entryname = varname + VAR_TYPE_ENTRY;
        Coffer *entryMeta, *dicMeta;
        if (DeCompressCapsule(entryname, entryMeta, 1) <= 0 || !entryMeta) return 0.0;
        if (DeCompressCapsule(dicname, dicMeta, 1) <= 0 || !dicMeta) return 0.0;
        for (int i = 0; i < entryMeta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int idx = atoi(entryMeta->data + i * entryMeta->eleLen, entryMeta->eleLen);
            if (idx < dicMeta->lines) {
                int len = 0; int off = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[varname], idx, len);
                m_api->RemovePadding(dicMeta->data + off, len, buffer);
                values.push_back(ParseNumeric(buffer, strlen(buffer)));
            }
        }
    } else {
        int targetVar = varname + (varType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR);
        Coffer* meta;
        if (DeCompressCapsule(targetVar, meta) <= 0 || !meta) return 0.0;
        for (int i = 0; i < meta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = (meta->eleLen > 0) ? ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer)) : ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) values.push_back(ParseNumeric(buffer, len));
        }
    }
    if (values.empty()) return 0.0;
    double sum = 0, sq_sum = 0;
    for (double v : values) { sum += v; sq_sum += v * v; }
    double mean = sum / values.size();
    double var = sq_sum / values.size() - mean * mean;
    return std::sqrt(std::max(0.0, var));
}

double StatisticsAPI::GetVarPercentile(int varname, double p, BitMap* filter) {
    std::vector<double> values; char buffer[MAX_VALUE_LEN]; bool useFilter = (filter != NULL && filter->GetSize() > 0);
    int varType = m_api->GetVarType(varname);
    if (varType == VAR_TYPE_DIC) {
        int dicname = varname + VAR_TYPE_DIC, entryname = varname + VAR_TYPE_ENTRY;
        Coffer *entryMeta, *dicMeta;
        if (DeCompressCapsule(entryname, entryMeta, 1) <= 0 || !entryMeta) return 0.0;
        if (DeCompressCapsule(dicname, dicMeta, 1) <= 0 || !dicMeta) return 0.0;
        for (int i = 0; i < entryMeta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int idx = atoi(entryMeta->data + i * entryMeta->eleLen, entryMeta->eleLen);
            if (idx < dicMeta->lines) {
                int len = 0; int off = m_api->GetDicOffsetByEntry(m_api->m_subpatterns[varname], idx, len);
                m_api->RemovePadding(dicMeta->data + off, len, buffer);
                values.push_back(ParseNumeric(buffer, strlen(buffer)));
            }
        }
    } else {
        int targetVar = varname + (varType == VAR_TYPE_SUB ? VAR_TYPE_SUB : VAR_TYPE_VAR);
        Coffer* meta;
        if (DeCompressCapsule(targetVar, meta) <= 0 || !meta) return 0.0;
        for (int i = 0; i < meta->lines; i++) {
            if (useFilter && filter->GetValue(i) == 0) continue;
            int len = (meta->eleLen > 0) ? ReadValue_Fixed(meta->data, i, meta->eleLen, buffer, sizeof(buffer)) : ReadValue_Diff(meta->data, meta->srcLen, i, buffer, sizeof(buffer));
            if (len > 0) values.push_back(ParseNumeric(buffer, len));
        }
    }
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    int idx = (int)(p * (values.size() - 1) / 100.0);
    return values[idx];
}
