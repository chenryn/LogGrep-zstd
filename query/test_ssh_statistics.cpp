/**
 * SSH 示例数据统计测试程序
 * 测试 Host 字段的分组统计功能
 * 
 * 编译：
 * cd query
 * g++ -o test_ssh_statistics test_ssh_statistics.cpp StatisticsAPI.cpp \
 *     LogStore_API.cpp LogStructure.cpp SearchAlgorithm.cpp \
 *     LogDispatcher.cpp var_alias.cpp ../compression/Coffer.cpp \
 *     -I. -I../compression -I../zstd-dev/lib \
 *     ../zstd-dev/lib/libzstd.a -ldl
 * 
 * 运行：
 * ./test_ssh_statistics ../lib_output_zip/Ssh 0.log.zip
 */

#include "StatisticsAPI.h"
#include "LogStore_API.h"
#include "var_alias.h"
#include <iostream>
#include <iomanip>
#include <cstdio>
#include <vector>
#include <map>

using namespace std;

// 将变量别名（如 "E7_V4"）转换为变量ID
int parseVarId(const string& varStr) {
    // 格式: E模板_V变量
    size_t ePos = varStr.find('E');
    size_t vPos = varStr.find('_V');
    
    if (ePos == string::npos || vPos == string::npos) {
        return -1;
    }
    
    int templateId = stoi(varStr.substr(ePos + 1, vPos - ePos - 1));
    int varId = stoi(varStr.substr(vPos + 2));
    
    // 变量ID格式: (templateId << 16) | (varId << 8) | (TYPE_VAR << 0)
    // TYPE_VAR = 2
    return (templateId << 16) | (varId << 8) | (2 << 0);
}

void printSeparator() {
    cout << "==========================================" << endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        cout << "用法: " << argv[0] << " <压缩日志目录> [日志文件名]" << endl;
        cout << "示例: " << argv[0] << " ../lib_output_zip/Ssh 0.log.zip" << endl;
        return 1;
    }
    
    const char* logStorePath = argv[1];
    const char* fileName = (argc >= 3) ? argv[2] : "0.log.zip";
    string zipPath = string(logStorePath) + "/" + string(fileName);
    
    cout << "==========================================" << endl;
    cout << "SSH 日志统计测试 - Host 字段分组统计" << endl;
    cout << "==========================================" << endl;
    cout << "日志路径: " << zipPath << endl;
    cout << endl;
    
    // 1. 连接日志存储
    cout << "步骤1: 连接日志存储..." << endl;
    LogStoreApi api;
    
    // 设置变量别名配置路径
    string aliasConfigPath = zipPath + ".var_alias";
    VarAliasManager* aliasMgr = VarAliasManager::getInstance();
    aliasMgr->setDefaultConfigPath(aliasConfigPath);
    aliasMgr->initializeForZip(zipPath);
    
    int ret = api.Connect((char*)logStorePath, (char*)fileName);
    
    if (ret <= 0) {
        cout << "错误: 无法连接到日志存储 (返回码: " << ret << ")" << endl;
        return 1;
    }
    
    cout << "✓ 成功连接，找到 " << ret << " 个模板" << endl;
    cout << endl;
    
    // 2. 创建统计 API
    cout << "步骤2: 初始化统计API..." << endl;
    StatisticsAPI stats(&api);
    cout << "✓ 统计API初始化完成" << endl;
    cout << endl;
    
    // 3. 查找 Host 相关的变量
    cout << "步骤3: 查找 Host 字段对应的变量..." << endl;
    
    // 尝试从变量别名配置中查找
    vector<int> hostVars;
    vector<string> hostVarNames;
    
    // 常见的 Host 相关别名（从配置文件中看到有 "ip" 别名）
    vector<string> hostAliases = {"Host", "hostname", "host", "rhost", "ip"};
    
    for (const string& alias : hostAliases) {
        // 使用 getVarIds 方法查找别名
        vector<int> varIds = aliasMgr->getVarIds(alias);
        if (!varIds.empty()) {
            cout << "  找到别名 \"" << alias << "\" 对应的变量: ";
            for (size_t i = 0; i < varIds.size(); i++) {
                if (i > 0) cout << ", ";
                cout << "0x" << hex << varIds[i] << dec;
                hostVars.push_back(varIds[i] | VAR_TYPE_VAR);
                hostVarNames.push_back(alias + "(" + to_string(i) + ")");
            }
            cout << endl;
        }
    }
    
    // 如果没有找到别名，尝试从配置文件中读取
    if (hostVars.empty()) {
        cout << "  未找到 Host 别名，尝试从配置文件读取..." << endl;
        FILE* aliasFile = fopen(aliasConfigPath.c_str(), "r");
        if (aliasFile) {
            char line[1024];
            while (fgets(line, sizeof(line), aliasFile)) {
                string lineStr(line);
                // 查找包含 host 的行
                if (lineStr.find("host") != string::npos || 
                    lineStr.find("Host") != string::npos) {
                    cout << "  找到配置行: " << lineStr;
                    // 解析变量ID
                    size_t colonPos = lineStr.find(':');
                    if (colonPos != string::npos) {
                        string varList = lineStr.substr(colonPos + 1);
                        // 解析变量列表，如 "E7_V4, E6_V4"
                        size_t start = 0;
                        while (start < varList.length()) {
                            size_t commaPos = varList.find(',', start);
                            string varStr = varList.substr(start, 
                                (commaPos == string::npos ? varList.length() : commaPos) - start);
                            // 去除空格
                            varStr.erase(0, varStr.find_first_not_of(" \t"));
                            varStr.erase(varStr.find_last_not_of(" \t") + 1);
                            
                            if (!varStr.empty()) {
                                int varId = parseVarId(varStr);
                                if (varId > 0) {
                                    hostVars.push_back(varId);
                                    hostVarNames.push_back(varStr);
                                    cout << "    解析变量: " << varStr << " -> 0x" << hex << varId << dec << endl;
                                }
                            }
                            
                            if (commaPos == string::npos) break;
                            start = commaPos + 1;
                        }
                    }
                }
            }
            fclose(aliasFile);
        }
    }
    
    if (hostVars.empty()) {
        cout << "  警告: 未找到 Host 相关变量，将尝试使用常见变量ID..." << endl;
        // 尝试一些常见的 Host 变量ID（根据SSH日志格式）
        // E7_V4, E6_V4 等可能是IP地址/Host
        vector<pair<int, int> > commonVars = {
            {7, 4}, {6, 4}, {1, 4}, {5, 7}, {12, 3}  // 从var_alias文件中看到的
        };
        
        for (auto& p : commonVars) {
            int varId = (p.first << 16) | (p.second << 8) | (2 << 0);
            hostVars.push_back(varId);
            hostVarNames.push_back("E" + to_string(p.first) + "_V" + to_string(p.second));
        }
    }
    
    cout << "  将测试 " << hostVars.size() << " 个可能的 Host 变量" << endl;
    cout << endl;
    
    // 4. 测试每个 Host 变量的统计
    for (size_t i = 0; i < hostVars.size(); i++) {
        int hostVar = hostVars[i];
        string varName = hostVarNames[i];
        
        printSeparator();
        cout << "测试变量: " << varName << " (0x" << hex << hostVar << dec << ")" << endl;
        printSeparator();
        
        try {
            // 基本统计
            int count = stats.GetVarCount(hostVar);
            cout << "  总记录数: " << count << endl;
            
            if (count <= 0) {
                cout << "  ⚠ 该变量没有数据，跳过" << endl;
                continue;
            }
            
            // 唯一值统计
            int distinct = stats.GetVarDistinctCount(hostVar);
            cout << "  唯一 Host 数量: " << distinct << endl;
            
            if (distinct > 0 && distinct < 100) {
                // 频率分布
                cout << "\n  Top 10 Host 频率分布:" << endl;
                map<string, int> freq = stats.GetVarFrequency(hostVar, 10);
                
                if (!freq.empty()) {
                    int rank = 1;
                    for (auto it = freq.begin(); it != freq.end(); ++it, rank++) {
                        double percentage = (it->second * 100.0) / count;
                        cout << "    " << setw(2) << rank << ". " 
                             << setw(30) << left << it->first 
                             << " : " << setw(8) << right << it->second 
                             << " 次 (" << fixed << setprecision(2) << percentage << "%)" << endl;
                    }
                } else {
                    cout << "    (无数据)" << endl;
                }
            } else if (distinct >= 100) {
                cout << "  (唯一值太多，跳过详细频率分布)" << endl;
            }
            
            // 分组计数统计
            cout << "\n  分组计数统计 (前10组):" << endl;
            map<string, int> groupCount = stats.GetVarGroupByCount(hostVar);
            
            if (!groupCount.empty()) {
                // 按计数排序
                vector<pair<string, int> > sorted;
                for (auto it = groupCount.begin(); it != groupCount.end(); ++it) {
                    sorted.push_back(*it);
                }
                sort(sorted.begin(), sorted.end(), 
                    [](const pair<string, int>& a, const pair<string, int>& b) {
                        return a.second > b.second;
                    });
                
                int shown = 0;
                for (auto& p : sorted) {
                    if (shown >= 10) break;
                    double percentage = (p.second * 100.0) / count;
                    cout << "    " << setw(30) << left << p.first 
                         << " : " << setw(8) << right << p.second 
                         << " 次 (" << fixed << setprecision(2) << percentage << "%)" << endl;
                    shown++;
                }
                
                if (sorted.size() > 10) {
                    cout << "    ... (共 " << sorted.size() << " 个不同的 Host)" << endl;
                }
            } else {
                cout << "    (无数据)" << endl;
            }
            
            cout << "\n  ✓ 统计完成" << endl;
            
        } catch (const exception& e) {
            cout << "  ✗ 错误: " << e.what() << endl;
        }
        
        cout << endl;
    }
    
    // 5. 如果有多个变量，尝试分组统计
    if (hostVars.size() >= 2) {
        printSeparator();
        cout << "额外测试: 多变量关联分析" << endl;
        printSeparator();
        
        // 尝试按第一个Host变量分组，统计第二个变量的计数
        int groupVar = hostVars[0];
        int valueVar = hostVars[1];
        
        cout << "按 " << hostVarNames[0] << " 分组，统计 " << hostVarNames[1] << " 的计数:" << endl;
        
        try {
            map<string, int> groupCount = stats.GetVarGroupByCount(valueVar);
            if (!groupCount.empty()) {
                cout << "  找到 " << groupCount.size() << " 个分组" << endl;
                // 显示前5个
                int shown = 0;
                for (auto it = groupCount.begin(); it != groupCount.end() && shown < 5; ++it, shown++) {
                    cout << "    " << it->first << ": " << it->second << " 次" << endl;
                }
            }
        } catch (const exception& e) {
            cout << "  ✗ 错误: " << e.what() << endl;
        }
        
        cout << endl;
    }
    
    api.DisConnect();
    
    printSeparator();
    cout << "测试完成！" << endl;
    printSeparator();
    
    return 0;
}
