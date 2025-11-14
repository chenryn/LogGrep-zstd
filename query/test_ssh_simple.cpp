/**
 * SSH 示例数据统计测试程序（简化版）
 * 测试 Host 字段的分组统计功能
 * 
 * 编译：
 * cd query
 * make
 * g++ -std=c++11 -o test_ssh_simple test_ssh_simple.cpp StatisticsAPI.o LogStore_API.o \
 *     LogStructure.o SearchAlgorithm.o LogDispatcher.o var_alias.o Coffer.o \
 *     -I. -I../compression -I../zstd-dev/lib ../zstd-dev/lib/libzstd.a -ldl
 * 
 * 运行：
 * ./test_ssh_simple ../lib_output_zip/Ssh 0.log.zip
 */

#include "StatisticsAPI.h"
#include "LogStore_API.h"
#include "var_alias.h"
#include <iostream>
#include <iomanip>
#include <cstdio>
#include <vector>
#include <map>
#include <string>

using namespace std;

// 将变量别名（如 "E7_V4"）转换为变量ID
int parseVarId(const string& varStr) {
    size_t ePos = varStr.find('E');
    size_t vPos = varStr.find('_V');
    
    if (ePos == string::npos || vPos == string::npos) {
        return -1;
    }
    
    int templateId = stoi(varStr.substr(ePos + 1, vPos - ePos - 1));
    int varId = stoi(varStr.substr(vPos + 2));
    
    return (templateId << 16) | (varId << 8) | (2 << 0);
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
    cout << "日志路径: " << zipPath << endl << endl;
    
    // 连接日志存储
    LogStoreApi api;
    VarAliasManager* aliasMgr = VarAliasManager::getInstance();
    aliasMgr->setDefaultConfigPath(zipPath + ".var_alias");
    aliasMgr->initializeForZip(zipPath);
    
    int ret = api.Connect((char*)logStorePath, (char*)fileName);
    if (ret <= 0) {
        cout << "错误: 无法连接到日志存储" << endl;
        return 1;
    }
    
    cout << "✓ 成功连接，找到 " << ret << " 个模板" << endl << endl;
    
    StatisticsAPI stats(&api);
    
    // 查找 IP/Host 相关变量（从配置文件中看到有 "ip" 别名）
    vector<int> hostVars;
    vector<string> hostVarNames;
    
    vector<string> hostAliases = {"ip", "Host", "hostname", "host"};
    for (const string& alias : hostAliases) {
        vector<int> varIds = aliasMgr->getVarIds(alias);
        if (!varIds.empty()) {
            cout << "找到别名 \"" << alias << "\" 对应的变量: ";
            for (size_t i = 0; i < varIds.size() && i < 5; i++) {  // 只取前5个
                if (i > 0) cout << ", ";
                cout << "0x" << hex << varIds[i] << dec;
                hostVars.push_back(varIds[i] | VAR_TYPE_VAR);
                hostVarNames.push_back(alias);
            }
            cout << endl;
        }
    }
    
    // 如果没找到，使用常见的IP变量（从配置文件中看到的）
    if (hostVars.empty()) {
        cout << "未找到别名，使用常见IP变量: E7_V4, E6_V4" << endl;
        hostVars.push_back((7 << 16) | (4 << 8) | (2 << 0));  // E7_V4
        hostVars.push_back((6 << 16) | (4 << 8) | (2 << 0));  // E6_V4
        hostVarNames.push_back("E7_V4");
        hostVarNames.push_back("E6_V4");
    }
    
    // 选择一个有数据的Host变量
    if (!hostVars.empty()) {
        int hostVar = hostVars[0];
        string varName = hostVarNames[0];
        for (size_t i = 0; i < hostVars.size(); ++i) {
            int cntTry = 0;
            try { cntTry = stats.GetVarCount(hostVars[i]); } catch (...) { cntTry = 0; }
            if (cntTry > 0) {
                hostVar = hostVars[i];
                varName = hostVarNames[i];
                break;
            }
        }
        
        cout << "\n==========================================" << endl;
        cout << "测试变量: " << varName << " (0x" << hex << hostVar << dec << ")" << endl;
        cout << "==========================================" << endl;
        
        try {
            int count = stats.GetVarCount(hostVar);
            if (count == 0) {
                int alt1 = (7 << 16) | (4 << 8) | VAR_TYPE_VAR;
                int alt2 = (6 << 16) | (4 << 8) | VAR_TYPE_VAR;
                int c1 = 0, c2 = 0;
                try { c1 = stats.GetVarCount(alt1); } catch (...) { c1 = 0; }
                try { c2 = stats.GetVarCount(alt2); } catch (...) { c2 = 0; }
                if (c1 > 0 || c2 > 0) {
                    if (c1 >= c2) { hostVar = alt1; varName = "E7_V4"; count = c1; }
                    else { hostVar = alt2; varName = "E6_V4"; count = c2; }
                }
            }
            cout << "总记录数: " << count << endl;
            
            if (count > 0) {
                int distinct = stats.GetVarDistinctCount(hostVar);
                cout << "唯一 Host 数量: " << distinct << endl;
                
                // Top 10 频率分布
                cout << "\nTop 10 Host 频率分布:" << endl;
                map<string, int> freq = stats.GetVarFrequency(hostVar, 10);
                
                int rank = 1;
                for (auto it = freq.begin(); it != freq.end() && rank <= 10; ++it, rank++) {
                    double percentage = (it->second * 100.0) / count;
                    cout << "  " << setw(2) << rank << ". " 
                         << setw(35) << left << it->first 
                         << " : " << setw(8) << right << it->second 
                         << " 次 (" << fixed << setprecision(2) << percentage << "%)" << endl;
                }
                
                // 分组计数统计
                cout << "\n分组计数统计 (前10组):" << endl;
                map<string, int> groupCount = stats.GetVarGroupByCount(hostVar);
                
                if (!groupCount.empty()) {
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
                        cout << "  " << setw(35) << left << p.first 
                             << " : " << setw(8) << right << p.second 
                             << " 次 (" << fixed << setprecision(2) << percentage << "%)" << endl;
                        shown++;
                    }
                    
                    if (sorted.size() > 10) {
                        cout << "  ... (共 " << sorted.size() << " 个不同的 Host)" << endl;
                    }
                }
                
                cout << "\n✓ 统计完成！" << endl;
            }
        } catch (const exception& e) {
            cout << "✗ 错误: " << e.what() << endl;
        }
    }
    
    api.DisConnect();
    cout << "\n测试完成！" << endl;
    return 0;
}
