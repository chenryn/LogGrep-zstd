/**
 * 统计功能使用示例
 * 
 * 编译方法：
 * g++ -o statistics_example statistics_example.cpp StatisticsAPI.cpp LogStore_API.cpp \
 *     LogStructure.cpp SearchAlgorithm.cpp LogDispatcher.cpp var_alias.cpp \
 *     ../compression/Coffer.cpp -I. -I../compression -I../zstd-dev/lib \
 *     ../zstd-dev/lib/libzstd.a -ldl
 * 
 * 注意：这只是一个示例，实际使用时需要根据项目结构调整编译命令
 */

#include "StatisticsAPI.h"
#include "LogStore_API.h"
#include <iostream>
#include <iomanip>
#include <cstdio>

using namespace std;

void printSeparator() {
    cout << "==========================================" << endl;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        cout << "用法: " << argv[0] << " <压缩日志目录> [日志文件名]" << endl;
        cout << "示例: " << argv[0] << " ../example_zip/Apache 0.log.zip" << endl;
        return 1;
    }
    
    const char* logStorePath = argv[1];
    const char* fileName = (argc >= 3) ? argv[2] : "0.log.zip";
    
    cout << "正在连接日志存储: " << logStorePath << "/" << fileName << endl;
    
    // 创建 LogStoreApi 实例
    LogStoreApi api;
    int ret = api.Connect((char*)logStorePath, (char*)fileName);
    
    if (ret <= 0) {
        cout << "错误: 无法连接到日志存储" << endl;
        return 1;
    }
    
    cout << "成功连接，找到 " << ret << " 个模板" << endl;
    printSeparator();
    
    // 创建统计 API 实例
    StatisticsAPI stats(&api);
    
    // 获取所有模板和变量
    vector<pair<string, LogPattern> > patterns;
    api.GetPatterns(patterns);
    
    if (patterns.empty()) {
        cout << "未找到任何模板" << endl;
        return 1;
    }
    
    // 遍历所有模板，查找数值和文本变量
    for (size_t i = 0; i < patterns.size() && i < 3; i++) {  // 只处理前3个模板作为示例
        const LogPattern& pattern = patterns[i].second;
        cout << "\n模板 " << pattern.Eid << ": " << patterns[i].first << endl;
        cout << "变量数量: " << pattern.varCount << endl;
        
        // 遍历变量
        for (int v = 0; v < pattern.varCount && v < 3; v++) {  // 只处理前3个变量
            int varTag = (pattern.Eid << 16) | (v << 8) | (2 << 0);  // TYPE_VAR = 2
            
            cout << "\n  变量 E" << pattern.Eid << "_V" << v << " (tag: 0x" 
                 << hex << varTag << dec << "):" << endl;
            
            // 基本统计
            int count = stats.GetVarCount(varTag);
            cout << "    计数: " << count << endl;
            
            if (count > 0) {
                // 尝试数值统计
                double avg = stats.GetVarAvg(varTag);
                if (avg != 0.0 || count < 100) {  // 简单判断是否为数值类型
                    double max = stats.GetVarMax(varTag);
                    double min = stats.GetVarMin(varTag);
                    double sum = stats.GetVarSum(varTag);
                    
                    cout << "    平均值: " << fixed << setprecision(2) << avg << endl;
                    cout << "    最大值: " << max << endl;
                    cout << "    最小值: " << min << endl;
                    cout << "    总和: " << sum << endl;
                    
                    if (count > 1) {
                        double median = stats.GetVarMedian(varTag);
                        double stddev = stats.GetVarStdDev(varTag);
                        double p95 = stats.GetVarPercentile(varTag, 95.0);
                        
                        cout << "    中位数: " << median << endl;
                        cout << "    标准差: " << stddev << endl;
                        cout << "    95百分位: " << p95 << endl;
                    }
                } else {
                    // 文本统计
                    int distinct = stats.GetVarDistinctCount(varTag);
                    cout << "    唯一值数量: " << distinct << endl;
                    
                    if (distinct > 0 && distinct < 100) {
                        map<string, int> freq = stats.GetVarFrequency(varTag, 10);
                        if (!freq.empty()) {
                            cout << "    Top 10 频率分布:" << endl;
                            for (auto it = freq.begin(); it != freq.end(); ++it) {
                                cout << "      \"" << it->first << "\": " << it->second << endl;
                            }
                        }
                    }
                }
            }
        }
        printSeparator();
    }
    
    // 示例：分组统计（如果有多个变量）
    if (patterns.size() >= 1 && patterns[0].second.varCount >= 2) {
        const LogPattern& pattern = patterns[0].second;
        int groupVar = (pattern.Eid << 16) | (0 << 8) | (2 << 0);
        int valueVar = (pattern.Eid << 16) | (1 << 8) | (2 << 0);
        
        cout << "\n分组统计示例:" << endl;
        cout << "按 E" << pattern.Eid << "_V0 分组，统计 E" << pattern.Eid << "_V1 的平均值" << endl;
        
        map<string, double> groupAvg = stats.GetVarGroupByAvg(groupVar, valueVar);
        if (!groupAvg.empty()) {
            cout << "分组结果 (最多显示10组):" << endl;
            int shown = 0;
            for (auto it = groupAvg.begin(); it != groupAvg.end() && shown < 10; ++it, shown++) {
                cout << "  \"" << it->first << "\": " << fixed << setprecision(2) << it->second << endl;
            }
        }
    }
    
    api.DisConnect();
    cout << "\n统计完成！" << endl;
    
    return 0;
}

