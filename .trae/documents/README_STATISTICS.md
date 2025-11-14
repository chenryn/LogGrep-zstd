# 列式存储统计功能使用指南

## 概述

基于 LogGrep-zstd 的列式存储特性，我们实现了高效的统计分析功能。这些功能充分利用了列式存储的优势，可以快速计算数值类参数的平均值、最大值、最小值等，以及文本类参数的分组数、频率分布等。

## 功能特性

### 数值类统计
- ✅ **平均值** (GetVarAvg)
- ✅ **最大值** (GetVarMax)
- ✅ **最小值** (GetVarMin)
- ✅ **总和** (GetVarSum)
- ✅ **计数** (GetVarCount)
- ✅ **中位数** (GetVarMedian)
- ✅ **标准差** (GetVarStdDev)
- ✅ **百分位数** (GetVarPercentile)

### 文本类统计
- ✅ **唯一值计数** (GetVarDistinctCount)
- ✅ **频率分布** (GetVarFrequency)
- ✅ **Top-K 值** (GetVarTopK)

### 组合统计
- ✅ **分组平均值** (GetVarGroupByAvg)
- ✅ **分组总和** (GetVarGroupBySum)
- ✅ **分组计数** (GetVarGroupByCount)

## 编译

统计功能已集成到主 Makefile 中，直接编译即可：

```bash
cd query
make
```

## 基本使用

### C++ API 使用

```cpp
#include "StatisticsAPI.h"
#include "LogStore_API.h"

// 1. 连接日志存储
LogStoreApi api;
api.Connect("/path/to/compressed/logs", "log.zip");

// 2. 创建统计 API
StatisticsAPI stats(&api);

// 3. 计算统计值
// 变量ID格式: (templateId << 16) | (varId << 8) | (type << 0)
int varTag = (5 << 16) | (1 << 8) | (2 << 0);  // E5_V1, TYPE_VAR=2

// 数值统计
double avg = stats.GetVarAvg(varTag);
double max = stats.GetVarMax(varTag);
double min = stats.GetVarMin(varTag);
int count = stats.GetVarCount(varTag);

// 文本统计
int distinct = stats.GetVarDistinctCount(varTag);
map<string, int> freq = stats.GetVarFrequency(varTag, 10);  // Top 10

// 带过滤条件的统计
BitMap* filter = new BitMap(10000);
// ... 设置过滤条件 ...
double filteredAvg = stats.GetVarAvg(varTag, filter);
```

### 变量ID计算

变量ID由三部分组成：
- **模板ID** (templateId): 左移16位
- **变量ID** (varId): 左移8位  
- **类型** (type): TYPE_VAR=2, TYPE_DIC=0, TYPE_SVAR=1

```cpp
// 示例：E5_V1 变量
int varTag = (5 << 16) | (1 << 8) | (2 << 0);  // 0x50012
```

## 使用示例

### 示例1：计算响应时间统计

```cpp
// 假设 E5_V1 是响应时间变量
int responseTimeVar = (5 << 16) | (1 << 8) | (2 << 0);

double avg = stats.GetVarAvg(responseTimeVar);
double max = stats.GetVarMax(responseTimeVar);
double min = stats.GetVarMin(responseTimeVar);
double median = stats.GetVarMedian(responseTimeVar);
double p95 = stats.GetVarPercentile(responseTimeVar, 95.0);
double stddev = stats.GetVarStdDev(responseTimeVar);

cout << "响应时间统计:" << endl;
cout << "  平均值: " << avg << " ms" << endl;
cout << "  最大值: " << max << " ms" << endl;
cout << "  最小值: " << min << " ms" << endl;
cout << "  中位数: " << median << " ms" << endl;
cout << "  95百分位: " << p95 << " ms" << endl;
cout << "  标准差: " << stddev << " ms" << endl;
```

### 示例2：统计IP地址分布

```cpp
// 假设 E3_V2 是IP地址变量
int ipVar = (3 << 16) | (2 << 8) | (2 << 0);

int distinctIPs = stats.GetVarDistinctCount(ipVar);
cout << "唯一IP地址数量: " << distinctIPs << endl;

// 获取Top 10的IP地址
vector<pair<string, int> > topIPs = stats.GetVarTopK(ipVar, 10);
cout << "Top 10 IP地址:" << endl;
for (auto& ip : topIPs) {
    cout << "  " << ip.first << ": " << ip.second << " 次" << endl;
}
```

### 示例3：按状态码分组统计响应时间

```cpp
// E5_V1: 响应时间, E5_V2: 状态码
int statusVar = (5 << 16) | (2 << 8) | (2 << 0);
int responseTimeVar = (5 << 16) | (1 << 8) | (2 << 0);

// 按状态码分组，计算每组响应时间的平均值
map<string, double> avgByStatus = stats.GetVarGroupByAvg(statusVar, responseTimeVar);

cout << "按状态码分组的平均响应时间:" << endl;
for (auto& pair : avgByStatus) {
    cout << "  状态码 " << pair.first << ": " << pair.second << " ms" << endl;
}
```

### 示例4：带过滤条件的统计

```cpp
// 只统计状态码为200的响应时间
BitMap* filter = new BitMap(10000);
// ... 通过查询设置过滤条件，只标记状态码为200的行 ...

double avg200 = stats.GetVarAvg(responseTimeVar, filter);
cout << "200状态码的平均响应时间: " << avg200 << " ms" << endl;
```

## 运行示例程序

我们提供了一个完整的示例程序：

```bash
cd query
# 编译示例（需要先编译主程序）
g++ -o statistics_example statistics_example.cpp StatisticsAPI.cpp \
    LogStore_API.cpp LogStructure.cpp SearchAlgorithm.cpp \
    LogDispatcher.cpp var_alias.cpp ../compression/Coffer.cpp \
    -I. -I../compression -I../zstd-dev/lib \
    ../zstd-dev/lib/libzstd.a -ldl

# 运行示例
./statistics_example ../example_zip/Apache 0.log.zip
```

## 性能特点

1. **列式访问**：只需解压需要的列，无需解压整行数据
2. **固定长度优化**：对于固定长度存储，可以直接按索引访问，O(1) 时间复杂度
3. **缓存友好**：连续存储的数据结构，CPU 缓存命中率高
4. **内存高效**：支持流式处理，避免一次性加载所有数据
5. **过滤支持**：可以使用 BitMap 进行高效的行级过滤

## 注意事项

1. **类型判断**：系统会自动判断变量类型，但对于混合类型可能需要手动指定
2. **内存管理**：对于大列，建议使用流式处理或分批处理
3. **过滤条件**：使用 BitMap 进行过滤时，需要先通过查询设置过滤条件
4. **变量ID**：确保变量ID计算正确，错误的ID会导致统计失败

## API 参考

### StatisticsAPI 类

#### 构造函数
```cpp
StatisticsAPI(LogStoreApi* api);
```

#### 数值统计方法
```cpp
double GetVarAvg(int varname, BitMap* filter = NULL);
double GetVarMax(int varname, BitMap* filter = NULL);
double GetVarMin(int varname, BitMap* filter = NULL);
double GetVarSum(int varname, BitMap* filter = NULL);
int GetVarCount(int varname, BitMap* filter = NULL);
double GetVarMedian(int varname, BitMap* filter = NULL);
double GetVarStdDev(int varname, BitMap* filter = NULL);
double GetVarPercentile(int varname, double percentile, BitMap* filter = NULL);
```

#### 文本统计方法
```cpp
int GetVarDistinctCount(int varname, BitMap* filter = NULL);
map<string, int> GetVarFrequency(int varname, int topK = 10, BitMap* filter = NULL);
vector<pair<string, int> > GetVarTopK(int varname, int k, BitMap* filter = NULL);
```

#### 组合统计方法
```cpp
map<string, double> GetVarGroupByAvg(int groupVar, int valueVar, BitMap* filter = NULL);
map<string, double> GetVarGroupBySum(int groupVar, int valueVar, BitMap* filter = NULL);
map<string, int> GetVarGroupByCount(int groupVar, BitMap* filter = NULL);
```

## 未来扩展

计划支持的功能：
- 命令行接口集成
- 更多统计指标（偏度、峰度等）
- 多变量关联分析
- 时间序列统计
- 统计结果缓存

## 相关文档

- [设计文档](../STATISTICS_DESIGN.md)
- [使用示例](../STATISTICS_USAGE_EXAMPLE.md)

