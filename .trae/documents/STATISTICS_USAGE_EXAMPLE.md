# 统计功能使用示例

## 概述

基于列式存储的统计功能可以高效地计算数值类参数的平均值、最大值、最小值等，以及文本类参数的分组数、频率分布等。

## 实现方式

### 方案1：将统计方法添加到 LogStoreApi（推荐）

在 `LogStore_API.h` 中添加友元声明或公共方法：

```cpp
class LogStoreApi {
    // ... 现有代码 ...
    
    // 添加友元声明
    friend class StatisticsAPI;
    
    // 或者直接添加统计方法到公共接口
public:
    double GetVarAvg(int varname, BitMap* filter = NULL);
    double GetVarMax(int varname, BitMap* filter = NULL);
    double GetVarMin(int varname, BitMap* filter = NULL);
    int GetVarDistinctCount(int varname, BitMap* filter = NULL);
    std::map<std::string, int> GetVarFrequency(int varname, int topK = 10, BitMap* filter = NULL);
    // ... 其他统计方法 ...
};
```

### 方案2：使用友元类

在 `LogStore_API.h` 中添加：

```cpp
class LogStoreApi {
    friend class StatisticsAPI;
    // ... 其他代码 ...
};
```

## 使用示例

### 示例1：计算响应时间平均值

假设日志模板 E5 有两个变量：
- E5_V1: 响应时间（数值类型）
- E5_V2: 状态码（文本类型）

```cpp
#include "LogStore_API.h"
#include "StatisticsAPI.h"

LogStoreApi api;
api.Connect("/path/to/compressed/logs", "log.zip");

// 计算所有日志的平均响应时间
StatisticsAPI stats(&api);
double avgResponseTime = stats.GetVarAvg(0x50001); // E5_V1 = (5<<16) | (1<<8)
printf("平均响应时间: %.2f ms\n", avgResponseTime);

// 只统计状态码为200的响应时间平均值
BitMap* filter = new BitMap(10000);
// ... 设置过滤条件，只标记状态码为200的行 ...
double avgResponseTime200 = stats.GetVarAvg(0x50001, filter);
printf("200状态码的平均响应时间: %.2f ms\n", avgResponseTime200);
```

### 示例2：统计IP地址分布

```cpp
// 假设 E3_V2 是IP地址变量
int distinctIPs = stats.GetVarDistinctCount(0x30002); // E3_V2
printf("唯一IP地址数量: %d\n", distinctIPs);

// 获取Top 10的IP地址及其访问次数
std::vector<std::pair<std::string, int> > topIPs = stats.GetVarTopK(0x30002, 10);
printf("Top 10 IP地址:\n");
for (auto& ip : topIPs) {
    printf("  %s: %d次\n", ip.first.c_str(), ip.second);
}
```

### 示例3：计算最大值和最小值

```cpp
double maxResponseTime = stats.GetVarMax(0x50001);
double minResponseTime = stats.GetVarMin(0x50001);
printf("响应时间范围: %.2f - %.2f ms\n", minResponseTime, maxResponseTime);
```

### 示例4：按状态码分组统计响应时间

```cpp
// 按状态码（E5_V2）分组，计算每组响应时间（E5_V1）的平均值
std::map<std::string, double> avgByStatus = stats.GetVarGroupByAvg(0x50002, 0x50001);
for (auto& pair : avgByStatus) {
    printf("状态码 %s 的平均响应时间: %.2f ms\n", pair.first.c_str(), pair.second);
}
```

## 性能优势

1. **列式访问**：只需解压需要的列，无需解压整行数据
2. **固定长度优化**：对于固定长度存储，可以直接按索引访问，O(1) 时间复杂度
3. **缓存友好**：连续存储的数据结构，CPU 缓存命中率高
4. **并行计算**：可以并行处理不同列或不同数据区间

## 命令行接口示例（未来扩展）

```bash
# 计算平均值
./thulr_cmdline [compressed_folder] "STATS E5_V1 AVG"

# 统计唯一值数量
./thulr_cmdline [compressed_folder] "STATS E3_V2 DISTINCT_COUNT"

# Top-K 值
./thulr_cmdline [compressed_folder] "STATS E3_V2 TOP_K 10"

# 带过滤条件的统计
./thulr_cmdline [compressed_folder] "STATS E5_V1 AVG WHERE E5_V2 = 200"

# 分组统计
./thulr_cmdline [compressed_folder] "STATS E5_V1 GROUP_BY E5_V2 AVG"
```

## 注意事项

1. **类型判断**：系统会自动判断变量类型，数值类型使用数值统计，文本类型使用文本统计
2. **内存管理**：对于大列，建议使用流式处理或分批处理，避免一次性加载所有数据
3. **过滤条件**：可以使用 BitMap 进行高效的行级过滤，只统计满足条件的行
4. **缓存机制**：统计结果可以缓存，避免重复计算

