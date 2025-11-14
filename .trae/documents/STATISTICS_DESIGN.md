# 列式存储统计功能设计文档

## 背景

LogGrep-zstd 使用列式存储（类似 doc_values）来存储变量值，这为高效的统计分析提供了基础。每个变量的所有值按固定长度填充后连续存储，可以高效地进行：

1. **数值类统计**：平均值、最大值、最小值、总和、中位数等
2. **文本类统计**：唯一值计数、分组统计、频率分布等
3. **组合统计**：多变量关联分析、条件统计等

## 存储格式分析

### 变量存储结构

```cpp
// 压缩时：Encoder::serializeVar
// 每个变量值被填充到 maxLen 长度，然后连续存储
// 格式：[值1(填充)][值2(填充)][值3(填充)]...
// 访问：offset = i * maxLen 可以快速定位第 i 个值
```

### 类型判断

系统已有 `getType()` 函数可以判断变量类型：
- `NUM_TY (1)`: 纯数字
- `AF_TY (2)`: A-F 十六进制
- `af_TY (4)`: a-f 十六进制  
- `GZ_TY (8)`: G-Z 字母
- `gz_TY (16)`: g-z 字母
- `symbol_TY (32)`: 符号

## 功能设计

### 1. 数值类统计

#### 支持的统计指标
- **平均值** (avg)
- **最大值** (max)
- **最小值** (min)
- **总和** (sum)
- **计数** (count)
- **中位数** (median)
- **标准差** (stddev)
- **百分位数** (percentile)

#### 实现方式
```cpp
// 直接从列式存储中读取，无需解压整列
// 对于固定长度存储，可以直接按索引访问
double CalculateAvg(int varname, BitMap* filter = NULL);
```

### 2. 文本类统计

#### 支持的统计指标
- **唯一值计数** (distinct_count)
- **频率分布** (frequency)
- **Top-K 值** (top_k)
- **分组统计** (group_by)

#### 实现方式
```cpp
// 遍历列式存储，使用哈希表统计
map<string, int> GetFrequency(int varname, BitMap* filter = NULL);
```

### 3. 条件统计

支持基于 BitMap 过滤的统计：
```cpp
// 只统计满足条件的行
double CalculateAvg(int varname, BitMap* filter);
```

## 性能优势

1. **列式访问**：只需读取需要的列，无需解压整行
2. **固定长度**：可以直接按索引定位，O(1) 访问
3. **缓存友好**：连续存储，CPU 缓存命中率高
4. **并行计算**：可以并行处理不同列或不同区间

## API 设计

### C++ API

```cpp
class LogStoreApi {
    // 数值统计
    double GetVarAvg(int varname, BitMap* filter = NULL);
    double GetVarMax(int varname, BitMap* filter = NULL);
    double GetVarMin(int varname, BitMap* filter = NULL);
    double GetVarSum(int varname, BitMap* filter = NULL);
    int GetVarCount(int varname, BitMap* filter = NULL);
    double GetVarMedian(int varname, BitMap* filter = NULL);
    double GetVarStdDev(int varname, BitMap* filter = NULL);
    
    // 文本统计
    int GetVarDistinctCount(int varname, BitMap* filter = NULL);
    map<string, int> GetVarFrequency(int varname, int topK = 10, BitMap* filter = NULL);
    vector<pair<string, int>> GetVarTopK(int varname, int k, BitMap* filter = NULL);
    
    // 组合统计
    map<string, double> GetVarGroupByAvg(int groupVar, int valueVar, BitMap* filter = NULL);
};
```

### 命令行接口

```bash
# 统计数值变量平均值
./thulr_cmdline [compressed_folder] "STATS E1_V2 AVG"

# 统计文本变量唯一值数量
./thulr_cmdline [compressed_folder] "STATS E1_V2 DISTINCT_COUNT"

# 带过滤条件的统计
./thulr_cmdline [compressed_folder] "STATS E1_V2 AVG WHERE E1_V3 > 100"

# Top-K 值
./thulr_cmdline [compressed_folder] "STATS E1_V2 TOP_K 10"

# 分组统计
./thulr_cmdline [compressed_folder] "STATS E1_V2 GROUP_BY E1_V3 AVG"
```

## 实现要点

1. **类型检测**：使用现有的 `getType()` 函数判断变量类型
2. **填充处理**：使用 `RemovePadding()` 去除填充空格
3. **内存效率**：对于大列，可以流式处理，避免一次性加载
4. **缓存机制**：统计结果可以缓存，避免重复计算

## 示例场景

### 场景1：计算响应时间平均值
```cpp
// 假设 E5_V1 是响应时间变量（数值类型）
double avgResponseTime = api->GetVarAvg(0x50001); // E5_V1
```

### 场景2：统计IP地址分布
```cpp
// 假设 E3_V2 是IP地址变量（文本类型）
map<string, int> ipFreq = api->GetVarFrequency(0x30002, 20); // Top 20
```

### 场景3：按状态码分组统计响应时间
```cpp
// E5_V1: 响应时间, E5_V2: 状态码
map<string, double> avgByStatus = api->GetVarGroupByAvg(0x50002, 0x50001);
```

