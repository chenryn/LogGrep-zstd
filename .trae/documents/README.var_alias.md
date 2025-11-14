# 变量别名系统使用说明（改进版）

## 概述

变量别名系统允许用户为LogGrep中的变量（格式为`<V,n>`或`E1_V2`）定义有意义的名称，如`Host`、`Port`、`IP`等，使日志分析和查询更加直观和易用。

**重要改进**：
- ✅ 统一配置格式：使用`别名: E模板_V变量, ...`格式，支持一对多映射
- ✅ 自动发现工具：`var_discover.py`可以自动分析模板，推断变量语义
- ✅ 批量添加：支持一次为多个变量添加相同别名
- ✅ 修复Bug：配置文件保存位置问题已修复

## 功能特点

- 支持为任意变量ID定义自定义别名
- **支持一对多映射**：一个别名可以对应多个变量（解决同一语义变量在不同模板中的问题）
- 在显示变量时自动使用别名代替原始ID
- 提供命令行工具管理变量别名
- **自动发现工具**：自动分析模板，生成配置建议
- 配置文件简单易用

## 配置文件格式

变量别名配置文件默认位于系统工作目录下的`var_alias.conf`，或压缩文件同目录下的`<文件名>.var_alias`。

### 统一格式（推荐）

```
# 变量别名配置文件
# 格式: 别名: E模板_V变量, E模板_V变量, ...
# 示例: Host: E7_V4, E6_V4, E30_V3

Host: E7_V4, E6_V4, E30_V3
Port: E7_V3, E6_V3, E30_V4
User: E7_V5, E6_V5, E30_V7
```

**格式说明**：
- 每行定义一个别名及其对应的变量列表
- 格式：`别名: E模板ID_V变量索引, E模板ID_V变量索引, ...`
- 支持一个别名对应多个变量（一对多映射）
- 变量之间用逗号和空格分隔

### 向后兼容格式

系统仍然支持旧的`变量ID=别名`格式，但建议使用新格式。

## 自动发现工具

### var_discover.py

自动分析压缩文件的模板，推断变量语义，生成别名配置建议。

**使用方法**：
```bash
# 基本用法
python3 query/var_discover.py <压缩文件目录>

# 指定输出文件
python3 query/var_discover.py <压缩文件目录> --output <输出文件路径>

# 指定压缩文件名
python3 query/var_discover.py <压缩文件目录> --zip-name <压缩文件名>
```

**示例**：
```bash
# 分析Ssh日志
python3 query/var_discover.py ../lib_output_zip/Ssh

# 分析并保存到指定文件
python3 query/var_discover.py ../lib_output_zip/Ssh --output ../lib_output_zip/Ssh/0.log.zip.var_alias
```

**工作原理**：
1. 读取`.zip.templates`文件，解析所有模板
2. 分析变量在模板中的上下文（前后文本）
3. 基于关键词推断变量语义（IP、端口、用户名、主机名、时间等）
4. 生成配置建议

**推断规则**：
- **IP地址**：上下文包含`ip`、`address`、`rhost`、`from`等关键词
- **端口**：上下文包含`port`、`:`等，且通常在IP地址附近
- **用户名**：上下文包含`user`、`username`、`logname`等
- **主机名**：上下文包含`host`、`hostname`、`server`等
- **时间**：上下文包含`time`、`date`、`timestamp`等

## 变量别名管理工具

系统提供了一个命令行工具`var_alias_tool`用于管理变量别名：

```bash
# 编译工具
cd query
make -f Makefile.var_alias

# 查看帮助
./var_alias_tool help
```

### 命令列表

#### 1. 列出所有变量别名
```bash
./var_alias_tool list [压缩文件路径]
```

示例：
```bash
./var_alias_tool list
./var_alias_tool list /path/to/Ssh
```

#### 2. 添加单个变量别名
```bash
./var_alias_tool add <变量ID> <别名> [压缩文件路径]
```

示例：
```bash
./var_alias_tool add E7_V4 Host
./var_alias_tool add 7_4.1 Host /path/to/Ssh
```

#### 3. 批量添加别名（一对多映射）⭐ **新功能**
```bash
./var_alias_tool add-multi <别名> <变量ID列表> [压缩文件路径]
```

示例：
```bash
# 为多个变量添加Host别名
./var_alias_tool add-multi Host E7_V4,E6_V4,E30_V3

# 为多个变量添加Port别名
./var_alias_tool add-multi Port E7_V3,E6_V3,E30_V4
```

**这是解决"同一语义变量在不同模板中"问题的关键功能！**

#### 4. 查找变量ID对应的别名
```bash
./var_alias_tool get <变量ID> [压缩文件路径]
```

示例：
```bash
./var_alias_tool get E7_V4
```

#### 5. 查找别名对应的变量ID
```bash
./var_alias_tool find <别名> [压缩文件路径]
```

示例：
```bash
./var_alias_tool find Host
```

#### 6. 重新加载配置文件
```bash
./var_alias_tool reload [压缩文件路径]
```

## 使用流程

### 方法1：自动发现（推荐）

1. **自动生成配置**：
   ```bash
   python3 query/var_discover.py ../lib_output_zip/Ssh
   ```

2. **检查生成的配置**：
   ```bash
   cat ../lib_output_zip/Ssh/0.log.zip.var_alias
   ```

3. **根据需要调整**：
   - 编辑配置文件，修改别名名称
   - 使用`var_alias_tool add-multi`添加更多变量

4. **验证配置**：
   ```bash
   ./var_alias_tool list ../lib_output_zip/Ssh/0.log.zip
   ```

### 方法2：手动配置

1. **查看模板文件**，了解变量分布：
   ```bash
   cat ../lib_output_zip/Ssh/0.log.zip.templates | head -10
   ```

2. **创建配置文件**：
   ```bash
   touch ../lib_output_zip/Ssh/0.log.zip.var_alias
   ```

3. **编辑配置文件**，添加别名：
   ```
   Host: E7_V4, E6_V4, E30_V3
   Port: E7_V3, E6_V3, E30_V4
   User: E7_V5, E6_V5, E30_V7
   ```

4. **或使用工具添加**：
   ```bash
   ./var_alias_tool add-multi Host E7_V4,E6_V4,E30_V3 ../lib_output_zip/Ssh/0.log.zip
   ```

## 查询使用

配置好别名后，可以在查询时使用：

```bash
# 使用别名查询
./thulr_cmdline ../lib_output_zip/Ssh "Host:LabSZ"

# 使用数值过滤
./thulr_cmdline ../lib_output_zip/Ssh "Port:22"

# 使用范围过滤
./thulr_cmdline ../lib_output_zip/Ssh "Port:1024..65535"

# 使用strict选项（仅变量位过滤）
./thulr_cmdline ../lib_output_zip/Ssh "Host:LabSZ|strict"

# 使用ci选项（大小写不敏感）
./thulr_cmdline ../lib_output_zip/Ssh "Host:LabSZ|ci"
```

## 系统集成

变量别名系统已集成到LogGrep的核心功能中，当系统启动时会自动加载变量别名配置。在显示变量时，如果变量有定义别名，将优先显示别名而不是原始ID。

## 配置文件优先级

1. **文件级配置**：`<压缩文件目录>/<文件名>.var_alias`（优先级最高）
2. **目录级配置**：`<压缩文件目录>/var_alias.conf`
3. **全局配置**：`<工作目录>/var_alias.conf`

## 常见问题

### Q: 如何为同一语义的变量在不同模板中定义别名？

**A**: 使用`add-multi`命令或直接在配置文件中使用一对多格式：
```
Host: E7_V4, E6_V4, E30_V3
```

### Q: 自动发现工具生成的别名不准确怎么办？

**A**: 自动发现工具只是提供建议，你可以：
1. 编辑配置文件，修改别名名称
2. 使用`var_alias_tool`手动添加或修改

### Q: 配置文件保存在哪里？

**A**: 
- 如果指定了压缩文件路径，保存到文件级配置：`<目录>/<文件名>.var_alias`
- 否则保存到全局配置：`<工作目录>/var_alias.conf`

### Q: 如何查看当前使用的配置文件？

**A**: 配置文件路径会在加载时显示，或使用`var_alias_tool list`查看。

## 示例

假设我们有以下模板：
- E7: `Dec <V7> <V6>:<V0>:<V1> LabSZ <V2>: <V8> password for <V5> from <V4> port <V3> ssh2`
- E6: `Dec <V6> <V0>:<V1>:<V2> LabSZ <V3>: pam_unix(sshd:auth): authentication failure; ... rhost=<V4> user=<V5>`
- E30: `Jan <V6> <V5>:<V0>:<V1> LabSZ <V2>: Failed password for <V7> from <V3> port <V4> ssh2`

通过分析，我们发现：
- `<V4>`在多个模板中都表示IP地址（`from <V4>`、`rhost=<V4>`、`from <V3>`）
- `<V3>`在多个模板中都表示端口（`port <V3>`、`port <V4>`）

配置示例：
```
Host: E7_V4, E6_V4, E30_V3
Port: E7_V3, E6_V3, E30_V4
User: E7_V5, E6_V5, E30_V7
```

这样，无论使用哪个模板，都可以用`Host:`和`Port:`查询。

## 注意事项

1. 配置文件路径在系统启动时确定，默认为工作目录下的`var_alias.conf`
2. 别名不应包含空格或特殊字符（建议使用字母、数字、下划线、点）
3. 如果修改了配置文件，可以使用`var_alias_tool reload`命令重新加载，无需重启系统
4. 变量别名仅影响显示和查询，不影响内部处理和存储
5. **一对多映射时，查询会在所有映射的变量中搜索**

## 变更历史

### v2.0 (当前版本)
- ✅ 统一配置格式：使用`别名: 变量列表`格式
- ✅ 支持一对多映射
- ✅ 修复保存配置的Bug
- ✅ 添加自动发现工具`var_discover.py`
- ✅ 添加批量添加功能`add-multi`
- ✅ 向后兼容旧格式

### v1.0
- 基础别名功能
- 支持单个变量别名
