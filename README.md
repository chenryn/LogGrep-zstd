This is the open-source code of paper "LogGrep: Fast and Cheap Cloud Log Storage by Exploiting both Static and Runtime Patterns" (Eurosys'2023), we use zstd as the packing method in this version.

# Paper Reference
Junyu Wei, Guangyan Zhang, Junchao Chen, Yang Wang, Weimin Zheng, Tingtao Sun, Jiesheng Wu, Jiangwei Jiang. LogGrep: Fast and Cheap Cloud Log Storage by Exploiting both Static and Runtime Patterns. in Proceedings of the 18th European Conference on Computer Systems (EuroSys’23), Roma, Italy, May 2023.

# Folder Description
## query
Query code to search on compressed logs. The entry is in CmdLineTool. Main logics are in LogStore_API.cpp and SearchAlgorithm.cpp
## compression
Compression code to compress original log files into zip files. Core logic are in main.cpp.
## example
A minimal working example for quick test. Each folder corresponds to a kind of log, inside which log files are cut into block (64MB for each).
## example_zip
Empty by default. Compressed results during quick test can be found here.
## LogHub_Seg_zip
Empty by default. Compressed results during large test can be found here.
## output
Empty by default.
## zstd-dev
zstd used by facebook to serve as the basic compression tool for LogGrep

# Supported environments
## Hardware
CPU: 2× Intel Xeon E5-2682 2.50GHz CPUs (with 16 cores)
RAM: 188GB

## OS Version
Red Hat 4.8.5 with Linux kernel 3.10.0

Ubuntu 11.3.0 with Linux kernel 5.15.0

Above are platforms we have tested suceffully, if you have trouble with other kernal version, please contact thuweijy@vip.163.com

## Compiler Version
gcc version 4.8.5 20150623

## Other requirements
Python version >= 3.6.8, use ``pip3 install --upgrade requests`` to install non-listed requirement

use ``yum groupinstall 'Development Tools`` to install other requirements.

# Compilation and quick test
## Compilation
``mkdir ./output``

``mkdir ./example_zip``

``cd ./zstd-dev/lib``

``make``

``cd ../../compression``

``make``

``cd ../``

``cd ./query``

``make``
## Quick test
``cd ./compression``

``python3 quickTest.py``

Then you can find compressed files in ./example_zip/.

``cd ./cmdline_loggrep``

``./thulr_cmdline [Compressed Folder] [QUERY]``

[QUERY] is the query statement

[Compressed Folder] is one of the folder under ./example_zip/

All testing query for quick test can be found at ./query4quicktest.txt. For example, to run query on Apache logs, you can use command as follow:

``./thulr_cmdline ../example_zip/Apache "error and Invalid URI in request"``
## Large test
Download large dataset from https://zenodo.org/record/7056802#.Yxm1RexBwq1 to ../LogHub_Seg/

First compress these log blocks.

``cd ./compression``

``python3 largeTest.py ../LogHub_Seg/``

Then you can find compressed files in ./LogHub_Seg_zip/

You can also find a compression summary under ./compression such as ./compression/Log_2022-09-21, which records whether there is an error and the compression speed.

Then query compressed logs.

``cd ./cmdline_loggrep``

``./thulr_cmdline [Compressed Folder] [QUERY]``

[QUERY] is the query statement

[Compressed Folder] is one of the folder under ./LogHub_Seg_zip/

All testing query for large test can be found at ./query4largetest.txt. For example, to run query on Hadoop logs, you can use command as follow:

``./thulr_cmdline ../LogHub_Seg_zip/Hadoop "ERROR and RECEIVED SIGNAL 15: SIGTERM and 2015-09-23"``

# 新增功能（中文）
- 字段别名查询与数值过滤

## 字段别名系统
- 配置文件优先级：优先读取压缩文件同目录下的`<文件名>.var_alias`，若不存在则回退到目录级`var_alias.conf`
- 支持两种格式：
  - 变量ID→别名：`E<模板>_V<变量>.<类型>=<别名>`（例如：`1_1.1=src.ip`）
  - 别名→多变量：`<别名>: E<模板>_V<变量>, ...`（例如：`Host: E7_V4, E6_V4, E30_V3`）

## 查询语法
- 别名查询：`./thulr_cmdline <压缩目录> "<别名>:<值>[|<选项>]"`
- 选项：
  - `strict`：仅在别名映射的变量位执行过滤；不回落到主模板常量匹配与异常匹配
  - `ci`：回落到主模板常量匹配时大小写不敏感（case-insensitive）

## 数值过滤（仅对VAR类型变量位生效）
- 支持操作符：`==`, `!=`, `>`, `>=`, `<`, `<=`
- 支持范围写法：`a..b`（闭区间），例如`1024..65535`
- 示例：
  - `Port:22`（等价于`Port:==22`）
  - `Port:>=1024`（数值大于等于）
  - `Port:1024..65535`（范围过滤）
  - `Port:22|strict`（仅变量位过滤，不回落常量）

## 常量匹配回退
- 当别名映射的变量位没有命中且未指定`strict`时，系统会回落到主模板常量匹配并物化输出
- 指定`ci`时，常量匹配大小写不敏感

## 示例
- `./thulr_cmdline ../lib_output_zip/Ssh "Host:LabSZ"`
- `./thulr_cmdline ../lib_output_zip/Ssh "Host:LabSZ|ci"`
- `./thulr_cmdline ../lib_output_zip/Ssh "Port:22"`
- `./thulr_cmdline ../lib_output_zip/Ssh "Port:>=1024"`
- `./thulr_cmdline ../lib_output_zip/Ssh "Port:1024..65535|strict"`

## 兼容性提示
- 某些数据集（例如示例Ssh）中，`Host`更像模板常量而非变量位；在此情况下`|strict`可能无命中，建议使用默认或`|ci`以获得常量匹配效果

## 变更摘要
- 查询入口支持`别名:值`语法与`strict`/`ci`选项
- 对VAR变量位新增数值比较与范围过滤
- 别名解析兼容两种配置格式，并支持一对多映射
