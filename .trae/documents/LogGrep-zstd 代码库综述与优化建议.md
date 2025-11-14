## 项目概览
- 目标：在压缩阶段利用日志的模板与变量模式，将原始日志块压缩为“胶囊（capsule）”并记录元数据；在查询阶段对压缩数据进行模式/变量级推下（pushdown）检索与物化输出。
- 核心模块：`compression`（压缩管线）、`query`（查询管线）、`zstd-dev`（Facebook zstd 依赖，内置源码）。
- 入口：压缩入口 `compression/main.cpp:671`；查询入口 `query/CmdLineTool.cpp:8`。

## 压缩实现
- 模板抽取与采样：对输入日志进行分词采样，构建长度模板池并统计变量位置信息（`compression/main.cpp:306-373`，`LengthParser`）。
- 匹配与收集：对全文进行模板匹配，收集各模板的变量实例与“失败行”（outliers）（`compression/main.cpp:169-232`、`101-167`）。
- 变量编码：
  - 词典模式：构建变量字典与`entry`索引，按填充长度写入（`compression/union.*`、`Encoder::serializeDic` `compression/Encoder.cpp:142-166`，`serializeEntry` `:128-136`）。
  - 子模式：对变量内子结构拆分并序列化（`Encoder::serializeSvar` `compression/Encoder.cpp:168-195`）。
  - 原始变量：定长左/右对齐填充（`Encoder::serializeVar` `compression/Encoder.cpp:108-126`）。
- 模板与异常：
  - 主模板列表（`serializeTemplate` `compression/Encoder.cpp:77-88`）。
  - 模板外异常行（`serializeTemplateOutlier` `compression/Encoder.cpp:90-106`）。
- 打包输出：生成压缩“胶囊”并写入ZIP样式文件，文件头含`meta`的zstd帧（`Encoder::output` `compression/Encoder.cpp:216-253`；`Coffer`读/压/解实现 `compression/Coffer.*`）。

## 查询实现
- 启动与装载：
  - 读取文件头，解压`meta`并解析所有胶囊位置与属性（`query/LogStore_API.cpp:93-224`）。
  - 加载主模板与子模式、异常行（`BootLoader` `query/LogStore_API.cpp:40-91`，`LoadMainPatternToGlbMap` `:313-383`，`LoadSubPatternToGlbMap` `:385-414`，`LoadOutliers` `:281-311`）。
- 检索算法：
  - 固定行长走 Boyer‑Moore；可变行长走 KMP；支持对齐类型（任意/左/右/全）（`query/SearchAlgorithm.cpp:360-679`、`328-358`）。
  - 子模式/字典匹配与推下：先在子模式或字典中定位，再对目标变量胶囊做推下过滤（`GetVals_Subpat*_Pushdown*` `query/LogStore_API.cpp:1273-1448`，`GetVals_Dic*_Pushdown*` `:1593-1632`）。
  - 逻辑表达式与多段查询：AND/OR/NOT 组合与多段序列对齐（`SearchByLogic_*` 与 `SearchMultiInPattern*` `query/LogStore_API.cpp:2597-3690`）。
- 物化输出：按模板拼接常量与变量，输出命中日志行；异常行单独物化（`Materialization` `query/LogStore_API.cpp:2085-2137`，`MaterializOutlier` `:2140-2147`）。
- 多文件调度：按目录批量加载多个压缩文件并顺序或并行查询（并行暂未启用）（`query/LogDispatcher.cpp:29-73`，`181-212`）。
- 变量别名：支持为变量ID定义人类可读别名（默认关闭部分集成）（`query/var_alias.h`，调用示例 `query/CmdManager.cpp:99-121`，文档 `query/README.var_alias.md`）。

## 数据格式与“胶囊”
- 元数据（meta）：记录每个胶囊的压缩标记、偏移、压缩后大小、原始大小、行数、元素长度（`compression/Encoder.cpp:22-37`，`Coffer(meta ctor)` `compression/Coffer.cpp:32-50`）。
- 胶囊类型：主模板、变量列表、字典、entry、变量、子变量（svar）、模板外异常等（`Encoder::*`，类型见`constant.h`）。
- 读写与安全：解压前检查帧内容大小与安全上限（`MAX_SAFE_DECOMPRESS_SIZE`），并进行越界校验（`query/LogStore_API.cpp:112-147`，`query/LogStore_API.cpp:621-673`，`compression/Coffer.cpp:134-181`）。

## 已实现的主要功能
- 原始日志分块的高效压缩，结合模板/变量/子模式/字典以提升压缩率与查询速度。
- 基于模式的快速检索，支持通配与逻辑组合，并对变量胶囊做推下过滤以减少解压与匹配成本。
- 异常行（未匹配模板）保留与可检索。
- 多文件目录批量查询、性能与统计输出（`LogDispatcher` 汇总）。

## 可优化改进方向
- 性能与并行
  - 启用并行查询：`LogDispatcher` 目前序列执行（`query/LogDispatcher.cpp:195-212`）；可引入线程池，对文件级并行与胶囊级并行分层调度。
  - 多模式匹配：多通配同时匹配建议采用 Aho‑Corasick 或 Hyperscan，替代逐个BM/KMP（`query/SearchAlgorithm.cpp`）。
  - 预索引：为高频变量构建轻量倒排或位图索引，缩短推下路径；对`entry`与`dic`可缓存热点段偏移。
- 内存与稳定性
  - RAII/智能指针替换原始`new/delete`、`malloc/free`；统一异常安全与释放路径（如`LogStore_API.cpp:1196-1210`、`compression/Coffer.cpp:52-62`）。
  - 流式解压：对大胶囊支持分块解压与扫描，避免一次性分配大缓冲。
  - 边界与溢出：集中封装字符串/填充操作，减少手工`memcpy/strncpy`越界风险（如`Encoder::padding`与`SearchAlgorithm::RemovePadding`）。
- 算法与准确性
  - 子模式匹配目前含 LCS 辅助与前后匹配（`query/SearchAlgorithm.cpp:179-238`、`1700-2372`）；建议引入更可控的状态机或正则自动机以提升可维护性与边界覆盖。
  - 对齐判定统一：当前左/右/全对齐在多处手工实现；可抽象成策略并单测覆盖。
- 代码结构与可维护性
  - 统一日志与统计：`Syslog*`/`SysTotCount`分散调用，建议集中封装并支持等级与采样。
  - 头文件常量与类型重复：`constant.h`、位定义、宏在压缩/查询共用；建议建立共享头并用`namespace`隔离。
  - 构建与依赖：顶层提供 CMake，优先链接系统 zstd 而非内置源码，便于升级与安全补丁。
- 接口与易用性
  - Python/Go 绑定：压缩已有`extern "C" compress_from_memory`（`compression/main.cpp:639-668`）；可补充查询侧绑定以便集成。
  - 变量别名系统统一：查询侧部分注释与两处初始化不一致（`query/LogStore_API.cpp:78-89` 注释掉、`query/CmdManager.cpp:99-121`启用）；建议在`BootLoader`阶段加载并作为显示层策略。
  - 错误信息国际化与更友好提示；命令行帮助覆盖更多示例与逻辑查询。
- 测试与质量保障
  - 为 BM/KMP/子模式/字典匹配与推下路径补充单测与随机/模糊测试（`query/SearchAlgorithm.cpp`）。
  - 基准与追踪：对不同日志类型/大小建立基准，输出胶囊级耗时与命中统计，形成回归基线。

## 验证建议
- 复用`README.md`的“Quick test”，对`example`数据跑通压缩与查询，采集耗时与命中数；对比开启/关闭子模式与字典的效果。
- 构建最小单元测试：针对`SearchAlgorithm`的对齐、边界与异常路径；针对`Encoder`与`Coffer`的大小校验与异常。

## 后续可交付
- 启用文件级并行与安全的流式解压的原型实现。
- 统一变量别名加载与显示策略，并提供查询侧语言绑定示例。
- 提交一套针对匹配与推下的核心单测与基准脚本。