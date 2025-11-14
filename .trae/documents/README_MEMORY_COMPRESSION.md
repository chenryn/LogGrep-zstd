# 从内存数据直接压缩到磁盘文件

本文档说明如何使用THULR从内存数据直接压缩到磁盘文件，而不需要先将数据保存为临时文件。

## 功能概述

我们为THULR添加了以下功能：

1. 从标准输入读取数据并压缩到文件
2. 通过C++接口函数直接从内存缓冲区压缩数据到文件
3. 提供Python示例代码，展示如何使用这些功能

## 使用方法

### 方法一：通过管道传递数据

可以使用管道将数据传递给THULR程序，使用`-I -`选项指示从标准输入读取数据：

```bash
# 从文件传递数据
cat input.log | ./THULR -I - -O output.zip

# 从字符串传递数据
echo "log data" | ./THULR -I - -O output.zip
```

### 方法二：使用C++接口函数

我们提供了C++接口函数`compress_from_memory`，可以直接从内存缓冲区压缩数据到文件：

```cpp
// 函数原型
int compress_from_memory(const char* buffer, int buffer_len, const char* output_path);

// 使用示例
const char* data = "log data";
int result = compress_from_memory(data, strlen(data), "output.zip");
```

### 方法三：从Python调用

我们提供了两种从Python调用的方式：

#### 1. 使用管道

```python
import subprocess

def compress_using_pipe(data, output_path):
    if isinstance(data, str):
        data = data.encode('utf-8')  # 转换为字节
    
    cmd = ["./THULR", "-B", "-O", output_path]
    process = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    process.communicate(input=data)
    
    return process.returncode == 0
```

#### 2. 使用ctypes调用C++库函数

```python
import ctypes
from ctypes import c_char_p, c_int

def compress_using_library(data, output_path):
    if isinstance(data, str):
        data = data.encode('utf-8')  # 转换为字节
    
    # 加载动态库
    lib_path = "./libthulr.so"  # 根据操作系统调整
    thulr_lib = ctypes.CDLL(lib_path)
    
    # 定义函数原型
    compress_func = thulr_lib.compress_from_memory
    compress_func.argtypes = [c_char_p, c_int, c_char_p]
    compress_func.restype = c_int
    
    # 调用C++函数
    result = compress_func(
        ctypes.create_string_buffer(data), 
        len(data),
        c_char_p(output_path.encode('utf-8'))
    )
    
    return result == 0
```

## 编译共享库

要使用C++接口函数，需要先编译共享库：

```bash
# 使用提供的脚本
chmod +x build_shared_lib.sh
./build_shared_lib.sh
```

这将生成`libthulr.so`（Linux）、`libthulr.dylib`（macOS）或`thulr.dll`（Windows）文件。

## 示例代码

我们提供了完整的Python示例代码，请参考`python_example.py`文件：

```bash
python3 python_example.py
```

## 注意事项

1. 使用管道方式时，数据会先完全读入内存，然后再处理，因此对于非常大的数据集可能会有内存限制
2. 使用C++接口函数时，数据会被复制一份，以确保处理过程中不会修改原始数据
3. 共享库需要在与Python脚本相同的目录下，或者在系统库路径中