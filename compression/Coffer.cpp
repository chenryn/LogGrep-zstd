#include "Coffer.h"
#include<cstring>
#include<cstdio>
#include<cstdlib>
#include<zstd.h>
using namespace std;
Coffer::Coffer(string filename, char* srcData, int srcL, int line, int typ, int ele){
    data = srcData;
    filenames = filename;
    cdata = NULL;
    srcLen = srcL;
    lines = line;
    type = typ;
    eleLen = ele;
    compressed = 0;
}

Coffer::Coffer(string filename, string srcData, int srcL, int line, int typ, int ele){
//cout << "Build coffer" << filename << " " << line << endl;
    data = new char[srcL + 5];
    memcpy(data, srcData.c_str(), sizeof(char)*srcL);
    data[srcL] = '\0';
    filenames = filename;
    cdata = NULL;
    srcLen = srcL;
    lines = line;
    eleLen = ele;
    type = typ;
    compressed = 0;
}

Coffer::Coffer(string metaStr){
    data = NULL;
    cdata = NULL;
    type = -1;
   // cout << "Build based: " << metaStr << endl;
    char filename[128];
    int _compressed, _offset, _destLen, _srcLen, _lines, _eleLen;
    sscanf(metaStr.c_str(), "%s %d %d %d %d %d %d", filename, &_compressed, &_offset, &_destLen, &_srcLen, &_lines, &_eleLen);
    //cout << filename << endl;
    //cout << _compressed << endl;
    //cout << _compressed << _offset << _destLen << _srcLen << _lines << _eleLen << endl;
    filenames = filename;
    compressed = _compressed;
    offset = _offset;
    destLen = _destLen;
    srcLen = _srcLen;
    lines = _lines;
    eleLen = _eleLen;
}

Coffer::~Coffer()
{
    if(data){
        delete[] data;
        data = NULL;
    }
    if(cdata){
        delete[] cdata;
        cdata = NULL;
    }
}

int Coffer::compress(string compression_method, int compression_level){
    size_t com_space_size = ZSTD_compressBound(srcLen);
    cdata = new Byte[com_space_size];
    destLen = ZSTD_compress(cdata, com_space_size, data, srcLen, compression_level);
    compressed = 1;
    return destLen;
}

int Coffer::readFile(FILE* zipFile, int fstart){
    // 检查输入参数的有效性
    if(zipFile == NULL) {
        printf("varName: %s 文件指针为空\n", filenames.c_str());
        return -1;
    }
    
    if(destLen <= 0) {
        printf("varName: %s 无效的压缩数据大小: %d\n", filenames.c_str(), destLen);
        return -1;
    }
    
    int totOffset = fstart + this->offset;
    
    // 设置文件位置
    if(fseek(zipFile, totOffset, SEEK_SET) != 0) {
        printf("varName: %s 设置文件位置失败: %d\n", filenames.c_str(), totOffset);
        return -1;
    }
    
    // 安全地分配内存
    try {
        cdata = new unsigned char[destLen + 5];
    } catch(std::bad_alloc& e) {
        printf("varName: %s 内存分配失败: %s (大小: %d)\n", filenames.c_str(), e.what(), destLen + 5);
        return -1;
    }
    
    // 读取数据
    size_t res = fread(cdata, sizeof(Byte), destLen, zipFile);
    if(res != (size_t)destLen){
        printf("varName: %s 读取失败: 预期 %d 字节，实际读取 %zu 字节\n", 
               filenames.c_str(), destLen, res);
        delete[] cdata;
        cdata = NULL;
        return -1;
    }
    
    return destLen;
}


int Coffer::decompress(){
    // 如果数据未压缩，直接复制
    if(compressed == 0){
        if(srcLen == 0) return 0;
        try {
            data = new char[srcLen + 5];
            memcpy(data, cdata, sizeof(char)*srcLen);
            return srcLen;
        } catch(std::bad_alloc& e) {
            printf("varName: %s 内存分配失败: %s (大小: %d)\n", filenames.c_str(), e.what(), srcLen + 5);
            return -1;
        }
    }
    
    // 检查压缩数据是否有效
    if(cdata == NULL || destLen <= 0) {
        printf("varName: %s 压缩数据无效\n", filenames.c_str());
        return -1;
    }
    
    // 获取解压后的大小
    size_t decom_buf_size = ZSTD_getFrameContentSize(cdata, destLen);
    if(decom_buf_size == ZSTD_CONTENTSIZE_ERROR || decom_buf_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        printf("varName: %s 解压缩失败: 无效或未知的内容大小\n", filenames.c_str());
        return -1;
    }
    
    // 检查解压后大小是否合理
    if(decom_buf_size > MAX_SAFE_DECOMPRESS_SIZE) {
        printf("varName: %s 解压后大小超过安全限制: %zu > %d\n", 
               filenames.c_str(), decom_buf_size, MAX_SAFE_DECOMPRESS_SIZE);
        return -1;
    }
    
    // 检查解压后大小与预期大小是否一致
    if(decom_buf_size != (size_t)srcLen) {
        printf("varName: %s 解压后大小与预期不符: 预期=%d, 实际=%zu\n", 
               filenames.c_str(), srcLen, decom_buf_size);
        return -1;
    }
    
    try {
        // 分配内存并初始化
        data = new char[decom_buf_size + 5];
        memset(data, 0, decom_buf_size + 5);
        
        // 执行解压缩
        int res = ZSTD_decompress(data, decom_buf_size, cdata, destLen);
        if(res != srcLen){
            printf("varName: %s 解压缩失败，返回值: %d\n", filenames.c_str(), res);
            delete[] data;
            data = NULL;
            return -1;
        }
        return srcLen;
    } catch(std::bad_alloc& e) {
        printf("varName: %s 解压缩内存分配失败: %s (大小: %zu)\n", 
               filenames.c_str(), e.what(), decom_buf_size + 5);
        return -1;
    } catch(std::exception& e) {
        printf("varName: %s 解压缩异常: %s\n", filenames.c_str(), e.what());
        if(data != NULL) {
            delete[] data;
            data = NULL;
        }
        return -1;
    }
}

void Coffer::output(FILE* zipFile, int typ){
    if(cdata == NULL || zipFile == NULL){
        cout << "coffer: " + filenames + " output failed" << endl;
        return;
    }
    if(compressed){
        fwrite(cdata, sizeof(Byte), destLen, zipFile);
    }else{
        fwrite(data, sizeof(Byte), srcLen, zipFile);
    }
}

string Coffer::print(){
    string name = filenames;
    return name;
}

void Coffer::printFile(string output_path){
    FILE* pFile = fopen((output_path + filenames).c_str(), "w");
    fwrite(data, sizeof(char), srcLen, pFile);
    fclose(pFile);
}

