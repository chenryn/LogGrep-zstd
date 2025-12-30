#include <fstream>
#include <sys/types.h>
#include <dirent.h>
#include <algorithm>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <errno.h>
#include "LogStore_API.h"
#include "var_alias.h"
#include "../compression/TimeParser.h"
#include <ctype.h>
#include <strings.h>
#include <mutex>
#include <sstream>
#include <iomanip>

// JSON string escaping utility
static std::string escape_json(const std::string& input) {
    std::ostringstream ss;
    for (char c : input) {
        switch (c) {
            case '\b': ss << "\\b"; break;
            case '\f': ss << "\\f"; break;
            case '\n': ss << "\\n"; break;
            case '\r': ss << "\\r"; break;
            case '\t': ss << "\\t"; break;
            case '"': ss << "\\\""; break;
            case '\\': ss << "\\\\"; break;
            default:
                if (c >= 0x20 && c <= 0x7e) {
                    ss << c;
                } else {
                    ss << "\\u" << std::hex << std::setw(4) << std::setfill('0') << (int)(unsigned char)c;
                }
                break;
        }
    }
    return ss.str();
}

static bool __contains_ic(const char* text, const char* pat);
static bool __parse_numeric_expr(const std::string& expr, long& outA, long& outB, int& opType);
static unsigned long long __hash64_str(const char* s){ unsigned long long h=1469598103934665603ULL; while(*s){ h^=(unsigned long long)(unsigned char)(*s++); h*=1099511628211ULL; } return h; }
static unsigned long long __mix64(unsigned long long x){ x+=0x9E3779B97F4A7C15ULL; x=(x^(x>>30))*0xBF58476D1CE4E5B9ULL; x=(x^(x>>27))*0x94D049BB133111EBULL; x^=x>>31; return x; }
static long long __parse_time_arg(const char* s){
    if(!s) return LLONG_MIN;
    long long out=0;
    if(parse_timestamp_ms(s, (int)strlen(s), out)) return out;
    return atoll(s);
}


using namespace std;

double materTime = 0;
////////////////////////////////////////////init & private//////////////////////////////////////////////////////////
LogStoreApi::LogStoreApi()
{
    m_nServerHandle =0;
    m_fd = -1;
    m_fptr = NULL;
    m_glbMetaHeadLen =0;
    m_maxBitmapSize =0;
    m_outliers = NULL;
    m_glbExchgLogicmap = NULL;
    m_glbExchgPatmap = NULL;
    m_glbExchgBitmap = NULL;
    m_glbExchgSubBitmap = NULL;
    m_glbExchgSubTempBitmap = NULL;
}
LogStoreApi::~LogStoreApi()
{
	if(m_nServerHandle != 0)
	{
		DisConnect();
	}
	delete m_glbExchgLogicmap;
	delete m_glbExchgPatmap;
	delete m_glbExchgBitmap;
	delete m_glbExchgSubBitmap;
	delete m_glbExchgSubTempBitmap;
}

//return success(>0) or failed(<0) flag  
int LogStoreApi::BootLoader(char* path, char* filename)
{
    size_t destLen, srcLen;
    char cFileTmp[MAX_FILE_NAMELEN] = {'\0'};
    sprintf(cFileTmp, "%s/%s", path, filename);
	//load meta header: 5+4+4
	int ret = LoadGlbMetaHeader(cFileTmp, destLen, srcLen);
	if(ret <= 0) return -1;
	//load meta data
	ret = LoadGlbMetadata(cFileTmp, destLen, srcLen);
	if(ret <= 0) return -2; 
	//load templates(pattern)
	Coffer* coffer = NULL;
	ret = DeCompressCapsule(MAIN_PAT_NAME, coffer);
	if(ret <= 0) return -3;
	ret = LoadMainPatternToGlbMap(coffer->data, coffer->srcLen);
	//ClearCoffer(coffer);
	if(ret <= 0) return -4;
	
	//load variables(subPattern)
	Coffer* coffer1 = NULL;
	ret = DeCompressCapsule(SUBV_PAT_NAME, coffer1);
	if(ret <= 0) return -5;
	//SyslogDebug("subpat: %s\n", coffer ->data);
	ret = LoadSubPatternToGlbMap(coffer1 ->data, coffer1->srcLen);
	//ClearCoffer(coffer1);
	if(ret <= 0) return -6;
	//load templates.outlier
	Coffer* coffer2 = NULL;
	ret = DeCompressCapsule(OUTL_PAT_NAME, coffer2);
	if(ret <= 0) return -7;
	if(coffer2->lines > 0)
	{
		ret = LoadOutliers(coffer2 ->data, coffer2->srcLen, coffer2->lines);
		//ClearCoffer(coffer2);
		if(ret <= 0) return -8;
	}
	
    char aliasConfigPath[MAX_FILE_NAMELEN] = {'\0'};
    sprintf(aliasConfigPath, "%s/var_alias.conf", path);
    VarAliasManager* aliasManager = VarAliasManager::getInstance();
    aliasManager->setDefaultConfigPath(string(aliasConfigPath));
    aliasManager->initializeForZip(string(path) + "/" + string(filename));
	// init global bitmap for caching
	m_glbExchgLogicmap = new BitMap(m_maxBitmapSize);
	m_glbExchgPatmap = new BitMap(m_maxBitmapSize);
	m_glbExchgBitmap = new BitMap(m_maxBitmapSize);
	m_glbExchgSubBitmap = new BitMap(m_maxBitmapSize);
	m_glbExchgSubTempBitmap = new BitMap(m_maxBitmapSize);

    // load time column & segment index if present
    LoadTimeColumn();
    LoadTimeIndex();
	return ret;
}

int LogStoreApi::LoadGlbMetaHeader(char* filename, size_t& destLen, size_t& srcLen)
{
	m_fptr = fopen(filename, "rb");
	if(m_fptr == NULL) return -1;
    fread(&destLen, sizeof(size_t), 1, m_fptr);
    fread(&srcLen, sizeof(size_t), 1, m_fptr);
	if(destLen <= 0 || srcLen <=0) return -2;
	m_glbMetaHeadLen = sizeof(size_t) + sizeof(size_t) + destLen;
	return 1;
}

//return entries of meta file
int LogStoreApi::LoadGlbMetadata(char* filename, size_t destLen, size_t srcLen)
{
	// 检查输入参数的有效性
	if (destLen <= 0 || srcLen <= 0) {
		SyslogError("无效的压缩数据大小: destLen=%zu, srcLen=%zu\n", destLen, srcLen);
		return -1;
	}

	// 检查解压后大小是否过大
	if (srcLen > MAX_SAFE_DECOMPRESS_SIZE) {
		SyslogError("解压后数据大小超过安全限制: %zu > %d\n", srcLen, MAX_SAFE_DECOMPRESS_SIZE);
		return -1;
	}

	//decompression
	unsigned char* pZstd = nullptr;
	try {
		pZstd = new unsigned char[destLen + 5];
	} catch(std::bad_alloc& e) {
		SyslogError("内存分配失败: pZstd (大小: %zu): %s\n", destLen + 5, e.what());
		return -1;
	}

	size_t readSize = fread(pZstd, sizeof(char), destLen, m_fptr);
	if (readSize != destLen) {
		SyslogError("读取压缩数据失败: 预期 %zu 字节，实际读取 %zu 字节\n", destLen, readSize);
		delete[] pZstd;
		return -1;
	}

	size_t decom_buf_size = ZSTD_getFrameContentSize(pZstd, destLen);
	if (decom_buf_size == ZSTD_CONTENTSIZE_ERROR || decom_buf_size == ZSTD_CONTENTSIZE_UNKNOWN) {
		SyslogError("无法确定解压后大小: %s\n", filename);
		delete[] pZstd;
		return -1;
	}

	// 检查解压后大小与预期大小是否一致
	if (decom_buf_size != srcLen) {
		SyslogError("解压后大小与预期不符: 预期=%zu, 实际=%zu\n", srcLen, decom_buf_size);
		delete[] pZstd;
		return -1;
	}

	try {
		char* meta = new char[decom_buf_size + 5];
		memset(meta, '\0', decom_buf_size + 5);
		int res = ZSTD_decompress(meta, decom_buf_size, pZstd, destLen);
		if(res != srcLen) {
			SyslogError("解压缩失败: %s, 预期大小 %zu, 实际大小 %d\n", filename, srcLen, res);
			delete[] meta;
			delete[] pZstd;
			return -1;
		}
	int offset =0, index =0;
	char* meta_buffer = new char[128];
	if (!meta_buffer) {
		SyslogError("内存分配失败: meta_buffer\n");
		delete[] meta;
		delete[] pZstd;
		return -1;
	}

	for (char *p= meta; *p && p-meta< srcLen + 5; p++)
	{
        if (*p == '\n')
		{
			meta_buffer[offset] = '\0';
			Coffer* newCoffer = new Coffer(string(meta_buffer));
			if (!newCoffer) {
				SyslogError("内存分配失败: newCoffer\n");
				delete[] meta;
				delete[] meta_buffer;
				delete[] pZstd;
				return -1;
			}

			int iname = atoi(newCoffer->filenames.c_str());
			if(newCoffer->srcLen > 0)
				m_glbMeta[iname] = newCoffer;
			if(newCoffer->eleLen == -3 && newCoffer->lines > 0)//var outliers
			{
				LoadVarOutliers(iname, newCoffer->lines, newCoffer->srcLen);
			}	
			offset=0;
			index++;

			//for stat
			if(newCoffer->lines > 0)
			{
				Statistic.total_capsule_cnt++;
			}

			if(iname <= 0) 
			{
				SyslogError("ErrorMeta:%d filename:%s %s\n", iname, FileName.c_str(), meta_buffer);
			}
			SyslogDebug("coffer filenames: %d (%s), srcLen:%d lines: %d eleLen:%d\n", iname, FormatVarName(iname), newCoffer->srcLen, newCoffer->lines, newCoffer->eleLen);
		}
        else
		{
			if (offset < 127) { // 防止缓冲区溢出
				meta_buffer[offset++] = *p;
			} else {
				SyslogError("meta_buffer溢出，行太长\n");
			}
		}
	}

	delete[] meta;
	delete[] meta_buffer;
	delete[] pZstd;
	return index;
	} catch(std::bad_alloc& e) {
		SyslogError("内存分配失败: %s\n", e.what());
		delete[] pZstd;
		return -1;
	}

}

//E18_V2.outlier: srcLen:87573 lines: 5253 eleLen:-1
int LogStoreApi::LoadVarOutliers(int filename, int lines, int srcLen)
{
	if(lines == 0)
	{
		m_varouts[filename] = NULL;
	}
	else
	{
		Coffer* coffer = NULL;
		int ret = DeCompressCapsule(filename, coffer);
		if(ret <= 0) return -1;
		VarOutliers* outs = new VarOutliers(lines);
		char* buf = coffer->data;
		int slim =0;
		int patStartPos = 0;
		int patEndPos = 0;
		int index=0;
		for (int i=0; i< srcLen; i++)
		{
			if (buf[i] == '\n')//a new line
			{
				patEndPos = i-1;
				int outLen = patEndPos - patStartPos + 1;
				outs->Outliers[index] = new char[outLen + 1];
				memcpy(outs->Outliers[index], buf + patStartPos, outLen);
				outs->Outliers[index][outLen] = '\0';
				//SyslogDebug("%s %d %s\n", filename.c_str(), index, outs->Outliers[index]);
				index = 0;
				slim = 0;
			}
			else if(buf[i] == ' ')
			{
				slim++;
				if(slim == 1) patStartPos = i+1;
			}
			else
			{
				if(slim == 0)
				{
					if((buf[i] - '0') >= 0 && (buf[i] - '0') <= 9)
					{
						index = index * 10 + (buf[i] - '0');
					}
					else
					{
						SyslogError("error read outlier file: %d\n", filename);
					}
				}
			}
		}
		m_varouts[filename] = outs;
	}
}

int LogStoreApi::LoadOutliers(IN char *buf, int srcLen, int lineCount)
{
	m_outliers = new CELL[lineCount];
	int index =0;
	int offset =0;
	for (char *p= buf;*p && p-buf< srcLen; p++)
	{
		if (*p == '\n')//a new line
		{
			m_outliers[index]= new char[offset+1];
			m_outliers[index][offset]='\0';
			memcpy(m_outliers[index], p-offset, offset);
			//offset can reach to 18960, can you believe it!
			//SyslogDebug("offset:%d\n", offset);
			index ++;
			offset =0;
		}
		else
		{
			offset++;
		}
	}
	//get the max count
	if(m_maxBitmapSize < lineCount)
		m_maxBitmapSize = lineCount;
	// for(int i=0;i<lineCount;i++)
	// {
	// 	SyslogDebug("%s\n",m_outliers[i]);
	// }
	return index;
}

int LogStoreApi::LoadMainPatternToGlbMap(IN char *buf, IN int srcLen)
{
	char etag = ' '; // should be 'E', otherwise is wrong
	int eid =0;//convert eid to int
	int cnt =0;
	char content[MAX_LINE_SIZE]={'\0'};
	int index=0;
	int slim=0;
	for (char *p= buf;*p && p-buf< srcLen; p++)
	{
		if (*p == '\n')//a new line
		{
			content[index] = '\0';
			int rt = AddMainPatternToMap(etag, eid, cnt, content);
			//if(rt < 0)  SyslogError("srcLen:%d buflen: %d\n ", srcLen, strlen(buf));	
			slim =0;
			index =0;
			cnt =0;
			content[0] = '\0';
		}
        else if(*p == ' ')// a new slim
		{
			if(slim == 2)
			{
				content[index++] = *p;
			}
			else if(slim == 1)
			{
				slim++;
			}
			else if(slim == 0)
			{
				index =0;
				slim++;
			}
		}
		else
		{
			if(slim == 2)//content
			{
				content[index++] = *p;
			}
			if(slim == 1)//count
			{
				cnt = cnt * 10 + (*p - '0');
			}
			else if(slim == 0)//E8
			{
				if(index == 0)
				{
					etag = *p;
					eid =0;
					index++;
				}
				else
				{
					if((*p - '0') >= 0 && (*p - '0') <= 9)
					{
						eid = eid * 10 + (*p - '0');
					}
					else
					{
						SyslogError("error read eid: %d\n", eid);
					}
				}
			}
		}
	}

	return m_patterns.size();
}

int LogStoreApi::LoadSubPatternToGlbMap(IN char *buf, IN int srcLen)
{
	char content[3][MAX_VALUE_LEN]={'\0'};
	int index =0;
	int slim =0;
	for (char *p= buf;*p && p-buf< srcLen; p++)
	{
		if (*p == '\n')//a new line
		{
			content[slim][index] = '\0';
			AddSubPatternToMap(atoi(content[0]), content[1][0], content[2]);
			slim =0;
			index =0;
			content[0][0] = '\0';
			content[1][0] = '\0';
			content[2][0] = '\0';
		}
        else if(*p == ' ' && slim < 2)// a new slim
		{
			content[slim][index] = '\0';
			index =0;
			slim++;
		}
		else
		{
			content[slim][index++] = *p;
		}
	}
	return m_subpatterns.size();
}

//add to map, or can use insert-ordered method if neccessary
int LogStoreApi::AddMainPatternToMap(char etag, int eid, int count, char* content)
{
	if(content == NULL || strlen(content) == 0) return -1;
	SyslogDebug("-----------%d: %d %s\n",eid, count, content);
	//if(etag != 'E' || eid <=0)
	if(eid <=0)
	{
		//SyslogError("error eid: %c%d %s : %d %s\n", etag, eid, FileName.c_str(), count, content);
		return -1;
	}
	eid = (eid <<16);
	LogPattern* logP = new LogPattern();
	logP->Count = count;
	logP->SetContent(content);
	if(logP->SegSize > MAX_CMD_PARAMS_COUNT)
	{
		Syslog("MainPat: out of segment size: %d (%s)\n", MAX_CMD_PARAMS_COUNT, FileName.c_str());
	}
	int segValue = 0;
	for(int i=0; i< logP->SegSize;i++)
	{	
		logP->SegAttr[i] = GetSegmentType(logP->Segment[i], segValue);
		if(logP->SegAttr[i] == SEG_TYPE_VAR)//store var names
		{
			logP->VarNames[i] = eid | (segValue<<8);
		}
		else
		{
			logP->VarNames[i] = NULL;
		}
	}
	//get the max count
	if(m_maxBitmapSize < count)
		m_maxBitmapSize = count;
	//SyslogDebug("\n");
	m_patterns[eid] =logP;
	return 0;
}

//add to map
int LogStoreApi::AddSubPatternToMap(int vid, char type, char* content)
{
	SubPattern* logP = new SubPattern();
	if(type =='S')
	{
		logP->Type = VAR_TYPE_SUB;
		logP->SetContent(content);
		logP->SetSegments(vid, content);
		SyslogDebug("%d (%s) %c %s \n", vid, FormatVarName(vid), type, content);
		// for(int i=0;i< logP->SegSize; i++)
		// {
		// 	if(logP->SegAttr[i] == SEG_TYPE_CONST)
		// 		SyslogDebug("--const--%s\n", logP->Segment[i]);
		// 	else
		// 	{
		// 		SyslogDebug("--%s--%c %d %d\n", logP->Segment[i], logP->SubVars[i]->mark, logP->SubVars[i]->tag, logP->SubVars[i]->len);
		// 	}
		// }
	}
	else if(type =='D')
	{
		logP->Type = VAR_TYPE_DIC;
		int slim =0;
		int patStartPos = 0;
		int patEndPos =0;
		int contLen = strlen(content);
		int len =0;//padding length
    	int lineCnt =0;//totol entries count
		int index=0;
		//get DicCnt
		int dicCnt =0;
		for (int i=0; i< contLen; i++)
		{
			if(content[i] == ' ')// DicCnt
			{
				logP->DicCnt = dicCnt;
				content += i+1; 
				break;
			}
			else
			{
				dicCnt= dicCnt * 10 + (content[i] - '0');
			}
		}
		for (int i=0; i< contLen; i++)
		{
        	if(content[i] == ' ' || content[i] == '\n')// a new slim
			{
				slim++;
				if(slim == 1)
				{
					patEndPos = i-1;
				}
				else if(slim == 3)
				{
					if(index == 0)
					{
						logP->DicVars[index] = new SubPattern::DicVarInfo(len, lineCnt, -1);
					}
					else
					{
						logP->DicVars[index] = new SubPattern::DicVarInfo(len, lineCnt, logP->DicVars[index-1]->lineEno);
					}
					logP->DicVars[index]->SetContent(content + patStartPos, patEndPos - patStartPos +1);
					slim =0;
					len =0;
					lineCnt =0;
					patStartPos = i+1;
					index++;
				}
			}
			else
			{
				if(slim == 1)
				{
					len = len * 10 + (content[i] - '0');
				}
				else if(slim ==2)
				{
					lineCnt = lineCnt * 10 + (content[i] - '0');
				}
				
			}
		}
		SyslogDebug("%d (%s) %c %s\n", vid, FormatVarName(vid), type, content);
		// for(int i=0; i<index; i++)
		// {
		// 	SyslogDebug("---%d--- lineLen:%d lineSno:%d lineCnt:%d %s \n", index, logP->DicVars[i] ->len, logP->DicVars[i] ->lineSno, logP->DicVars[i] ->lineCnt, logP->DicVars[i] ->pat);
		// 	for(int j=0;j< logP->DicVars[i] ->segSize;j++)
		// 	{
		// 		if(logP->DicVars[i] ->segTag[j] == SEG_TYPE_CONST)
		// 			SyslogDebug("------------- %s \n", logP->DicVars[i] ->segCont[j]);
		// 		else
		// 			SyslogDebug("------------- %d \n", logP->DicVars[i] ->segTag[j]);
		// 	}
		// }
	}
	else if(type =='V')
	{
		logP->Type = VAR_TYPE_VAR;
		int slim =0;
		int contLen = strlen(content);
		int tag=0;
		int len=0;
		for (int i=0; i< contLen; i++)
		{
        	if(content[i] == ' ')// a new slim
			{
				slim++;
				if(slim == 1)
				{
					logP->Tag = tag;
				}
			}
			else
			{
				if(slim == 0)
				{
					tag = tag * 10 + (content[i] - '0');
				}
				else if(slim == 1)
				{
					len = len * 10 + (content[i] - '0');
				}
				
			}
		}
		logP->ContSize = len;
		SyslogDebug("%d(%s) %c %d %d\n", vid, FormatVarName(vid), type, logP->Tag, logP->ContSize);
	}
	m_subpatterns[vid] =logP;
	return 0;
}

//decompress patterns
int LogStoreApi::DeCompressCapsule(int patName, OUT Coffer* &coffer, int type)
{
	int ret = 1;
	//if find in cache, then fetch directly
	coffer = m_glbMeta[patName];
	if(coffer == NULL || m_fptr == NULL) {
		SyslogError("错误: coffer或文件指针为空，patName=%d\n", patName);
		return -1; //error patName
	}
	
	// 如果数据已经被缓存，直接返回
	if(coffer->data != NULL) {
		return ret;
	}
	
	// 检查压缩数据的大小是否合理
	if(coffer->destLen <= 0 || coffer->srcLen <= 0) {
		SyslogError("错误: 无效的压缩数据大小，patName=%d, destLen=%d, srcLen=%d\n", 
			patName, coffer->destLen, coffer->srcLen);
		return -1;
	}
	
	// 检查解压后数据大小是否过大（防止内存分配失败）
	if(coffer->srcLen > MAX_SAFE_DECOMPRESS_SIZE) {
		SyslogError("错误: 解压后数据大小超过安全限制，patName=%d, srcLen=%d\n", 
			patName, coffer->srcLen);
		return -1;
	}

	timeval tt1 = ___StatTime_Start();
	
	// 读取压缩数据
	int res = coffer->readFile(m_fptr, m_glbMetaHeadLen);
	if(res < 0) {
		SyslogError("错误: 读取压缩数据失败，patName=%d\n", patName);
		return -2;
	}
	
	// 在解压缩前检查压缩数据的有效性
	size_t decom_buf_size = ZSTD_getFrameContentSize(coffer->cdata, coffer->destLen);
	if(decom_buf_size == ZSTD_CONTENTSIZE_ERROR || decom_buf_size == ZSTD_CONTENTSIZE_UNKNOWN) {
		SyslogError("错误: 无法确定解压后大小或压缩数据无效，patName=%d\n", patName);
		return -3;
	}
	
	// 检查解压后大小与预期大小是否一致
	if(decom_buf_size != (size_t)coffer->srcLen) {
		SyslogError("错误: 解压后大小与预期不符，patName=%d, 预期=%d, 实际=%zu\n", 
			patName, coffer->srcLen, decom_buf_size);
		return -3;
	}
	
	// 解压缩数据
	try {
		res = coffer->decompress();
		if(res < 0) {
			SyslogError("错误: 解压缩失败，patName=%d\n", patName);
			ret = -3;
		}
	} catch(std::bad_alloc& e) {
		SyslogError("错误: 解压缩内存分配失败，patName=%d, 错误信息: %s\n", patName, e.what());
		ret = -4;
	} catch(std::exception& e) {
		SyslogError("错误: 解压缩异常，patName=%d, 错误信息: %s\n", patName, e.what());
		ret = -3;
	}

	double tt2 = ___StatTime_End(tt1);
	//for stat
	if(type != 1 && ret > 0)
	{
		SyslogDebug("decompress[%d]: %d %s %d %d\n", type, patName, FormatVarName(patName), coffer->eleLen, coffer->lines);
		Statistic.total_decom_capsule_cnt++;
		Statistic.total_decom_capsule_time += tt2; 
	}
	
	if(ret < 0)
	{
		SyslogError("Error: not find varname:%s in meta:%s!\n", FormatVarName(patName), FileName.c_str());
	}
	return ret;
}

int LogStoreApi::LoadTimeColumn()
{
    Coffer* coffer = NULL;
    if(m_glbMeta.find(TIME_COL_NAME) == m_glbMeta.end()) return 0;
    int ret = DeCompressCapsule(TIME_COL_NAME, coffer);
    if(ret <= 0) return 0;
    if(!coffer || !coffer->data) return 0;
    m_timeValues.clear();
    int count = coffer->lines;
    int width = coffer->eleLen;
    m_timeValues.reserve(count);
    for(int i=0;i<count;i++){
        const char* p = coffer->data + i*width;
        char buf[32]; int n=0;
        // trim leading spaces
        while(n<width && p[n]==' ') n++;
        int mlen = width-n; if(mlen > 31) mlen = 31;
        memcpy(buf, p+n, mlen); buf[mlen] = '\0';
        long long v = atoll(buf);
        m_timeValues.push_back(v);
    }
    return m_timeValues.size();
}

int LogStoreApi::LoadTimeIndex()
{
    Coffer* coffer = NULL;
    if(m_glbMeta.find(TIME_INDEX_NAME) == m_glbMeta.end()) return 0;
    int ret = DeCompressCapsule(TIME_INDEX_NAME, coffer);
    if(ret <= 0) return 0;
    if(!coffer || !coffer->data) return 0;
    m_segments.clear();
    int sLen = coffer->srcLen;
    int idx = 0; int field = 0; long long segid=0; SegInfo seg{}; char numbuf[64]; int nb=0;
    for(int i=0;i<sLen;i++){
        char c = coffer->data[i];
        if(c=='\n'){
            if(field==5){ m_segments.push_back(seg); }
            field=0; nb=0; memset(&seg,0,sizeof(seg));
            continue;
        }
        if(c==' '){
            numbuf[nb]='\0';
            long long val = atoll(numbuf);
            if(field==0) { segid=val; }
            else if(field==1) { seg.sline=(int)val; }
            else if(field==2) { seg.eline=(int)val; }
            else if(field==3) { seg.tmin=val; }
            else if(field==4) { seg.tmax=val; }
            field++; nb=0; continue;
        }
        if(nb<63){ numbuf[nb++]=c; }
    }
    return m_segments.size();
}

BitMap* LogStoreApi::BuildTimeBitmap(long long start_ms, long long end_ms)
{
    if(m_timeValues.empty()) return NULL;
    BitMap* bm = new BitMap(m_timeValues.size());
    for(size_t i=0;i<m_timeValues.size();i++){
        long long v = m_timeValues[i];
        if(v >= start_ms && v <= end_ms){
            bm->Union((int)i);
        }
    }
    return bm;
}

void LogStoreApi::ApplyTimeFilterToBitmaps(LISTBITMAPS& bitmaps, long long start_ms, long long end_ms)
{
    BitMap* tbm = BuildTimeBitmap(start_ms, end_ms);
    if(!tbm) return;
    // intersect for each pattern bitmap except outliers which has its own count
    for(auto &kv : bitmaps){
        if(kv.first == OUTL_PAT_NAME) continue;
        if(kv.second == NULL) continue;
        kv.second->Inset(tbm);
    }
    delete tbm;
}

//load file segment to mem using mmap, abandoned
int LogStoreApi::LoadFileToMem(const char *varname, int startPos, int bufLen, OUT char *mbuf)
{
	if(m_fd == -1)
	{
		m_fd = open(varname,O_RDONLY);
	}
	if(m_fd != -1)
	{
		lseek(m_fd,startPos,SEEK_SET);
		mbuf = (char *)mmap(NULL,bufLen,PROT_READ,MAP_PRIVATE,m_fd,0);
	}
	return m_fd;
}

//load file segment to mem using mmap, abandoned
unsigned char* LogStoreApi::LoadFileToMem(const char *varname, int startPos, int bufLen)
{
	if(m_fd == -1)
	{
		m_fd = open(varname, O_RDONLY);
	}
	if(m_fd != -1)
	{
		int len = lseek(m_fd,0,SEEK_END);
		if(len > 0) 
		{
			return (unsigned char *)mmap(NULL,len,PROT_READ,MAP_PRIVATE,m_fd,0);
		}
	}
	return NULL;
}

int LogStoreApi::QueryInMmapByKMP(int varname, const char* queryStr, BitMap* bitmap)
{
	Coffer* meta;
	int len = DeCompressCapsule(varname, meta);
	if(len <=0)
	{
		return 0;
	}
	return KMP(meta->data, queryStr, bitmap, QTYPE_ALIGN_ANY);
}

//load vals by Boyer-Moore,  loading with calculating, may accelerate query
int LogStoreApi::QueryByBM_Union(int varname, const char* queryStr, int queryType, BitMap* bitmap)
{
    if(queryType == QTYPE_ALIGN_FULL){ int pass = CheckBloom(varname, queryStr); if(pass == 0) return 0; }
    Coffer* meta=NULL;
    int len = DeCompressCapsule(varname, meta);
	if(len <=0)
	{
		return 0;
	}
	//SyslogDebug("%s: meta: Len:%d line:%d ele: %d\n", FormatVarName(varname), meta->srcLen, meta->lines, meta->eleLen);
	//SyslogDebug("------------%s\n", meta->data);
	int queryLen = strlen(queryStr);
	//int sLen = strlen(meta->data);
	int sLen = meta->srcLen;
	if(INC_TEST_FIXED && meta->eleLen > 0)//same length of each line
	{
		if(queryLen == meta->eleLen)
		{
			return BM_Fixed_Align(meta->data, 0, sLen, queryStr, bitmap, meta->eleLen);
		}
		else if(queryType == QTYPE_ALIGN_ANY)
		{
			return BM_Fixed_Anypos(meta->data, 0, sLen, queryStr, bitmap, meta->eleLen);
		}
		else
		{
			return BM_Fixed_Align(meta->data, 0, sLen, queryStr, bitmap, meta->eleLen, queryType);
		}
	}
	else
	{
		SyslogDebug("-----------------using kmp\n");
		int ret = KMP(meta ->data, queryStr, bitmap, queryType);
		return ret;
	}
}

int LogStoreApi::QueryByBM_Union_RefRange(int varname, const char* queryStr, int queryType, BitMap* bitmap, RegMatrix* refRange)
{
	Coffer* meta;
	int varfname = varname +  VAR_TYPE_DIC;
	int len = DeCompressCapsule(varfname, meta);
	if(len <=0)
	{
		return 0;
	}
	int queryLen = strlen(queryStr);
	int offset=0;
	if(INC_TEST_FIXED == 0)
	{
		int num=0;
		SyslogDebug("------------%s type: %d-----using kmp2\n", queryStr, queryType);
		num = KMP(meta ->data, queryStr, bitmap, queryType);
		return bitmap->GetSize();
	}
	for(int i= 0; i< refRange->Count; i++)
	{
		int sLen = refRange->Matches[i].Match[0].Eo * refRange->Matches[i].Match[0].Index;
		int dicLen;
		int offset = GetDicOffsetByEntry(m_subpatterns[varname], refRange->Matches[i].Match[0].So, dicLen);
		if(queryLen == refRange->Matches[i].Match[0].Index)
		{
			BM_Fixed_Align(meta->data + offset, refRange->Matches[i].Match[0].So, sLen, queryStr, bitmap, refRange->Matches[i].Match[0].Index);
		}
		else if(queryType == QTYPE_ALIGN_ANY)
		{
			BM_Fixed_Anypos(meta->data + offset, refRange->Matches[i].Match[0].So, sLen, queryStr, bitmap, refRange->Matches[i].Match[0].Index);
		}
		else
		{
			BM_Fixed_Align(meta->data + offset, refRange->Matches[i].Match[0].So, sLen, queryStr, bitmap, refRange->Matches[i].Match[0].Index, queryType);
		}
	}
	return bitmap->GetSize();
}

int LogStoreApi::QueryByBM_AxB_Union(int varname, const char* queryStrA, const char* queryStrB, BitMap* bitmap)
{
	Coffer* meta;
	int len = DeCompressCapsule(varname, meta);
	if(len <=0)
	{
		return 0;
	}
	//SyslogDebug("%s: meta: Len:%d line:%d ele: %d\n", FormatVarName(varname), meta->srcLen, meta->lines, meta->eleLen);
	//SyslogDebug("------------%s\n", meta->data);
	int aLen = strlen(queryStrA);
	int bLen = strlen(queryStrB);
	if(meta->eleLen >= (aLen + bLen))//same length of each line
	{
		return BMwildcard_AxB(meta->data, meta->lines, meta->eleLen, queryStrA, queryStrB, bitmap);
	}
	else
	{
		return 0;
	}
}

int LogStoreApi::QueryByBM_Union_ForDic(int varname, const char* querySegs, int querySegCnt, BitMap* bitmap)
{
	Coffer* meta;
	int len = DeCompressCapsule(varname, meta);
	if(len <=0)
	{
		return 0;
	}
	int ret =0;
	if(INC_TEST_FIXED && meta->eleLen > 0)//same length of each line
	{
		if(querySegCnt == 1)
		{
			//ret = SimdCmpEnqual_Union_U8();
			ret = BM_Fixed_Align(meta->data, 0, meta->srcLen, querySegs, bitmap, meta->eleLen);
		}
		else
		{
			ret = BM_Fixed_MutiFul(meta->data, meta->srcLen, querySegs, querySegCnt, bitmap, meta->eleLen);
		}
	}
	else
	{
		SyslogDebug("-----------------using kmp3 %d\n", querySegCnt);
		if(querySegCnt == 1)
		{
			ret = KMP(meta ->data, querySegs, bitmap, QTYPE_ALIGN_FULL);
		}
		else
		{
			for(int i=0;i<querySegCnt;i++)
			{
				ret = KMP(meta ->data, querySegs + i*MAX_DICENTY_LEN, bitmap, QTYPE_ALIGN_FULL);
			}
		}
	}
	return ret;
}

//load vals by Boyer-Moore,  loading with calculating, may accelerate query
//type: 0: full len matched   1: alignleft  2 alignright  3 anypos
int LogStoreApi::QueryByBM_Pushdown(int varname, const char* queryStr, BitMap* bitmap, int type)
{
    if(type == QTYPE_ALIGN_FULL){ int pass = CheckBloom(varname, queryStr); if(pass == 0){ bitmap->Reset(); return 0; } }
    Coffer* meta;
    int len = DeCompressCapsule(varname, meta);
	if(len <=0)
	{
		bitmap->Reset();
		return bitmap->GetSize();
	}
	//SyslogDebug("%s: meta: Len:%d line:%d ele: %d\n", FormatVarName(varname), meta->srcLen, meta->lines, meta->eleLen);
	
	if(INC_TEST_FIXED && meta->eleLen > 0)//same length of each line
	{
		if(INC_TEST_PUSHDOWN)
		{
			int size = bitmap->GetSize();
			int querySize = BM_Fixed_Pushdown(meta->data, meta ->srcLen, queryStr, bitmap, meta->eleLen, type);
			if(querySize != size && querySize != 0)
			{
				Statistic.valid_cap_filter_cnt++;
			}
			return querySize;
		}
		else
		{
			SyslogDebug("-----------------using low pushdown1.\n");
			BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
			tempBitmap->SetSize();
			int rst = BM_Fixed_Pushdown(meta->data, meta ->srcLen, queryStr, tempBitmap, meta->eleLen, type);
			if(bitmap->GetSize() > 0)
			{
				bitmap->Inset(tempBitmap);
			}
			else if(bitmap->BeSizeFul())
			{
				bitmap->Reset();
				bitmap->Union(tempBitmap);
			}
		}
	}
	else
	{
		SyslogDebug("-----------------using kmp4\n");
		if(INC_TEST_PUSHDOWN)
		{
			return BM_Diff_Pushdown(meta ->data, meta ->srcLen, queryStr, bitmap, type);
		}
		else
		{
			SyslogDebug("-----------------using low pushdown2.\n");
			BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
			tempBitmap->SetSize();
			BM_Diff_Pushdown(meta ->data, meta ->srcLen, queryStr, tempBitmap, type);
			bitmap->Inset(tempBitmap);
		}
	}
	return bitmap->GetSize();
}

int LogStoreApi::QueryByBM_Pushdown_RefMap(int varname, const char* queryStr, BitMap* bitmap, BitMap* refBitmap, int type)
{
    if(type == QTYPE_ALIGN_FULL){ int pass = CheckBloom(varname, queryStr); if(pass == 0){ return 0; } }
    Coffer* meta;
    int len = DeCompressCapsule(varname, meta);
	if(len <=0)
	{
		return 0;
	}
	SyslogDebug("%s: meta: Len:%d line:%d ele: %d\n", FormatVarName(varname), meta->srcLen, meta->lines, meta->eleLen);
	//SyslogDebug("------------%s\n", meta->data);
	if(INC_TEST_FIXED && meta->eleLen > 0)//same length of each line
	{
		if(INC_TEST_PUSHDOWN)
		{
			int size = bitmap->GetSize();
			int rst = BM_Fixed_Pushdown_RefMap(meta->data, meta ->srcLen, queryStr, bitmap, refBitmap, meta->eleLen, type);
			if(rst != 0 && size != rst)
			{
				Statistic.valid_cap_filter_cnt++;
			}
			SyslogDebug("QueryByBM_Pushdown_RefMap %d\n", rst);
			return rst;
		}
		else
		{
			SyslogDebug("-----------------using low pushdown3.\n");
			BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
			tempBitmap->SetSize();
			BM_Fixed_Pushdown(meta->data, meta ->srcLen, queryStr, tempBitmap, meta->eleLen, type);
			tempBitmap->Inset(refBitmap);
			bitmap->Union(tempBitmap);
		}
	}
	else
	{
		SyslogDebug("-----------------using kmp5\n");
		if(INC_TEST_PUSHDOWN)
		{
			int rst = BM_Diff_Pushdown_RefMap(meta ->data, meta ->srcLen, queryStr, bitmap, refBitmap, type);
			return rst;
		}
		else
		{
			SyslogDebug("-----------------using low pushdown4.\n");
			BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
			tempBitmap->SetSize();
			BM_Diff_Pushdown(meta ->data, meta ->srcLen, queryStr, tempBitmap, type);
			tempBitmap->Inset(refBitmap);
			bitmap->Union(tempBitmap);
		}
	}
	return bitmap->GetSize();
}

int LogStoreApi::QueryByBM_Pushdown_ForDic(int varname, const char* querySegs, int querySegCnt, BitMap* bitmap)
{
	Coffer* meta;
	int len = DeCompressCapsule(varname, meta);
	if(len <=0)
	{
		return 0;
	}
	int ret =0;
	//SyslogDebug("%s: meta: Len:%d line:%d ele: %d\n", FormatVarName(varname), meta->srcLen, meta->lines, meta->eleLen);
	//SyslogDebug("------------%s %d\n", meta->data, bitmap->GetSize());
	if(INC_TEST_FIXED && meta->eleLen > 0)//same length of each line
	{
		if(querySegCnt == 1)
		{
			if(INC_TEST_PUSHDOWN)
			{
				ret = BM_Fixed_Pushdown(meta->data, meta ->srcLen, querySegs, bitmap, meta->eleLen, 0);
			}
			else
			{
				SyslogDebug("-----------------using low pushdown5.\n");
				BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
				tempBitmap->SetSize();
				BM_Fixed_Pushdown(meta->data, meta ->srcLen, querySegs, tempBitmap, meta->eleLen, 0);
				bitmap->Inset(tempBitmap);
			}
		}
		else
		{
			if(INC_TEST_PUSHDOWN)
			{
				ret = BM_Fixed_Pushdown_MutiFul(meta->data, meta->srcLen, querySegs, querySegCnt, bitmap, meta->eleLen);
			}
			else
			{
				SyslogDebug("-----------------using low pushdown6.\n");
				BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
				tempBitmap->SetSize();
				BM_Fixed_Pushdown_MutiFul(meta->data, meta->srcLen, querySegs, querySegCnt, tempBitmap, meta->eleLen);
				bitmap->Inset(tempBitmap);
			}
		}
	}
	else
	{
		SyslogDebug("--------cnt: %d----%s type: %d--bitmap: %d---using kmp6\n", querySegCnt, querySegs, 0, bitmap->GetSize());
		if(querySegCnt == 1)
		{
			if(INC_TEST_PUSHDOWN)
			{
				ret = BM_Diff_Pushdown(meta->data, meta ->srcLen, querySegs, bitmap, -1);
			}
			else
			{
				SyslogDebug("-----------------using low pushdown7.\n");
				BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
				tempBitmap->SetSize();
				BM_Diff_Pushdown(meta ->data, meta ->srcLen, querySegs, tempBitmap, QTYPE_ALIGN_ANY);
				bitmap->Inset(tempBitmap);
			}
		}
		else
		{
			if(INC_TEST_PUSHDOWN)
			{
				m_glbExchgSubTempBitmap->Reset();
				for(int i=0;i<querySegCnt-1;i++)
				{
					BM_Diff_Pushdown_RefMap(meta ->data, meta ->srcLen, querySegs + i*MAX_DICENTY_LEN, m_glbExchgSubTempBitmap, bitmap, QTYPE_ALIGN_ANY);
				}
				BM_Diff_Pushdown(meta->data, meta ->srcLen, querySegs + (querySegCnt-1) *MAX_DICENTY_LEN, bitmap, QTYPE_ALIGN_ANY);
				bitmap->Union(m_glbExchgSubTempBitmap);
			}
			else
			{
				SyslogDebug("------no support-----------using low pushdown8.\n");
				BitMap* tempOutmap = new BitMap(m_maxBitmapSize);
				for(int i=0;i<querySegCnt;i++)
				{
					BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
					tempBitmap->SetSize();
					BM_Diff_Pushdown(meta ->data, meta ->srcLen, querySegs + i*MAX_DICENTY_LEN, tempBitmap, QTYPE_ALIGN_ANY);
					tempOutmap->Union(tempBitmap);
				}
				bitmap->Inset(tempOutmap);
			}
		}
	}
	return bitmap->GetSize();
}

int LogStoreApi::QueryByBM_Pushdown_ForDic_RefMap(int varname, const char* querySegs, int querySegCnt, BitMap* bitmap, BitMap* refBitmap)
{
	Coffer* meta;
	int len = DeCompressCapsule(varname, meta);
	if(len <=0)
	{
		return 0;
	}
	int ret =0;
	//SyslogDebug("%s: meta: Len:%d line:%d ele: %d\n", FormatVarName(varname), meta->srcLen, meta->lines, meta->eleLen);
	//SyslogDebug("------------%s %d\n", meta->data, bitmap->GetSize());
	if(INC_TEST_FIXED && meta->eleLen > 0)//same length of each line
	{
		if(querySegCnt == 1)
		{
			if(INC_TEST_PUSHDOWN)
			{
				ret = BM_Fixed_Pushdown_RefMap(meta->data, meta ->srcLen, querySegs, bitmap, refBitmap, meta->eleLen, 0);
			}
			else
			{
				SyslogDebug("-----------------using low pushdown5.\n");
				BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
				tempBitmap->SetSize();
				BM_Fixed_Pushdown(meta->data, meta ->srcLen, querySegs, tempBitmap, meta->eleLen, 0);
				tempBitmap->Inset(refBitmap);
				bitmap->Union(tempBitmap);
			}
		}
		else
		{
			if(INC_TEST_PUSHDOWN)
			{

				ret = BM_Fixed_Pushdown_MutiFul_RefMap(meta->data, meta->srcLen, querySegs, querySegCnt, bitmap, refBitmap, meta->eleLen);
			}
			else
			{
				SyslogDebug("-----------------using low pushdown6.\n");
				BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
				tempBitmap->SetSize();
				BM_Fixed_Pushdown_MutiFul(meta->data, meta->srcLen, querySegs, querySegCnt, tempBitmap, meta->eleLen);
				bitmap->Inset(tempBitmap);
				tempBitmap->Inset(refBitmap);
				bitmap->Union(tempBitmap);
			}
		}
	}
	else
	{
		SyslogDebug("------------cnt: %d  %s-----using ref kmp6\n", querySegCnt, querySegs);
		if(querySegCnt == 1)
		{
			if(INC_TEST_PUSHDOWN)
			{
				ret = BM_Diff_Pushdown_RefMap(meta->data, meta ->srcLen, querySegs, bitmap, refBitmap, -1);
			}
			else
			{
				SyslogDebug("-----------------using low pushdown7.\n");
				BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
				tempBitmap->SetSize();
				BM_Diff_Pushdown(meta ->data, meta ->srcLen, querySegs, tempBitmap, QTYPE_ALIGN_ANY);
				tempBitmap->Inset(refBitmap);
				bitmap->Union(tempBitmap);
			}
		}
		else
		{
			if(INC_TEST_PUSHDOWN)
			{
				m_glbExchgSubTempBitmap->Reset();
				for(int i=0;i<querySegCnt-1;i++)
				{
					BM_Diff_Pushdown_RefMap(meta ->data, meta ->srcLen, querySegs + i*MAX_DICENTY_LEN, m_glbExchgSubTempBitmap, refBitmap, QTYPE_ALIGN_ANY);
				}
				BM_Diff_Pushdown_RefMap(meta->data, meta ->srcLen, querySegs + (querySegCnt-1) *MAX_DICENTY_LEN, bitmap, refBitmap, QTYPE_ALIGN_ANY);
				bitmap->Union(m_glbExchgSubTempBitmap);
			}
			else
			{
				SyslogDebug("------no support-----------using low pushdown8.\n");
				BitMap* tempOutmap = new BitMap(m_maxBitmapSize);
				for(int i=0;i<querySegCnt;i++)
				{
					BitMap* tempBitmap = new BitMap(m_maxBitmapSize);
					tempBitmap->SetSize();
					BM_Diff_Pushdown(meta ->data, meta ->srcLen, querySegs + i*MAX_DICENTY_LEN, tempBitmap, QTYPE_ALIGN_ANY);
					tempOutmap->Union(tempBitmap);
				}
				bitmap->Inset(tempOutmap);
				bitmap->Inset(refBitmap);
			}
		}
	}
	return bitmap->GetSize();
}

//load vals by bitmap, achieve efficient skip
//return  0: false    1:true
int LogStoreApi::LoadcVarsByBitmap(int varname, BitMap* bitmap, OUT char *vars, int entryCnt, int varsLineLen, int flag)
{
	Coffer* meta;
	int ret = DeCompressCapsule(varname, meta, 1);
	if(ret <=0) return 0;
	if(INC_TEST_FIXED && meta->eleLen > 0)
	{
		ret = GetCvarsByBitmap_Fixed(meta->data, meta->eleLen, bitmap, vars, entryCnt, varsLineLen, flag);
	}
	else
	{
		ret = GetCvarsByBitmap_Diff(meta->data, meta->srcLen, 0, bitmap, vars, entryCnt, varsLineLen, flag);
	}
	return ret;
}

//return  0: false    1:true
int LogStoreApi::LoadcVars(int varname, int entryCnt, OUT char *vars, int varsLineLen, int flag)
{
	Coffer* meta;
	int ret = DeCompressCapsule(varname, meta, 1);
	if(ret <=0) return 0;
	if(INC_TEST_FIXED && meta->eleLen > 0)
	{
		ret = GetCvars_Fixed(meta->data, meta->eleLen, entryCnt, vars, varsLineLen, flag);
	}
	else
	{
		ret = GetCvars_Diff(meta->data, meta->srcLen, vars, entryCnt, varsLineLen, flag);
	}
	return ret;
}

int LogStoreApi::ClearCoffer(Coffer* &coffer)
{
	if(coffer && coffer ->data)
	{
		delete[] coffer ->data;
		coffer ->data = NULL;
	} 
	if(coffer && coffer ->cdata) 
	{
		delete[] coffer ->cdata;
		coffer ->cdata = NULL;
	}
	return 1;
}
int LogStoreApi::ClearVarFromCache()
{
	LISTMETAS::iterator it = m_glbMeta.begin();
	for (; it != m_glbMeta.end();it++)
	{
		ClearCoffer(it->second);
		it->second = NULL;
	}
	if(m_outliers)
	{
		delete[] m_outliers;
		m_outliers = NULL;
	}
	return 0;
}

int LogStoreApi::DeepCloneMap(LISTBITMAPS source, LISTBITMAPS& des)
{
	des.clear();
	LISTBITMAPS::iterator itor = source.begin();
	for (; itor != source.end();itor++)
	{
		if(itor->second != NULL)
		{
			BitMap* temp = new BitMap(itor ->second ->TotalSize);
			temp->CloneFrom(itor ->second);
			des[itor->first] = temp;
		}
		else
		{
			des[itor->first] = NULL;
		}
	}
}

///////////////////dic & subpat//////////////////////////
//return: length of bitmap, DEF_BITMAP_FULL means matched in sub-pattern, return all
int LogStoreApi::GetVals_Subpat(int varName, const char* regPattern, int queryType, BitMap* bitmap)
{
	m_glbExchgBitmap->Reset();
	m_glbExchgBitmap->SetSize();
	int bitmapSize = GetVals_Subpat_Pushdown(varName, regPattern, queryType, m_glbExchgBitmap);
	//union
	if(bitmapSize !=0)
	{
		bitmap->Union(m_glbExchgBitmap);
	}
	return bitmap->GetSize();
}

int LogStoreApi::GetVals_AxB_Subpat(int varName, const char* queryA, const char* queryB, BitMap* bitmap)
{
	m_glbExchgBitmap->Reset();
	m_glbExchgBitmap->SetSize();
	int bitmapSize = 0;
	//union
	if(bitmapSize !=0)
	{
		bitmap->Union(m_glbExchgBitmap);
	}
	return bitmap->GetSize();
}

int LogStoreApi::GetVals_Subpat_Pushdown(int varName, const char* regPattern, int queryType, BitMap* bitmap)
{
	SyslogDebug("pat: %s %d (%s)\n", regPattern, queryType, FormatVarName(varName));
	//first check outliers
	int varFullName = varName + VAR_TYPE_OUTLIER;
	BitMap* tempBitmap = NULL;
	if(m_varouts[varFullName] != NULL)
	{
		tempBitmap = new BitMap(bitmap->TotalSize);
		GetVarOutliers_BM(varFullName, regPattern, queryType, tempBitmap, bitmap);
		SyslogDebug("out: %d  (%s)\n", tempBitmap->GetSize(), FormatVarName(varName));
	}
	//search in subpattern
	RegMatrix* regResult = new RegMatrix();
	int subPatRst = SubPatternMatch(m_subpatterns[varName], regPattern, queryType, regResult);
	if(subPatRst == MATCH_ONPAT)//match on pat
	{
		Statistic.hit_at_subpat_cnt++;
		SyslogDebug("----------matched only on subpat: %s\n", FormatVarName(varName));
	}
	else if(subPatRst <= MATCH_MISS)//no matched in subpat
	{
		Statistic.total_queried_cap_cnt +=m_subpatterns[varName]->VarNum;
		SyslogDebug("-----------no matched at subpat: %s\n", FormatVarName(varName));
		bitmap->Reset();
	}
	else
	{
		Statistic.total_queried_cap_cnt +=m_subpatterns[varName]->VarNum;
		int realSetNum =regResult->ValidCnt;
		SyslogDebug("---matched at subpat: %s %d %d\n", FormatVarName(varName), realSetNum, bitmap->GetSize());
		int setX = regResult->Count;
		int bitmapSize =0;
		if(realSetNum == 1)//can pushdown directly
		{
			bitmapSize = GetSubVals_Pushdown(regResult->Matches[0], regPattern, queryType, bitmap);
		}
		else if(realSetNum > 1)//must union multi records
		{
			int setY;
			int validCnt =0;
			for(int i= 0; i< setX; i++)
			{
				if(regResult->Matches[i].BeValid == false) continue;
				validCnt++;
				if(validCnt == 1)//the first pushdown merges into m_glbExchgSubBitmap
				{
					m_glbExchgSubBitmap->Reset();
					GetSubVals_Pushdown(regResult->Matches[i], regPattern, queryType, m_glbExchgSubBitmap, bitmap);
				}
				else if(validCnt == realSetNum)//the last directly merges into bitmap
				{
					GetSubVals_Pushdown(regResult->Matches[i], regPattern, queryType, bitmap, NULL);
					bitmap->Union(m_glbExchgSubBitmap);
					break;
				}
				else//the middle, if only one, then merges into m_glbExchgSubBitmap，else into m_glbExchgSubTempBitmap for buffering
				{
					int setY=regResult->Matches[i].Count;
					if(setY == 1)
					{
						GetSubVals_Pushdown(regResult->Matches[i], regPattern, queryType, m_glbExchgSubBitmap, bitmap);
					}
					else
					{
						m_glbExchgSubTempBitmap->Reset();
						GetSubVals_Pushdown(regResult->Matches[i], regPattern, queryType, m_glbExchgSubTempBitmap, bitmap);
						m_glbExchgSubBitmap->Union(m_glbExchgSubTempBitmap);
					}
				}
			}
		}
		else
		{
			SyslogError("Exception occured at matching subpat: %s\n", FormatVarName(varName));
		}
		//SyslogDebug("pat: %s %d (%s) %d\n", regPattern, queryType, FormatVarName(varName), bitmap->GetSize());
	}
	//union result
	if(tempBitmap != NULL)
	{
		SyslogDebug("merge: %d-%d\n", bitmap->GetSize(), tempBitmap->GetSize());
		if(tempBitmap->GetSize() > 0 && !bitmap->BeSizeFul())
		{
			
			bitmap->Union(tempBitmap);
		}
		delete tempBitmap;
	}
	delete regResult;
	return bitmap->GetSize();
}

int LogStoreApi::GetVals_Subpat_Pushdown_RefMap(int varName, const char* regPattern, int queryType, BitMap* bitmap, BitMap* refBitmap)
{
	//first check outliers
	int varFullName = varName + VAR_TYPE_OUTLIER;
	BitMap* tempBitmap = NULL;
	if(m_varouts[varFullName] != NULL)
	{
		tempBitmap = new BitMap(refBitmap->TotalSize);
		GetVarOutliers_BM(varFullName, regPattern, queryType, tempBitmap, refBitmap);
	}
	//search in subpattern
	RegMatrix* regResult = new RegMatrix();
	int subPatRst = SubPatternMatch(m_subpatterns[varName], regPattern, queryType, regResult);
	if(subPatRst == MATCH_ONPAT)//match on pat
	{
		SyslogDebug("----------matched only on subpat: %d\n", varName);
	}
	else if(subPatRst <= MATCH_MISS)//no matched in subpat
	{
		SyslogDebug("-----------no matched at subpat: %d\n", varName);
		bitmap->Reset();
	}
	else
	{
		int realSetNum =regResult->ValidCnt;
		int setX = regResult->Count;
		int bitmapSize =0;
		if(realSetNum == 1)//can pushdown directly
		{
			bitmapSize = GetSubVals_Pushdown(regResult->Matches[0], regPattern, queryType, bitmap, refBitmap);
		}
		else if(realSetNum > 1)//must union multi records
		{
			int setY;
			int validCnt =0;
			for(int i= 0; i< setX; i++)
			{
				if(regResult->Matches[i].BeValid == false) continue;
				validCnt++;
				if(validCnt == 1)
				{
					m_glbExchgSubBitmap->Reset();
					GetSubVals_Pushdown(regResult->Matches[i], regPattern, queryType, m_glbExchgSubBitmap, refBitmap);
				}
				else if(validCnt == realSetNum)
				{
					GetSubVals_Pushdown(regResult->Matches[i], regPattern, queryType, bitmap, refBitmap);
					bitmap->Union(m_glbExchgSubBitmap);
					break;
				}
				else
				{
					int setY=regResult->Matches[i].Count;
					if(setY == 1)
					{
						GetSubVals_Pushdown(regResult->Matches[i], regPattern, queryType, m_glbExchgSubBitmap, refBitmap);
					}
					else
					{
						m_glbExchgSubTempBitmap->Reset();
						GetSubVals_Pushdown(regResult->Matches[i], regPattern, queryType, m_glbExchgSubTempBitmap, refBitmap);
						m_glbExchgSubBitmap->Union(m_glbExchgSubTempBitmap);
					}
				}
			}
		}
		else
		{
			SyslogError("Exception occured at matching subpat: %d\n", varName);
		}
	}
	//union result
	if(tempBitmap != NULL)
	{
		if(tempBitmap->GetSize() > 0 && !bitmap->BeSizeFul())
		{
			bitmap->Union(tempBitmap);
		}
		delete tempBitmap;
	}
	delete regResult;
	return bitmap->GetSize();
}

int LogStoreApi::GetSubVals_Pushdown(RegMatches regmatches, const char* regPattern, int queryType, BitMap* bitmap, BitMap* refBitmap)
{
	int setY=regmatches.Count;
	int type;
	char queryStr[MAX_VALUE_LEN];
	int bitmapSize=0;
	for(int j=0; j< setY;j++)
	{
		memset(queryStr,'\0', MAX_VALUE_LEN);
		if(regmatches.Match[j].Eo == 0)//allow empty space in vars
			queryStr[0] = ' ';
		else
			strncpy(queryStr, regPattern + regmatches.Match[j].So, regmatches.Match[j].Eo);
		if(regmatches.Match[j].So == 0)
		{
			if(regmatches.Match[j].Eo == strlen(queryStr))//<>
				type = queryType;
			else//<>d
				type = QTYPE_ALIGN_RIGHT;
		}
		else
		{
			if(regmatches.Match[j].Eo == strlen(queryStr))//d<>
				type = QTYPE_ALIGN_LEFT;
			else//d<>d
				type = QTYPE_ALIGN_FULL;
		}

		SyslogDebug("queryString %s %d/%d varname: %s type:%d   %d-%d\n", queryStr, j, setY, FormatVarName(regmatches.Match[j].Vname), type, regmatches.Match[j].So, regmatches.Match[j].Eo);
		if(refBitmap == NULL || j >0)
		{
			bitmapSize = QueryByBM_Pushdown(regmatches.Match[j].Vname, queryStr, bitmap, type);
		}
		else
		{
			bitmapSize = QueryByBM_Pushdown_RefMap(regmatches.Match[j].Vname, queryStr, bitmap, refBitmap, type);
		}
		if(bitmapSize == 0)
		{
			break;//no need to continue
		}		
	}	
	return bitmapSize;
}

int LogStoreApi::GetDicIndexs(int varName, const char* regPattern, int queryType, OUT char* &dicQuerySegs)
{
	int num = 0;
	m_glbExchgBitmap->Reset();
	//search in .dic
	RegMatrix* regResult = new RegMatrix();
	int subPatRst = DicPatternMatch(m_subpatterns[varName], regPattern, queryType, regResult);
	if(subPatRst > 0)
	{
		// for(int i= 0; i< regResult->Count; i++)
		// {
		// 	SyslogDebug("---%s-%s--DicPatternMatch: %d %d %d\n", FormatVarName(varName), regPattern, regResult->Matches[i].Match[0].Index, regResult->Matches[i].Match[0].So, regResult->Matches[i].Match[0].Eo);
		// }
		num = QueryByBM_Union_RefRange(varName, regPattern, queryType, m_glbExchgBitmap, regResult);
	}
	//if matched in .dic, then get bitmap in .entry
	if(num > 0)
	{
		SyslogDebug("----in dic query index: %d %d\n", varName, num);
		int varfname = varName + VAR_TYPE_ENTRY;
		int entryLen = m_glbMeta[varfname]->eleLen;
		//dic search result may be bigger than 1
		dicQuerySegs = new char[MAX_DICENTY_LEN * num];
		memset(dicQuerySegs, '\0', MAX_DICENTY_LEN * num);
		for(int i=0; i< num; i++)
		{
			if(INC_TEST_FIXED)
			{
				if(!IntPadding(m_glbExchgBitmap->GetIndex(i), entryLen, dicQuerySegs + MAX_DICENTY_LEN * i))
				{
					SyslogDebug("dic entry padding error. %d\n",varName);
				}
				//SyslogDebug("----%s\n", dicQuerySegs + MAX_DICENTY_LEN * i);
			}
			else
			{
				char temp[MAX_DICENTY_LEN]={'\0'};
				sprintf(temp, "%d", m_glbExchgBitmap->GetIndex(i));
				memcpy(dicQuerySegs + MAX_DICENTY_LEN * i, temp, strlen(temp));
			}
		}
		// for(int i=0; i< num; i++)
		// {
		// 	SyslogDebug("dicQuerySegs %s %d\n", dicQuerySegs, strlen(dicQuerySegs));
		// }
	}
	delete regResult;
	return num;
}

//queryType: 0: aligned left   1:regular matched
int LogStoreApi::GetVals_Dic(int varName, const char* regPattern, int queryType, OUT BitMap* bitmap)
{
	char* dicQuerySegs = NULL;
	int num = GetDicIndexs(varName, regPattern, queryType, dicQuerySegs);
	//if matched in .dic, then get bitmap in .entry
	if(num > 0)
	{
		Statistic.valid_cap_filter_cnt+=2;//dic+entry
		int varfname = varName +  VAR_TYPE_ENTRY;
		QueryByBM_Union_ForDic(varfname, dicQuerySegs, num, bitmap);
	}
	if(dicQuerySegs) delete[] dicQuerySegs;
	return bitmap->GetSize();
}

int LogStoreApi::GetVals_AxB_Dic(int varName, const char* queryA, const char* queryB, OUT BitMap* bitmap)
{
	char* dicQuerySegs = NULL;
	int num = 0;
	m_glbExchgBitmap->Reset();
	int varfname = varName +  VAR_TYPE_DIC;
	//search in .dic
	num = QueryByBM_AxB_Union(varfname, queryA, queryB, m_glbExchgBitmap);
	//if matched in .dic, then get bitmap in .entry
	if(num > 0)
	{
		varfname = varName +  VAR_TYPE_ENTRY;
		int entryLen = m_glbMeta[varfname]->eleLen;
		//dic search result may be bigger than 1
		char* paddingStr = new char[MAX_DICENTY_LEN * num];
		memset(paddingStr, '\0', MAX_DICENTY_LEN * num);
		for(int i=0; i< num; i++)
		{
			//index start from 1
			if(!IntPadding(m_glbExchgBitmap->GetIndex(i), entryLen, paddingStr + MAX_DICENTY_LEN * i))
			{
				SyslogError("dic entry padding error. %d\n",varName);
			}
			//SyslogDebug("---------%d--------%s\n", i, paddingStr + MAX_DICENTY_LEN * i);
		}
		QueryByBM_Union_ForDic(varfname, paddingStr, num, bitmap);
		delete paddingStr;
	}
	return bitmap->GetSize();
}

//queryType: aligned type
int LogStoreApi::GetVals_Dic_Pushdown(int varName, const char* regPattern, int queryType, OUT BitMap* bitmap)
{
	char* dicQuerySegs = NULL;
	int num = GetDicIndexs(varName, regPattern, queryType, dicQuerySegs);
	//if matched in .dic, then get bitmap in .entry
	if(num > 0)
	{
		Statistic.total_queried_cap_cnt++;
		Statistic.valid_cap_filter_cnt+=2;
		SyslogDebug("dic: %s %d %s (%s) %d %d\n", regPattern, queryType, dicQuerySegs, FormatVarName(varName), num, bitmap->GetSize());
		int varfname = varName +  VAR_TYPE_ENTRY;
		QueryByBM_Pushdown_ForDic(varfname, dicQuerySegs, num, bitmap);
		delete dicQuerySegs;
	}
	else//no matched in dic
	{
		bitmap->Reset();
	}
	return bitmap->GetSize();
}

int LogStoreApi::GetVals_Dic_Pushdown_RefMap(int varName, const char* regPattern, int queryType, OUT BitMap* bitmap, BitMap* refBitmap)
{
	char* dicQuerySegs = NULL;
	int num = GetDicIndexs(varName, regPattern, queryType, dicQuerySegs);
	//if matched in .dic, then get bitmap in .entry
	if(num > 0)
	{
		Statistic.total_queried_cap_cnt++;
		Statistic.valid_cap_filter_cnt +=2;
		int varfname = varName +  VAR_TYPE_ENTRY;
		QueryByBM_Pushdown_ForDic_RefMap(varfname, dicQuerySegs, num, bitmap, refBitmap);
		delete dicQuerySegs;
	}
	else//no matched in dic
	{
		bitmap->Reset();
	}
	return bitmap->GetSize();
}

int LogStoreApi::GetDicOffsetByEntry(SubPattern* subpat, int dicNo, int& dicLen)
{
	int offset =0;
	for(int i=0; i< subpat->DicCnt; i++)
	{
		if(dicNo > subpat->DicVars[i]->lineEno)
		{
			offset += subpat->DicVars[i]->lineCnt * subpat->DicVars[i]->len;
		}
		else
		{
			dicNo = dicNo - subpat->DicVars[i]->lineSno;
			offset += dicNo * subpat->DicVars[i]->len;
			dicLen = subpat->DicVars[i]->len;
			break;
		}
	}
	return offset;
}

int LogStoreApi::GetVarOutliers_BM(int varName, const char *queryStr, int queryType, BitMap* bitmap, BitMap* refBitmap)
{
	int matchResult;
	int bitmapIndex =0;
	VarOutliers* outliers = m_varouts[varName];
	int flag = 0;
	if(refBitmap->GetSize() == DEF_BITMAP_FULL)
	{
		for (map<int, char*>::iterator itor = outliers->Outliers.begin(); itor != outliers->Outliers.end(); itor++)
		{
			matchResult = SeqMatching(itor->second, strlen(itor->second), queryStr, strlen(queryStr), queryType);
			if(matchResult >= 0)
			{
				flag=1;
				bitmap->Union(itor->first);
			}
		}
	}
	else
	{
		int bitmapSize = refBitmap->GetSize();
		for(int i=0;i< bitmapSize;i++)
		{
			map<int, char*>::iterator itor = outliers->Outliers.find(refBitmap->GetIndex(i));
			if(itor == outliers->Outliers.end()) 
			{
				continue;//not find in outliers
			}
			matchResult = SeqMatching(itor->second, strlen(itor->second), queryStr, strlen(queryStr), queryType);
			if(matchResult >= 0)
			{
				flag=1;
				bitmap->Union(itor->first);
			}
		}
	}
	if(flag == 1)
	{
		Statistic.valid_cap_filter_cnt++;
	}
	return bitmap->GetSize();
}

//A:B C
int LogStoreApi::GetOutliers_MultiToken(char *args[MAX_CMD_ARG_COUNT], int argCountS, int argCountE, BitMap* bitmap, bool beReverse)
{
	char queryStr[MAX_PATTERN_SIZE]={'\0'};
	RecombineString(args, argCountS, argCountE, queryStr);
	int lineCount = m_glbMeta[OUTL_PAT_NAME] ->lines;
	if(beReverse)
	{
		return QueryInStrArray_BM_Reverse(m_outliers, lineCount, queryStr, bitmap);
	}
	return QueryInStrArray_BM(m_outliers, lineCount, queryStr, bitmap);
}

int LogStoreApi::GetOutliers_MultiToken_RefMap(char *args[MAX_CMD_ARG_COUNT], int argCountS, int argCountE, BitMap* bitmap, BitMap* refbitmap, bool beReverse)
{
	char queryStr[MAX_PATTERN_SIZE]={'\0'};
	RecombineString(args, argCountS, argCountE, queryStr);
	int lineCount = m_glbMeta[OUTL_PAT_NAME] ->lines;
	if(beReverse)
	{
		return QueryInStrArray_BM_Reverse_RefMap(m_outliers, lineCount, queryStr, bitmap, refbitmap);
	}
	return QueryInStrArray_BM_RefMap(m_outliers, lineCount, queryStr, bitmap, refbitmap);
}


int LogStoreApi::GetOutliers_SinglToken(char *arg, BitMap* bitmap, bool beReverse)
{
	char *wArray[MAX_CMD_PARAMS_COUNT];//split by wildcard
	//the style maybe:  	abcd, ab*, a*d, *cd, *bc*.
	//spit seq with '*':	abcd, ab,  [a,b], cd, bc.
	int mCount = Split_NoDelim(arg, WILDCARD, wArray);
	int lineCount = m_glbMeta[OUTL_PAT_NAME] ->lines;
	int bitmapSize =0;
	if(mCount == 1)
	{
		if(beReverse)
		{
			bitmapSize = QueryInStrArray_BM_Reverse(m_outliers, lineCount, wArray[0], bitmap);
		}
		else
		{
			bitmapSize = QueryInStrArray_BM(m_outliers, lineCount, wArray[0], bitmap);
		}
	}
	else if(mCount == 2)//a*b
	{
		string queryAxB(wArray[0]);
		queryAxB += ".*";
		queryAxB += wArray[1];
		if(beReverse)
		{
			bitmapSize = QueryInStrArray_CReg_Reverse(m_outliers, lineCount, queryAxB.c_str(), bitmap);
		}
		else
		{
			bitmapSize = QueryInStrArray_CReg(m_outliers, lineCount, queryAxB.c_str(), bitmap);
		}
	}
	return bitmapSize;
}

int LogStoreApi::GetOutliers_SinglToken_RefMap(char *arg, BitMap* bitmap, BitMap* refbitmap, bool beReverse)
{
	char *wArray[MAX_CMD_PARAMS_COUNT];//split by wildcard
	//the style maybe:  	abcd, ab*, a*d, *cd, *bc*.
	//spit seq with '*':	abcd, ab,  [a,b], cd, bc.
	int mCount = Split_NoDelim(arg, WILDCARD, wArray);
	int lineCount = m_glbMeta[OUTL_PAT_NAME] ->lines;
	int bitmapSize =0;
	if(mCount == 1)
	{
		if(beReverse)
		{
			bitmapSize = QueryInStrArray_BM_Reverse_RefMap(m_outliers, lineCount, wArray[0], bitmap, refbitmap);
		}
		else
		{
			bitmapSize = QueryInStrArray_BM_RefMap(m_outliers, lineCount, wArray[0], bitmap, refbitmap);
		}
	}
	else if(mCount == 2)//a*b
	{
		string queryAxB(wArray[0]);
		queryAxB += ".*";
		queryAxB += wArray[1];
		if(beReverse)
		{
			bitmapSize = QueryInStrArray_CReg_Reverse_RefMap(m_outliers, lineCount, queryAxB.c_str(), bitmap, refbitmap);
		}
		else
		{
			bitmapSize = QueryInStrArray_CReg_RefMap(m_outliers, lineCount, queryAxB.c_str(), bitmap, refbitmap);
		}
	}
	return bitmapSize;
}

///////////////////Materializ///////////////
//return true or false
int LogStoreApi::Materializ_Dic(int varname, BitMap* bitmap, int entryCnt, OUT char* vars)
{
	int dicname = varname + VAR_TYPE_DIC;
	int entryname = varname + VAR_TYPE_ENTRY;
	//load entries and dic
	Coffer* entryMeta; Coffer* dicMeta;
	int ret = DeCompressCapsule(entryname, entryMeta, 1);
	if(ret <=0) return 0;
	ret = DeCompressCapsule(dicname, dicMeta, 1);
	if(ret <=0) return 0;
	char* entryBuf = entryMeta->data;
	char* dicBuf = dicMeta->data;
	int entryLen = entryMeta->eleLen;
	int dicOffset=0;
	int dicLen =0;
	if(!bitmap->BeSizeFul())
	{
		for(int i=0;i< entryCnt;i++)
		{
			//calc dic offset
			if(bitmap->GetIndex(i) < entryMeta->lines)
			{
				dicOffset = atoi(entryBuf + bitmap->GetIndex(i) * entryLen, entryLen);
				if(dicOffset < dicMeta->lines)
				{
					int offset = GetDicOffsetByEntry(m_subpatterns[varname], dicOffset, dicLen);
					RemovePadding(dicBuf + offset, dicLen, vars + i * MAX_VALUE_LEN);
				}
			}
			else//may be bug
			{
				SyslogError("Materializ_Dic: %s , out of range.\n", FormatVarName(varname));
			}
		}
	}
	else
	{
		for(int i=0;i< entryCnt;i++)
		{
			if(i < entryMeta->lines)
			{
				dicOffset = atoi(entryBuf, entryLen);
				if(dicOffset < dicMeta->lines)
				{
					int offset = GetDicOffsetByEntry(m_subpatterns[varname], dicOffset, dicLen);
					RemovePadding(dicBuf + offset, dicLen, vars + i * MAX_VALUE_LEN);
				}
				entryBuf += entryLen;
			}
			else
			{
				SyslogError("Materializ_Dic: %s , out of range.\n", FormatVarName(varname));
			}
		}
	}
	return 1;
}

int LogStoreApi::Materializ_Dic_Kmp(int varname, BitMap* bitmap, int entryCnt, OUT char* vars)
{
	int dicname = varname + VAR_TYPE_DIC;
	int entryname = varname + VAR_TYPE_ENTRY;
	//load entries and dic
	Coffer* entryMeta; Coffer* dicMeta;
	int ret = DeCompressCapsule(entryname, entryMeta, 1);
	if(ret <=0) return 0;
	ret = DeCompressCapsule(dicname, dicMeta, 1);
	if(ret <=0) return 0;
	char* dicBuf = dicMeta->data;
	int entryLen = entryMeta->eleLen;
	int dicOffset=0;
	int dicLen =0;

	char* entryVars = new char[MAX_DICENTY_LEN * entryMeta->lines];
	memset(entryVars, '\0', MAX_DICENTY_LEN * entryMeta->lines);
	char* dicVars = new char[MAX_VALUE_LEN * dicMeta->lines];
	memset(dicVars, '\0', MAX_VALUE_LEN * dicMeta->lines);
	if(entryCnt != DEF_BITMAP_FULL)
	{
		GetCvarsByBitmap_Diff(entryMeta->data, entryMeta->srcLen, 0, bitmap, entryVars, entryCnt, MAX_DICENTY_LEN);
		BitMap* newbitmap = new BitMap(m_maxBitmapSize);
		for(int ii=0; ii< entryCnt; ii++)
		{
			dicOffset = atoi(entryVars + ii * MAX_DICENTY_LEN, MAX_DICENTY_LEN);
			newbitmap->Union(dicOffset);
		}
		GetCvarsByBitmap_Diff(dicMeta->data, dicMeta->srcLen, 0, newbitmap, dicVars, entryCnt, MAX_VALUE_LEN);
		for(int i=0;i< entryCnt;i++)
		{
			memcpy(vars + i * MAX_VALUE_LEN, dicVars + i * MAX_VALUE_LEN, strlen(dicVars + i * MAX_VALUE_LEN));
		}
	}
	else
	{
		GetCvars_Diff(entryMeta->data, entryMeta->srcLen, entryVars, entryCnt, MAX_DICENTY_LEN);
		GetCvars_Diff(dicMeta->data, dicMeta->srcLen, dicVars, entryCnt, MAX_VALUE_LEN);
		for(int i=0;i< entryCnt;i++)
		{
			if(i < entryMeta->lines)
			{
				dicOffset = atoi(entryVars + i * MAX_DICENTY_LEN, MAX_DICENTY_LEN);
				if(dicOffset < dicMeta->lines)
				{
					memcpy(vars + i * MAX_VALUE_LEN, dicVars + dicOffset * MAX_VALUE_LEN, strlen(dicVars + dicOffset * MAX_VALUE_LEN));
				}
			}
			else
			{
				SyslogError("Materializ_Dic: %s, out of range. \n", FormatVarName(varname));
			}
		}
	}
	if(entryVars)
	{
		delete entryVars;
		entryVars = NULL;
	}
	delete dicVars;
	return 1;
}

//return: index of outlier or -1
int LogStoreApi::RebuiltData_Subpat(char* data, int entryLen, int index, int no, int outfilename, string constStr, OUT char* vars)
{
	int offsetT = index * entryLen;
	char* content = data + offsetT;
	//data in outlier
	if(content[entryLen-1] == ' ' && (m_varouts[outfilename] != NULL) && (m_varouts[outfilename] ->Outliers.find(index) != m_varouts[outfilename] ->Outliers.end()))
	{
		return index;
	}
	//copy vals
	int offsetV = no * MAX_VALUE_LEN;
	offsetV += strlen(vars + offsetV);
	int constStrLen = strlen(constStr.c_str());
	if(constStrLen > 0)
	{
		memcpy(vars + offsetV, constStr.c_str(), constStrLen);
		offsetV += constStrLen;
	}
	if(content[entryLen-1] != ' ')
		RemovePadding(content, entryLen, vars + offsetV);
	return -1;
}


int LogStoreApi::Materializ_Subpat(SubPattern* subpat, int varname, BitMap* bitmap, int entryCnt, OUT char* vars)
{
    string constStr = "";
    int varIndex =0;
    int offsetT, offsetV, constStrLen =0;
    int subVarName = 0;
    int outfilename = varname + VAR_TYPE_OUTLIER; 
    std::vector<int> out_idx_vars; out_idx_vars.reserve(entryCnt);
    std::vector<int> out_idx_outs; out_idx_outs.reserve(entryCnt);
    for(int i=0;i< subpat->SegSize; i++)
    {
        if (subpat->SubSegAttr[i] == SEG_TYPE_CONST)
        {
            constStr = subpat->SubSegment[i];
            constStrLen = strlen(constStr.c_str());
        }
        else
        {
            subVarName = varname | (varIndex<<4) | VAR_TYPE_SUB;
            Coffer* entryMeta;
            int ret = DeCompressCapsule(subVarName, entryMeta, 1);
            if(ret <=0)
            {
                SyslogError("Materializ_Subpat: load subpat failed. %d\n", subVarName);
                return 0;
            } 
            int entryLen = entryMeta->eleLen;
            if(!bitmap->BeSizeFul())
            {
                if(INC_TEST_FIXED && entryLen > 0)
                {
                    for(int j=0; j< entryCnt;j++)
                    {
                        int bitmapIndex = bitmap->GetIndex(j);
                        int tempIndex = RebuiltData_Subpat(entryMeta->data, entryLen, bitmapIndex, j, outfilename, constStr, vars);
                        if(tempIndex >=0)
                        {
                            out_idx_vars.push_back(j);
                            out_idx_outs.push_back(tempIndex);
                        }
                    }
                }
                else
                {
                    printf("under implementation!");
                }
            }
            else
            {
                if(INC_TEST_FIXED && entryLen > 0)
                {
                    for(int j=0;j< entryCnt;j++)
                    {
                        int tempIndex = RebuiltData_Subpat(entryMeta->data, entryLen, j, j, outfilename, constStr, vars);
                        if(tempIndex >=0)
                        {
                            out_idx_vars.push_back(j);
                            out_idx_outs.push_back(tempIndex);
                        }
                    }
                }
                else
                {
                    printf("under implementation!");
                }
            }
            varIndex++;
            constStr = "";
        }
    }
    
    if(constStr !="")
    {
        for(int i=0; i< entryCnt; i++)
        {
            offsetV = i * MAX_VALUE_LEN;
            offsetV += strlen(vars + offsetV);
            memcpy(vars + offsetV, constStr.c_str(), constStrLen);
        }
    }
    
    if(m_varouts[outfilename] != NULL)
    {
        for(size_t t=0; t< out_idx_outs.size(); ++t){
            char* outData = m_varouts[outfilename] ->Outliers[out_idx_outs[t]];
            if(outData != NULL)
                memcpy(vars + out_idx_vars[t] * MAX_VALUE_LEN, outData, strlen(outData));
        }
    }
    return 1;
}

//return:  count of records, -1: not find subpat
int LogStoreApi::Materializ_Pats(int varname, BitMap* bitmap, int entryCnt, OUT char* vars)
{
	int ret = 0;
	LISTSUBPATS::iterator itor = m_subpatterns.find(varname);
	if(itor != m_subpatterns.end())//subpattern being found
	{
		if(itor->second->Type == VAR_TYPE_DIC) //dictionary
		{
			if(INC_TEST_FIXED)
			{
				ret = Materializ_Dic(varname, bitmap, entryCnt, vars);
			}
			else
			{
				ret = Materializ_Dic_Kmp(varname, bitmap, entryCnt, vars);
			}
		}
		else if(itor->second->Type == VAR_TYPE_SUB && itor->second->Content != NULL)// subpattern
		{
			ret = Materializ_Subpat(itor->second, varname, bitmap, entryCnt, vars);
		}
		else//.var
		{
			ret = Materializ_Var(varname, bitmap, entryCnt, vars);
		}
	}
	else
	{
		SyslogDebug("do not find in subpat. varname: %s\n", FormatVarName(varname));
	}
	return ret;
}

//return true or false
int LogStoreApi::Materializ_Var(int varname, BitMap* bitmap, int entryCnt, OUT char* vars)
{
	int ret = 0;
	varname += VAR_TYPE_VAR;
	if(bitmap->BeSizeFul())
	{
		ret = LoadcVars(varname, entryCnt, vars, MAX_VALUE_LEN);
	}
	else
	{
		ret = LoadcVarsByBitmap(varname, bitmap, vars, entryCnt, MAX_VALUE_LEN);
	}
	return ret;
}

int LogStoreApi::Materialization(int pid, BitMap* bitmap, int bitmapSize, int matSize)
{
	int entryCnt = bitmapSize >= matSize ? matSize : bitmapSize;
	if(entryCnt <= 0) return entryCnt;

	LogPattern* pat = m_patterns[pid];
	CELL* output = new CELL[pat->SegSize];
	for(int i=0;i< pat->SegSize;i++)
	{
		if(pat->SegAttr[i] == SEG_TYPE_CONST || pat->SegAttr[i] == SEG_TYPE_DELIM)//const string
		{
			output[i] = pat->Segment[i];
		}
		else
		{
			output[i] = new char[MAX_VALUE_LEN * entryCnt];
			memset(output[i], '\0', MAX_VALUE_LEN * entryCnt);
			
			timeval tt1 = ___StatTime_Start();
			Materializ_Pats(pat->VarNames[i], bitmap, entryCnt, output[i]);
			double  time2 = ___StatTime_End(tt1);
			materTime += time2;
			SyslogDebug("----varname:(%d)%s, %lf sec.\n", pat->VarNames[i], FormatVarName(pat->VarNames[i]), time2);
		}
	}
	//print
	for(int k=0; k< entryCnt; k++)
	{
		for(int i=0;i< pat->SegSize;i++)
		{
			if(pat->SegAttr[i] == SEG_TYPE_CONST || pat->SegAttr[i] == SEG_TYPE_DELIM)
			{
				SyslogOut("%s",output[i]);
			}
			else
			{
				SyslogOut("%s",output[i] + MAX_VALUE_LEN * k);
			}
		}
		SyslogOut("\n");
	}
	if(output)
	{
		for(int i=0;i< pat->SegSize;i++)
		{
			if(pat->SegAttr[i] != SEG_TYPE_CONST && pat->SegAttr[i] != SEG_TYPE_DELIM)
			{
				delete[] output[i];
				output[i] = NULL;
			}
		}
	}
	return entryCnt;
}

int LogStoreApi::MaterializOutlier(BitMap* bitmap, int cnt, int refNum)
{
	int doCnt = refNum > cnt ? cnt : refNum;
	for(int i=0; i< doCnt; i++)
	{
		SyslogOut("%s\n", m_outliers[bitmap->GetIndex(i)]);
	}
}

int LogStoreApi::Materialization_JSON(int pid, BitMap* bitmap, int bitmapSize, int matSize, std::string &json_out)
{
    int realSize = bitmapSize;
    bool isFull = (bitmapSize == DEF_BITMAP_FULL);
    
    // 如果是全集，我们需要知道这个 Pattern 到底有多少条日志
    if(isFull) {
        // 这里暂时假设如果标记为 FULL，我们取 matSize 和一个合理上限的最小值
        // 实际上应该从 Pattern 结构中获取该 pid 对应的总行数
        // 为了安全起见，我们先支持物化请求的数量
        realSize = matSize; 
    }

    int entryCnt = realSize >= matSize ? matSize : realSize;
    if(entryCnt <= 0) return 0;

    LogPattern* pat = m_patterns[pid]; 
    if(!pat) return 0;

    CELL* output = new CELL[pat->SegSize];
    for(int i=0;i< pat->SegSize;i++)
    {
        if(pat->SegAttr[i] == SEG_TYPE_CONST || pat->SegAttr[i] == SEG_TYPE_DELIM)
        {
            output[i] = pat->Segment[i];
        }
        else
        {
            output[i] = new char[MAX_VALUE_LEN * entryCnt];
            memset(output[i], '\0', MAX_VALUE_LEN * entryCnt);
            Materializ_Pats(pat->VarNames[i], bitmap, entryCnt, output[i]);
        }
    }
    
    for(int k=0; k < entryCnt; k++)
    {
        char* log_line = new char[MAX_LINE_SIZE];
        memset(log_line, '\0', MAX_LINE_SIZE);
        
        for(int i=0;i< pat->SegSize;i++)
        {
            if(pat->SegAttr[i] == SEG_TYPE_CONST || pat->SegAttr[i] == SEG_TYPE_DELIM)
            {
                strcat(log_line, output[i]);
            }
            else
            {
                strcat(log_line, output[i] + MAX_VALUE_LEN * k);
            }
        }
        
        std::string escaped_log = escape_json(std::string(log_line));
        std::string escaped_template = escape_json(std::string(pat->Content));
            
            if(k > 0) json_out.append(",\n");
            json_out.append("  {\n");
        json_out.append("    \"log_line\": \""); json_out.append(escaped_log); json_out.append("\",\n");
        char buf[64];
        sprintf(buf, "    \"template_id\": %d,\n", pid); json_out.append(buf);
                json_out.append("    \"template\": \""); json_out.append(escaped_template); json_out.append("\",\n");
                
                int lineNum = isFull ? (k + 1) : (bitmap->GetIndex(k) + 1);
                sprintf(buf, "    \"line_number\": %d\n", lineNum); json_out.append(buf);
                
                json_out.append("  }");
        
        delete[] log_line;
    }
    
    if(output)
    {
        for(int i=0;i< pat->SegSize;i++)
        {
            if(pat->SegAttr[i] != SEG_TYPE_CONST && pat->SegAttr[i] != SEG_TYPE_DELIM)
            {
                delete[] output[i];
                output[i] = NULL;
            }
        }
    }
    delete[] output;
    
    return entryCnt;
}

int LogStoreApi::MaterializOutlier_JSON(BitMap* bitmap, int cnt, int refNum, std::string &json_out)
{
    int doCnt = refNum > cnt ? cnt : refNum;
    if(doCnt <= 0) return doCnt;
    
    for(int i=0; i < doCnt; i++)
    {
        std::string escaped_log = escape_json(std::string(m_outliers[bitmap->GetIndex(i)]));
        
        if(i > 0) json_out.append(",\n");
        json_out.append("  {\n");
        json_out.append("    \"log_line\": \""); json_out.append(escaped_log); json_out.append("\",\n");
        json_out.append("    \"template_id\": -1,\n");
        json_out.append("    \"template\": \"OUTLIER\",\n");
        char buf[64];
        sprintf(buf, "    \"line_number\": %d\n", bitmap->GetIndex(i) + 1); json_out.append(buf);
        
        json_out.append("  }");
    }
    
    return doCnt;
}
///////////////////Connect & Disconnect///////////////

int LogStoreApi::IsConnect()
{
	return m_nServerHandle;
}

int LogStoreApi::Connect(char *logStorePath, char* fileName)
{
	if(m_nServerHandle == 1) return m_nServerHandle;
	if(logStorePath == NULL || strlen(logStorePath) <=3)
	{
		logStorePath = DIR_PATH_DEFAULT;
	}
	if(fileName == NULL || strlen(fileName) <=3)
	{
		fileName = FILE_NAME_DEFAULT;
	}
	FileName = string(fileName);
	
	struct timeval t1 = ___StatTime_Start();
	int loadNum = BootLoader(logStorePath, fileName);
	double  time2 = ___StatTime_End(t1);
	SyslogDebug("BootLoader : %lfs %d\n", time2, loadNum);
	RunStatus.LogMetaTime = time2;

	if(loadNum > 0)
	{
		memset(m_filePath,'\0',MAX_DIR_PATH);
		strncpy(m_filePath,logStorePath,strlen(logStorePath));
		m_nServerHandle = 1;
	}
	else
	{
		loadNum = 0;
		m_nServerHandle = 0;
	}
	//test_SimdCmpEqual_Union_U8();
	//test_SimdCmpEqual_Union_U32();
	//test_SimdCmpEqual_Pushdown_U8();
	//test_SimdCmpEqual_Pushdown_U32();
	return loadNum;
}

int LogStoreApi::DisConnect()
{
	//delete old patterns
	for (LISTPATS::iterator itor = m_patterns.begin(); itor != m_patterns.end();itor++)
	{
		if(itor->second)
		{
			delete itor->second;
		}
	}
	m_patterns.clear();
	//delete old subpatterns
	for (LISTSUBPATS::iterator itor = m_subpatterns.begin(); itor != m_subpatterns.end();itor++)
	{
		if(itor->second)
		{
			delete itor->second;
		}
	}
	m_subpatterns.clear();
	memset(m_filePath,'\0',MAX_DIR_PATH);
	if(m_fd > 0)
	{
		close(m_fd);//release file handle
		m_fd = -1;
	}
	if(m_fptr)
	{
		fclose(m_fptr);//release file handle
		m_fptr = NULL;
	}
	ClearVarFromCache();//clear cached vars to release mem
	Release_SearchTemp();
	m_nServerHandle = 0;
	return m_nServerHandle;
}

///////////////////////SELECT///////////////////////////

int LogStoreApi::GetPatterns(OUT vector< pair<string, LogPattern> > &patterns)
{
	// vector< pair<string, LogPattern> > name_score_vec(m_patterns.begin(), m_patterns.end()); 
	// sort(name_score_vec.begin(), name_score_vec.end(), CmpLogPatternByValue()); 
	// patterns = name_score_vec;
	// int vecSize = name_score_vec.size();
	// for (int i=0; i< vecSize;i++)
	// {
	// 	patterns[i] = name_score_vec[i];
	// }
	return 0;
}

int LogStoreApi::GetPatternById(int patId, OUT char** patBody)
{
	LISTPATS::iterator itor = m_patterns.find(patId);
	if(itor != m_patterns.end())
	{
		int len = itor->second->ContSize;
		if(*patBody == NULL)
		{
			*patBody = new char[len + 1];
			(*patBody)[len] = '\0'; // Ensure null termination
		}
		strncpy(*patBody, itor->second->Content, len);
		return len;
	}
	return 0;
}

int LogStoreApi::GetVariablesByPatId(int patId, RegMatch *regResult)
{
	LISTPATS::iterator itor = m_patterns.find(patId);
	if(itor != m_patterns.end())
	{
		return 0;//RegMultiQueryAll(itor->second.Content,"<([^<>]*)>", regResult);
	}
	return 0;
}

//select vals -P E10 -V V3~0 -reg 1582675736   3
//select vals -P E10 -V V5~0 -reg 5699476115821218558   2
int LogStoreApi::GetValuesByVarName_Reg(int varName, const char* regPattern, OUT char* vars, OUT BitMap* bitmap)
{
    int num = QueryByBM_Union(varName, regPattern, QTYPE_ALIGN_ANY, bitmap);
    SyslogDebug("QueryByBM_Union 返回 num=%d\n", num);
    for(int i=0;i<num;i++)
    {
        SyslogDebug("BM_Union bitmap:%d\n", bitmap->GetIndex(i));
    }
    bitmap->Reset();
    num = QueryInMmapByKMP(varName, regPattern, bitmap);
    SyslogDebug("QueryInMmapByKMP 返回 num=%d\n", num);
    for(int i=0;i<num;i++)
    {
        SyslogDebug("KMP bitmap:%d\n", bitmap->GetIndex(i));
    }
    return num;
}

int LogStoreApi::GetValuesByPatId_VarId_Reg(char *args[MAX_CMD_ARG_COUNT], int argCount, OUT char* vars, BitMap* bitmap)
{
	int varname = (atoi(args[3]) <<16) | (atoi(args[5])<<8);
	return GetValuesByVarName_Reg(varname, args[7], vars, bitmap);
}

int LogStoreApi::GetValuesByPatId_VarId(int patId, int varId, OUT char* vars)
{
	return 0;
}

///////////////////////wildcard///////////////////////////////////

int LogStoreApi::SearchInVar_Union(int varName, char *querySeg, short querySegTag, OUT BitMap* bitmap)
{
	int bitmapLen = bitmap->GetSize();//matched or missmatched only at this proc
	LISTSUBPATS::iterator itorsub = m_subpatterns.find(varName);
	//printf("Search '%s' in E%d_V%d\n", querySeg, varName >> POS_TEMPLATE, varName >> POS_VAR & 0xff);
	if(itorsub != m_subpatterns.end())//subpattern being found
	{
		if(itorsub->second->Type == VAR_TYPE_DIC) //.dic
		{
			Statistic.total_queried_cap_cnt++;
			bitmapLen = GetVals_Dic(varName, querySeg, QTYPE_ALIGN_ANY, bitmap);
			if(bitmapLen > 0 || bitmapLen == DEF_BITMAP_FULL) 
			{
				SyslogDebug("dic: %s query num: %d\n", FormatVarName(varName), bitmapLen);
			}
		}
		else if(itorsub->second->Type == VAR_TYPE_SUB && itorsub->second->Content != NULL)//.svar
		{	
			bitmapLen = GetVals_Subpat(varName, querySeg, QTYPE_ALIGN_ANY, bitmap);
			if(bitmapLen > 0  || bitmapLen == DEF_BITMAP_FULL) 
			{
				SyslogDebug("subattr: %s query num: %d\n", FormatVarName(varName), bitmapLen);
			}
		}
		else if(itorsub->second->Type == VAR_TYPE_VAR) //.var
		{
			Statistic.total_queried_cap_cnt++;
			//filter length and tag
			//bool beFilterFailed = (itorsub->second->Tag & querySegTag != querySegTag) || strlen(querySeg) > itorsub->second->ContSize;
			bool beLenFilterFailed = strlen(querySeg) > itorsub->second->ContSize;
			bool beTagFilterFailed = itorsub->second->Tag & querySegTag != querySegTag;
			if(INC_TEST_JUDGETAG && beLenFilterFailed)
			{
				return bitmapLen;
			}
			if(INC_TEST_JUDGETAG && beTagFilterFailed)
			{
				return bitmapLen;
			}

			int varfname = varName + VAR_TYPE_VAR;
			bitmapLen = QueryByBM_Union(varfname, querySeg, QTYPE_ALIGN_ANY, bitmap);
			if(bitmapLen > 0 || bitmapLen == DEF_BITMAP_FULL)
			{
				Statistic.valid_cap_filter_cnt++;
				SyslogDebug(".var:%s, query num: %d\n", FormatVarName(varName), bitmapLen);
			}
		}
		else//unknown type, may skip
		{
			SyslogError("Error: Name= %s\n", FormatVarName(varName));
		}

	}
	else//not find, output error
	{
		SyslogError("Error: not find var in variables.txt(SearchInVar_Union), varname: %s(%d), filename:%s\n", FormatVarName(varName), varName, FileName.c_str());
	}
	return bitmapLen;
}

int LogStoreApi::SearchInVar_AxB_Union(int varName, char *queryA, char *queryB, short qATag, short qBTag, OUT BitMap* bitmap)
{
	int bitmapLen = bitmap->GetSize();//matched or missmatched only at this proc
	int abLen = strlen(queryA) + strlen(queryB);
	LISTSUBPATS::iterator itorsub = m_subpatterns.find(varName);
	//printf("Search %s in E%d_V%d\n", querySeg, varName >> POS_TEMPLATE, varName >> POS_VAR & 0xff);
	if(itorsub != m_subpatterns.end())//subpattern being found
	{
		if(itorsub->second->Type == VAR_TYPE_DIC) //.dic
		{
			bitmapLen = GetVals_AxB_Dic(varName, queryA, queryB, bitmap);
			if(bitmapLen > 0 || bitmapLen == DEF_BITMAP_FULL) 
			{
				SyslogDebug("dic: %s query num: %d\n", FormatVarName(varName), bitmapLen);
			}
		}
		else if(itorsub->second->Type == VAR_TYPE_SUB && itorsub->second->Content != NULL)//.svar
		{
			bitmapLen = GetVals_AxB_Subpat(varName, queryA, queryB, bitmap);
			if(bitmapLen > 0  || bitmapLen == DEF_BITMAP_FULL) 
			{
				SyslogDebug("subattr: %s query num: %d\n", FormatVarName(varName), bitmapLen);
			}
		}
		else if(itorsub->second->Type == VAR_TYPE_VAR) //.var
		{
			bool beOk = (itorsub->second->Tag & qATag != qATag) || strlen(queryA) > itorsub->second->ContSize
				|| (itorsub->second->Tag & qBTag != qBTag) || strlen(queryB) > itorsub->second->ContSize;
			if(INC_TEST_JUDGETAG && beOk)
			{
				return bitmapLen;
			}
			//because no predict filtering, query will slow
			int varfname = varName + VAR_TYPE_VAR;
			bitmapLen = QueryByBM_AxB_Union(varfname, queryA, queryB, bitmap);
			if(bitmapLen > 0 || bitmapLen == DEF_BITMAP_FULL)
			{
				SyslogDebug(".var:%s, query num: %d\n", FormatVarName(varName), bitmapLen);
			}
		}
		else//unknown type, may skip
		{
			SyslogError("Error: name= %s\n", FormatVarName(varName));
		}

	}
	else//not find, output error
	{
		SyslogError("Error: not find var in variables.txt, varname: %s(%d), filename:%s\n", FormatVarName(varName), varName, FileName.c_str());
	}
	return bitmapLen;
}

//queryType: 0: align left   1: align right
int LogStoreApi::SearchInVar_Pushdown(int varName, char *querySeg, short querySegTag, int queryType, OUT BitMap* bitmap)
{
	if(bitmap->GetSize() == 0) return 0;
	//match subpatterns
	int bitmapLen =0;
	LISTSUBPATS::iterator itorsub = m_subpatterns.find(varName);
	//printf("Search '%s' in E%d_V%d\n", querySeg, varName >> POS_TEMPLATE, varName >> POS_VAR & 0xff);
	if(itorsub != m_subpatterns.end())//subpattern being found
	{
		if(itorsub->second->Type == VAR_TYPE_DIC) //it is dictionary
		{
			Statistic.total_queried_cap_cnt++;
			bitmapLen = GetVals_Dic_Pushdown(varName, querySeg, queryType, bitmap);
		}
		else if(itorsub->second->Type == VAR_TYPE_SUB && itorsub->second->Content != NULL)//it is subpattern
		{
			bitmapLen = GetVals_Subpat_Pushdown(varName, querySeg, queryType, bitmap);
		}
		else if(itorsub->second->Type == VAR_TYPE_VAR) //.var
		{
			Statistic.total_queried_cap_cnt++;
			bool beOk = (itorsub->second->Tag & querySegTag != querySegTag) || strlen(querySeg) > itorsub->second->ContSize;
			if(INC_TEST_JUDGETAG && beOk)
			{
				bitmap->Reset();
				return 0;
			}
			//because no predict filtering, query will slow
			int varfname = varName + VAR_TYPE_VAR;
			bitmapLen = QueryByBM_Pushdown(varfname, querySeg, bitmap, queryType);
		}
		else//unknown type, may skip
		{
			SyslogError("Error: Type= %s\n", FormatVarName(varName));
		}

	}
	else//no subpattern
	{
		SyslogError("Error: not find var in variables.txt, varname: %s(%d), filename:%s\n", FormatVarName(varName), varName, FileName.c_str());
	}
	SyslogDebug("----%s : %d\n", FormatVarName(varName), bitmapLen);
	return bitmapLen;
}

int LogStoreApi::SearchInVar_Pushdown_RefMap(int varName, char *querySeg, short querySegTag, int queryType, OUT BitMap* bitmap, BitMap* refBitmap)
{
	//match subpatterns
	int bitmapLen =0;
	LISTSUBPATS::iterator itorsub = m_subpatterns.find(varName);
	//printf("Search '%s' in E%d_V%d\n", querySeg, varName >> POS_TEMPLATE, varName >> POS_VAR & 0xff);
	if(itorsub != m_subpatterns.end())//subpattern being found
	{
		if(itorsub->second->Type == VAR_TYPE_DIC) //it is dictionary
		{
			bitmapLen = GetVals_Dic_Pushdown_RefMap(varName, querySeg, queryType, bitmap, refBitmap);
		}
		else if(itorsub->second->Type == VAR_TYPE_SUB && itorsub->second->Content != NULL)//it is subpattern
		{
			bitmapLen = GetVals_Subpat_Pushdown_RefMap(varName, querySeg, queryType, bitmap, refBitmap);
		}
		else if(itorsub->second->Type == VAR_TYPE_VAR) //.var
		{
			bool beOk = (itorsub->second->Tag & querySegTag != querySegTag) || strlen(querySeg) > itorsub->second->ContSize;
			if(INC_TEST_JUDGETAG && beOk)
			{
				bitmap->Reset();
				return 0;
			}
			//because no predict filtering, query will slow
			int varfname = varName + VAR_TYPE_VAR;
			bitmapLen = QueryByBM_Pushdown_RefMap(varfname, querySeg, bitmap, refBitmap, queryType);
		}
		else//unknown type, may skip
		{
			SyslogError("Error: name= %s\n", FormatVarName(varName));
		}

	}
	else//no subpattern
	{
		SyslogError("Error: not find var in variables.txt, varname: %s(%d), filename:%s\n", FormatVarName(varName), varName, FileName.c_str());
	}
	SyslogDebug("----%s : %d\n", FormatVarName(varName), bitmapLen);
	return bitmapLen;
}

//querySegTag  0: int  1: str
int LogStoreApi::SearchSingleInPattern(LogPattern* logPat, char *queryStr, short queryStrTag, BitMap* bitmap)
{
	int* badc;
	int* goods;
	InitBM(queryStr, badc, goods);
	//assume: first find in main pattern, if matched, then return
	//match with each segment
	for(int i=0; i< logPat->SegSize;i++)
	{
		if(logPat->SegAttr[i] != SEG_TYPE_VAR)//this segment is var, must pushdown to query in vars
		{
			int matchedPos = BM_Once(logPat->Segment[i], queryStr, strlen(logPat->Segment[i]), badc, goods);
			if (matchedPos >=0)
			{
				SyslogDebug("---matched on logPat!----------------\n");
				bitmap->SetSize();
				Statistic.hit_at_mainpat_cnt++;
				return bitmap->GetSize();
			}
			else
			{
				SyslogDebug("[SearchSingle] Mismatch at CONST: patSeg='%s', queryStr='%s'\n", logPat->Segment[i], queryStr);
			}
		}
	}
	for(int i=0; i< logPat->SegSize;i++)
	{
		if(logPat->SegAttr[i] == SEG_TYPE_VAR)//this segment is var, must pushdown to query in vars
		{
			//each var query is isolate, so should union each query result
			int num = SearchInVar_Union(logPat->VarNames[i], queryStr, queryStrTag, bitmap);
			if(num == DEF_BITMAP_FULL)
			{
				//!!! matched in sub-pattern, means matched all
				bitmap->SetSize();
				break;
			}
		}
	}
	return bitmap->GetSize();
}

int LogStoreApi::SearchSingleInPattern_RefMap(LogPattern* logPat, char *queryStr, short queryStrTag, BitMap* bitmap, BitMap* refBitmap)
{
	int* badc;
	int* goods;
	InitBM(queryStr, badc, goods);
	//assume: first find in main pattern, if matched, then return
	//match with each segment
	for(int i=0; i< logPat->SegSize;i++)
	{
		if(logPat->SegAttr[i] != SEG_TYPE_VAR)//this segment is var, must pushdown to query in vars
		{
			int matchedPos = BM_Once(logPat->Segment[i], queryStr, strlen(logPat->Segment[i]), badc, goods);
			if (matchedPos >=0)
			{
				bitmap->Union(refBitmap);
				SyslogDebug("---matched on logPat!-----\n");
				return bitmap->GetSize();
			}
		}
	}
	for(int i=0; i< logPat->SegSize;i++)
	{
		if(logPat->SegAttr[i] == SEG_TYPE_VAR)//this segment is var, must pushdown to query in vars
		{
			m_glbExchgPatmap->Reset();
			//each var query is isolate, so should union each query result
			int num = SearchInVar_Pushdown_RefMap(logPat->VarNames[i], queryStr, queryStrTag, QTYPE_ALIGN_ANY, m_glbExchgPatmap, refBitmap);
			if(num > 0)
			{
				bitmap->Union(m_glbExchgPatmap);
			}
		}
	}
	return bitmap->GetSize();
}

inline int LogStoreApi::GetMatchedAlignType(int segSize, int curIndex)
{
	int type;
	if(segSize == 1)
	{
		type = QTYPE_ALIGN_ANY;
	}
	else
	{
		if(curIndex == 0) type = QTYPE_ALIGN_RIGHT;
		else if(curIndex == segSize-1) type = QTYPE_ALIGN_LEFT;
		else type = QTYPE_ALIGN_FULL;
	}
	return type;
}

int LogStoreApi::SearchMultiInPattern(LogPattern* logPat, char **querySegs, int argCountS, int argCountE, short* querySegTags, int* querySegLens, BitMap* bitmap)
{
	int isMatched;
	int iPos, j, maxI, segLen;
	bitset<MAX_INPUT_TOKENSIZE> flag;//identity that if matched const segment, at least matched one (flag = 1)
	int matchedPatternSegs[MAX_INPUT_TOKENSIZE];
	int segSize = argCountE - argCountS + 1;
	maxI = logPat->SegSize - segSize;// get the max comparing pos
	for(int index=0; index<= maxI;index++)
	{
		memset(matchedPatternSegs, -1, sizeof(matchedPatternSegs));
		iPos = index;
		flag.reset();
		for(j=0; j< segSize; j++)
		{
			// Try to match querySegs[j] against logPat->Segment[iPos...end]
			while (iPos < logPat->SegSize)
			{
				if (logPat->SegAttr[iPos] == SEG_TYPE_DELIM)
				{
					if (querySegTags[j] == TAG_DELIM && querySegs[j + argCountS][0] == logPat->Segment[iPos][0])
					{
						flag.set(j);
						matchedPatternSegs[j] = iPos;
						iPos++;
						break; // Matched DELIM, move to next j
					}
					else if (querySegTags[j] != TAG_DELIM)
					{
						iPos++; // Skip DELIM in pattern
						continue;
					}
					else { break; } // Query is DELIM but doesn't match this pattern DELIM
				}
				else if (logPat->SegAttr[iPos] == SEG_TYPE_VAR)
				{
					if (querySegTags[j] != TAG_DELIM)
					{
						// Check if current query segment matches any LATER CONST segment in the pattern
						bool matchedLater = false;
						for (int k = iPos + 1; k < logPat->SegSize; k++)
						{
							if (logPat->SegAttr[k] == SEG_TYPE_CONST)
							{
								int nextSegLen = strlen(logPat->Segment[k]);
								int nextAlignType = GetMatchedAlignType(segSize, j);
								if (SeqMatching(logPat->Segment[k], nextSegLen, querySegs[j + argCountS], querySegLens[j], nextAlignType) >= 0)
								{
									matchedLater = true;
									break;
								}
							}
							else if (logPat->SegAttr[k] == SEG_TYPE_VAR) {
								// Keep going
							}
							else { // DELIM
								if (querySegTags[j] == TAG_DELIM && querySegs[j + argCountS][0] == logPat->Segment[k][0]) {
									matchedLater = true;
									break;
								}
							}
						}
						
						if (matchedLater)
						{
							iPos++; // Skip this VAR, try to match j later
							continue;
						}
						else
						{
							// Check if j actually matches this VAR's dictionary
							bool isValid = true;
							int vName = logPat->VarNames[iPos];
							if (m_subpatterns.count(vName) && m_subpatterns[vName]->DicCnt > 0)
							{
								char* tmpDicPtr = NULL;
								int aType = GetMatchedAlignType(segSize, j);
								if (GetDicIndexs(vName, querySegs[j + argCountS], aType, tmpDicPtr) <= 0)
								{
									isValid = false;
								}
								if (tmpDicPtr) delete[] tmpDicPtr;
							}

							if (isValid)
							{
								matchedPatternSegs[j] = iPos;
								iPos++;
								break; // Matched VAR (via pushdown later), move to next j
							}
							else
							{
								iPos++; // Not a valid value, skip this VAR
								continue;
							}
						}
					}
					else
					{
						break; // Query is DELIM, cannot match VAR
					}
				}
				else // SEG_TYPE_CONST
				{
					if (querySegTags[j] == TAG_DELIM)
					{
						break; // Mismatch
					}
					else
					{
						segLen = strlen(logPat->Segment[iPos]);
						int alignType = GetMatchedAlignType(segSize, j);
						isMatched = SeqMatching(logPat->Segment[iPos], segLen, querySegs[j + argCountS], querySegLens[j], alignType);
						if (isMatched >= 0)
						{
							flag.set(j);
							matchedPatternSegs[j] = iPos;
							iPos++;
							break; // Matched CONST, move to next j
						}
						else
						{
							break; // Mismatch
						}
					}
				}
			}
			if (iPos > logPat->SegSize || (iPos == logPat->SegSize && j < segSize - 1)) {
				// Failed to match all segments
				j = -1; // Mark as failed
				break;
			}
			if (matchedPatternSegs[j] == -1)
			{
				j = -1;
				break;
			}
		}
		
		if (j == segSize) // Success!
		{
			SyslogDebug("---SearchMultiInPattern Success! Index: %d, Flag Count: %ld---\n", index, flag.count());
			if(flag.count() == segSize)
			{//all matched in main pattern,return full matched
				bitmap->SetSize();
				return bitmap->GetSize();
			}
			m_glbExchgPatmap->Reset();
			int count=0;
			for(int k = 0; k < segSize; k++)
			{
				if(flag[k] == 0)//pushdown to query vars
				{
					if (matchedPatternSegs[k] >= 0 && matchedPatternSegs[k] < logPat->SegSize)
					{
						int alignType = GetMatchedAlignType(segSize, k);
						SyslogDebug("---Pushing down to var: %d, Query: %s---\n", logPat->VarNames[matchedPatternSegs[k]], querySegs[k + argCountS]);
						if(count == 0)
						{
							SearchInVar_Union(logPat->VarNames[matchedPatternSegs[k]], querySegs[k + argCountS], querySegTags[k], m_glbExchgPatmap);
						}
						else
						{
							SearchInVar_Pushdown(logPat->VarNames[matchedPatternSegs[k]], querySegs[k + argCountS], querySegTags[k], alignType, m_glbExchgPatmap);
						}
						count++;
					}
				}
			}
			bitmap->Union(m_glbExchgPatmap);
		}
	}
	if (bitmap->GetSize() == 0) {
		SyslogDebug("---SearchMultiInPattern Failed to find any matches---\n");
	}
	return bitmap->GetSize();
}

int LogStoreApi::SearchMultiInPattern_RefMap(LogPattern* logPat, char **querySegs, int argCountS, int argCountE, short* querySegTags, int* querySegLens, BitMap* bitmap, BitMap* refBitmap)
{
	int isMatched;
	int iPos, j, maxI, segLen;
	bitset<MAX_INPUT_TOKENSIZE> flag;//identity that if matched const segment, at least matched one (flag = 1)
	int matchedPatternSegs[MAX_INPUT_TOKENSIZE];
	int segSize = argCountE - argCountS + 1;
	maxI = logPat->SegSize - segSize;// get the max comparing pos
	for(int index=0; index<= maxI;index++)
	{
		memset(matchedPatternSegs, -1, sizeof(matchedPatternSegs));
		iPos = index;
		flag.reset();
		for(j=0; j< segSize; j++)
		{
			// Try to match querySegs[j] against logPat->Segment[iPos...end]
			while (iPos < logPat->SegSize)
			{
				if (logPat->SegAttr[iPos] == SEG_TYPE_DELIM)
				{
					if (querySegTags[j] == TAG_DELIM && querySegs[j + argCountS][0] == logPat->Segment[iPos][0])
					{
						flag.set(j);
						matchedPatternSegs[j] = iPos;
						iPos++;
						break; // Matched DELIM, move to next j
					}
					else if (querySegTags[j] != TAG_DELIM)
					{
						iPos++; // Skip DELIM in pattern
						continue;
					}
					else { break; } // Query is DELIM but doesn't match this pattern DELIM
				}
				else if (logPat->SegAttr[iPos] == SEG_TYPE_VAR)
				{
					if (querySegTags[j] != TAG_DELIM)
					{
						// Check if current query segment matches any LATER CONST segment in the pattern
						bool matchedLater = false;
						for (int k = iPos + 1; k < logPat->SegSize; k++)
						{
							if (logPat->SegAttr[k] == SEG_TYPE_CONST)
							{
								int nextSegLen = strlen(logPat->Segment[k]);
								int nextAlignType = GetMatchedAlignType(segSize, j);
								if (SeqMatching(logPat->Segment[k], nextSegLen, querySegs[j + argCountS], querySegLens[j], nextAlignType) >= 0)
								{
									matchedLater = true;
									break;
								}
							}
							else if (logPat->SegAttr[k] == SEG_TYPE_VAR) {
								// Keep going
							}
							else { // DELIM
								if (querySegTags[j] == TAG_DELIM && querySegs[j + argCountS][0] == logPat->Segment[k][0]) {
									matchedLater = true;
									break;
								}
							}
						}
						
						if (matchedLater)
						{
							iPos++; // Skip this VAR, try to match j later
							continue;
						}
						else
						{
							// Check if j actually matches this VAR's dictionary
							bool isValid = true;
							int vName = logPat->VarNames[iPos];
							if (m_subpatterns.count(vName) && m_subpatterns[vName]->DicCnt > 0)
							{
								char* tmpDicPtr = NULL;
								int aType = GetMatchedAlignType(segSize, j);
								if (GetDicIndexs(vName, querySegs[j + argCountS], aType, tmpDicPtr) <= 0)
								{
									isValid = false;
								}
								if (tmpDicPtr) delete[] tmpDicPtr;
							}

							if (isValid)
							{
								matchedPatternSegs[j] = iPos;
								iPos++;
								break; // Matched VAR (via pushdown later), move to next j
							}
							else
							{
								iPos++; // Not a valid value, skip this VAR
								continue;
							}
						}
					}
					else
					{
						break; // Query is DELIM, cannot match VAR
					}
				}
				else // SEG_TYPE_CONST
				{
					if (querySegTags[j] == TAG_DELIM)
					{
						break; // Mismatch
					}
					else
					{
						segLen = strlen(logPat->Segment[iPos]);
						int alignType = GetMatchedAlignType(segSize, j);
						isMatched = SeqMatching(logPat->Segment[iPos], segLen, querySegs[j + argCountS], querySegLens[j], alignType);
						if (isMatched >= 0)
						{
							flag.set(j);
							matchedPatternSegs[j] = iPos;
							iPos++;
							break; // Matched CONST, move to next j
						}
						else
						{
							break; // Mismatch
						}
					}
				}
			}
			if (iPos > logPat->SegSize || (iPos == logPat->SegSize && j < segSize - 1)) {
				// Failed to match all segments
				j = -1; // Mark as failed
				break;
			}
			if (matchedPatternSegs[j] == -1)
			{
				j = -1;
				break;
			}
		}
		
		if (j == segSize) // Success!
		{
			SyslogDebug("---SearchMultiInPattern_RefMap Success! Index: %d, Flag Count: %ld---\n", index, flag.count());
			if(flag.count() == segSize)
			{//all matched in main pattern,return full matched
				bitmap->Union(refBitmap);
				break;
			}
			m_glbExchgPatmap->Reset();
			int count=0;
			for(int k = 0; k < segSize; k++)
			{
				if(flag[k] == 0)//pushdown to query vars
				{
					if (matchedPatternSegs[k] >= 0 && matchedPatternSegs[k] < logPat->SegSize)
					{
						int queryType = GetMatchedAlignType(segSize, k);
						SyslogDebug("---Pushing down to var (RefMap): %d, Query: %s---\n", logPat->VarNames[matchedPatternSegs[k]], querySegs[k + argCountS]);
						if(count == 0)
						{
							SearchInVar_Pushdown_RefMap(logPat->VarNames[matchedPatternSegs[k]], querySegs[k + argCountS], querySegTags[k], queryType, m_glbExchgPatmap, refBitmap);
						}
						else
						{
							SearchInVar_Pushdown(logPat->VarNames[matchedPatternSegs[k]], querySegs[k + argCountS], querySegTags[k], queryType, m_glbExchgPatmap);
						}
						count++;
					}
				}
			}
			bitmap->Union(m_glbExchgPatmap);
		}
	}
	if (bitmap->GetSize() == 0) {
		SyslogDebug("---SearchMultiInPattern_RefMap Failed to find any matches---\n");
	}
	return bitmap->GetSize();
}

//query A*B in one token
int LogStoreApi::Search_AxB_InPattern(LogPattern* logPat, char* queryStrA, char* queryStrB, short qATag, short qBTag, BitMap* bitmap)
{
    SyslogDebug("---Search_AxB_InPattern: A='%s', B='%s'---\n", queryStrA, queryStrB);
    int* badc_a; int* goods_a;
    int* badc_b; int* goods_b;
    InitBM(queryStrA, badc_a, goods_a);
    InitBM(queryStrB, badc_b, goods_b);
    int aLen = strlen(queryStrA);
    for(int i=0; i< logPat->SegSize; i++)
    {
        if(logPat->SegAttr[i] == SEG_TYPE_DELIM)
        {
            if(qATag != SEG_TYPE_DELIM) continue;
        }
        if(logPat->SegAttr[i] != SEG_TYPE_VAR)
        {
            int segLenA = strlen(logPat->Segment[i]);
            int posA = BM_Once(logPat->Segment[i], queryStrA, segLenA, badc_a, goods_a);
            if(posA >= 0)
            {
                SyslogDebug("---A matched at Seg[%d]: '%s'---\n", i, logPat->Segment[i]);
                int rem = segLenA - (posA + aLen);
                if(rem > 0)
                {
                    int posB = BM_Once(logPat->Segment[i] + posA + aLen, queryStrB, rem, badc_b, goods_b);
                    if(posB >= 0)
                    {
                        SyslogDebug("---B matched in SAME Seg[%d]---\n", i);
                        bitmap->SetSize();
                        return bitmap->GetSize();
                    }
                }
                for(int j=i+1; j< logPat->SegSize; j++)
                {
                    if(logPat->SegAttr[j] == SEG_TYPE_DELIM)
                    {
                        if(qBTag != SEG_TYPE_DELIM) continue; // Skip DELIM when looking for B
                    }
                    if(logPat->SegAttr[j] != SEG_TYPE_VAR)
                    {
                        int posB2 = BM_Once(logPat->Segment[j], queryStrB, strlen(logPat->Segment[j]), badc_b, goods_b);
                        if(posB2 >= 0)
                        {
                            SyslogDebug("---B matched at LATER Seg[%d]: '%s'---\n", j, logPat->Segment[j]);
                            bitmap->SetSize();
                            return bitmap->GetSize();
                        }
                    }
                    else
                    {
                        m_glbExchgPatmap->Reset();
                        m_glbExchgPatmap->SetSize();
                        int rst = SearchInVar_Pushdown(logPat->VarNames[j], queryStrB, qBTag, QTYPE_ALIGN_ANY, m_glbExchgPatmap);
                        if(rst > 0 || m_glbExchgPatmap->BeSizeFul())
                        {
                            SyslogDebug("---B matched at LATER VAR Seg[%d]: %d---\n", j, logPat->VarNames[j]);
                            bitmap->Union(m_glbExchgPatmap);
                            return bitmap->GetSize();
                        }
                    }
                }
            }
        }
        else
        {
            m_glbExchgPatmap->Reset();
            int rstAxB = SearchInVar_AxB_Union(logPat->VarNames[i], queryStrA, queryStrB, qATag, qBTag, m_glbExchgPatmap);
            if(rstAxB > 0 || m_glbExchgPatmap->BeSizeFul())
            {
                SyslogDebug("---A*B matched in VAR Seg[%d]: %d---\n", i, logPat->VarNames[i]);
                bitmap->Union(m_glbExchgPatmap);
                return bitmap->GetSize();
            }
            m_glbExchgBitmap->Reset();
            m_glbExchgBitmap->SetSize();
            int rstA = SearchInVar_Pushdown(logPat->VarNames[i], queryStrA, qATag, QTYPE_ALIGN_ANY, m_glbExchgBitmap);
            if(rstA > 0 || m_glbExchgBitmap->BeSizeFul())
            {
                SyslogDebug("---A matched in VAR Seg[%d]: %d, looking for B later---\n", i, logPat->VarNames[i]);
                for(int j=i+1; j< logPat->SegSize; j++)
                {
                    if(logPat->SegAttr[j] == SEG_TYPE_DELIM)
                    {
                        if(qBTag != SEG_TYPE_DELIM) continue;
                    }
                    if(logPat->SegAttr[j] != SEG_TYPE_VAR)
                    {
                        int posB3 = BM_Once(logPat->Segment[j], queryStrB, strlen(logPat->Segment[j]), badc_b, goods_b);
                        if(posB3 >= 0)
                        {
                            SyslogDebug("---B matched at LATER Seg[%d]: '%s'---\n", j, logPat->Segment[j]);
                            bitmap->Union(m_glbExchgBitmap);
                            return bitmap->GetSize();
                        }
                    }
                    else
                    {
                        m_glbExchgSubTempBitmap->Reset();
                        m_glbExchgSubTempBitmap->SetSize();
                        int rstB = SearchInVar_Pushdown(logPat->VarNames[j], queryStrB, qBTag, QTYPE_ALIGN_ANY, m_glbExchgSubTempBitmap);
                        if(rstB > 0 || m_glbExchgSubTempBitmap->BeSizeFul())
                        {
                            SyslogDebug("---B matched at LATER VAR Seg[%d]: %d---\n", j, logPat->VarNames[j]);
                            m_glbExchgSubTempBitmap->Inset(m_glbExchgBitmap);
                            bitmap->Union(m_glbExchgSubTempBitmap);
                            return bitmap->GetSize();
                        }
                    }
                }
            }
        }
    }
    return bitmap->GetSize();
}
int LogStoreApi::Search_AxB_InPattern_RefMap(LogPattern* logPat, char* queryStrA, char* queryStrB, short qATag, short qBTag, BitMap* bitmap, BitMap* refBitmap)
{
    SyslogDebug("---Search_AxB_InPattern_RefMap: A='%s', B='%s'---\n", queryStrA, queryStrB);
    int* badc_a; int* goods_a;
    int* badc_b; int* goods_b;
    InitBM(queryStrA, badc_a, goods_a);
    InitBM(queryStrB, badc_b, goods_b);
    int aLen = strlen(queryStrA);
    for(int i=0; i< logPat->SegSize; i++)
    {
        if(logPat->SegAttr[i] == SEG_TYPE_DELIM)
        {
            if(qATag != SEG_TYPE_DELIM) continue;
        }
        if(logPat->SegAttr[i] != SEG_TYPE_VAR)
        {
            int segLenA = strlen(logPat->Segment[i]);
            int posA = BM_Once(logPat->Segment[i], queryStrA, segLenA, badc_a, goods_a);
            if(posA >= 0)
            {
                SyslogDebug("---A matched at Seg[%d]: '%s'---\n", i, logPat->Segment[i]);
                int rem = segLenA - (posA + aLen);
                if(rem > 0)
                {
                    int posB = BM_Once(logPat->Segment[i] + posA + aLen, queryStrB, rem, badc_b, goods_b);
                    if(posB >= 0)
                    {
                        SyslogDebug("---B matched in SAME Seg[%d]---\n", i);
                        bitmap->Union(refBitmap);
                        return bitmap->GetSize();
                    }
                }
                for(int j=i+1; j< logPat->SegSize; j++)
                {
                    if(logPat->SegAttr[j] == SEG_TYPE_DELIM)
                    {
                        if(qBTag != SEG_TYPE_DELIM) continue; // Skip DELIM when looking for B
                    }
                    if(logPat->SegAttr[j] != SEG_TYPE_VAR)
                    {
                        int posB2 = BM_Once(logPat->Segment[j], queryStrB, strlen(logPat->Segment[j]), badc_b, goods_b);
                        if(posB2 >= 0)
                        {
                            SyslogDebug("---B matched at LATER Seg[%d]: '%s'---\n", j, logPat->Segment[j]);
                            bitmap->Union(refBitmap);
                            return bitmap->GetSize();
                        }
                    }
                    else
                    {
                        m_glbExchgPatmap->Reset();
                        m_glbExchgPatmap->SetSize();
                        int rst = SearchInVar_Pushdown_RefMap(logPat->VarNames[j], queryStrB, qBTag, QTYPE_ALIGN_ANY, m_glbExchgPatmap, refBitmap);
                        if(rst > 0)
                        {
                            SyslogDebug("---B matched at LATER VAR Seg[%d]: %d---\n", j, logPat->VarNames[j]);
                            bitmap->Union(m_glbExchgPatmap);
                            return bitmap->GetSize();
                        }
                    }
                }
            }
        }
        else
        {
            m_glbExchgPatmap->Reset();
            // SearchInVar_AxB_Union doesn't have a RefMap version, so we use it and then intersect with refBitmap
            int rstAxB = SearchInVar_AxB_Union(logPat->VarNames[i], queryStrA, queryStrB, qATag, qBTag, m_glbExchgPatmap);
            if(rstAxB > 0 || m_glbExchgPatmap->BeSizeFul())
            {
                SyslogDebug("---A*B matched in VAR Seg[%d]: %d---\n", i, logPat->VarNames[i]);
                if (m_glbExchgPatmap->BeSizeFul())
                {
                    bitmap->Union(refBitmap);
                }
                else
                {
                    m_glbExchgPatmap->Inset(refBitmap);
                    bitmap->Union(m_glbExchgPatmap);
                }
                return bitmap->GetSize();
            }
            
            m_glbExchgBitmap->Reset();
            m_glbExchgBitmap->SetSize();
            int rstA = SearchInVar_Pushdown_RefMap(logPat->VarNames[i], queryStrA, qATag, QTYPE_ALIGN_ANY, m_glbExchgBitmap, refBitmap);
            if(rstA > 0)
            {
                SyslogDebug("---A matched in VAR Seg[%d]: %d, looking for B later---\n", i, logPat->VarNames[i]);
                for(int j=i+1; j< logPat->SegSize; j++)
                {
                    if(logPat->SegAttr[j] == SEG_TYPE_DELIM)
                    {
                        if(qBTag != SEG_TYPE_DELIM) continue;
                    }
                    if(logPat->SegAttr[j] != SEG_TYPE_VAR)
                    {
                        int posB3 = BM_Once(logPat->Segment[j], queryStrB, strlen(logPat->Segment[j]), badc_b, goods_b);
                        if(posB3 >= 0)
                        {
                            SyslogDebug("---B matched at LATER Seg[%d]: '%s'---\n", j, logPat->Segment[j]);
                            bitmap->Union(m_glbExchgBitmap);
                            return bitmap->GetSize();
                        }
                    }
                    else
                    {
                        m_glbExchgSubTempBitmap->Reset();
                        m_glbExchgSubTempBitmap->SetSize();
                        // Important: here we must pushdown using m_glbExchgBitmap (logs where A matched) as refBitmap
                        int rstB = SearchInVar_Pushdown_RefMap(logPat->VarNames[j], queryStrB, qBTag, QTYPE_ALIGN_ANY, m_glbExchgSubTempBitmap, m_glbExchgBitmap);
                        if(rstB > 0)
                        {
                            SyslogDebug("---B matched at LATER VAR Seg[%d]: %d---\n", j, logPat->VarNames[j]);
                            bitmap->Union(m_glbExchgSubTempBitmap);
                            return bitmap->GetSize();
                        }
                    }
                }
            }
        }
    }
    return bitmap->GetSize();
}

int LogStoreApi::Search_SingleSegment(char *querySeg, OUT LISTBITMAPS &bitmaps)
{
    char* pos = NULL;
    pos = strchr(querySeg, ':');
    bool aliasHit = false;
    if(pos && pos != querySeg)
    {
        int alen = pos - querySeg;
        std::string alias(querySeg, alen);
        std::string raw(pos + 1);
        std::string value = raw;
        bool optStrict = false;
        bool optCI = false;
        size_t bar = raw.find('|');
        if(bar != std::string::npos){
            value = raw.substr(0, bar);
            std::string opts = raw.substr(bar+1);
            if(opts.find("strict") != std::string::npos) optStrict = true;
            if(opts.find("ci") != std::string::npos) optCI = true;
        }
        VarAliasManager* mgr = VarAliasManager::getInstance();
        std::vector<int> vids = mgr->getVarIds(alias);
        if(!vids.empty())
        {
            short tag = 0;
            for(size_t idx=0; idx<vids.size(); idx++)
            {
                int varId = vids[idx];
                int pid = (varId & 0xFFFF0000);
                LISTPATS::iterator itor = m_patterns.find(pid);
                if(itor != m_patterns.end())
                {
                    BitMap* bitmap = new BitMap(itor->second->Count);
                    int bitmapLen = 0;
                    LISTSUBPATS::iterator isub = m_subpatterns.find(varId);
                    if(isub != m_subpatterns.end() && isub->second->Type == VAR_TYPE_VAR)
                    {
                        int varfname = varId + VAR_TYPE_VAR;
                        long tmpA=0,tmpB=0; int opt=0;
                        if(__parse_numeric_expr(value, tmpA, tmpB, opt))
                        {
                            bitmapLen = FilterNumericVar(varfname, value.c_str(), bitmap);
                        }
                        else
                        {
                            bitmapLen = QueryByBM_Union(varfname, value.c_str(), QTYPE_ALIGN_ANY, bitmap);
                        }
                    }
                    else
                    {
                        bitmap->SetSize();
                        bitmapLen = SearchInVar_Pushdown(varId, (char*)value.c_str(), tag, QTYPE_ALIGN_ANY, bitmap);
                    }
                    if(bitmapLen > 0 || bitmap->BeSizeFul())
                    {
                        aliasHit = true;
                        LISTBITMAPS::iterator ib = bitmaps.find(pid);
                        if(ib != bitmaps.end() && ib->second != NULL)
                        {
                            ib->second->Union(bitmap);
                            delete bitmap;
                        }
                        else
                        {
                            bitmaps[pid] = bitmap;
                        }
                    }
                    else
                    {
                        delete bitmap;
                        if(!optStrict){
                            BitMap* cbitmap = new BitMap(itor->second->Count);
                            if(!optCI){
                                SearchSingleInPattern(itor->second, (char*)value.c_str(), 0, cbitmap);
                            } else {
                                for(int si=0; si< itor->second->SegSize; si++){
                                    if(itor->second->SegAttr[si] != SEG_TYPE_VAR){
                                        if(strcasestr(itor->second->Segment[si], value.c_str())){ cbitmap->SetSize(); break; }
                                    }
                                }
                            }
                            if(cbitmap->GetSize() > 0 || cbitmap->BeSizeFul())
                            {
                                aliasHit = true;
                                LISTBITMAPS::iterator ib = bitmaps.find(pid);
                                if(ib != bitmaps.end() && ib->second != NULL)
                                {
                                    ib->second->Union(cbitmap);
                                    delete cbitmap;
                                }
                                else
                                {
                                    bitmaps[pid] = cbitmap;
                                }
                            }
                            else
                            {
                                delete cbitmap;
                            }
                        }
                    }
                }
            }
            if(aliasHit) return 0;
            if(!optStrict){
                LISTPATS::iterator pit = m_patterns.begin();
                for (; pit != m_patterns.end(); pit++)
                {
                    BitMap* cbitmap = new BitMap(pit->second->Count);
                    if(!optCI){
                        SearchSingleInPattern(pit->second, (char*)value.c_str(), 0, cbitmap);
                    } else {
                        for(int si=0; si< pit->second->SegSize; si++){
                            if(pit->second->SegAttr[si] != SEG_TYPE_VAR){
                                if(strcasestr(pit->second->Segment[si], value.c_str())){ cbitmap->SetSize(); break; }
                            }
                        }
                    }
                    if(cbitmap->GetSize() > 0 || cbitmap->BeSizeFul())
                    {
                        LISTBITMAPS::iterator ib = bitmaps.find(pit->first);
                        if(ib != bitmaps.end() && ib->second != NULL)
                        {
                            ib->second->Union(cbitmap);
                            delete cbitmap;
                        }
                        else
                        {
                            bitmaps[pit->first] = cbitmap;
                        }
                    }
                    else
                    {
                        delete cbitmap;
                    }
                }
            }
            return 0;
        }
    }
    char *wArray[MAX_CMD_PARAMS_COUNT];//split by wildcard
    //the style maybe:  	abcd, ab*, a*d, *cd, *bc*.
    //spit seq with '*':	abcd, ab,  [a,b], cd, bc.
    int mCount = Split_NoDelim(querySeg, WILDCARD, wArray);
    int num = 0;
	if(mCount == 1)//abcd, ab*, *cd, *bc*.
	{
		short queryStrTag = GetStrTag(wArray[0], strlen(wArray[0]));
		//match with each main pattern
		LISTPATS::iterator itor = m_patterns.begin();
		for (; itor != m_patterns.end();itor++)
		{
			BitMap* bitmap = new BitMap(itor->second->Count);
			num += SearchSingleInPattern(itor->second, wArray[0], queryStrTag, bitmap);
			if(bitmap->GetSize() > 0 || bitmap->BeSizeFul())
			{
				bitmaps[itor->first] = bitmap;
			}
			else
			{
				bitmaps[itor->first] = NULL;
				delete bitmap;
				bitmap = NULL;
			}
		}
	}
	else if(mCount == 2)//a*b
	{
		short queryATag = GetStrTag(wArray[0], strlen(wArray[0]));
		short queryBTag = GetStrTag(wArray[1], strlen(wArray[1]));
		//match with each main pattern
		LISTPATS::iterator itor = m_patterns.begin();
		for (; itor != m_patterns.end();itor++)
		{
			BitMap* bitmap = new BitMap(itor->second->Count);
			num += Search_AxB_InPattern(itor->second, wArray[0], wArray[1], queryATag, queryBTag, bitmap);
			if(bitmap->GetSize() > 0 || bitmap->BeSizeFul())
			{
				bitmaps[itor->first] = bitmap;
			}
			else
			{
				bitmaps[itor->first] = NULL;
				delete bitmap;
				bitmap = NULL;
			}
		}
	}
	//delete
	for(int i=0;i<mCount;i++)
	{
		if (wArray[i])
		{
			delete[] wArray[i];
		}
	}
	return num;
}

//select * -m token:1576667788536595
int LogStoreApi::Search_MultiSegments(char **querySegs, int segSize, OUT LISTBITMAPS& bitmaps)
{
	short* querySegTags = new short[segSize];
	int* querySegLens = new int[segSize];
	for(int i=0; i< segSize; i++)
	{
		querySegLens[i] = strlen(querySegs[i]);
		querySegTags[i] = GetStrTag(querySegs[i], querySegLens[i]);
		//SyslogDebug("%s %d %d\n", querySegs[i], querySegTags[i], querySegLens[i]);
	}
	//match with each main pattern
	LISTPATS::iterator itor = m_patterns.begin();
	for (; itor != m_patterns.end();itor++)
	{
		BitMap* bitmap = new BitMap(itor->second->Count);
		SearchMultiInPattern(itor->second, querySegs, 0, segSize-1, querySegTags, querySegLens, bitmap);
		if(bitmap->GetSize() > 0 || bitmap->BeSizeFul())
		{
			bitmaps[itor->first] = bitmap;
			SyslogDebug("%s %s: entryCnt: %d. [%d] %s }\n", FileName.c_str(), FormatVarName(itor->first), bitmap->GetSize(), itor->second->Count, itor->second->Content);
		}
		else
		{
			bitmaps[itor->first] = NULL;
			delete bitmap;
			bitmap = NULL;
		}
	}
	delete[] querySegTags;
	delete[] querySegLens;
	return 0;
}

int LogStoreApi::IsSearchWithLogic(char *args[MAX_CMD_ARG_COUNT], int argCount)
{
	for(int i=0; i< argCount; i++)
	{
		if(stricmp(args[i], LOGIC_AND) == 0 || stricmp(args[i], LOGIC_OR) == 0 || stricmp(args[i], LOGIC_NOT) == 0)
		{
			return 1;
		}
	}
	return 0;
}

int LogStoreApi::SearchByLogic_not(char *args[MAX_CMD_ARG_COUNT], int argCountS, int argCountE, OUT LISTBITMAPS& bitmaps)
{
	char *wArray[MAX_CMD_PARAMS_COUNT];
	int mCount = 0;
	short queryStrTag = 0;short queryStr2Tag = 0;
	short* querySegTags = NULL;
	int* querySegLens = NULL;
	int segSize = argCountE - argCountS + 1;
	if(segSize == 1)//single
	{
		mCount = Split_NoDelim(args[argCountS], WILDCARD, wArray);
		queryStrTag = GetStrTag(wArray[0], strlen(wArray[0]));
		if(mCount >1)//for A*B
		{
			queryStr2Tag = GetStrTag(wArray[1], strlen(wArray[1]));
		}
	}
	else//multi
	{
		mCount = 0;
		querySegTags = new short[segSize];
		querySegLens = new int[segSize];
		for(int i=0; i< segSize; i++)
		{
			querySegLens[i] = strlen(args[i + argCountS]);
			querySegTags[i] = GetStrTag(args[i + argCountS], querySegLens[i]);
		}
	}
	//match with each main pattern
	LISTPATS::iterator itor = m_patterns.begin();
	for (; itor != m_patterns.end();itor++)
	{
		if(bitmaps[itor->first] == NULL)// query end
		{
			continue;
		}
		else if(bitmaps[itor->first]->BeSizeFul())
		{
			bitmaps[itor->first] ->Reset();
			if(mCount == 1)//abcd, ab*, *cd, *bc*.
			{
				SearchSingleInPattern(itor->second, wArray[0], queryStrTag, bitmaps[itor->first]);
			}
			else if(mCount == 0)//A:B
			{
				SearchMultiInPattern(itor->second, args, argCountS, argCountE, querySegTags, querySegLens, bitmaps[itor->first]);
			}
			else
			{
				Search_AxB_InPattern(itor->second, wArray[0], wArray[1], queryStrTag, queryStr2Tag, bitmaps[itor->first]);
			}
			if(bitmaps[itor->first]->BeSizeFul())
			{
				delete bitmaps[itor->first];
				bitmaps[itor->first] = NULL;
			}
			else
			{
				bitmaps[itor->first]->Reverse();
			}		
		}
		else if(bitmaps[itor->first]->GetSize() > 0)
		{
			if(INC_TEST_PUSHDOWN)
			{
				m_glbExchgLogicmap ->Reset();
				if(mCount == 1)
				{
					SearchSingleInPattern_RefMap(itor->second, wArray[0], queryStrTag, m_glbExchgLogicmap, bitmaps[itor->first]);
				}
				else if(mCount == 0)//A:B
				{
					SearchMultiInPattern_RefMap(itor->second, args, argCountS, argCountE, querySegTags, querySegLens, m_glbExchgLogicmap, bitmaps[itor->first]);
				}
				else
				{
					Search_AxB_InPattern_RefMap(itor->second, wArray[0], wArray[1], queryStrTag, queryStr2Tag, m_glbExchgLogicmap, bitmaps[itor->first]);
				}
				bitmaps[itor->first]->Complement(m_glbExchgLogicmap);
				if(bitmaps[itor->first]->GetSize() == 0)
				{
					delete bitmaps[itor->first];
					bitmaps[itor->first] = NULL;
				}
			}
			else
			{
				m_glbExchgLogicmap ->Reset();
				if(mCount == 1)//abcd, ab*, *cd, *bc*.
				{
					SearchSingleInPattern(itor->second, wArray[0], queryStrTag, m_glbExchgLogicmap);
				}
				else if(mCount == 0)//A:B
				{
					SearchMultiInPattern(itor->second, args, argCountS, argCountE, querySegTags, querySegLens, m_glbExchgLogicmap);
				}
				else
				{
					Search_AxB_InPattern(itor->second, wArray[0], wArray[1], queryStrTag, queryStr2Tag, m_glbExchgLogicmap);
				}
				bitmaps[itor->first]->Complement(m_glbExchgLogicmap);
				if(bitmaps[itor->first]->GetSize() == 0)
				{
					delete bitmaps[itor->first];
					bitmaps[itor->first] = NULL;
				}
			}
		}
	}
	//search in outliers
	if(bitmaps[OUTL_PAT_NAME] == NULL || bitmaps[OUTL_PAT_NAME]->GetSize() == 0)
	{
		;
	}
	else if(bitmaps[OUTL_PAT_NAME]->GetSize() > 0)
	{
		if(INC_TEST_PUSHDOWN)
		{
			if(mCount == 1)
			{
				GetOutliers_SinglToken(wArray[0], bitmaps[OUTL_PAT_NAME], true);
			}
			else if(mCount == 0)
			{
				GetOutliers_MultiToken(args, argCountS, argCountE, bitmaps[OUTL_PAT_NAME], true);
			}
			else if(mCount == 2)//A*B
			{
				string queryAxB(wArray[0]);
				queryAxB += ".*";
				queryAxB += wArray[1];
				int lineCount = m_glbMeta[OUTL_PAT_NAME]->lines;
				QueryInStrArray_CReg_RefMap(m_outliers, lineCount, queryAxB.c_str(), bitmaps[OUTL_PAT_NAME], bitmaps[OUTL_PAT_NAME]);
			}
			if(bitmaps[OUTL_PAT_NAME]->GetSize() == 0)
			{
				delete bitmaps[OUTL_PAT_NAME];
				bitmaps[OUTL_PAT_NAME] = NULL;
			}
		}
		else
		{
			BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
			bitmap_outlier->SetSize();
			if(mCount == 1)
			{
				GetOutliers_SinglToken(wArray[0], bitmap_outlier);
			}
			else if(mCount == 0)
			{
				GetOutliers_MultiToken(args, argCountS, argCountE, bitmap_outlier);
			}
			else if(mCount == 2)//A*B
			{
				string queryAxB(wArray[0]);
				queryAxB += ".*";
				queryAxB += wArray[1];
				int lineCount = m_glbMeta[OUTL_PAT_NAME]->lines;
				QueryInStrArray_CReg_RefMap(m_outliers, lineCount, queryAxB.c_str(), bitmaps[OUTL_PAT_NAME], bitmaps[OUTL_PAT_NAME]);
			}
			if(bitmap_outlier->BeSizeFul())
			{
				delete bitmap_outlier;
				bitmaps[OUTL_PAT_NAME] = NULL;
			}
			else
			{
				bitmaps[OUTL_PAT_NAME]->Complement(bitmap_outlier);
			}
		}
	}
	//delete
	for(int i=0;i<mCount;i++)
	{
		if (wArray[i])
		{
			delete (wArray[i]);
		}
	}
	if(querySegTags) delete[] querySegTags;
	if(querySegLens) delete[] querySegLens;
	return 0;
}

int LogStoreApi::SearchByLogic_norm(char *args[MAX_CMD_ARG_COUNT], int argCountS, int argCountE, OUT LISTBITMAPS& bitmaps)
{
	char *wArray[MAX_CMD_PARAMS_COUNT];
	int mCount = 0;
	short queryStrTag = 0;short queryStr2Tag = 0;
	short* querySegTags = NULL;
	int* querySegLens = NULL;
	int segSize = argCountE - argCountS + 1;
	if(segSize == 1)//single
	{
		mCount = Split_NoDelim(args[argCountS], WILDCARD, wArray);
		queryStrTag = GetStrTag(wArray[0], strlen(wArray[0]));
		if(mCount >1)//for A*B
		{
			queryStr2Tag = GetStrTag(wArray[1], strlen(wArray[1]));
		}
	}
	else//multi
	{
		mCount = 0;
		querySegTags = new short[segSize];
		querySegLens = new int[segSize];
		for(int i=0; i< segSize; i++)
		{
			querySegLens[i] = strlen(args[i + argCountS]);
			querySegTags[i] = GetStrTag(args[i + argCountS], querySegLens[i]);
		}
	}
	//match with each main pattern
	LISTPATS::iterator itor = m_patterns.begin();
	LISTBITMAPS::iterator ifind;
	for (; itor != m_patterns.end();itor++)
	{
		ifind = bitmaps.find(itor->first);
		//not find, means first step to search
		if(ifind == bitmaps.end())
		{
			BitMap* bitmap = new BitMap(itor->second->Count);
			if(mCount == 1)//abcd, ab*, *cd, *bc*.
			{
				SearchSingleInPattern(itor->second, wArray[0], queryStrTag, bitmap);
			}
			else if(mCount == 0)//A:B
			{
				SearchMultiInPattern(itor->second, args, argCountS, argCountE, querySegTags, querySegLens, bitmap);
			}
			else
			{
				Search_AxB_InPattern(itor->second, wArray[0], wArray[1], queryStrTag, queryStr2Tag, bitmap);
			}
			if(bitmap->GetSize() == 0)
			{
				bitmaps[itor->first] = NULL;
				delete bitmap;
				bitmap = NULL;
			}
			else
			{
				bitmaps[itor->first] = bitmap;
			}
		}
		else if(bitmaps[itor->first] == NULL)// query end
		{
			continue;
		}
		else if(bitmaps[itor->first]->BeSizeFul())
		{
			bitmaps[itor->first] ->Reset();
			if(mCount == 1)//abcd, ab*, *cd, *bc*.
			{
				SearchSingleInPattern(itor->second, wArray[0], queryStrTag, bitmaps[itor->first]);
			}
			else if(mCount == 0)//A:B
			{
				SearchMultiInPattern(itor->second, args, argCountS, argCountE, querySegTags, querySegLens, bitmaps[itor->first]);
			}
			else
			{
				Search_AxB_InPattern(itor->second, wArray[0], wArray[1], queryStrTag, queryStr2Tag, bitmaps[itor->first]);
			}
			if(bitmaps[itor->first]->GetSize() == 0)
			{
				delete bitmaps[itor->first];
				bitmaps[itor->first] = NULL;
			}			
		}
		else if(bitmaps[itor->first]->GetSize() > 0)
		{
			BitMap* bitmap = new BitMap(itor->second->Count);
			if(INC_TEST_PUSHDOWN)
			{
				if(mCount == 1)
				{
					SearchSingleInPattern_RefMap(itor->second, wArray[0], queryStrTag, bitmap, bitmaps[itor->first]);
				}
				else if(mCount == 0)//A:B
				{
					SearchMultiInPattern_RefMap(itor->second, args, argCountS, argCountE, querySegTags, querySegLens, bitmap, bitmaps[itor->first]);
				}
				else
				{
					//Search_AxB_InPattern_Logic(itor->second, wArray[0], wArray[1], rangeSize, range, bitmaps[itor->first]);
				}
				if(bitmap->GetSize() == 0)
				{
					delete bitmaps[itor->first];
					bitmaps[itor->first] = NULL;
				}
				else
				{
					delete bitmaps[itor->first];
					bitmaps[itor->first] = bitmap;
				}
			}
			else
			{
				if(mCount == 1)//abcd, ab*, *cd, *bc*.
				{
					SearchSingleInPattern(itor->second, wArray[0], queryStrTag, bitmap);
				}
				else if(mCount == 0)//A:B
				{
					SearchMultiInPattern(itor->second, args, argCountS, argCountE, querySegTags, querySegLens, bitmap);
				}
				else
				{
					Search_AxB_InPattern(itor->second, wArray[0], wArray[1], queryStrTag, queryStr2Tag, bitmap);
				}
				if(bitmap->GetSize() == 0)
				{
					delete bitmaps[itor->first];
					bitmaps[itor->first] = NULL;
				}
				else if(bitmap->BeSizeFul())
				{
					;//do nothing
				}
				else
				{
					bitmaps[itor->first]->Inset(bitmap);
				}
			}
		}
		else
		{
			if(bitmaps[itor->first])
			{
				delete bitmaps[itor->first];
				bitmaps[itor->first] = NULL;
			}
		}
	}
	//search in outliers
	ifind = bitmaps.find(OUTL_PAT_NAME);
	if(ifind == bitmaps.end())
	{
		BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
		bitmap_outlier->SetSize();
		if(mCount == 1)
		{
			GetOutliers_SinglToken(wArray[0], bitmap_outlier);
		}
		else if(mCount == 0)
		{
			GetOutliers_MultiToken(args, argCountS, argCountE, bitmap_outlier);
		}
		else if(mCount == 2)//A*B
		{
			string queryAxB(wArray[0]);
			queryAxB += ".*";
			queryAxB += wArray[1];
			int lineCount = m_glbMeta[OUTL_PAT_NAME]->lines;
			QueryInStrArray_CReg(m_outliers, lineCount, queryAxB.c_str(), bitmap_outlier);
		}
		if(bitmap_outlier->GetSize() == 0)
		{
			delete bitmap_outlier;
			bitmaps[OUTL_PAT_NAME] = NULL;
		}
		else
		{
			bitmaps[OUTL_PAT_NAME] = bitmap_outlier;
		}
	}
	else if(bitmaps[OUTL_PAT_NAME] == NULL || bitmaps[OUTL_PAT_NAME]->GetSize() == 0)
	{
		;//query end
	}
	else if(bitmaps[OUTL_PAT_NAME]->GetSize() > 0)
	{
		if(INC_TEST_PUSHDOWN)
		{
			if(mCount == 1)
			{
				GetOutliers_SinglToken(wArray[0], bitmaps[OUTL_PAT_NAME]);
			}
			else if(mCount == 0)
			{
				GetOutliers_MultiToken(args, argCountS, argCountE, bitmaps[OUTL_PAT_NAME]);
			}
			else//A*B
			{
				;
			}
			if(bitmaps[OUTL_PAT_NAME]->GetSize() == 0)
			{
				delete bitmaps[OUTL_PAT_NAME];
				bitmaps[OUTL_PAT_NAME] = NULL;
			}
		}
		else
		{
			BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
			bitmap_outlier->SetSize();
			if(mCount == 1)
			{
				GetOutliers_SinglToken(wArray[0], bitmap_outlier);
			}
			else if(mCount == 0)
			{
				GetOutliers_MultiToken(args, argCountS, argCountE, bitmap_outlier);
			}
			else//A*B
			{
				;
			}
			if(bitmap_outlier->GetSize() == 0)
			{
				delete bitmap_outlier;
				bitmaps[OUTL_PAT_NAME] = NULL;
			}
			else
			{
				bitmaps[OUTL_PAT_NAME]->Inset(bitmap_outlier);
			}
		}
	}
	else
	{
		SyslogError("bitmaps[OUTL_PAT_NAME]->GetSize() is -99!");
	}

	//delete
	for(int i=0;i<mCount;i++)
	{
		if (wArray[i])
		{
			delete (wArray[i]);
		}
	}
	if(querySegTags) delete[] querySegTags;
	if(querySegLens) delete[] querySegLens;
	return 0;
}

int LogStoreApi::SearchByLogic_norm_RefMap(char *args[MAX_CMD_ARG_COUNT], int argCountS, int argCountE, OUT LISTBITMAPS& bitmaps, LISTBITMAPS refbitmaps)
{
	char *wArray[MAX_CMD_PARAMS_COUNT];
	int mCount = 0;
	short queryStrTag = 0;short queryStr2Tag = 0;
	short* querySegTags = NULL;
	int* querySegLens = NULL;
	int segSize = argCountE - argCountS + 1;
	if(segSize == 1)//single
	{
		mCount = Split_NoDelim(args[argCountS], WILDCARD, wArray);
		queryStrTag = GetStrTag(wArray[0], strlen(wArray[0]));
		if(mCount >1)//for A*B
		{
			queryStr2Tag = GetStrTag(wArray[1], strlen(wArray[1]));
		}
	}
	else//multi
	{
		mCount = 0;
		querySegTags = new short[segSize];
		querySegLens = new int[segSize];
		for(int i=0; i< segSize; i++)
		{
			querySegLens[i] = strlen(args[i + argCountS]);
			querySegTags[i] = GetStrTag(args[i + argCountS], querySegLens[i]);
		}
	}
	//match with each main pattern
	LISTPATS::iterator itor = m_patterns.begin();
	LISTBITMAPS::iterator ifind;
	for (; itor != m_patterns.end();itor++)
	{
		ifind = refbitmaps.find(itor->first);
		//not find, means first step to search
		if(ifind == refbitmaps.end() || (refbitmaps[itor->first] != NULL && refbitmaps[itor->first]->BeSizeFul()))
		{
			BitMap* bitmap = new BitMap(itor->second->Count);
			if(mCount == 1)//abcd, ab*, *cd, *bc*.
			{
				SearchSingleInPattern(itor->second, wArray[0], queryStrTag, bitmap);
			}
			else if(mCount == 0)//A:B
			{
				SearchMultiInPattern(itor->second, args, argCountS, argCountE, querySegTags, querySegLens, bitmap);
			}
			else
			{
				Search_AxB_InPattern(itor->second, wArray[0], wArray[1], queryStrTag, queryStr2Tag, bitmap);
			}
			if(bitmap->GetSize() == 0)
			{
				delete bitmap;
				LISTBITMAPS::iterator ifind = bitmaps.find(itor->first);
				if(ifind != bitmaps.end())
				{
					;
				}
				else
				{
					bitmaps[itor->first] = NULL;
				}
			}
			else
			{
				LISTBITMAPS::iterator ifind = bitmaps.find(itor->first);
				if(ifind != bitmaps.end() && bitmaps[itor->first] != NULL)
				{
					bitmaps[itor->first]->Union(bitmap);
					delete bitmap;
				}
				else
				{
					bitmaps[itor->first] = bitmap;
				}
			}
		}
		else if(refbitmaps[itor->first] == NULL)// query end
		{
			continue;
		}
		else if(refbitmaps[itor->first]->GetSize() > 0)
		{
			BitMap* bitmap = new BitMap(itor->second->Count);
			if(INC_TEST_PUSHDOWN)
			{
				if(mCount == 1)
				{
					SearchSingleInPattern_RefMap(itor->second, wArray[0], queryStrTag, bitmap, refbitmaps[itor->first]);
				}
				else if(mCount == 0)//A:B
				{
					SearchMultiInPattern_RefMap(itor->second, args, argCountS, argCountE, querySegTags, querySegLens, bitmap, refbitmaps[itor->first]);
				}
				else
				{
					//Search_AxB_InPattern_Logic(itor->second, wArray[0], wArray[1], rangeSize, range, bitmaps[itor->first]);
				}
				if(bitmap->GetSize() == 0)
				{
					LISTBITMAPS::iterator ifind = bitmaps.find(itor->first);
					if(ifind != bitmaps.end())
					{
						;
					}
					else
					{
						bitmaps[itor->first] = NULL;
					}
				}
				else
				{
					LISTBITMAPS::iterator ifind = bitmaps.find(itor->first);
					if(ifind != bitmaps.end() && bitmaps[itor->first] != NULL)
					{
						bitmaps[itor->first]->Union(bitmap);
						delete bitmap;
					}
					else
					{
						bitmaps[itor->first] = bitmap;
					}
				}
			}
			else
			{
				if(mCount == 1)//abcd, ab*, *cd, *bc*.
				{
					SearchSingleInPattern(itor->second, wArray[0], queryStrTag, bitmap);
				}
				else if(mCount == 0)//A:B
				{
					SearchMultiInPattern(itor->second, args, argCountS, argCountE, querySegTags, querySegLens, bitmap);
				}
				else
				{
					Search_AxB_InPattern(itor->second, wArray[0], wArray[1], queryStrTag, queryStr2Tag, bitmap);
				}
				if(bitmap->GetSize() == 0)
				{
					LISTBITMAPS::iterator ifind = bitmaps.find(itor->first);
					if(ifind != bitmaps.end())
					{
						;
					}
					else
					{
						bitmaps[itor->first] = NULL;
					}
				}
				else if(bitmap->BeSizeFul())
				{
					if(bitmaps[itor->first]) delete bitmaps[itor->first];
					bitmap->Reset();
					bitmaps[itor->first] = bitmap;
					bitmaps[itor->first]->Union(refbitmaps[itor->first]);
				}
				else
				{
					bitmap->Inset(refbitmaps[itor->first]);
					LISTBITMAPS::iterator ifind = bitmaps.find(itor->first);
					if(ifind != bitmaps.end() && bitmaps[itor->first] != NULL)
					{
						bitmaps[itor->first]->Union(bitmap);
					}
					else
					{
						bitmaps[itor->first] = bitmap;
					}

				}
			}
		}
	}
	//search in outliers
	ifind = refbitmaps.find(OUTL_PAT_NAME);
	if(ifind == refbitmaps.end())
	{
		BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
		bitmap_outlier->SetSize();
		if(mCount == 1)
		{
			GetOutliers_SinglToken(wArray[0], bitmap_outlier);
		}
		else if(mCount == 0)
		{
			GetOutliers_MultiToken(args, argCountS, argCountE, bitmap_outlier);
		}
		else//A*B
		{
			;
		}
		if(bitmap_outlier->GetSize() == 0)
		{
			delete bitmap_outlier;
			LISTBITMAPS::iterator ifind = bitmaps.find(OUTL_PAT_NAME);
			if(ifind != bitmaps.end())
			{
				;
			}
			else
			{
				bitmaps[OUTL_PAT_NAME] = NULL;
			}
		}
		else
		{
			LISTBITMAPS::iterator ifind = bitmaps.find(OUTL_PAT_NAME);
			if(ifind != bitmaps.end() && bitmaps[itor->first] != NULL)
			{
				bitmaps[OUTL_PAT_NAME]->Union(bitmap_outlier);
				delete bitmap_outlier;
			}
			else
			{
				bitmaps[OUTL_PAT_NAME] = bitmap_outlier;
			}
		}
	}
	else if(refbitmaps[OUTL_PAT_NAME] == NULL || refbitmaps[OUTL_PAT_NAME]->GetSize() == 0)
	{
		;//query end
	}
	else if(refbitmaps[OUTL_PAT_NAME]->GetSize() > 0)
	{
		if(INC_TEST_PUSHDOWN)
		{
			LISTBITMAPS::iterator ifind = bitmaps.find(OUTL_PAT_NAME);
			if(ifind == bitmaps.end() || bitmaps[OUTL_PAT_NAME] != NULL)
			{
				bitmaps[OUTL_PAT_NAME] = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
			}
			if(mCount == 1)
			{
				GetOutliers_SinglToken_RefMap(wArray[0], bitmaps[OUTL_PAT_NAME], refbitmaps[OUTL_PAT_NAME]);
			}
			else if(mCount == 0)
			{
				GetOutliers_MultiToken_RefMap(args, argCountS, argCountE, bitmaps[OUTL_PAT_NAME], refbitmaps[OUTL_PAT_NAME]);
			}
			else//A*B
			{
				;
			}
			if(bitmaps[OUTL_PAT_NAME]->GetSize() == 0)
			{
				delete bitmaps[OUTL_PAT_NAME];
				bitmaps[OUTL_PAT_NAME] = NULL;
			}
		}
		else
		{
			BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
			bitmap_outlier->SetSize();
			if(mCount == 1)
			{
				GetOutliers_SinglToken(wArray[0], bitmap_outlier);
			}
			else if(mCount == 0)
			{
				GetOutliers_MultiToken(args, argCountS, argCountE, bitmap_outlier);
			}
			else//A*B
			{
				;
			}
			if(bitmap_outlier->GetSize() == 0)
			{
				delete bitmap_outlier;
				LISTBITMAPS::iterator ifind = bitmaps.find(OUTL_PAT_NAME);
				if(ifind != bitmaps.end())
				{
					;
				}
				else
				{
					bitmaps[OUTL_PAT_NAME] = NULL;
				}
			}
			else
			{
				LISTBITMAPS::iterator ifind = bitmaps.find(OUTL_PAT_NAME);
				if(ifind != bitmaps.end() && bitmaps[itor->first] != NULL)
				{
					bitmaps[OUTL_PAT_NAME]->Union(bitmap_outlier);
					delete bitmap_outlier;
				}
				else
				{
					bitmaps[OUTL_PAT_NAME] = bitmap_outlier;
				}
			}
		}
	}

	//delete
	for(int i=0;i<mCount;i++)
	{
		if (wArray[i])
		{
			delete (wArray[i]);
		}
	}
	if(querySegTags) delete[] querySegTags;
	if(querySegLens) delete[] querySegLens;
	return 0;
}

int LogStoreApi::SearchByLogic_norm_or(char *args[MAX_CMD_ARG_COUNT], int argCountS, int argCountE, OUT LISTBITMAPS& bitmaps)
{
	int temp =argCountS;
	int orLen = strlen(LOGIC_or);
	LISTBITMAPS tempbitmaps;
	bool union_flag = false;
	for(int i=argCountS; i<= argCountE; i++)
	{
		if(strlen(args[i]) == orLen && stricmp(args[i], LOGIC_or) == 0)
		{
			union_flag = true;
			SearchByLogic_norm_RefMap(args, temp, i-2, tempbitmaps, bitmaps);
			temp = i+2;
			i++;
		}
	}
	SearchByLogic_norm(args, temp, argCountE, bitmaps);
	if(union_flag)//union
	{
		LISTBITMAPS::iterator itor = tempbitmaps.begin();
		LISTBITMAPS::iterator ifind;
		for (; itor != tempbitmaps.end();itor++)
		{
			ifind = bitmaps.find(itor->first);
			if(ifind != bitmaps.end() && bitmaps[itor->first]!= NULL)
			{
				bitmaps[itor->first]->Union(itor->second);
			}
			else		
				bitmaps[itor->first] = itor->second;
		}
	}
	return 0;
}

int LogStoreApi::SearchByLogic_and(char *args[MAX_CMD_ARG_COUNT], int argCountS, int argCountE, OUT LISTBITMAPS& bitmaps)
{
    int flag = 0;
    int temp = argCountS;
    int notLen = strlen(LOGIC_NOT);
    LISTBITMAPS acc;
    for(int i=argCountS; i<= argCountE; i++)
    {
        if(strlen(args[i]) == notLen && stricmp(args[i], LOGIC_NOT) == 0)
        {
            LISTBITMAPS part;
            if(flag == 1)
            {
                SearchByLogic_not(args, temp, i-2, part);
            }
            else if(temp <= i-2)
            {
                SearchByLogic_norm_or(args, temp, i-2, part);
            }
            if(acc.empty())
            {
                acc.swap(part);
            }
            else
            {
                LISTBITMAPS::iterator it = acc.begin();
                while(it != acc.end())
                {
                    int pid = it->first;
                    BitMap* cur = it->second;
                    LISTBITMAPS::iterator ip = part.find(pid);
                    BitMap* nxt = (ip!=part.end())? ip->second : NULL;
                    if(!cur || !nxt)
                    {
                        if(cur) { delete cur; }
                        it->second = NULL;
                        ++it;
                        continue;
                    }
                    if(cur->BeSizeFul())
                    {
                        delete cur;
                        it->second = nxt;
                    }
                    else if(nxt->BeSizeFul())
                    {
                        delete nxt;
                    }
                    else
                    {
                        cur->Inset(nxt);
                        delete nxt;
                    }
                    ++it;
                }
                for(LISTBITMAPS::iterator ip=part.begin(); ip!=part.end(); ++ip)
                {
                    if(acc.find(ip->first) == acc.end())
                    {
                        acc[ip->first] = NULL;
                        if(ip->second) delete ip->second;
                    }
                }
            }
            flag = 1;
            temp = i+2;
            i++;
        }
    }
    LISTBITMAPS last;
    if(flag == 1)
    {
        SearchByLogic_not(args, temp, argCountE, last);
    }
    else
    {
        SearchByLogic_norm_or(args, temp, argCountE, last);
    }
    if(acc.empty())
    {
        acc.swap(last);
    }
    else
    {
        LISTBITMAPS::iterator it = acc.begin();
        while(it != acc.end())
        {
            int pid = it->first;
            BitMap* cur = it->second;
            LISTBITMAPS::iterator ip = last.find(pid);
            BitMap* nxt = (ip!=last.end())? ip->second : NULL;
            if(!cur || !nxt)
            {
                if(cur) { delete cur; }
                it->second = NULL;
                ++it;
                continue;
            }
            if(cur->BeSizeFul())
            {
                delete cur;
                it->second = nxt;
            }
            else if(nxt->BeSizeFul())
            {
                delete nxt;
            }
            else
            {
                cur->Inset(nxt);
                delete nxt;
            }
            ++it;
        }
        for(LISTBITMAPS::iterator ip=last.begin(); ip!=last.end(); ++ip)
        {
            if(acc.find(ip->first) == acc.end())
            {
                acc[ip->first] = NULL;
                if(ip->second) delete ip->second;
            }
        }
    }
    for(LISTBITMAPS::iterator it=acc.begin(); it!=acc.end(); ++it){ bitmaps[it->first] = it->second; }
    return 0;
}

int LogStoreApi::SearchByLogic_OR(char *args[MAX_CMD_ARG_COUNT], int argCountS, int argCountE, OUT LISTBITMAPS& bitmaps)
{
    int temp = argCountS;
    int andLen = strlen(LOGIC_AND);
    bool first = true;
    LISTBITMAPS acc;
    for(int i=argCountS; i<= argCountE; i++)
    {
        if(strlen(args[i]) == andLen && stricmp(args[i], LOGIC_AND) == 0)
        {
            LISTBITMAPS part;
            SearchByLogic_and(args, temp, i-2, part);
            if(first)
            {
                acc.swap(part);
                first = false;
            }
            else
            {
                for(LISTBITMAPS::iterator it=acc.begin(); it!=acc.end(); ++it)
                {
                    int pid = it->first;
                    BitMap* cur = it->second;
                    LISTBITMAPS::iterator ip = part.find(pid);
                    BitMap* nxt = (ip!=part.end())? ip->second : NULL;
                    if(!cur || !nxt)
                    {
                        if(cur) { delete cur; }
                        it->second = NULL;
                        continue;
                    }
                    if(cur->BeSizeFul())
                    {
                        delete cur;
                        it->second = nxt;
                    }
                    else if(nxt->BeSizeFul())
                    {
                        delete nxt;
                    }
                    else
                    {
                        cur->Inset(nxt);
                        delete nxt;
                    }
                }
                for(LISTBITMAPS::iterator ip=part.begin(); ip!=part.end(); ++ip)
                {
                    if(acc.find(ip->first) == acc.end())
                    {
                        acc[ip->first] = NULL;
                        if(ip->second) delete ip->second;
                    }
                }
            }
            temp = i+2;
            i++;
        }
    }
    LISTBITMAPS last;
    SearchByLogic_and(args, temp, argCountE, last);
    if(first)
    {
        acc.swap(last);
    }
    else
    {
        for(LISTBITMAPS::iterator it=acc.begin(); it!=acc.end(); ++it)
        {
            int pid = it->first;
            BitMap* cur = it->second;
            LISTBITMAPS::iterator ip = last.find(pid);
            BitMap* nxt = (ip!=last.end())? ip->second : NULL;
            if(!cur || !nxt)
            {
                if(cur) { delete cur; }
                it->second = NULL;
                continue;
            }
            if(cur->BeSizeFul())
            {
                delete cur;
                it->second = nxt;
            }
            else if(nxt->BeSizeFul())
            {
                delete nxt;
            }
            else
            {
                cur->Inset(nxt);
                delete nxt;
            }
        }
        for(LISTBITMAPS::iterator ip=last.begin(); ip!=last.end(); ++ip)
        {
            if(acc.find(ip->first) == acc.end())
            {
                acc[ip->first] = NULL;
                if(ip->second) delete ip->second;
            }
        }
    }
    for(LISTBITMAPS::iterator it=acc.begin(); it!=acc.end(); ++it){ bitmaps[it->first]=it->second; }
    return 0;
}

int LogStoreApi::SearchByLogic(char *args[MAX_CMD_ARG_COUNT], int argCount, OUT LISTBITMAPS& bitmaps)
{
    std::vector<std::string> toks; toks.reserve(argCount*2);
    for(int i=0;i<argCount;i++){
        const char* s=args[i]; if(!s) continue; int n=(int)strlen(s); std::string buf; buf.reserve(n);
        for(int j=0;j<n;j++){
            char c=s[j];
            if(c=='('||c==')'){
                if(!buf.empty()){ toks.push_back(buf); buf.clear(); }
                std::string t; t.push_back(c); toks.push_back(t);
            } else { buf.push_back(c); }
        }
        if(!buf.empty()){ toks.push_back(buf); }
    }
    std::vector<char*> arr; arr.reserve(toks.size());
    for(size_t i=0;i<toks.size();i++){ arr.push_back((char*)toks[i].c_str()); }
    char** aargs = arr.empty()? args : (char**)arr.data();
    int aCount = arr.empty()? argCount : (int)arr.size();
    auto is_lp = [&](const char* s){ return s && strlen(s)==1 && s[0]=='('; };
    auto is_rp = [&](const char* s){ return s && strlen(s)==1 && s[0]==')'; };

    struct Node { int t; int s; int e; Node* l; Node* r; Node* u; Node():t(0),s(0),e(0),l(NULL),r(NULL),u(NULL){} };
    auto is_and = [&](const char* s){ return stricmp(s, LOGIC_AND)==0; };
    auto is_or = [&](const char* s){ return stricmp(s, LOGIC_OR)==0 || stricmp(s, LOGIC_or)==0; };
    auto is_not = [&](const char* s){ return stricmp(s, LOGIC_NOT)==0; };

    int pos = 0;
    function<Node*()> parse_expr;
    function<Node*()> parse_or;
    function<Node*()> parse_and;
    function<Node*()> parse_term;

    parse_term = [&](){
        if(pos<aCount && is_lp(aargs[pos])){ pos++; Node* n = parse_expr(); if(pos<aCount && is_rp(aargs[pos])) pos++; return n; }
        int s = pos;
        while(pos<aCount && !is_and(aargs[pos]) && !is_or(aargs[pos]) && !is_rp(aargs[pos])) pos++;
        Node* n = new Node(); n->t = 0; n->s = s; n->e = pos-1; return n;
    };
    parse_and = [&](){
        Node* left = NULL;
        bool neg = false;
        while(pos<aCount){
            if(is_rp(aargs[pos])) break;
            if(is_or(aargs[pos])) break;
            if(is_and(aargs[pos])){ pos++; continue; }
            if(is_not(aargs[pos])){ neg = true; pos++; }
            Node* term = parse_term();
            if(neg){ Node* nn = new Node(); nn->t = 3; nn->u = term; term = nn; neg=false; }
            if(!left){ left = term; }
            else { Node* nn = new Node(); nn->t = 1; nn->l = left; nn->r = term; left = nn; }
        }
        return left;
    };
    parse_or = [&](){
        Node* left = parse_and();
        while(pos<aCount && is_or(aargs[pos])){ pos++; Node* right = parse_and(); Node* nn = new Node(); nn->t = 2; nn->l = left; nn->r = right; left = nn; }
        return left;
    };
    parse_expr = [&](){ return parse_or(); };

    Node* root = parse_expr();

    auto get_total_lines = [&](int pid) -> int {
        if(pid == OUTL_PAT_NAME) {
            if(m_glbMeta.count(OUTL_PAT_NAME)) return m_glbMeta[OUTL_PAT_NAME]->lines;
            return 0;
        }
        if(m_patterns.count(pid)) return m_patterns[pid]->Count;
        return 0;
    };

    auto build_full = [&](){ 
        LISTBITMAPS full; 
        for(auto& it : m_patterns){ 
            BitMap* bm = new BitMap(it.second->Count); 
            bm->SetSize(); 
            full[it.first]=bm; 
        } 
        if(m_glbMeta.count(OUTL_PAT_NAME)) {
            BitMap* bm = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
            bm->SetSize();
            full[OUTL_PAT_NAME] = bm;
        }
        return full; 
    };

    auto clear_bitmaps = [](LISTBITMAPS& bms) {
        for(auto& kv : bms) { if(kv.second) delete kv.second; }
        bms.clear();
    };

    function<LISTBITMAPS(Node*)> eval = [&](Node* n)->LISTBITMAPS{
        LISTBITMAPS res;
        if(!n) return res;
        if(n->t==0){ 
            SearchByLogic_norm(aargs, n->s, n->e, res); 
            return res; 
        }
        if(n->t==3){ // NOT
            LISTBITMAPS a = eval(n->u); 
            LISTBITMAPS full = build_full(); 
            for(auto& it : full){ 
                auto ia = a.find(it.first); 
                if(ia!=a.end() && ia->second){ it.second->Complement(ia->second); } 
                res[it.first]=it.second; 
            } 
            clear_bitmaps(a);
            return res; 
        }
        if(n->t==1){ // AND
            LISTBITMAPS a = eval(n->l); 
            LISTBITMAPS b = eval(n->r); 
            for(auto& it : a){ 
                auto ib = b.find(it.first); 
                if(ib!=b.end() && ib->second && it.second){ 
                    BitMap* bm = new BitMap(get_total_lines(it.first)); 
                    bm->CloneFrom(it.second); 
                    bm->Inset(ib->second); 
                    res[it.first]=bm; 
                } 
            } 
            clear_bitmaps(a);
            clear_bitmaps(b);
            return res; 
        }
        if(n->t==2){ // OR
            LISTBITMAPS a = eval(n->l); 
            LISTBITMAPS b = eval(n->r); 
            res = a; 
            for(auto& it : b){ 
                if(res.count(it.first) && res[it.first]){ 
                    res[it.first]->Union(it.second); 
                    if(it.second) delete it.second;
                } else { 
                    res[it.first]=it.second; 
                } 
            } 
            b.clear(); // pointers moved to res or deleted
            return res; 
        }
        return res;
    };

    LISTBITMAPS tmp = eval(root);
    int nonNull = 0;
    for(LISTBITMAPS::iterator it=tmp.begin(); it!=tmp.end(); ++it){ bitmaps[it->first] = it->second; if(it->second && it->second->GetSize()>0) nonNull++; }
    return nonNull>0 ? 1 : 0;
}

int LogStoreApi::SearchByWildcard_Token(char *args[MAX_CMD_ARG_COUNT], int argCount, int matNum)
{
    long long tstart = LLONG_MIN;
    long long tend = LLONG_MAX;
    bool hasTime = false;
    char* fargs[MAX_CMD_ARG_COUNT];
    int fcount = 0;
    for(int i=0;i<argCount;i++){
        if(args[i] && strcmp(args[i], "-time")==0 && i+2<argCount){
            tstart = __parse_time_arg(args[i+1]);
            tend = __parse_time_arg(args[i+2]);
            hasTime = true;
            i += 2;
            continue;
        }
        fargs[fcount++] = args[i];
    }
    LISTBITMAPS bitmaps;
    if(fcount == 1)
    {
    char* colon = strchr(fargs[0], ':');
    if(colon && colon != args[0])
    {
        std::string raw = std::string(colon + 1);
        std::string value = raw;
        bool optStrict = false;
        size_t bar = raw.find('|');
        if(bar != std::string::npos){
            value = raw.substr(0, bar);
            std::string opts = raw.substr(bar+1);
            if(opts.find("strict") != std::string::npos) optStrict = true;
        }
        timeval tt1 = ___StatTime_Start();
        Search_SingleSegment((char*)value.c_str(), bitmaps);
        RunStatus.SearchPatternTime = ___StatTime_End(tt1);
        timeval tt2 = ___StatTime_Start();
        BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
        bitmap_outlier->SetSize();
        if(!optStrict){
            GetOutliers_SinglToken((char*)value.c_str(), bitmap_outlier);
        }
        else
        {
            if(bitmap_outlier->GetSize() == DEF_BITMAP_FULL){ bitmap_outlier->Reset(); }
        }
        bitmaps[OUTL_PAT_NAME] = bitmap_outlier;
        {
                std::lock_guard<std::mutex> lock(m_runStatusMutex);
                RunStatus.SearchOutlierTime = ___StatTime_End(tt2);
                RunStatus.SearchTotalTime = RunStatus.SearchPatternTime + RunStatus.SearchOutlierTime;
            }
        SyslogPerf("It takes %lfs to single query.\n",RunStatus.SearchPatternTime);
        SyslogPerf("It takes %lfs to single outliers query.\n",RunStatus.SearchOutlierTime);
        }
        else
        {
        LISTSESSIONS::iterator ifind = m_sessions.find(fargs[0]);
        if(ifind == m_sessions.end())
        {
            timeval tt1 = ___StatTime_Start();
        Search_SingleSegment(fargs[0], bitmaps);
        {
            std::lock_guard<std::mutex> lock(m_runStatusMutex);
            {
                std::lock_guard<std::mutex> lock(m_runStatusMutex);
                RunStatus.SearchPatternTime = ___StatTime_End(tt1);
            }
        }
        timeval tt2 = ___StatTime_Start();
        BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
        bitmap_outlier->SetSize();
        GetOutliers_SinglToken(fargs[0], bitmap_outlier);
        bitmaps[OUTL_PAT_NAME] = bitmap_outlier;
        {
            std::lock_guard<std::mutex> lock(m_runStatusMutex);
            RunStatus.SearchOutlierTime = ___StatTime_End(tt2);
            RunStatus.SearchTotalTime = RunStatus.SearchPatternTime + RunStatus.SearchOutlierTime;
        }
			SyslogPerf("It takes %lfs to single query.\n",RunStatus.SearchPatternTime);
			SyslogPerf("It takes %lfs to single outliers query.\n",RunStatus.SearchOutlierTime);
			//session
			if(INC_TEST_SESSION)
			{
                LISTBITMAPS cache_bitmaps;
                for(auto& kv : bitmaps) {
                    if(kv.second) {
                        BitMap* clone = new BitMap(kv.second->TotalSize);
                        clone->CloneFrom(kv.second);
                        cache_bitmaps[kv.first] = clone;
                    }
                }
                m_sessions[fargs[0]] = cache_bitmaps;
			}
		}
		else
		{
            timeval tt1 = ___StatTime_Start();
            LISTBITMAPS& cached = m_sessions[fargs[0]];
            for(auto& kv : cached) {
                if(kv.second) {
                    BitMap* clone = new BitMap(kv.second->TotalSize);
                    clone->CloneFrom(kv.second);
                    bitmaps[kv.first] = clone;
                }
            }
            RunStatus.SearchPatternTime = ___StatTime_End(tt1);
            RunStatus.SearchOutlierTime = 0;
            RunStatus.SearchTotalTime = RunStatus.SearchPatternTime + RunStatus.SearchOutlierTime;
            SyslogPerf("It takes %lfs to single query with session cache(cur: %d items).\n",RunStatus.SearchTotalTime, m_sessions.size());
        }
        }
    }
    else
    {
        int flag = IsSearchWithLogic(fargs, fcount);
        if(flag == 0)
        {
            //rebuild querystring
            char queryChars[MAX_PATTERN_SIZE]={'\0'};
            RecombineString(fargs, 0, fcount-1, queryChars);
            string queryStr(queryChars);
            //searching
            LISTSESSIONS::iterator ifind = m_sessions.find(queryStr);
            if(ifind == m_sessions.end())
            {
                timeval tt1 = ___StatTime_Start();
                Search_MultiSegments(fargs, fcount, bitmaps);
                RunStatus.SearchPatternTime = ___StatTime_End(tt1);
                timeval tt2 = ___StatTime_Start();
                BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
                bitmap_outlier->SetSize();
                GetOutliers_MultiToken(fargs, 0, fcount-1, bitmap_outlier);
                bitmaps[OUTL_PAT_NAME] = bitmap_outlier;
                RunStatus.SearchOutlierTime = ___StatTime_End(tt2);
                RunStatus.SearchTotalTime = RunStatus.SearchPatternTime + RunStatus.SearchOutlierTime;
                SyslogPerf("It takes %lfs to multi query.\n",RunStatus.SearchPatternTime);
                SyslogPerf("It takes %lfs to multi outliers query.\n",RunStatus.SearchOutlierTime);
                //session
                if(INC_TEST_SESSION)
                {
                    LISTBITMAPS cache_bitmaps;
                    for(auto& kv : bitmaps) {
                        if(kv.second) {
                            BitMap* clone = new BitMap(kv.second->TotalSize);
                            clone->CloneFrom(kv.second);
                            cache_bitmaps[kv.first] = clone;
                        }
                    }
                    m_sessions[queryStr] = cache_bitmaps;
                }
            }
            else
            {
                timeval tt1 = ___StatTime_Start();
                LISTBITMAPS& cached = m_sessions[queryStr];
                for(auto& kv : cached) {
                    if(kv.second) {
                        BitMap* clone = new BitMap(kv.second->TotalSize);
                        clone->CloneFrom(kv.second);
                        bitmaps[kv.first] = clone;
                    }
                }
                RunStatus.SearchPatternTime = ___StatTime_End(tt1);
                RunStatus.SearchOutlierTime = 0;
                RunStatus.SearchTotalTime = RunStatus.SearchPatternTime + RunStatus.SearchOutlierTime;
                SyslogPerf("It takes %lfs to multi query with session cache(cur: %d items).\n",RunStatus.SearchTotalTime, m_sessions.size());
            }
        }
        else
        {
            timeval tt1 = ___StatTime_Start();
            SearchByLogic(fargs, fcount, bitmaps);
            {
                std::lock_guard<std::mutex> lock(m_runStatusMutex);
                RunStatus.SearchTotalTime = ___StatTime_End(tt1);
            }
            SyslogPerf("It takes %lfs to logic query.\n",RunStatus.SearchTotalTime);
        }
        
    }
    if(hasTime){ ApplyTimeFilterToBitmaps(bitmaps, tstart, tend); }
    
    SysCodeRead("--------- Materialization --------------\n");
    materTime = 0;
    LISTBITMAPS::iterator itor = bitmaps.begin();//match with each main pattern
    int num = 0;
	int matnum = 0;
	timeval tt= ___StatTime_Start();
	//first check outliers, then check pats
	if(bitmaps[OUTL_PAT_NAME] != NULL)
	{
		int entryCnt = bitmaps[OUTL_PAT_NAME]->GetSize();
		RunStatus.SearchOutliersNum = entryCnt;
		SysCodeRead("%s: entryCnt: %d.\n", FormatVarName(OUTL_PAT_NAME), entryCnt);
		matnum = entryCnt;
		num += entryCnt;
		MaterializOutlier(bitmaps[OUTL_PAT_NAME], bitmaps[OUTL_PAT_NAME]->GetSize(), matNum);
		delete bitmaps[OUTL_PAT_NAME];
		bitmaps[OUTL_PAT_NAME] = NULL;
	}
	for (; itor != bitmaps.end();itor++)
	{
		if(itor ->second != NULL && itor->first != OUTL_PAT_NAME)
		{
			int entryCnt =0;
			if(itor->second->BeSizeFul())
			{
				entryCnt = m_patterns[itor->first]->Count;
			}
			else
			{
				entryCnt = itor->second->GetSize();
			}
			SysCodeRead("%s: entryCnt: %d.\n", FormatVarName(itor->first), entryCnt);
			if(matNum - matnum > 0)
			{
				matnum += Materialization(itor->first, itor->second, entryCnt, matNum - matnum);
			}
			num += entryCnt;
			delete (itor ->second);
			itor ->second = NULL;
		}
	}
	double  timem = ___StatTime_End(tt);
	RunStatus.SearchTotalEntriesNum = num;
	if(num > 0)
	{
		SysCodeRead("%s: Total query num: %d\n",FileName.c_str(), num);
		SyslogPerf("It takes %lfs (%lfs) to Materialization(%d).\n",timem, materTime, matnum);
	}
	RunStatus.MaterializFulTime = timem;
	RunStatus.MaterializAlgTime = materTime;
	bitmaps.clear();
	return matnum;
}


//https://blog.csdn.net/yangbingzhou/article/details/51352648
int LogStoreApi::SearchByReg(const char *regPattern)
{
    
    return 0;
}

// #include "var_alias.h"

char sName[128]={'\0'};
char* LogStoreApi::FormatVarName(int varName)
{
    memset(sName, '\0', 128);
    VarAliasManager* aliasManager = VarAliasManager::getInstance();
    std::string alias = aliasManager->getAlias(varName);
    if (!alias.empty()) {
        strncpy(sName, alias.c_str(), 127);
        sName[127] = '\0';
        return sName;
    }
    if(varName <= 15)
        sprintf(sName, "%d",varName);
    else
    {
        int e = varName >>16;
        int v = (varName >>8) & 0xFF;
        int s = (varName >>4) & 0x0F;
        int t = varName & 0x0F;
        if(t == VAR_TYPE_SUB)
            sprintf(sName, "%d_%d~%d.%d",e,v,s,t);
        else
            sprintf(sName, "%d_%d.%d",e,v,t);
    }
    return sName;
}

int LogStoreApi::SearchByWildcard_Token_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, int matNum, std::string &json_out)
{
    long long tstart = LLONG_MIN;
    long long tend = LLONG_MAX;
    bool hasTime = false;
    char* fargs[MAX_CMD_ARG_COUNT];
    int fcount = 0;
    for(int i=0;i<argCount;i++){
        if(args[i] && strcmp(args[i], "-time")==0 && i+2<argCount){
            tstart = __parse_time_arg(args[i+1]);
            tend = __parse_time_arg(args[i+2]);
            hasTime = true;
            i += 2;
            continue;
        }
        fargs[fcount++] = args[i];
    }

    LISTBITMAPS bitmaps;
    int ret = 0;
    if(fcount == 1)
    {
        Search_SingleSegment(fargs[0], bitmaps);
        BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
        bitmap_outlier->SetSize();
        GetOutliers_SinglToken(fargs[0], bitmap_outlier);
        bitmaps[OUTL_PAT_NAME] = bitmap_outlier;
        ret = 1;
    }
    else
    {
        int flag = IsSearchWithLogic(fargs, fcount);
        if(flag == 0)
        {
            Search_MultiSegments(fargs, fcount, bitmaps);
            BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
            bitmap_outlier->SetSize();
            GetOutliers_MultiToken(fargs, 0, fcount-1, bitmap_outlier);
            bitmaps[OUTL_PAT_NAME] = bitmap_outlier;
            ret = 1;
        }
        else
        {
            SearchByLogic(fargs, fcount, bitmaps);
            ret = 1;
        }
    }
    if(hasTime){ ApplyTimeFilterToBitmaps(bitmaps, tstart, tend); }

    int totalCnt = 0;
    json_out.clear();
    json_out.append("[");
    bool firstItem = true;

    // Handle outliers first
    if(bitmaps.count(OUTL_PAT_NAME) && bitmaps[OUTL_PAT_NAME] != NULL)
    {
        BitMap* bitmap = bitmaps[OUTL_PAT_NAME];
        int got = MaterializOutlier_JSON(bitmap, bitmap->GetSize(), matNum - totalCnt, json_out);
        if(got > 0) firstItem = false;
        totalCnt += got;
        delete bitmap;
        bitmaps.erase(OUTL_PAT_NAME);
    }

    // Handle patterns
    for(LISTBITMAPS::iterator itor = bitmaps.begin(); itor != bitmaps.end(); ++itor)
    {
        if(totalCnt >= matNum) break;
        int pid = itor->first;
        BitMap* bitmap = itor->second;
        if(bitmap == NULL) continue;
        
        if(!firstItem && totalCnt < matNum && bitmap->GetSize() > 0)
        {
            json_out.append(",");
        }

        int got = Materialization_JSON(pid, bitmap, bitmap->GetSize(), matNum - totalCnt, json_out);
        if(got > 0) firstItem = false;
        totalCnt += got;
        delete bitmap;
    }
    json_out.append("]");
    bitmaps.clear();
    return totalCnt;
}

int LogStoreApi::CountByWildcard_Token(char *args[MAX_CMD_ARG_COUNT], int argCount)
{
    long long tstart = LLONG_MIN;
    long long tend = LLONG_MAX;
    bool hasTime = false;
    char* fargs[MAX_CMD_ARG_COUNT];
    int fcount = 0;
    for(int i=0;i<argCount;i++){
        if(args[i] && strcmp(args[i], "-time")==0 && i+2<argCount){
            tstart = __parse_time_arg(args[i+1]);
            tend = __parse_time_arg(args[i+2]);
            hasTime = true;
            i += 2;
            continue;
        }
        fargs[fcount++] = args[i];
    }
    LISTBITMAPS bitmaps;
    int ret = 0;
    if(fcount == 1)
    {
        timeval tt1 = ___StatTime_Start();
        Search_SingleSegment(fargs[0], bitmaps);
        RunStatus.SearchPatternTime = ___StatTime_End(tt1);
        timeval tt2 = ___StatTime_Start();
        BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
        bitmap_outlier->SetSize();
        GetOutliers_SinglToken(fargs[0], bitmap_outlier);
        bitmaps[OUTL_PAT_NAME] = bitmap_outlier;
        RunStatus.SearchOutlierTime = ___StatTime_End(tt2);
        RunStatus.SearchTotalTime = RunStatus.SearchPatternTime + RunStatus.SearchOutlierTime;
        ret = 1;
    }
    else
    {
        int flag = IsSearchWithLogic(fargs, fcount);
        if(flag == 0)
        {
            char queryChars[MAX_PATTERN_SIZE]={'\0'};
            RecombineString(fargs, 0, fcount-1, queryChars);
            string queryStr(queryChars);
            timeval tt1 = ___StatTime_Start();
            Search_MultiSegments(fargs, fcount, bitmaps);
            RunStatus.SearchPatternTime = ___StatTime_End(tt1);
            timeval tt2 = ___StatTime_Start();
            BitMap* bitmap_outlier = new BitMap(m_glbMeta[OUTL_PAT_NAME]->lines);
            bitmap_outlier->SetSize();
            GetOutliers_MultiToken(fargs, 0, fcount-1, bitmap_outlier);
            bitmaps[OUTL_PAT_NAME] = bitmap_outlier;
            RunStatus.SearchOutlierTime = ___StatTime_End(tt2);
            RunStatus.SearchTotalTime = RunStatus.SearchPatternTime + RunStatus.SearchOutlierTime;
            ret = 1;
        }
        else
        {
            std::vector<std::string> ptoks; ptoks.reserve(fcount*2);
            for(int i=0;i<fcount;i++){ const char* s=fargs[i]; if(!s) continue; int n=(int)strlen(s); std::string buf; buf.reserve(n); for(int j=0;j<n;j++){ char c=s[j]; if(c=='('||c==')'){ if(!buf.empty()){ ptoks.push_back(buf); buf.clear(); } } else { buf.push_back(c); } } if(!buf.empty()){ ptoks.push_back(buf); } }
            std::vector<char*> cargs; cargs.reserve(ptoks.size()); for(size_t i=0;i<ptoks.size();i++){ cargs.push_back((char*)ptoks[i].c_str()); }
            int ccount = (int)cargs.size();
            bool has_and=false, has_or=false, has_not=false;
            for(int i=0;i<ccount;i++){ if(stricmp(cargs[i], LOGIC_AND)==0) has_and=true; else if(stricmp(cargs[i], LOGIC_OR)==0 || stricmp(cargs[i], LOGIC_or)==0) has_or=true; else if(stricmp(cargs[i], LOGIC_NOT)==0) has_not=true; }
            if(has_and && !has_or && !has_not)
            {
                int temp=0; LISTBITMAPS acc; bool first=true;
                for(int i=0;i<ccount;i++)
                {
                    if(stricmp(cargs[i], LOGIC_AND)==0)
                    {
                        LISTBITMAPS part; SearchByLogic_norm((char**)cargs.data(), temp, i-1, part);
                        if(first){ acc.swap(part); first=false; }
                        else
                        {
                            for(LISTBITMAPS::iterator it=acc.begin(); it!=acc.end(); ++it)
                            {
                                int pid=it->first; BitMap* cur=it->second; LISTBITMAPS::iterator ip=part.find(pid); BitMap* nxt=(ip!=part.end())?ip->second:NULL;
                                if(!cur||!nxt){ if(cur) delete cur; it->second=NULL; continue; }
                                if(cur->BeSizeFul()){ delete cur; it->second=nxt; }
                                else if(nxt->BeSizeFul()){ delete nxt; }
                                else { cur->Inset(nxt); delete nxt; }
                            }
                            for(LISTBITMAPS::iterator ip=part.begin(); ip!=part.end(); ++ip){ if(acc.find(ip->first)==acc.end()){ acc[ip->first]=NULL; if(ip->second) delete ip->second; } }
                        }
                        temp = i+1;
                    }
                }
                LISTBITMAPS last; SearchByLogic_norm((char**)cargs.data(), temp, ccount-1, last);
                if(first){ acc.swap(last); }
                else
                {
                    for(LISTBITMAPS::iterator it=acc.begin(); it!=acc.end(); ++it)
                    {
                        int pid=it->first; BitMap* cur=it->second; LISTBITMAPS::iterator ip=last.find(pid); BitMap* nxt=(ip!=last.end())?ip->second:NULL;
                        if(!cur||!nxt){ if(cur) delete cur; it->second=NULL; continue; }
                        if(cur->BeSizeFul()){ delete cur; it->second=nxt; }
                        else if(nxt->BeSizeFul()){ delete nxt; }
                        else { cur->Inset(nxt); delete nxt; }
                    }
                    for(LISTBITMAPS::iterator ip=last.begin(); ip!=last.end(); ++ip){ if(acc.find(ip->first)==acc.end()){ acc[ip->first]=NULL; if(ip->second) delete ip->second; } }
                }
                bitmaps.swap(acc);
                RunStatus.SearchTotalTime = 0;
                ret = 1;
            }
            else
            {
                timeval tt1 = ___StatTime_Start();
                SearchByLogic(fargs, fcount, bitmaps);
                RunStatus.SearchTotalTime = ___StatTime_End(tt1);
                ret = 1;
            }
        }
    }
    if(hasTime){ ApplyTimeFilterToBitmaps(bitmaps, tstart, tend); }

    int totalCnt = 0;
    LISTBITMAPS::iterator itor = bitmaps.begin();
    for(; itor != bitmaps.end(); ++itor)
    {
        int pid = itor->first;
        BitMap* bitmap = itor->second;
        if(bitmap == NULL) continue;
        if(pid == OUTL_PAT_NAME){
            totalCnt += bitmap->GetSize();
        } else {
            if(bitmap->BeSizeFul()) totalCnt += m_patterns[pid]->Count; else totalCnt += bitmap->GetSize();
        }
        delete bitmap;
    }
    return totalCnt;
}

int LogStoreApi::BuildBitmapsForQuery(char *args[MAX_CMD_ARG_COUNT], int argCount, LISTBITMAPS &bitmaps)
{
    long long tstart = LLONG_MIN;
    long long tend = LLONG_MAX;
    bool hasTime = false;
    char* fargs[MAX_CMD_ARG_COUNT];
    int fcount = 0;
    for(int i=0;i<argCount;i++){
        if(args[i] && strcmp(args[i], "-time")==0 && i+2<argCount){
            tstart = __parse_time_arg(args[i+1]);
            tend = __parse_time_arg(args[i+2]);
            hasTime = true;
            i += 2;
            continue;
        }
        fargs[fcount++] = args[i];
    }
    bitmaps.clear();
    int ret = 0;
    if(fcount == 1)
    {
        Search_SingleSegment(fargs[0], bitmaps);
        ret = 1;
    }
    else
    {
        int flag = IsSearchWithLogic(fargs, fcount);
        if(flag == 0)
        {
            Search_MultiSegments(fargs, fcount, bitmaps);
            ret = 1;
        }
        else
        {
            SearchByLogic(fargs, fcount, bitmaps);
            ret = 1;
        }
    }
    if(hasTime){ ApplyTimeFilterToBitmaps(bitmaps, tstart, tend); }
    return ret;
}

int LogStoreApi::GetMatchedTimeRange(char *args[MAX_CMD_ARG_COUNT], int argCount, long long& tmin, long long& tmax)
{
    tmin = LLONG_MAX; tmax = LLONG_MIN;
    LISTBITMAPS bitmaps; int r = BuildBitmapsForQuery(args, argCount, bitmaps);
    if(r <= 0) { for(auto &kv: bitmaps){ if(kv.second) delete kv.second; } return 0; }
    if(m_timeValues.empty()) { for(auto &kv: bitmaps){ if(kv.second) delete kv.second; } return 0; }
    for(LISTBITMAPS::iterator it=bitmaps.begin(); it!=bitmaps.end(); ++it){ BitMap* bm=it->second; if(!bm) continue; int n=bm->GetSize(); for(int i=0;i<n;i++){ int idx=bm->GetIndex(i); if(idx>=0 && (size_t)idx<m_timeValues.size()){ long long v=m_timeValues[idx]; if(v<tmin) tmin=v; if(v>tmax) tmax=v; } } delete bm; }
    if(tmin==LLONG_MAX) return 0; return 1;
}

int LogStoreApi::Timechart_Count_BySpan(char *args[MAX_CMD_ARG_COUNT], int argCount, long long span_ms, std::map<long long,int>& buckets)
{
    buckets.clear(); if(span_ms<=0) return 0; LISTBITMAPS bitmaps; int r=BuildBitmapsForQuery(args, argCount, bitmaps); if(r<=0) { for(auto &kv: bitmaps){ if(kv.second) delete kv.second; } return 0; }
    if(m_timeValues.empty()){ for(auto &kv: bitmaps){ if(kv.second) delete kv.second; } return 0; }
    for(LISTBITMAPS::iterator it=bitmaps.begin(); it!=bitmaps.end(); ++it){ BitMap* bm=it->second; if(!bm) continue; int n=bm->GetSize(); for(int i=0;i<n;i++){ int idx=bm->GetIndex(i); if(idx>=0 && (size_t)idx<m_timeValues.size()){ long long v=m_timeValues[idx]; long long b = (v / span_ms) * span_ms; buckets[b] += 1; } } delete bm; }
    return (int)buckets.size();
}

int LogStoreApi::Timechart_Count_ByBins(char *args[MAX_CMD_ARG_COUNT], int argCount, long long start_ms, long long end_ms, int bins, std::vector<int>& counts)
{
    counts.clear(); if(bins<=0) return 0; if(end_ms<=start_ms) return 0; long long width = (end_ms - start_ms) / bins; if(width<=0) width = 1; counts.resize(bins, 0);
    LISTBITMAPS bitmaps; int r=BuildBitmapsForQuery(args, argCount, bitmaps); if(r<=0) { for(auto &kv: bitmaps){ if(kv.second) delete kv.second; } return 0; }
    if(m_timeValues.empty()){ for(auto &kv: bitmaps){ if(kv.second) delete kv.second; } return 0; }
    for(LISTBITMAPS::iterator it=bitmaps.begin(); it!=bitmaps.end(); ++it){ BitMap* bm=it->second; if(!bm) continue; int n=bm->GetSize(); for(int i=0;i<n;i++){ int idx=bm->GetIndex(i); if(idx>=0 && (size_t)idx<m_timeValues.size()){ long long v=m_timeValues[idx]; if(v<start_ms || v>end_ms) continue; long long off = v - start_ms; int bi = (int)(off / width); if(bi >= bins) bi = bins-1; counts[bi] += 1; } } delete bm; }
    return (int)counts.size();
}

static int __read_line_str(Coffer* meta, int index, char* buf, int buflen){
    if(!meta || !meta->data || buflen<=0) return 0;
    if(meta->eleLen > 0){ RemovePadding(meta->data + index * meta->eleLen, meta->eleLen, buf); int l=strlen(buf); return l; }
    int lineIdx=0; int offset=0; char* p = meta->data; int sLen = meta->srcLen; while(p && (p - meta->data) < sLen && lineIdx < index){ if(*p=='\n') lineIdx++; p++; }
    if(lineIdx != index) return 0; while(p && *p && *p!='\n' && offset<buflen-1){ buf[offset++] = *p++; } buf[offset]='\0'; return offset;
}

int LogStoreApi::Timechart_Count_BySpan_Group(char *args[MAX_CMD_ARG_COUNT], int argCount, long long span_ms, const std::string& groupAlias, std::map<std::string, std::map<long long,int> >& gmap)
{
    gmap.clear(); if(span_ms<=0) return 0; 
    bool isEmpty = (argCount == 1 && (args[0] == NULL || args[0][0] == '\0'));
    LISTBITMAPS bitmaps; int r=BuildBitmapsForQuery(args, argCount, bitmaps); 
    if(r<=0 && !isEmpty){ for(auto &kv: bitmaps){ if(kv.second) delete kv.second; } return 0; }
    if(m_timeValues.empty()){ for(auto &kv: bitmaps){ if(kv.second) delete kv.second; } return 0; }
    VarAliasManager* mgr=VarAliasManager::getInstance(); std::vector<int> vids=mgr->getVarIds(groupAlias);
    for(size_t gi=0; gi<vids.size(); gi++){
        int gvar = vids[gi]; int pid = (gvar & 0xFFFF0000);
        LISTBITMAPS::iterator ib = bitmaps.find(pid);
        BitMap* filter = NULL;
        if(ib == bitmaps.end()){
            if(isEmpty) filter = NULL;
            else continue;
        } else {
            filter = ib->second;
            if(!isEmpty && filter == NULL) continue;
        }
        Coffer* meta=nullptr; int ret=DeCompressCapsule(gvar + VAR_TYPE_VAR, meta, 1); if(ret<=0 || !meta) continue;
        if(filter == NULL){
            int n = (int)m_timeValues.size(); char buf[1024];
            for(int i=0;i<n;i++){
                long long v = m_timeValues[i]; long long b = (v / span_ms) * span_ms;
                int glen = __read_line_str(meta, i, buf, sizeof(buf)); if(glen<=0) continue; std::string key(buf, (size_t)glen);
                gmap[key][b] += 1;
            }
        } else {
            int n = filter->GetSize(); char buf[1024];
            for(int i=0;i<n;i++){
                int idx = filter->GetIndex(i);
                if(idx<0 || (size_t)idx>=m_timeValues.size()) continue;
                long long v = m_timeValues[idx]; long long b = (v / span_ms) * span_ms;
                int glen = __read_line_str(meta, idx, buf, sizeof(buf)); if(glen<=0) continue; std::string key(buf, (size_t)glen);
                gmap[key][b] += 1;
            }
        }
    }
    for(LISTBITMAPS::iterator it=bitmaps.begin(); it!=bitmaps.end(); ++it){ if(it->second) delete it->second; }
    return (int)gmap.size();
}

int LogStoreApi::Timechart_Count_ByBins_Group(char *args[MAX_CMD_ARG_COUNT], int argCount, long long start_ms, long long end_ms, int bins, const std::string& groupAlias, std::map<std::string, std::vector<int> >& gout)
{
    gout.clear(); if(bins<=0) return 0; if(end_ms<=start_ms) return 0; long long width=(end_ms-start_ms)/bins; if(width<=0) width=1;
    bool isEmpty = (argCount == 1 && (args[0] == NULL || args[0][0] == '\0'));
    LISTBITMAPS bitmaps; int r=BuildBitmapsForQuery(args, argCount, bitmaps); 
    if(r<=0 && !isEmpty){ for(auto &kv: bitmaps){ if(kv.second) delete kv.second; } return 0; }
    if(m_timeValues.empty()){ for(auto &kv: bitmaps){ if(kv.second) delete kv.second; } return 0; }
    VarAliasManager* mgr=VarAliasManager::getInstance(); std::vector<int> vids=mgr->getVarIds(groupAlias);
    for(size_t gi=0; gi<vids.size(); gi++){
        int gvar = vids[gi]; int pid = (gvar & 0xFFFF0000);
        LISTBITMAPS::iterator ib = bitmaps.find(pid);
        BitMap* filter = NULL;
        if(ib == bitmaps.end()){
            if(isEmpty) filter = NULL;
            else continue;
        } else {
            filter = ib->second;
            if(!isEmpty && filter == NULL) continue;
        }
        Coffer* meta=nullptr; int ret=DeCompressCapsule(gvar + VAR_TYPE_VAR, meta, 1); if(ret<=0 || !meta) continue;
        
        if(filter == NULL){
            int n = (int)m_timeValues.size(); char buf[1024];
            for(int i=0;i<n;i++){
                long long v=m_timeValues[i]; if(v<start_ms || v>end_ms) continue; long long off=v-start_ms; int bi=(int)(off/width); if(bi>=bins) bi=bins-1;
                int glen = __read_line_str(meta, i, buf, sizeof(buf)); if(glen<=0) continue; std::string key(buf, (size_t)glen);
                std::vector<int>& kc = gout[key]; if((int)kc.size()<bins) kc.resize(bins,0); kc[bi] += 1;
            }
        } else {
            int n = filter->GetSize(); char buf[1024];
            for(int i=0;i<n;i++){
                int idx = filter->GetIndex(i);
                if(idx<0 || (size_t)idx>=m_timeValues.size()) continue; long long v=m_timeValues[idx]; if(v<start_ms || v>end_ms) continue; long long off=v-start_ms; int bi=(int)(off/width); if(bi>=bins) bi=bins-1;
                int glen = __read_line_str(meta, idx, buf, sizeof(buf)); if(glen<=0) continue; std::string key(buf, (size_t)glen);
                std::vector<int>& kc = gout[key]; if((int)kc.size()<bins) kc.resize(bins,0); kc[bi] += 1;
            }
        }
    }
    for(LISTBITMAPS::iterator it=bitmaps.begin(); it!=bitmaps.end(); ++it){ if(it->second) delete it->second; }
    return (int)gout.size();
}

int LogStoreApi::Test_AddPattern(const char* content, int eid, int count)
{
    char etag = 'E';
    int len = strlen(content);
    char* buf = new char[len+1];
    memcpy(buf, content, len);
    buf[len]='\0';
    int r = AddMainPatternToMap(etag, eid, count, buf);
    delete[] buf;
    return r;
}

int LogStoreApi::Test_QuerySingle(const char* token, LISTBITMAPS& bitmaps)
{
    int len = strlen(token);
    char* buf = new char[len+1];
    memcpy(buf, token, len);
    buf[len]='\0';
    int r = Search_SingleSegment(buf, bitmaps);
    delete[] buf;
    return r;
}

int LogStoreApi::Test_QueryLogic(char *args[MAX_CMD_ARG_COUNT], int argCount, LISTBITMAPS& bitmaps)
{
    return SearchByLogic(args, argCount, bitmaps);
}

int LogStoreApi::Test_InitOutliersMeta(int lines)
{
    Coffer* c = new Coffer(string("OUTL"), string(""), 0, lines, VAR_TYPE_OUTLIER, -1);
    m_glbMeta[OUTL_PAT_NAME] = c;
    return 0;
}

static bool __contains_ic(const char* text, const char* pat){
    if(!text || !pat) return false;
    int n = strlen(text);
    int m = strlen(pat);
    if(m == 0) return true;
    for(int i=0;i<=n-m;i++){
        int j=0;
        while(j<m && tolower(text[i+j]) == tolower(pat[j])) j++;
        if(j==m) return true;
    }
    return false;
}
static bool __contains_ic(const char* text, const char* pat);
static bool __parse_numeric_expr(const std::string& expr, long& outA, long& outB, int& opType);

static void __trim(std::string& s){
    size_t i = s.find_first_not_of(" \t");
    if(i == std::string::npos){ s.clear(); return; }
    size_t j = s.find_last_not_of(" \t");
    s = s.substr(i, j - i + 1);
}
static bool __startswith(const std::string& s, const char* p){ return s.compare(0, (int)strlen(p), p) == 0; }
static bool __parse_numeric_expr(const std::string& expr, long& outA, long& outB, int& opType){
    std::string s = expr; __trim(s); if(s.empty()) return false;
    size_t dots = s.find("..");
    if(dots != std::string::npos){
        std::string a = s.substr(0,dots); std::string b = s.substr(dots+2);
        __trim(a); __trim(b);
        char* ea=NULL; char* eb=NULL; long va = strtol(a.c_str(), &ea, 10); long vb = strtol(b.c_str(), &eb, 10);
        if(ea && *ea == '\0' && eb && *eb == '\0'){ outA=va; outB=vb; opType=6; return true; } else { return false; }
    }
    int t = -1; const char* rest = s.c_str();
    if(__startswith(s, ">=")){ t=3; rest = s.c_str()+2; }
    else if(__startswith(s, "<=")){ t=4; rest = s.c_str()+2; }
    else if(__startswith(s, "==")){ t=0; rest = s.c_str()+2; }
    else if(__startswith(s, "!=")){ t=5; rest = s.c_str()+2; }
    else if(__startswith(s, ">")){ t=1; rest = s.c_str()+1; }
    else if(__startswith(s, "<")){ t=2; rest = s.c_str()+1; }
    else { t=0; rest = s.c_str(); }
    std::string numStr(rest); __trim(numStr);
    char* e=NULL; long v = strtol(numStr.c_str(), &e, 10);
    if(e && *e == '\0'){ outA=v; opType=t; return true; }
    return false;
}

int LogStoreApi::FilterNumericVar(int varfname, const char* expr, BitMap* bitmap){
    long A=0,B=0; int op=0; if(!__parse_numeric_expr(std::string(expr), A, B, op)) return 0;
    if(op==0){ char buf[64]; snprintf(buf,sizeof(buf),"%ld",A); int pass = CheckBloom(varfname, buf); if(pass==0) return 0; }
    Coffer* meta=nullptr; int ret = DeCompressCapsule(varfname, meta, 1); if(ret <= 0) return 0;
    int matched = 0;
    if(meta->eleLen > 0){
        for(int i=0; i< meta->lines; i++){
            char buf[MAX_VALUE_LEN]={0};
            RemovePadding(meta->data + i * meta->eleLen, meta->eleLen, buf);
            char* e=nullptr; long v = strtol(buf, &e, 10);
            if(!(e && (*e=='\0' || isspace(*e)))) continue;
            bool ok=false;
            switch(op){
                case 0: ok = (v == A); break;
                case 1: ok = (v >  A); break;
                case 2: ok = (v <  A); break;
                case 3: ok = (v >= A); break;
                case 4: ok = (v <= A); break;
                case 5: ok = (v != A); break;
                case 6: ok = (v >= A && v <= B); break;
                default: ok=false; break;
            }
            if(ok){ bitmap->Union(i); matched++; }
        }
    } else {
        int lineIdx=0; int offset=0; char* p = meta->data; int sLen = meta->srcLen; char buf[MAX_VALUE_LEN]={0};
        while(p && (p - meta->data) < sLen){
            offset = 0; while(*p && *p != '\n' && (p - meta->data) < sLen){ buf[offset++] = *p; p++; }
            buf[offset] = '\0';
            char* e=nullptr; long v = strtol(buf, &e, 10);
            if(e && (*e=='\0' || isspace(*e))){
                bool ok=false;
                switch(op){
                    case 0: ok = (v == A); break;
                    case 1: ok = (v >  A); break;
                    case 2: ok = (v <  A); break;
                    case 3: ok = (v >= A); break;
                    case 4: ok = (v <= A); break;
                    case 5: ok = (v != A); break;
                    case 6: ok = (v >= A && v <= B); break;
                }
                if(ok){ bitmap->Union(lineIdx); matched++; }
            }
            lineIdx++; if(*p == '\n') p++; else break;
        }
    }
    return matched;
}

int LogStoreApi::CheckBloom(int varfname, const char* value){
    int base = (varfname & (~0xF));
    int bloomId = base + VAR_TYPE_BLOOM;
    LISTMETAS::iterator it = m_glbMeta.find(bloomId);
    if(it == m_glbMeta.end() || it->second == NULL) return 1;
    Coffer* meta=nullptr; int r = DeCompressCapsule(bloomId, meta, 1); if(r <= 0 || !meta || !meta->data) return 1;
    if(meta->srcLen < 24) return 1;
    unsigned long long m_bits = *(unsigned long long*)(meta->data + 0);
    unsigned int k = *(unsigned int*)(meta->data + 8);
    unsigned long long seed = *(unsigned long long*)(meta->data + 12);
    size_t byteSize = (size_t)((m_bits + 7) >> 3);
    if(meta->srcLen < (int)(24 + byteSize)) return 1;
    const unsigned char* bits = (const unsigned char*)(meta->data + 24);
    unsigned long long h1 = __hash64_str(value);
    unsigned long long h2 = __mix64(h1 ^ seed);
    for(unsigned int t=0; t<k; t++){
        unsigned long long hv = h1 + t * h2;
        unsigned long long bit = hv % m_bits;
        size_t b = (size_t)(bit >> 3);
        int off = (int)(bit & 7);
        if(((bits[b] >> off) & 1u) == 0) return 0;
    }
    return 1;
}
