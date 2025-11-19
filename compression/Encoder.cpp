#include "Encoder.h"
#include "Coffer.h"
#include <vector>
#include <cmath>
#include <cstring>
#include <algorithm>
#include "constant.h"
#include "union.h"
#include <zstd.h>
using namespace std;
Encoder::Encoder(string cp_mode, string zip_mode, int compression_level){
    data.clear();
    _meta_out = (zip_mode == "O") ? true : false;
    _cp_mode = cp_mode;
    cp_level = compression_level;
}

bool sortCoffer(Coffer* co1, Coffer* co2){
    return co1 -> type < co2 -> type;
}

string Encoder::compress(){
    string meta = "";
    sort(data.begin(), data.end(), sortCoffer);
    int nowOffset = 0;
    for(auto &temp: data){
        if(temp -> srcLen == 0){
            meta += temp -> filenames + " 0 " +  to_string(nowOffset) + " 0 0 0 " + to_string(temp->eleLen) + "\n";
            continue;
        }
        int destLen = temp -> compress(_cp_mode, cp_level);
        meta += temp ->filenames + " " + to_string(temp -> compressed) + " " +  to_string(nowOffset) + " " + to_string(destLen) + " " + to_string(temp -> srcLen) + " " + to_string(temp -> lines) + " " + to_string(temp -> eleLen) + "\n";
        
        nowOffset += destLen;
    }
   return meta;
}

string Encoder::padding(string filename, int Idx, int maxIdx){
    int totLen = 1 + log10(maxIdx + 1);
    int nowLen = 1 + log10(Idx + 1);
    if (totLen < nowLen){
        printf("%s Int Padding Error: totLen: %d, nowLen:%d!\n", filename.c_str(), totLen, nowLen);
        return to_string(Idx);
    }
    int padSize = totLen - nowLen;
    if(padSize == 0) return to_string(Idx);
    char* PADS = new char[padSize];
    for(int i = 0; i < padSize; i++) PADS[i] = ' ';
    PADS[padSize] = '\0';
    return string(PADS) + to_string(Idx);
}

void Encoder::padding(string filename, char* buffer, int startPos, int padSize, int typ){
    char PAD = ' ';
    for(int i = 0; i < padSize; i++){
        buffer[startPos + i] = PAD;
    }
}

string Encoder::padding(string filename, string target, int maxLen, int typ){
    char PAD = ' ';
    int t_size = target.size();
    if(maxLen < t_size){
        printf("%s String Padding Error: maxLen: %d, target size: %d\n", filename.c_str(), maxLen, t_size);
        cout << target << endl;
        return target;
    }
    int padSize = maxLen - target.size();
    if(padSize == 0) return target;
    char* PADS = new char[padSize + 5];
    for(int i = 0; i < padSize; i ++) PADS[i] = PAD;
    PADS[padSize] = '\0';
    return string(PADS) + target;
}

void Encoder::serializeTemplate(string zip_path, LengthParser* parser){
    char* longStr = NULL;
    int ll = parser -> getTemplate(&longStr);
    if(_meta_out){
        FILE* test = fopen((zip_path + ".templates").c_str(), "w");
        fprintf(test, "%s", longStr);
        fclose(test);
    }
    int srcL = strlen(longStr);
    Coffer* nCoffer = new Coffer(to_string(TYPE_TEMPLATE << POS_TYPE), longStr, srcL, ll, 0, -1);
    data.push_back(nCoffer);
}

void Encoder::serializeTemplateOutlier(char** failed_log, int failLine){
    string longStr = "";
    int count = 0;
    for(int i = 0; i < failLine; i++, count++){
        string temp(failed_log[i]);
        if(longStr.size() + temp.size() > MAXCOMPRESS){
            Coffer* nCoffer = new Coffer(to_string(TYPE_OUTLIER << POS_TYPE), longStr, longStr.size(), count, 1, -1);
            data.push_back(nCoffer);
            longStr = "";
            count = 0;
        }
        longStr += temp + "\n";
        
    }
    Coffer* nCoffer = new Coffer(to_string(TYPE_OUTLIER << POS_TYPE), longStr, longStr.size(), count, 1, -1);
    data.push_back(nCoffer);
}

void Encoder::serializeVar(string filename, char* globuf, VarArray* var, int maxLen){
    int length = var ->nowPos;
    int tot = length * (maxLen+1);
    char* temp = new char[tot + 5];
    int nowPtr = 0;
    const int KMV_K = 1024;
    std::vector<unsigned long long> kmv;
    kmv.reserve(KMV_K);
    auto hash64 = [](const char* s, int len)->unsigned long long{
        unsigned long long h = 1469598103934665603ULL;
        for(int i=0;i<len;i++){ h ^= (unsigned long long)(unsigned char)s[i]; h *= 1099511628211ULL; }
        return h;
    };
    for(int i=0; i< length; i++)
    {
        int varLen = var->len[i];
        int padSize = maxLen - varLen;
        if (padSize < 0) SysWarning("Error pad size < 0\n");
        padding(filename, temp, nowPtr, padSize, 0);
        nowPtr += padSize;
        strncpy(temp + nowPtr, globuf + var->startPos[i], varLen);
        unsigned long long hv = hash64(globuf + var->startPos[i], varLen);
        if((int)kmv.size() < KMV_K){ kmv.push_back(hv); }
        else {
            int idxMax = 0; unsigned long long vmax = kmv[0];
            for(int j=1;j<KMV_K;j++){ if(kmv[j] > vmax){ vmax = kmv[j]; idxMax = j; } }
            if(hv < vmax){ kmv[idxMax] = hv; }
        }
        nowPtr += varLen;
    }

    Coffer* nCoffer = new Coffer(filename, temp, nowPtr, length, 5, maxLen);
    data.push_back(nCoffer);

    if(length >= 10000){
        if(kmv.empty()) return;
        unsigned long long vmax = kmv[0];
        for(size_t j=1;j<kmv.size();j++){ if(kmv[j] > vmax) vmax = kmv[j]; }
        double Uk = (double)vmax / (double)0xFFFFFFFFFFFFFFFFULL;
        if(Uk <= 0.) Uk = 1e-12;
        double Dhat = ((double)kmv.size() - 1.0) / Uk;
        if(Dhat < 1.0) Dhat = 1.0;
        double r = Dhat / (double)length;
        if(r >= 0.7){
            size_t var_bytes = (size_t)nowPtr;
            size_t budget = (size_t)(var_bytes * 0.01);
            if(budget < 1024) budget = 1024;
            if(budget > (size_t)(2*1024*1024)) budget = (size_t)(2*1024*1024);
            double bpi = (double)(budget * 8) / Dhat;
            int k = (int)(bpi * 0.69314718056);
            if(k < 1) k = 1; if(k > 16) k = 16;
            size_t m_bits = budget * 8;
            std::vector<unsigned char> bloom;
            bloom.resize(budget + 24);
            unsigned long long* hdr_ptr = (unsigned long long*)&bloom[0];
            hdr_ptr[0] = (unsigned long long)m_bits;
            unsigned int* kptr = (unsigned int*)&bloom[8];
            *kptr = (unsigned int)k;
            unsigned long long* seedptr = (unsigned long long*)&bloom[12];
            *seedptr = 0x9E3779B97F4A7C15ULL;
            auto mix = [](unsigned long long x){ x += 0x9E3779B97F4A7C15ULL; x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL; x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL; x ^= x >> 31; return x; };
            auto setbit = [&](size_t bit){ size_t byte = bit >> 3; int off = bit & 7; bloom[24 + byte] |= (unsigned char)(1u << off); };
            for(int i=0;i<length;i++){
                int varLen = var->len[i];
                unsigned long long h1 = hash64(globuf + var->startPos[i], varLen);
                unsigned long long h2 = mix(h1);
                for(int t=0;t<k;t++){
                    unsigned long long hv = h1 + t * h2;
                    size_t bit = (size_t)(hv % m_bits);
                    setbit(bit);
                }
            }
            int id = atoi(filename.c_str());
            int base = (id & (~0xF));
            int bloomId = base | (TYPE_BLOOM << POS_TYPE);
            Coffer* bCoffer = new Coffer(to_string(bloomId), (char*)&bloom[0], (int)bloom.size(), 1, 1, -4);
            data.push_back(bCoffer);
        }
    }
}

void Encoder::serializeEntry(string filename, int* entry, int maxEntry, int total){
   string longStr = "";
   int paddingSize = 1 + log10(maxEntry);
   for(int i = 0; i < total; i++){
       longStr += padding(filename, to_string(entry[i]), paddingSize, 0);
   }
   Coffer* nCoffer = new Coffer(filename, longStr, longStr.size(), total, 4, paddingSize);
   data.push_back(nCoffer);
}

bool pairCmp (pair<string, int>&t1, pair<string, int>& t2){
    return (t1.second < t2.second);
}

void Encoder::serializeDic(string filename, char* globuf, VarArray* varMapping, Union* root){
    vector<pair<unsigned int, int> >* container = root -> getContainer();
    int count = 0;
    int paddingSize = root -> nowPaddingSize[0];
    int patIdx = 0;
    int nowPtr = 0;
    int bufferSize = (root -> dicMax >= 10000) ? MAXBUFFER : MAX_VALUE_LEN * root ->dicMax;
    char* temp = new char[bufferSize];
    for(vector<pair<unsigned int, int> >::iterator it = container -> begin(); it != container -> end(); it++,count++){

        if(count == root -> nowCounter[patIdx]){
            count = 0;
            patIdx++;
            paddingSize = root -> nowPaddingSize[patIdx]; 
        }
        int nowPos = root ->posDictionary[it ->first];
        int nowLen = varMapping->len[nowPos];
        padding(filename, temp, nowPtr, paddingSize - nowLen, 0);
        nowPtr += paddingSize - nowLen;
        strncpy(temp + nowPtr, globuf + varMapping->startPos[nowPos], nowLen);
        nowPtr += varMapping->len[nowPos];
    } 
    Coffer* nCoffer = new Coffer(filename, temp, nowPtr, root -> dicMax, 3, -2);
    data.push_back(nCoffer);
}

void Encoder::serializeSvar(string filename, SubPattern* subPattern){
    bool debug = false;
    if(debug) cout << subPattern -> type << endl;
    
    if(subPattern -> type == 1){  //fint, fstr
        string longStr = "";
        for(int i = 0; i < subPattern -> data_count; i++){
            string tempStr = subPattern -> data[i];
            if(tempStr.size() == 0) tempStr = padding(filename, subPattern ->data[i], subPattern -> length, subPattern -> type);
            longStr += tempStr;
        }
        Coffer* nCoffer = new Coffer(filename, longStr, longStr.size(), subPattern -> data_count, 6, subPattern -> length);
        data.push_back(nCoffer);
    }
    if(subPattern -> type == 2){ //int, str
        string longStr = "";
        for(int i = 0; i < subPattern -> data_count; i++){
            string tempStr = padding(filename, subPattern -> data[i], subPattern -> maxLen, subPattern -> type);
            int ssize = subPattern ->data[i].size();
            if(ssize  < subPattern ->maxLen && debug) cout << subPattern -> data[i] << " " << tempStr << endl;
            longStr += tempStr;
        } 
        int paddingSize = subPattern -> maxLen;
        if(debug) cout << longStr << endl;
        Coffer* nCoffer = new Coffer(filename, longStr, longStr.size(), subPattern -> data_count, 6, paddingSize);
        data.push_back(nCoffer);
    }
}

void Encoder::serializeOutlier(string filename, vector<pair<int, string> > outliers){
    string longStr = "";
    for(auto &temp: outliers){
        longStr += to_string(temp.first) + " " + temp.second + "\n";
    }
    Coffer* nCoffer = new Coffer(filename, longStr, longStr.size(), outliers.size(), 7, -3);
    data.push_back(nCoffer);
}

void Encoder::serializeSubpattern(string zip_path, string SUBPATTERN, int SUBCOUNT){
    if(_meta_out){
        FILE* test = fopen((zip_path + ".variables").c_str(), "w");
        fprintf(test, "%s", SUBPATTERN.c_str());
        fclose(test);
    }
    Coffer* nCoffer = new Coffer(to_string(TYPE_VARIABLELIST << POS_TYPE), SUBPATTERN, SUBPATTERN.size(), SUBCOUNT, 2, -1);
    data.push_back(nCoffer);
}

void Encoder::output(string zip_path, int typ){
    if(typ == 1){
       for(auto &temp: data){
           temp -> printFile(zip_path);
       }
       return;
    }
    FILE* zipFile = fopen(zip_path.c_str(), "w");
    if (zipFile == NULL){
        cout << "open zip file failed" << endl;
        return;
    }

    string meta = compress();//Build meta
    
    if(_meta_out){
        FILE* test = fopen((zip_path + ".meta").c_str(),"w");
        fprintf(test, "%s", meta.c_str());
        fclose(test);
    }

	size_t com_space_size = ZSTD_compressBound(meta.size());
	Byte* pZstd = new Byte[com_space_size];
	size_t srcLen = meta.size();
	size_t destLen = ZSTD_compress(pZstd, com_space_size, meta.c_str(), srcLen, cp_level);
	fwrite(&destLen, sizeof(size_t), 1, zipFile);
	fwrite(&srcLen, sizeof(size_t), 1, zipFile);
	fwrite(pZstd, sizeof(Byte), destLen, zipFile);
	delete [] pZstd;

    //Output templates
    printf("cp_mode:%s\n", _cp_mode.c_str());
    for(auto &temp: data){
        if(temp -> lines == 0) continue;
        temp -> output(zipFile, typ);
    }
    fclose(zipFile);
}

void Encoder::serializeTimeColumn(const std::vector<long long>& times){
    if(times.empty()){
        Coffer* nCoffer = new Coffer(to_string(TYPE_TIME_COL << POS_TYPE), NULL, 0, 0, 0, -1);
        data.push_back(nCoffer);
        return;
    }
    // encode as fixed-width 64-bit decimal strings for simplicity
    int paddingSize = 13; // enough for epoch ms
    string longStr;
    longStr.reserve(times.size() * (paddingSize));
    for(size_t i=0;i<times.size();i++){
        string v = to_string(times[i]);
        if((int)v.size() < paddingSize){
            longStr += string(paddingSize - v.size(), ' ') + v;
        } else {
            longStr += v;
        }
    }
    Coffer* nCoffer = new Coffer(to_string(TYPE_TIME_COL << POS_TYPE), longStr, longStr.size(), times.size(), 8, paddingSize);
    data.push_back(nCoffer);
}

void Encoder::serializeTimeIndex(const std::vector<int>& seg_starts,
                                const std::vector<int>& seg_ends,
                                const std::vector<long long>& seg_min,
                                const std::vector<long long>& seg_max){
    string longStr;
    int n = seg_starts.size();
    for(int i=0;i<n;i++){
        longStr += to_string(i);
        longStr += " ";
        longStr += to_string(seg_starts[i]);
        longStr += " ";
        longStr += to_string(seg_ends[i]);
        longStr += " ";
        longStr += to_string(seg_min[i]);
        longStr += " ";
        longStr += to_string(seg_max[i]);
        longStr += "\n";
    }
    Coffer* nCoffer = new Coffer(to_string(TYPE_TIME_INDEX << POS_TYPE), longStr, longStr.size(), n, 8, -1);
    data.push_back(nCoffer);
}

