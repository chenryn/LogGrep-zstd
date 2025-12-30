#include "Ingestor.h"
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <mutex>
extern "C" int compress_from_memory(const char* buffer, int buffer_len, const char* output_path);
#include <string>
#include <sys/stat.h>
#include <cstdio>
#include <cstdlib>
#include <dirent.h>
#include <limits>
#include <fstream>
#ifdef LOGGREP_LOCAL_STUB
extern "C" int compress_from_memory(const char* buffer, int buffer_len, const char* output_path){
    FILE* f = fopen(output_path, "w");
    if(!f) return -1;
    size_t w = fwrite(buffer, 1, (size_t)buffer_len, f);
    fclose(f);
    return (w==(size_t)buffer_len)?0:-2;
}
#endif

static size_t parse_size_bytes(const char* v){
    if(!v || !*v) return 0;
    while(*v==' '||*v=='\t') v++;
    const char* p=v; while((*p>='0'&&*p<='9')||*p=='.') p++;
    double num = strtod(v, nullptr);
    size_t mult = 1;
    std::string suf(p);
    for(size_t i=0;i<suf.size();i++){ char c=suf[i]; if(c>='A'&&c<='Z') suf[i]=c-'A'+'a'; }
    if(!suf.empty()){
        if(suf[0]=='k') mult = 1024ULL;
        else if(suf[0]=='m') mult = 1024ULL*1024ULL;
        else if(suf[0]=='g') mult = 1024ULL*1024ULL*1024ULL;
        else if(suf[0]=='t') mult = 1024ULL*1024ULL*1024ULL*1024ULL;
    }
    double val = num * (double)mult;
    if(val <= 0.0) return 0;
    if(val > (double)std::numeric_limits<size_t>::max()) return std::numeric_limits<size_t>::max();
    return (size_t)val;
}

static std::string join_path(const std::string& a, const std::string& b){
    if(a.empty()) return b;
    if(a.back()=='/') return a + b;
    return a + std::string("/") + b;
}

static uint32_t crc32_calc(const char* data, size_t len){
    uint32_t crc = 0xFFFFFFFFu;
    for(size_t i=0;i<len;i++){
        uint8_t byte = (uint8_t)data[i];
        crc ^= byte;
        for(int j=0;j<8;j++){
            uint32_t mask = -(int)(crc & 1);
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return ~crc;
}

long long RollingWriter::now_ms(){
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000LL + (long long)(ts.tv_nsec / 1000000LL);
}

void RollingWriter::ensure_dir(){
    auto mk = [](const std::string& p){ struct stat st; if(stat(p.c_str(), &st)==0) return 0; return mkdir(p.c_str(), 0755); };
    if(m_dir.empty()) return;
    size_t i = 0; if(m_dir[0]=='/') i = 1;
    while(true){ size_t pos = m_dir.find('/', i); std::string sub = m_dir.substr(0, pos==std::string::npos? m_dir.size() : pos); if(!sub.empty()) mk(sub); if(pos==std::string::npos) break; i = pos+1; }
}

void RollingWriter::open_new_wal(){
    if(m_wal_fd>0){ ::close(m_wal_fd); m_wal_fd=-1; }
    long long now = now_ms();
    std::string wname = std::string("wal_") + std::to_string(now) + std::string("_") + std::to_string(m_seq) + std::string(".log");
    m_wal_path = join_path(m_dir, wname);
    m_wal_fd = ::open(m_wal_path.c_str(), O_CREAT|O_APPEND|O_WRONLY, 0644);
}

void RollingWriter::fsync_wal(){ if(m_fsync_wal && m_wal_fd>0){ ::fsync(m_wal_fd); } }

RollingWriter::RollingWriter(const std::string& dir)
    : m_dir(dir), m_buf(), m_bytes(0), m_records(0), m_flush_bytes(64*1024*1024), m_flush_records(50000),
      m_last_flush_ms(0), m_flush_interval_ms(3000), m_segments(), m_max_segments(100), m_max_disk_bytes(0), m_segments_bytes(0), m_seq(0), m_wal_fd(-1), m_wal_path(), m_fsync_wal(false), m_start_ms(0), m_flusher(), m_stop(false){
    load_index_config();
    const char* v;
    v=getenv("LOGGREP_FLUSH_BYTES"); if(v){ size_t x=parse_size_bytes(v); if(x>0) m_flush_bytes=x; }
    v=getenv("LOGGREP_FLUSH_RECORDS"); if(v){ long long x=strtoll(v,nullptr,10); if(x>0) m_flush_records=(int)x; }
    v=getenv("LOGGREP_FLUSH_INTERVAL_MS"); if(v){ long long x=strtoll(v,nullptr,10); if(x>=0) m_flush_interval_ms=x; }
    v=getenv("LOGGREP_MAX_SEGMENTS"); if(v){ long long x=strtoll(v,nullptr,10); if(x>0) m_max_segments=(int)x; }
    v=getenv("LOGGREP_MAX_DISK_BYTES"); if(v){ size_t x=parse_size_bytes(v); if(x>0) m_max_disk_bytes=x; }
    v=getenv("LOGGREP_WAL_FSYNC"); if(v){ int x=atoi(v); m_fsync_wal = (x>0); }
    ensure_dir();
    open_new_wal();
    load_existing_segments();
    m_flusher = std::thread([this](){ this->bg_worker(); });
}

int RollingWriter::append(const std::string& line){
    std::lock_guard<std::mutex> lk(mtx);
    if(m_wal_fd>0){ ::write(m_wal_fd, line.c_str(), line.size()); if(line.empty() || line.back()!='\n'){ const char nl='\n'; ::write(m_wal_fd, &nl, 1); } }
    m_buf.append(line);
    if(line.empty() || line.back()!='\n') m_buf.push_back('\n');
    m_bytes = m_buf.size();
    if(m_records==0) m_start_ms = now_ms();
    m_records += 1;
    m_cv.notify_one();
    return 1;
}

int RollingWriter::bulk_append(const std::vector<std::string>& lines, std::string& out_segment, bool& flushed){
    std::lock_guard<std::mutex> lk(mtx);
    for(size_t i=0;i<lines.size();i++){
        const std::string& ln = lines[i];
        if(m_wal_fd>0){ ::write(m_wal_fd, ln.c_str(), ln.size()); if(ln.empty() || ln.back()!='\n'){ const char nl='\n'; ::write(m_wal_fd, &nl, 1); } }
        m_buf.append(ln);
        if(ln.empty() || ln.back()!='\n') m_buf.push_back('\n');
        if(m_records==0) m_start_ms = now_ms();
        m_records += 1;
    }
    m_bytes = m_buf.size();
    bool do_flush = false;
    long long now = now_ms();
    if(m_bytes >= m_flush_bytes) do_flush = true;
    if(m_records >= m_flush_records) do_flush = true;
    if(m_last_flush_ms==0) m_last_flush_ms = now;
    if(now - m_last_flush_ms >= m_flush_interval_ms) do_flush = true;
    flushed = false;
    if(!do_flush) return (int)lines.size();
    fsync_wal();
    std::string fname = std::string("ing_") + std::to_string(now) + std::string("_") + std::to_string(m_seq++) + std::string(".log.zip");
    std::string fpath = join_path(m_dir, fname);
    size_t prev_bytes = m_bytes;
    int prev_records = m_records;
    long long start_ms = m_start_ms==0? now : m_start_ms;
    long long end_ms = now;
    uint32_t crc = crc32_calc(m_buf.c_str(), m_buf.size());
    int rc = compress_from_memory(m_buf.c_str(), (int)m_buf.size(), fpath.c_str());
    if(rc==0){
        m_segments.push_back(fpath);
        struct stat stsz; if(stat(fpath.c_str(), &stsz)==0){ m_segments_bytes += (size_t)stsz.st_size; }
        m_buf.clear();
        m_bytes = 0;
        m_records = 0;
        m_last_flush_ms = now;
        m_start_ms = 0;
        out_segment = fpath;
        flushed = true;
        std::string mpath = fpath + std::string(".meta");
        FILE* mf = fopen(mpath.c_str(), "w");
        if(mf){
            std::string meta;
            meta.append("{");
            meta.append("\"wal\":\""); meta.append(m_wal_path); meta.append("\",");
            meta.append("\"bytes\":"); meta.append(std::to_string(prev_bytes)); meta.append(",");
            meta.append("\"records\":"); meta.append(std::to_string(prev_records)); meta.append(",");
            meta.append("\"start_ms\":"); meta.append(std::to_string(start_ms)); meta.append(",");
            meta.append("\"end_ms\":"); meta.append(std::to_string(end_ms)); meta.append(",");
            meta.append("\"crc32\":"); meta.append(std::to_string(crc));
            meta.append("}");
            fwrite(meta.c_str(), 1, meta.size(), mf);
            fclose(mf);
        }
        if(m_wal_fd>0){ ::close(m_wal_fd); m_wal_fd=-1; ::unlink(m_wal_path.c_str()); }
        open_new_wal();
        while((int)m_segments.size() > m_max_segments || (m_max_disk_bytes>0 && m_segments_bytes > m_max_disk_bytes)){
            std::string old = m_segments.front();
            m_segments.pop_front();
            struct stat stod; if(stat(old.c_str(), &stod)==0){ size_t osz=(size_t)stod.st_size; if(m_segments_bytes>=osz) m_segments_bytes -= osz; }
            unlink(old.c_str());
        }
    }
    return (int)lines.size();
}

int RollingWriter::sync_wal(){ std::lock_guard<std::mutex> lk(mtx); fsync_wal(); return 0; }
int RollingWriter::flush(std::string& out_segment){
    std::lock_guard<std::mutex> lk(mtx);
    out_segment.clear();
    if(m_bytes==0 && m_records==0) return 0;
    fsync_wal();
    long long now = now_ms();
    std::string fname = std::string("ing_") + std::to_string(now) + std::string("_") + std::to_string(m_seq++) + std::string(".log.zip");
    std::string fpath = join_path(m_dir, fname);
    size_t prev_bytes = m_bytes;
    int prev_records = m_records;
    long long start_ms = m_start_ms==0? now : m_start_ms;
    long long end_ms = now;
    uint32_t crc = crc32_calc(m_buf.c_str(), m_buf.size());
    int rc = compress_from_memory(m_buf.c_str(), (int)m_buf.size(), fpath.c_str());
    if(rc==0){
        m_segments.push_back(fpath);
        struct stat stsz; if(stat(fpath.c_str(), &stsz)==0){ m_segments_bytes += (size_t)stsz.st_size; }
        m_buf.clear();
        m_bytes = 0;
        m_records = 0;
        m_last_flush_ms = now;
        m_start_ms = 0;
        out_segment = fpath;
        std::string mpath = fpath + std::string(".meta");
        FILE* mf = fopen(mpath.c_str(), "w");
        if(mf){
            std::string meta;
            meta.append("{");
            meta.append("\"wal\":\""); meta.append(m_wal_path); meta.append("\",");
            meta.append("\"bytes\":"); meta.append(std::to_string(prev_bytes)); meta.append(",");
            meta.append("\"records\":"); meta.append(std::to_string(prev_records)); meta.append(",");
            meta.append("\"start_ms\":"); meta.append(std::to_string(start_ms)); meta.append(",");
            meta.append("\"end_ms\":"); meta.append(std::to_string(end_ms)); meta.append(",");
            meta.append("\"crc32\":"); meta.append(std::to_string(crc));
            meta.append("}");
            fwrite(meta.c_str(), 1, meta.size(), mf);
            fclose(mf);
        }
        if(m_wal_fd>0){ ::close(m_wal_fd); m_wal_fd=-1; ::unlink(m_wal_path.c_str()); }
        open_new_wal();
        while((int)m_segments.size() > m_max_segments || (m_max_disk_bytes>0 && m_segments_bytes > m_max_disk_bytes)){
            std::string old = m_segments.front();
            m_segments.pop_front();
            struct stat stod; if(stat(old.c_str(), &stod)==0){ size_t osz=(size_t)stod.st_size; if(m_segments_bytes>=osz) m_segments_bytes -= osz; }
            unlink(old.c_str());
        }
    }
    return prev_records;
}

RollingWriter::~RollingWriter(){ m_stop.store(true); if(m_flusher.joinable()) m_flusher.join(); }

void RollingWriter::bg_worker(){
    while(!m_stop.load()){
        long long wait_ms = 0;
        {
            std::lock_guard<std::mutex> lk(mtx);
            if(m_flush_interval_ms>0 && m_records>0){
                long long now = now_ms();
                long long base = (m_last_flush_ms==0)? m_start_ms : m_last_flush_ms;
                if(base==0) base = now;
                long long elapsed = now - base;
                if(elapsed < m_flush_interval_ms) wait_ms = m_flush_interval_ms - elapsed;
            }
        }
        if(wait_ms > 0){
            std::unique_lock<std::mutex> ul(mtx);
            m_cv.wait_for(ul, std::chrono::milliseconds(wait_ms));
        } else {
            std::unique_lock<std::mutex> ul(mtx);
            m_cv.wait_for(ul, std::chrono::milliseconds(m_flush_interval_ms>0? m_flush_interval_ms: 100));
        }
        bool need=false;
        {
            std::lock_guard<std::mutex> lk(mtx);
            if(m_records>0){
                long long now = now_ms();
                long long base = (m_last_flush_ms==0)? m_start_ms : m_last_flush_ms;
                if(base==0) base = now;
                if(m_flush_interval_ms>0 && now - base >= m_flush_interval_ms) need=true;
                if(m_bytes >= m_flush_bytes) need=true;
                if(m_records >= m_flush_records) need=true;
            }
        }
        if(need){ std::string seg; flush(seg); }
    }
}
void RollingWriter::load_existing_segments(){
    DIR* d = opendir(m_dir.c_str());
    if(!d) return;
    struct dirent* ent;
    struct Item{ std::string path; long long ts; int seq; size_t sz; };
    std::vector<Item> items;
    while((ent = readdir(d))){
        std::string n = ent->d_name;
        if(n.size()>12 && n.find("ing_")==0 && n.rfind(".log.zip")==n.size()-8){
            size_t u = n.find('_', 4);
            size_t dot = n.rfind('.')==std::string::npos? n.size() : n.rfind('.');
            if(u!=std::string::npos){
                std::string tsStr = n.substr(4, u-4);
                size_t u2 = n.find('_', u+1);
                if(u2==std::string::npos) continue;
                std::string seqStr = n.substr(u+1, u2-(u+1));
                long long ts = strtoll(tsStr.c_str(), nullptr, 10);
                int seq = atoi(seqStr.c_str());
                std::string p = join_path(m_dir, n);
                struct stat st; if(stat(p.c_str(), &st)==0){ items.push_back({p, ts, seq, (size_t)st.st_size}); }
            }
        }
    }
    closedir(d);
    std::sort(items.begin(), items.end(), [](const Item& a, const Item& b){ if(a.ts==b.ts) return a.seq<b.seq; return a.ts<b.ts; });
    m_segments.clear();
    m_segments_bytes = 0;
    for(auto &it: items){ m_segments.push_back(it.path); m_segments_bytes += it.sz; }
    while((int)m_segments.size() > m_max_segments || (m_max_disk_bytes>0 && m_segments_bytes > m_max_disk_bytes)){
        std::string old = m_segments.front();
        m_segments.pop_front();
        struct stat stod; if(stat(old.c_str(), &stod)==0){ size_t osz=(size_t)stod.st_size; if(m_segments_bytes>=osz) m_segments_bytes -= osz; }
        unlink(old.c_str());
    }
}

void RollingWriter::load_index_config(){
    std::string cfg = join_path(m_dir, std::string("ingest.conf"));
    std::ifstream in(cfg.c_str());
    if(!in.good()) return;
    std::string line;
    while(std::getline(in, line)){
        size_t pos = line.find('=');
        if(pos==std::string::npos) continue;
        std::string k = line.substr(0,pos);
        std::string v = line.substr(pos+1);
        if(k=="FLUSH_BYTES"){ size_t x=parse_size_bytes(v.c_str()); if(x>0) m_flush_bytes=x; }
        else if(k=="FLUSH_RECORDS"){ long long x=strtoll(v.c_str(),nullptr,10); if(x>0) m_flush_records=(int)x; }
        else if(k=="FLUSH_INTERVAL_MS"){ long long x=strtoll(v.c_str(),nullptr,10); if(x>=0) m_flush_interval_ms=x; }
        else if(k=="MAX_SEGMENTS"){ long long x=strtoll(v.c_str(),nullptr,10); if(x>0) m_max_segments=(int)x; }
        else if(k=="MAX_DISK_BYTES"){ size_t x=parse_size_bytes(v.c_str()); if(x>0) m_max_disk_bytes=x; }
        else if(k=="WAL_FSYNC"){ int x=atoi(v.c_str()); m_fsync_wal = (x>0); }
    }
    in.close();
}
