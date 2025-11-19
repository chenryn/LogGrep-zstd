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
#ifdef LOGGREP_LOCAL_STUB
extern "C" int compress_from_memory(const char* buffer, int buffer_len, const char* output_path){
    FILE* f = fopen(output_path, "w");
    if(!f) return -1;
    size_t w = fwrite(buffer, 1, (size_t)buffer_len, f);
    fclose(f);
    return (w==(size_t)buffer_len)?0:-2;
}
#endif

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
      m_last_flush_ms(0), m_flush_interval_ms(3000), m_segments(), m_max_segments(100), m_seq(0), m_wal_fd(-1), m_wal_path(), m_fsync_wal(false), m_start_ms(0){
    const char* v;
    v=getenv("LOGGREP_FLUSH_BYTES"); if(v){ long long x=strtoll(v,nullptr,10); if(x>0) m_flush_bytes=(size_t)x; }
    v=getenv("LOGGREP_FLUSH_RECORDS"); if(v){ long long x=strtoll(v,nullptr,10); if(x>0) m_flush_records=(int)x; }
    v=getenv("LOGGREP_FLUSH_INTERVAL_MS"); if(v){ long long x=strtoll(v,nullptr,10); if(x>0) m_flush_interval_ms=x; }
    v=getenv("LOGGREP_MAX_SEGMENTS"); if(v){ long long x=strtoll(v,nullptr,10); if(x>0) m_max_segments=(int)x; }
    v=getenv("LOGGREP_WAL_FSYNC"); if(v){ int x=atoi(v); m_fsync_wal = (x>0); }
    ensure_dir();
    open_new_wal();
}

int RollingWriter::append(const std::string& line){
    std::lock_guard<std::mutex> lk(mtx);
    if(m_wal_fd>0){ ::write(m_wal_fd, line.c_str(), line.size()); if(line.empty() || line.back()!='\n'){ const char nl='\n'; ::write(m_wal_fd, &nl, 1); } }
    m_buf.append(line);
    if(line.empty() || line.back()!='\n') m_buf.push_back('\n');
    m_bytes = m_buf.size();
    if(m_records==0) m_start_ms = now_ms();
    m_records += 1;
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
        while((int)m_segments.size() > m_max_segments){
            std::string old = m_segments.front();
            m_segments.pop_front();
            unlink(old.c_str());
        }
    }
    return (int)lines.size();
}

int RollingWriter::sync_wal(){ std::lock_guard<std::mutex> lk(mtx); fsync_wal(); return 0; }