#include "Ingestor.h"
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
extern "C" int compress_from_memory(const char* buffer, int buffer_len, const char* output_path);

static std::string join_path(const std::string& a, const std::string& b){
    if(a.empty()) return b;
    if(a.back()=='/') return a + b;
    return a + std::string("/") + b;
}

long long RollingWriter::now_ms(){
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000LL + (long long)(ts.tv_nsec / 1000000LL);
}

void RollingWriter::ensure_dir(){
    struct stat st;
    if(stat(m_dir.c_str(), &st)!=0){
        mkdir(m_dir.c_str(), 0755);
    }
}

RollingWriter::RollingWriter(const std::string& dir)
    : m_dir(dir), m_buf(), m_bytes(0), m_records(0), m_flush_bytes(4*1024*1024), m_flush_records(50000),
      m_last_flush_ms(0), m_flush_interval_ms(3000), m_segments(), m_max_segments(100), m_seq(0){
    ensure_dir();
}

int RollingWriter::append(const std::string& line){
    m_buf.append(line);
    if(line.empty() || line.back()!='\n') m_buf.push_back('\n');
    m_bytes = m_buf.size();
    m_records += 1;
    return 1;
}

int RollingWriter::bulk_append(const std::vector<std::string>& lines, std::string& out_segment, bool& flushed){
    for(size_t i=0;i<lines.size();i++) append(lines[i]);
    bool do_flush = false;
    long long now = now_ms();
    if(m_bytes >= m_flush_bytes) do_flush = true;
    if(m_records >= m_flush_records) do_flush = true;
    if(m_last_flush_ms==0) m_last_flush_ms = now;
    if(now - m_last_flush_ms >= m_flush_interval_ms) do_flush = true;
    flushed = false;
    if(!do_flush) return (int)lines.size();
    std::string fname = std::string("ing_") + std::to_string(now) + std::string("_") + std::to_string(m_seq++) + std::string(".log.zip");
    std::string fpath = join_path(m_dir, fname);
    int rc = compress_from_memory(m_buf.c_str(), (int)m_buf.size(), fpath.c_str());
    if(rc==0){
        m_segments.push_back(fpath);
        m_buf.clear();
        m_bytes = 0;
        m_records = 0;
        m_last_flush_ms = now;
        out_segment = fpath;
        flushed = true;
        while((int)m_segments.size() > m_max_segments){
            std::string old = m_segments.front();
            m_segments.pop_front();
            unlink(old.c_str());
        }
    }
    return (int)lines.size();
}