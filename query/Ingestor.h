#ifndef LOGGREP_INGESTOR_H
#define LOGGREP_INGESTOR_H
#include <string>
#include <vector>
#include <deque>
#include <mutex>
#include <sys/stat.h>
#include <unistd.h>

class RollingWriter {
private:
    std::string m_dir;
    std::string m_buf;
    size_t m_bytes;
    int m_records;
    size_t m_flush_bytes;
    int m_flush_records;
    long long m_last_flush_ms;
    long long m_flush_interval_ms;
    std::deque<std::string> m_segments;
    int m_max_segments;
    size_t m_max_disk_bytes;
    size_t m_segments_bytes;
    int m_seq;
    std::mutex mtx;
    int m_wal_fd;
    std::string m_wal_path;
    bool m_fsync_wal;
    long long m_start_ms;
    long long now_ms();
    void ensure_dir();
    void open_new_wal();
    void fsync_wal();
    void load_existing_segments();
public:
    RollingWriter(const std::string& dir);
    int append(const std::string& line);
    int bulk_append(const std::vector<std::string>& lines, std::string& out_segment, bool& flushed);
    int sync_wal();
    int flush(std::string& out_segment);
};

#endif
