#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>
#include <map>
#include <vector>
#include <cstring>
#include <cstdio>
#include "LogDispatcher.h"
#include "Ingestor.h"
#include "SPLParser.h"
#include "zstd.h"
#include "../compression/TimeParser.h"
#include <map>
#include <fstream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <dirent.h>
extern "C" int compress_from_memory(const char* buffer, int buffer_len, const char* output_path);
static void recover_wal_dir(const std::string& dir);

static std::map<std::string,std::string> g_index_map;
static std::string g_index_cfg = std::string("indices.conf");
static std::map<std::string, RollingWriter*> g_writers;
static std::mutex g_writers_mtx;
static std::atomic<int> g_pending_count{0};
static void load_indices(){ std::ifstream in(g_index_cfg.c_str()); if(in.good()){ g_index_map.clear(); std::string line; while(std::getline(in, line)){ size_t eq=line.find('='); if(eq==std::string::npos) continue; std::string k=line.substr(0,eq); std::string v=line.substr(eq+1); if(!k.empty() && !v.empty()){ g_index_map[k]=v; } } in.close(); }
  if(g_index_map.empty()){ g_index_map["main"]=std::string("../lib_output_zip/Ssh"); g_index_map["ssh"]=std::string("../lib_output_zip/Ssh"); std::ofstream out(g_index_cfg.c_str(), std::ios::out|std::ios::trunc); if(out.good()){ for(auto &it: g_index_map){ out<<it.first<<"="<<it.second<<"\n"; } out.close(); } }
}
static void save_indices(){ std::ofstream out(g_index_cfg.c_str(), std::ios::out|std::ios::trunc); if(out.good()){ for(auto &it: g_index_map){ out<<it.first<<"="<<it.second<<"\n"; } out.close(); } }
static RollingWriter* get_writer(const std::string& index){ std::lock_guard<std::mutex> lk(g_writers_mtx); auto it=g_index_map.find(index); if(it==g_index_map.end()) return nullptr; auto wit=g_writers.find(index); if(wit!=g_writers.end()) return wit->second; RollingWriter* w=new RollingWriter(it->second); g_writers[index]=w; return w; }

static void write_all(int fd, const char* buf, size_t len){ size_t off=0; while(off<len){ ssize_t n=::write(fd, buf+off, len-off); if(n<=0) return; off+=n; } }
static std::string read_headers(int fd){ std::string s; s.reserve(1024); char c; int state=0; while(true){ ssize_t n=::read(fd, &c, 1); if(n<=0) break; s.push_back(c); if(state==0 && c=='\r') state=1; else if(state==1 && c=='\n') state=2; else if(state==2 && c=='\r') state=3; else if(state==3 && c=='\n') break; else state=0; } return s; }
static size_t find_content_length(const std::string& hdr){ size_t hpos=hdr.find("Content-Length:"); if(hpos==std::string::npos) return 0; size_t eol=hdr.find("\r\n", hpos); std::string v=hdr.substr(hpos+15, eol-(hpos+15)); return strtoul(v.c_str(), nullptr, 10); }
static bool has_chunked(const std::string& hdr){ size_t p=hdr.find("Transfer-Encoding:"); if(p==std::string::npos) return false; size_t e=hdr.find("\r\n", p); std::string v=hdr.substr(p+18, e-(p+18)); for(size_t i=0;i<v.size();i++){ char c=v[i]; if(c>='A'&&c<='Z') v[i]=c-'A'+'a'; }
  return v.find("chunked")!=std::string::npos; }
static std::string find_content_encoding(const std::string& hdr){ size_t p=hdr.find("Content-Encoding:"); if(p==std::string::npos) return std::string(); size_t e=hdr.find("\r\n", p); std::string v=hdr.substr(p+17, e-(p+17)); while(!v.empty() && (v.back()==' '||v.back()=='\t')) v.pop_back(); size_t i=0; while(i<v.size() && (v[i]==' '||v[i]=='\t')) i++; return v.substr(i); }
static ssize_t read_exact(int fd, char* buf, size_t len){ size_t off=0; while(off<len){ ssize_t n=::read(fd, buf+off, len-off); if(n<=0) return n; off+=n; } return (ssize_t)off; }
static int stream_lines_bytes(const char* data, size_t len, RollingWriter* w, int& total, std::string& lastSeg, bool& anyFlushed){ static const size_t B=2048; std::vector<std::string> lines; lines.reserve(B); size_t i=0; size_t start=0; while(i<len){ if(data[i]=='\n'){ std::string ln(data+start, i-start); if(!ln.empty()){ std::string t=ln; size_t k=0; while(k<t.size() && (t[k]==' '||t[k]=='\t'||t[k]=='\r')) k++; if(k<t.size() && t[k]=='{' && t.find("\"index\"")!=std::string::npos){ } else { lines.push_back(ln); if(lines.size()>=B){ bool flushed=false; std::string seg; int n=w->bulk_append(lines, seg, flushed); total+=n; if(flushed){ anyFlushed=true; if(!seg.empty()) lastSeg=seg; } lines.clear(); } } }
      start=i+1; }
    i++; }
  if(start<len){ std::string ln(data+start, len-start); std::string t=ln; size_t k=0; while(k<t.size() && (t[k]==' '||t[k]=='\t'||t[k]=='\r')) k++; if(!(k<t.size() && t[k]=='{' && t.find("\"index\"")!=std::string::npos)){ lines.push_back(ln); } }
  if(!lines.empty()){ bool flushed=false; std::string seg; int n=w->bulk_append(lines, seg, flushed); total+=n; if(flushed){ anyFlushed=true; if(!seg.empty()) lastSeg=seg; } }
  return 0; }
static int stream_fixed(int fd, size_t cl, RollingWriter* w, int& total, std::string& lastSeg, bool& anyFlushed){ std::string pending; const size_t BUFSZ=1<<20; std::vector<char> buf; buf.resize(BUFSZ); size_t remain=cl; while(remain>0){ size_t toRead = remain>BUFSZ? BUFSZ: remain; ssize_t n=read_exact(fd, buf.data(), toRead); if(n<=0) break; remain -= (size_t)n; if(!pending.empty()){ std::string tmp=pending; tmp.append(buf.data(), buf.data()+n); stream_lines_bytes(tmp.data(), tmp.size(), w, total, lastSeg, anyFlushed); pending.clear(); }
    else { stream_lines_bytes(buf.data(), (size_t)n, w, total, lastSeg, anyFlushed); }
  }
  return 0; }
static int stream_chunked(int fd, RollingWriter* w, int& total, std::string& lastSeg, bool& anyFlushed){ const size_t BUFSZ=1<<20; std::vector<char> buf; buf.resize(BUFSZ); while(true){ std::string line; char c; while(true){ ssize_t n=::read(fd, &c, 1); if(n<=0) return -1; if(c=='\r'){ ssize_t n2=::read(fd, &c, 1); if(n2<=0) return -1; if(c=='\n') break; } else { line.push_back(c); } }
    size_t sz=strtoull(line.c_str(), nullptr, 16); if(sz==0){ char crlf[2]; if(read_exact(fd, crlf, 2)<=0) return -1; break; } size_t off=0; while(off<sz){ size_t toRead = sz-off>BUFSZ? BUFSZ: sz-off; ssize_t n=read_exact(fd, buf.data(), toRead); if(n<=0) return -1; stream_lines_bytes(buf.data(), (size_t)n, w, total, lastSeg, anyFlushed); off += (size_t)n; }
    char crlf2[2]; if(read_exact(fd, crlf2, 2)<=0) return -1; }
  return 0; }
static int stream_zstd_fixed(int fd, size_t cl, RollingWriter* w, int& total, std::string& lastSeg, bool& anyFlushed){ const size_t ZIN=1<<20; const size_t ZOUT=1<<20; std::vector<char> in; in.resize(ZIN); std::vector<char> out; out.resize(ZOUT); ZSTD_DStream* zds = ZSTD_createDStream(); ZSTD_initDStream(zds); ZSTD_inBuffer ibuf; ZSTD_outBuffer obuf; size_t remain=cl; while(remain>0){ size_t toRead = remain>ZIN? ZIN: remain; ssize_t n=read_exact(fd, in.data(), toRead); if(n<=0) break; remain -= (size_t)n; ibuf.src = in.data(); ibuf.size = (size_t)n; ibuf.pos = 0; while(ibuf.pos < ibuf.size){ obuf.dst = out.data(); obuf.size = ZOUT; obuf.pos = 0; size_t rc = ZSTD_decompressStream(zds, &obuf, &ibuf); if(ZSTD_isError(rc)) break; if(obuf.pos>0){ stream_lines_bytes(out.data(), obuf.pos, w, total, lastSeg, anyFlushed); } }
  }
  ZSTD_freeDStream(zds); return 0; }
static int stream_zstd_chunked(int fd, RollingWriter* w, int& total, std::string& lastSeg, bool& anyFlushed){ const size_t ZIN=1<<20; const size_t ZOUT=1<<20; std::vector<char> in; in.resize(ZIN); std::vector<char> out; out.resize(ZOUT); ZSTD_DStream* zds = ZSTD_createDStream(); ZSTD_initDStream(zds); while(true){ std::string line; char c; while(true){ ssize_t n=::read(fd, &c, 1); if(n<=0){ ZSTD_freeDStream(zds); return -1; } if(c=='\r'){ ssize_t n2=::read(fd, &c, 1); if(n2<=0){ ZSTD_freeDStream(zds); return -1; } if(c=='\n') break; } else { line.push_back(c); } }
    size_t sz=strtoull(line.c_str(), nullptr, 16); if(sz==0){ char crlf[2]; if(read_exact(fd, crlf, 2)<=0){ ZSTD_freeDStream(zds); return -1; } break; } size_t off=0; while(off<sz){ size_t toRead = sz-off>ZIN? ZIN: sz-off; ssize_t n=read_exact(fd, in.data(), toRead); if(n<=0){ ZSTD_freeDStream(zds); return -1; } ZSTD_inBuffer ibuf; ZSTD_outBuffer obuf; ibuf.src = in.data(); ibuf.size = (size_t)n; ibuf.pos = 0; while(ibuf.pos < ibuf.size){ obuf.dst = out.data(); obuf.size = ZOUT; obuf.pos = 0; size_t rc = ZSTD_decompressStream(zds, &obuf, &ibuf); if(ZSTD_isError(rc)) break; if(obuf.pos>0){ stream_lines_bytes(out.data(), obuf.pos, w, total, lastSeg, anyFlushed); } }
      off += (size_t)n; }
    char crlf2[2]; if(read_exact(fd, crlf2, 2)<=0){ ZSTD_freeDStream(zds); return -1; } }
  ZSTD_freeDStream(zds); return 0; }
static void parse_request_line(const std::string& req, std::string& method, std::string& path){ size_t sp=req.find(' '); size_t sp2=req.find(' ', sp+1); method=req.substr(0, sp); path=req.substr(sp+1, sp2-sp-1); }
static std::string url_decode(const std::string& s){ std::string o; o.reserve(s.size()); for(size_t i=0;i<s.size();){ char c=s[i]; if(c=='+'){ o.push_back(' '); i++; } else if(c=='%' && i+2<s.size()){ auto hex = [](char x)->int{ if(x>='0'&&x<='9') return x-'0'; if(x>='a'&&x<='f') return 10+(x-'a'); if(x>='A'&&x<='F') return 10+(x-'A'); return -1; }; int h1=hex(s[i+1]); int h2=hex(s[i+2]); if(h1>=0 && h2>=0){ o.push_back((char)((h1<<4)|h2)); i+=3; } else { o.push_back(c); i++; } } else { o.push_back(c); i++; } } return o; }
static std::map<std::string,std::string> parse_kv(const std::string& body){ std::map<std::string,std::string> m; size_t i=0; while(i<body.size()){ size_t eq=body.find('=', i); if(eq==std::string::npos) break; size_t amp=body.find('&', eq+1); std::string k=body.substr(i, eq-i); std::string v=body.substr(eq+1, amp==std::string::npos? std::string::npos : amp-eq-1); k=url_decode(k); v=url_decode(v); m[k]=v; if(amp==std::string::npos) break; i=amp+1; } return m; }
static std::vector<std::string> tokenize_query(const std::string& s){ std::vector<std::string> toks; size_t i=0; auto emit=[&](size_t a,size_t b){ if(b>a) toks.emplace_back(s.substr(a,b-a)); }; while(i<s.size()){ while(i<s.size() && (s[i]==' '||s[i]=='\t')) i++; if(i>=s.size()) break; size_t j=i; while(j<s.size() && s[j]!=' ' && s[j]!='\t' && s[j]!=':' && s[j]!='=' && s[j]!=',' && s[j]!='(' && s[j]!=')') j++; emit(i,j); if(j<s.size()){ char c=s[j]; if(c==':'||c=='='||c==','||c=='('||c==')'){ std::string d; d.push_back(c); toks.emplace_back(d); } j++; } i=j; } return toks; }
static void respond_json(int cfd, int code, const std::string& body){ char hdr[256]; snprintf(hdr, sizeof(hdr), "HTTP/1.1 %d OK\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n", code, body.size()); write_all(cfd, hdr, strlen(hdr)); write_all(cfd, body.c_str(), body.size()); }
static void respond_busy(int cfd){ const char* body = "{\"error\":\"server busy\"}"; char hdr[256]; snprintf(hdr, sizeof(hdr), "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n", strlen(body)); write_all(cfd, hdr, strlen(hdr)); write_all(cfd, body, strlen(body)); }

static std::string pretty_json(const std::string& s){
  std::string out; out.reserve(s.size()*2);
  int indent=0; bool inStr=false; bool esc=false;
  for(size_t i=0;i<s.size();i++){
    char c=s[i];
    if(esc){ out.push_back(c); esc=false; continue; }
    if(c=='\\'){ out.push_back(c); if(inStr) esc=true; continue; }
    if(c=='"'){ out.push_back(c); inStr=!inStr; continue; }
    if(inStr){ out.push_back(c); continue; }
    if(c=='{' || c=='['){ out.push_back(c); out.push_back('\n'); indent++; for(int k=0;k<indent;k++){ out.append("  "); } continue; }
    if(c=='}' || c==']'){ out.push_back('\n'); if(indent>0) indent--; for(int k=0;k<indent;k++){ out.append("  "); } out.push_back(c); continue; }
    if(c==','){ out.push_back(c); out.push_back('\n'); for(int k=0;k<indent;k++){ out.append("  "); } continue; }
    if(c==':'){ out.push_back(c); out.push_back(' '); continue; }
    if(c=='\r' || c=='\n' || c=='\t'){ continue; }
    out.push_back(c);
  }
  return out;
}

static void recover_wal_dir(const std::string& dir){
  auto crc32_calc = [](const char* data, size_t len){ uint32_t crc = 0xFFFFFFFFu; for(size_t i=0;i<len;i++){ uint8_t byte = (uint8_t)data[i]; crc ^= byte; for(int j=0;j<8;j++){ uint32_t mask = -(int)(crc & 1); crc = (crc >> 1) ^ (0xEDB88320u & mask); } } return ~crc; };
  int retries = 3; const char* rv=getenv("LOGGREP_RECOVER_RETRIES"); if(rv){ int x=atoi(rv); if(x>0) retries=x; }
  int throttle_ms = 50; const char* tv=getenv("LOGGREP_RECOVER_THROTTLE_MS"); if(tv){ int x=atoi(tv); if(x>=0) throttle_ms=x; }
  DIR* d = opendir(dir.c_str());
  if(!d) return;
  struct dirent* ent;
  while((ent = readdir(d))){
    std::string n = ent->d_name;
    if(n.size()>5 && n.find("wal_")==0 && n.find(".log")==n.size()-4){
      std::string p = dir + std::string("/") + n;
      FILE* f = fopen(p.c_str(), "r");
      if(!f) continue;
      std::string buf;
      char tmp[8192];
      size_t r;
      while((r=fread(tmp,1,sizeof(tmp),f))>0){ buf.append(tmp,tmp+r); }
      fclose(f);
      long long ts = (long long)time(NULL)*1000LL;
      std::string out = dir + std::string("/ing_recover_") + std::to_string(ts) + std::string(".log.zip");
      int rc = -1; for(int i=0;i<retries;i++){ rc = compress_from_memory(buf.c_str(), (int)buf.size(), out.c_str()); if(rc==0) break; usleep(1000*throttle_ms); }
      if(rc==0){
        std::string mpath = out + std::string(".meta");
        FILE* mf = fopen(mpath.c_str(), "w");
        if(mf){
          uint32_t crc = crc32_calc(buf.c_str(), buf.size());
          int recs = 0; for(size_t i=0;i<buf.size();i++){ if(buf[i]=='\n') recs++; }
          std::string meta;
          meta.append("{");
          meta.append("\"wal\":\""); meta.append(p); meta.append("\",");
          meta.append("\"bytes\":"); meta.append(std::to_string(buf.size())); meta.append(",");
          meta.append("\"records\":"); meta.append(std::to_string(recs)); meta.append(",");
          meta.append("\"start_ms\":"); meta.append(std::to_string(ts)); meta.append(",");
          meta.append("\"end_ms\":"); meta.append(std::to_string(ts)); meta.append(",");
          meta.append("\"crc32\":"); meta.append(std::to_string(crc));
          meta.append("}");
          fwrite(meta.c_str(), 1, meta.size(), mf);
          fclose(mf);
        }
        unlink(p.c_str());
      }
      usleep(1000*throttle_ms);
    }
  }
  closedir(d);
}

static void handle_client(int cfd){ std::string hdrs=read_headers(cfd); if(hdrs.empty()){ ::close(cfd); return; } std::string method,path; parse_request_line(hdrs, method, path); size_t qpos=path.find('?'); std::string rpath = qpos==std::string::npos? path : path.substr(0,qpos); std::string qs = qpos==std::string::npos? std::string() : path.substr(qpos+1); if(rpath.size()>1 && rpath.back()=='/') rpath.pop_back(); size_t hdrEnd=hdrs.find("\r\n\r\n"); size_t contentLen = find_content_length(hdrs); std::string body;
    if((method=="GET" || method=="HEAD") && rpath=="/health"){ respond_json(cfd, 200, std::string("{\"status\":\"ok\"}")); }
    else if((method=="POST" || method=="GET") && rpath=="/query"){ if(contentLen>0){ body.reserve(contentLen); const size_t BUFSZ=1<<16; std::vector<char> tmp; tmp.resize(BUFSZ); size_t remain=contentLen; while(remain>0){ size_t toRead = remain>BUFSZ? BUFSZ: remain; ssize_t n=read_exact(cfd, tmp.data(), toRead); if(n<=0) break; body.append(tmp.data(), tmp.data()+n); remain -= (size_t)n; } }
      auto kv=parse_kv(body); auto qkv=parse_kv(qs); for(auto &it: qkv){ if(!kv.count(it.first)) kv[it.first]=it.second; } std::string index = kv.count("index")? kv["index"]: std::string(); std::string q= kv.count("q")? kv["q"]: std::string(); int limit= kv.count("limit")? atoi(kv["limit"].c_str()) : 100; bool want_pretty = kv.count("pretty") ? (!kv["pretty"].empty()) : false; if(index.empty()||q.empty()){ respond_json(cfd, 400, std::string("{\"error\":\"missing index or q\"}")); }
      else{
        auto it = g_index_map.find(index); if(it==g_index_map.end()){ respond_json(cfd, 400, std::string("{\"error\":\"unknown index\"}")); }
        else { std::string dir = it->second; LogDispatcher disp; int c=disp.Connect((char*)dir.c_str()); if(c<=0){ respond_json(cfd, 500, std::string("{\"error\":\"connect failed\"}")); }
        else{
          std::string baseq=q; bool handled=false; size_t barpos=baseq.find('|'); std::string right; std::string left;
          if(barpos!=std::string::npos){ right=baseq.substr(barpos+1); left=baseq.substr(0,barpos); SPLCommand cmd; if(parse_spl(right, cmd)){
              if(cmd.type==SPL_TIMECHART){
                auto lower = [](std::string s){ for(size_t i=0;i<s.size();i++){ if(s[i]>='A'&&s[i]<='Z') s[i]=s[i]-'A'+'a'; } return s; };
                std::string opt = lower(right.substr(std::string("timechart").size()));
                long long span_ms = 0; int bins = 0; long long start_ms = 0; long long end_ms = 0; std::string groupAlias;
                size_t p=opt.find("span="); if(p!=std::string::npos){ size_t e=opt.find_first_of(" &", p+5); std::string v=opt.substr(p+5, e==std::string::npos? std::string::npos : e-(p+5)); if(!v.empty()){ char* endp=nullptr; long long num = strtoll(v.c_str(), &endp, 10); long long mult=1; if(endp){ if(*endp=='m') mult=60LL*1000LL; else if(*endp=='s') mult=1000LL; else if(*endp=='h') mult=3600LL*1000LL; else if(*endp=='d') mult=24LL*3600LL*1000LL; else if(*endp=='\0') mult=1; } span_ms = num * mult; } }
                p=opt.find("bins="); if(p!=std::string::npos){ size_t e=opt.find_first_of(" &", p+5); std::string v=opt.substr(p+5, e==std::string::npos? std::string::npos : e-(p+5)); if(!v.empty()){ bins = atoi(v.c_str()); if(bins<0) bins=0; } }
                size_t pb = opt.find(" by "); if(pb!=std::string::npos){ size_t e=opt.find_first_of(" &", pb+4); std::string v=opt.substr(pb+4, e==std::string::npos? std::string::npos : e-(pb+4)); if(!v.empty()) groupAlias=v; }
                if(groupAlias.empty()){ pb = opt.find("by("); if(pb!=std::string::npos){ size_t r2 = opt.find(')', pb+3); if(r2!=std::string::npos){ std::string v=opt.substr(pb+3, r2-(pb+3)); if(!v.empty()) groupAlias=v; } } }
                auto parse_time = [&](const std::string& v)->long long{ if(v.empty()) return 0; bool allDigit=true; for(size_t i=0;i<v.size();i++){ if(v[i]<'0'||v[i]>'9'){ allDigit=false; break; } } if(allDigit){ return strtoll(v.c_str(), nullptr, 10); } long long ms=0; if(parse_timestamp_ms(v.c_str(), (int)v.size(), ms)) return ms; return 0; };
                p=opt.find("start="); if(p!=std::string::npos){ size_t e=opt.find_first_of(" &", p+6); std::string v=opt.substr(p+6, e==std::string::npos? std::string::npos : e-(p+6)); start_ms = parse_time(v); }
                p=opt.find("end="); if(p!=std::string::npos){ size_t e=opt.find_first_of(" &", p+4); std::string v=opt.substr(p+4, e==std::string::npos? std::string::npos : e-(p+4)); end_ms = parse_time(v); }
                std::vector<std::string> mem = tokenize_query(left);
                char* args[MAX_CMD_ARG_COUNT]; int ac = (int)mem.size(); if(ac<=0){ mem.push_back(std::string()); ac=1; }
                for(int i=0;i<ac;i++){ args[i]=(char*)mem[i].c_str(); }
                std::string jout;
                if(span_ms>0){ if(groupAlias.empty()){ disp.Timechart_Count_BySpan_JSON(args, ac, span_ms, jout); } else { disp.Timechart_BySpan_Group_JSON(args, ac, span_ms, groupAlias, jout); } respond_json(cfd, 200, want_pretty? pretty_json(jout) : jout); handled=true; }
                else {
                  if(bins<=0) bins=60;
                  if(start_ms==0 || end_ms==0 || end_ms<=start_ms){ long long tmin=0,tmax=0; int ok=disp.GetMatchedTimeRange(args, ac, tmin, tmax); if(ok>0){ start_ms=tmin; end_ms=tmax; }
                  }
                  if(groupAlias.empty()){ disp.Timechart_Count_ByBins_JSON(args, ac, start_ms, end_ms, bins, jout); } else { disp.Timechart_ByBins_Group_JSON(args, ac, start_ms, end_ms, bins, groupAlias, jout); } respond_json(cfd, 200, want_pretty? pretty_json(jout) : jout); handled=true;
                }
              }
              else if(cmd.type==SPL_COUNT){ std::vector<std::string> mem = tokenize_query(left); char* args[MAX_CMD_ARG_COUNT]; int ac=(int)mem.size(); if(ac<=0){ mem.push_back(std::string()); ac=1; } for(int i=0;i<ac;i++){ args[i]=(char*)mem[i].c_str(); } int total = disp.CountByWildcard(args, ac); std::string out; out.append("{"); out.append("\"count\":"); out.append(std::to_string(total)); out.append("}"); respond_json(cfd, 200, out); handled=true; }
              else if(cmd.type==SPL_COUNT_BY){ std::vector<std::string> mem = tokenize_query(left); char* args[MAX_CMD_ARG_COUNT]; int ac=(int)mem.size(); if(ac<=0){ mem.push_back(std::string()); ac=1; } for(int i=0;i<ac;i++){ args[i]=(char*)mem[i].c_str(); } std::string jout; disp.Aggregate_Group_JSON(args, ac, cmd.field, 10, std::string(), jout); respond_json(cfd, 200, jout); handled=true; }
              else if(cmd.type==SPL_SUM || cmd.type==SPL_AVG || cmd.type==SPL_MIN || cmd.type==SPL_MAX){ int op=0; std::string func; if(cmd.type==SPL_SUM){ op=0; func="sum"; } else if(cmd.type==SPL_AVG){ op=1; func="avg"; } else if(cmd.type==SPL_MIN){ op=2; func="min"; } else { op=3; func="max"; } std::vector<std::string> mem = tokenize_query(left); char* args[MAX_CMD_ARG_COUNT]; int ac=(int)mem.size(); if(ac<=0){ mem.push_back(std::string()); ac=1; } for(int i=0;i<ac;i++){ args[i]=(char*)mem[i].c_str(); } double val=0.0; disp.Aggregate_Scalar(args, ac, op, cmd.field, val); std::string out; out.append("{"); out.append("\"op\":\""); out.append(func); out.append("\",\"field\":\""); out.append(cmd.field); out.append("\",\"value\":"); out.append(std::to_string(val)); out.append("}"); respond_json(cfd, 200, out); handled=true; }
              else if(cmd.type==SPL_TOP){ std::vector<std::string> mem = tokenize_query(left); char* args[MAX_CMD_ARG_COUNT]; int ac=(int)mem.size(); if(ac<=0){ mem.push_back(std::string()); ac=1; } for(int i=0;i<ac;i++){ args[i]=(char*)mem[i].c_str(); } std::string jout; disp.Aggregate_TopK_JSON(args, ac, cmd.field, cmd.k, jout); respond_json(cfd, 200, jout); handled=true; }
              else if(cmd.type==SPL_DISTINCT){ std::vector<std::string> mem = tokenize_query(left); char* args[MAX_CMD_ARG_COUNT]; int ac=(int)mem.size(); if(ac<=0){ mem.push_back(std::string()); ac=1; } for(int i=0;i<ac;i++){ args[i]=(char*)mem[i].c_str(); } int dv=0; disp.Aggregate_Distinct(args, ac, cmd.field, dv); std::string out; out.append("{"); out.append("\"op\":\"distinct\",\"field\":\""); out.append(cmd.field); out.append("\",\"value\":"); out.append(std::to_string(dv)); out.append("}"); respond_json(cfd, 200, out); handled=true; }
              else if(cmd.type==SPL_GROUP_BY){ std::vector<std::string> mem = tokenize_query(left); char* args[MAX_CMD_ARG_COUNT]; int ac=(int)mem.size(); if(ac<=0){ mem.push_back(std::string()); ac=1; } for(int i=0;i<ac;i++){ args[i]=(char*)mem[i].c_str(); } std::string jout; disp.Aggregate_Group_JSON(args, ac, cmd.group, cmd.op, cmd.valueAlias, jout); respond_json(cfd, 200, jout); handled=true; }
              }
          }
          if(!handled){ std::vector<std::string> mem = tokenize_query(baseq); auto has_logic = [&](const std::vector<std::string>& v){ for(size_t i=0;i<v.size();i++){ const std::string& t=v[i]; if(t=="and"||t=="OR"||t=="or"||t=="not") return true; } return false; }; std::vector<std::string> mem2; if(mem.size()>=2 && !has_logic(mem)){ mem2.reserve(mem.size()*2-1); for(size_t i=0;i<mem.size();i++){ if(i>0) mem2.emplace_back(std::string("and")); mem2.emplace_back(mem[i]); } } else { mem2.swap(mem); } char* args[MAX_CMD_ARG_COUNT]; int ac=(int)mem2.size(); if(ac<=0){ mem2.push_back(std::string()); ac=1; } for(int i=0;i<ac;i++){ args[i]=(char*)mem2[i].c_str(); } std::string json; disp.SearchByWildcard_JSON(args, ac, limit, json); if(want_pretty){ std::string pj = pretty_json(json); respond_json(cfd, 200, pj); } else { respond_json(cfd, 200, json); } }
          disp.DisConnect(); }
        }
      }
    }
    else if(method=="POST" && rpath=="/_bulk"){ auto kv=parse_kv(std::string()); auto qkv=parse_kv(qs); for(auto &it: qkv){ if(!kv.count(it.first)) kv[it.first]=it.second; } const char* lms = getenv("LOGGREP_TEST_LATENCY_MS"); if(lms){ int ms = atoi(lms); if(ms>0){ usleep(ms*1000); } } std::string index = kv.count("index")? kv["index"]: std::string(); if(index.empty()){ respond_json(cfd, 400, std::string("{\"error\":\"missing index\"}")); }
      else { RollingWriter* w=get_writer(index); if(!w){ respond_json(cfd, 400, std::string("{\"error\":\"unknown index\"}")); }
        else { int total=0; std::string lastSeg; bool anyFlushed=false; std::string enc = find_content_encoding(hdrs); for(size_t i=0;i<enc.size();i++){ char c=enc[i]; if(c>='A'&&c<='Z') enc[i]=c-'A'+'a'; }
          bool chunked = has_chunked(hdrs); if(enc=="zstd"){ if(chunked){ stream_zstd_chunked(cfd, w, total, lastSeg, anyFlushed); } else { size_t cl = find_content_length(hdrs); stream_zstd_fixed(cfd, cl, w, total, lastSeg, anyFlushed); } }
          else { if(chunked){ stream_chunked(cfd, w, total, lastSeg, anyFlushed); } else { size_t cl = find_content_length(hdrs); stream_fixed(cfd, cl, w, total, lastSeg, anyFlushed); } }
          bool synced=false; if(kv.count("sync") && !kv["sync"].empty()){ w->sync_wal(); synced=true; }
          std::string out; out.reserve(160); out.append("{"); out.append("\"ingested\":"); out.append(std::to_string(total)); out.append(",\"flushed\":"); out.append(anyFlushed?"true":"false"); if(anyFlushed){ out.append(",\"segment\":\""); out.append(lastSeg); out.append("\""); } out.append(",\"synced\":"); out.append(synced?"true":"false"); out.append("}"); respond_json(cfd, 200, out);
        }
      }
    }
    else if((method=="GET" || method=="HEAD") && rpath=="/indices"){ std::string out; out.append("{"); out.append("\"indices\":{"); bool first=true; for(auto &it: g_index_map){ if(!first) out.append(","); first=false; out.append("\""); out.append(it.first); out.append("\":\""); out.append(it.second); out.append("\""); } out.append("},\"pending\":"); out.append(std::to_string(g_pending_count.load())); out.append("}"); respond_json(cfd, 200, out); }
    else if(method=="POST" && rpath=="/indices"){ if(contentLen>0){ body.reserve(contentLen); const size_t BUFSZ=1<<16; std::vector<char> tmp; tmp.resize(BUFSZ); size_t remain=contentLen; while(remain>0){ size_t toRead = remain>BUFSZ? BUFSZ: remain; ssize_t n=read_exact(cfd, tmp.data(), toRead); if(n<=0) break; body.append(tmp.data(), tmp.data()+n); remain -= (size_t)n; } }
      auto kv=parse_kv(body); auto qkv=parse_kv(qs); for(auto &it: qkv){ if(!kv.count(it.first)) kv[it.first]=it.second; } std::string index = kv.count("index")? kv["index"]: std::string(); std::string pathv = kv.count("path")? kv["path"]: std::string(); std::string del = kv.count("delete")? kv["delete"]: std::string(); if(index.empty()){ respond_json(cfd, 400, std::string("{\"error\":\"missing index\"}")); }
      else { if(!del.empty()){ auto it=g_index_map.find(index); if(it!=g_index_map.end()){ g_index_map.erase(it); save_indices(); } respond_json(cfd, 200, std::string("{\"ok\":true}")); }
        else if(pathv.empty()){ respond_json(cfd, 400, std::string("{\"error\":\"missing path\"}")); }
        else { g_index_map[index]=pathv; save_indices(); respond_json(cfd, 200, std::string("{\"ok\":true}")); } }
    }
    else{ respond_json(cfd, 404, std::string("{\"error\":\"not found\"}")); }
    ::close(cfd);
}

int main(int argc, char** argv){ int port=8080; if(argc>1){ port=atoi(argv[1]); if(port<=0) port=8080; } int sfd=::socket(AF_INET, SOCK_STREAM, 0); int opt=1; setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)); sockaddr_in addr; memset(&addr,0,sizeof(addr)); addr.sin_family=AF_INET; addr.sin_addr.s_addr=htonl(INADDR_ANY); addr.sin_port=htons(port); if(::bind(sfd, (sockaddr*)&addr, sizeof(addr))<0){ perror("bind"); return 1; } if(::listen(sfd, 1024)<0){ perror("listen"); return 1; } printf("listening at http://localhost:%d\n", port);
  load_indices();
  const char* recover_flag = getenv("LOGGREP_RECOVER_AT_START");
  if(recover_flag && atoi(recover_flag) > 0){ for(auto &it: g_index_map){ recover_wal_dir(it.second); } }
  std::queue<int> q; std::mutex q_mtx; std::condition_variable q_cv; std::atomic<bool> stop(false);
  int max_pending = 4096; const char* pv = getenv("LOGGREP_MAX_PENDING"); if(pv){ int x=atoi(pv); if(x>0) max_pending=x; }
  unsigned int nWorkers = std::thread::hardware_concurrency(); if(nWorkers==0) nWorkers = 8; const char* wv=getenv("LOGGREP_WORKERS"); if(wv){ int x=atoi(wv); if(x>0) nWorkers=(unsigned int)x; }
  std::vector<std::thread> workers; workers.reserve(nWorkers);
  for(unsigned int i=0;i<nWorkers;i++){ workers.emplace_back([&](){ while(!stop.load()){ int cfd=-1; { std::unique_lock<std::mutex> lk(q_mtx); q_cv.wait(lk, [&](){ return stop.load() || !q.empty(); }); if(stop.load() && q.empty()) return; cfd = q.front(); q.pop(); g_pending_count.fetch_sub(1); } handle_client(cfd); } }); }
  while(true){ sockaddr_in caddr; socklen_t clen=sizeof(caddr); int cfd=::accept(sfd, (sockaddr*)&caddr, &clen); if(cfd<0) continue; bool dropped=false; { std::lock_guard<std::mutex> lk(q_mtx); if((int)q.size() >= max_pending){ dropped=true; } else { q.push(cfd); g_pending_count.fetch_add(1); } }
    if(dropped){ respond_busy(cfd); ::close(cfd); } else { q_cv.notify_one(); }
  }
  stop.store(true); q_cv.notify_all(); for(auto &t: workers){ if(t.joinable()) t.join(); }
  return 0;
}
