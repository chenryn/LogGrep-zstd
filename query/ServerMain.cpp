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
#include <map>
#include <fstream>

static std::map<std::string,std::string> g_index_map;
static std::string g_index_cfg = std::string("indices.conf");
static std::map<std::string, RollingWriter*> g_writers;
static void load_indices(){ std::ifstream in(g_index_cfg.c_str()); if(in.good()){ g_index_map.clear(); std::string line; while(std::getline(in, line)){ size_t eq=line.find('='); if(eq==std::string::npos) continue; std::string k=line.substr(0,eq); std::string v=line.substr(eq+1); if(!k.empty() && !v.empty()){ g_index_map[k]=v; } } in.close(); }
  if(g_index_map.empty()){ g_index_map["main"]=std::string("../lib_output_zip/Ssh"); g_index_map["ssh"]=std::string("../lib_output_zip/Ssh"); std::ofstream out(g_index_cfg.c_str(), std::ios::out|std::ios::trunc); if(out.good()){ for(auto &it: g_index_map){ out<<it.first<<"="<<it.second<<"\n"; } out.close(); } }
}
static void save_indices(){ std::ofstream out(g_index_cfg.c_str(), std::ios::out|std::ios::trunc); if(out.good()){ for(auto &it: g_index_map){ out<<it.first<<"="<<it.second<<"\n"; } out.close(); } }
static RollingWriter* get_writer(const std::string& index){ auto it=g_index_map.find(index); if(it==g_index_map.end()) return nullptr; auto wit=g_writers.find(index); if(wit!=g_writers.end()) return wit->second; RollingWriter* w=new RollingWriter(it->second); g_writers[index]=w; return w; }

static void write_all(int fd, const char* buf, size_t len){ size_t off=0; while(off<len){ ssize_t n=::write(fd, buf+off, len-off); if(n<=0) return; off+=n; } }
static std::string read_all(int fd){ std::string s; char buf[4096]; ssize_t n; while((n=::read(fd, buf, sizeof(buf)))>0){ s.append(buf, buf+n); if(s.find("\r\n\r\n")!=std::string::npos) break; } size_t pos=s.find("\r\n\r\n"); if(pos!=std::string::npos){ size_t hdrEnd=pos+4; size_t cl=0; size_t hpos=s.find("Content-Length:"); if(hpos!=std::string::npos){ size_t eol=s.find("\r\n", hpos); std::string v=s.substr(hpos+15, eol-(hpos+15)); cl=strtoul(v.c_str(), nullptr, 10); } while(s.size()-hdrEnd<cl){ n=::read(fd, buf, sizeof(buf)); if(n<=0) break; s.append(buf, buf+n); } } return s; }
static void parse_request_line(const std::string& req, std::string& method, std::string& path){ size_t sp=req.find(' '); size_t sp2=req.find(' ', sp+1); method=req.substr(0, sp); path=req.substr(sp+1, sp2-sp-1); }
static std::map<std::string,std::string> parse_kv(const std::string& body){ std::map<std::string,std::string> m; size_t i=0; while(i<body.size()){ size_t eq=body.find('=', i); if(eq==std::string::npos) break; size_t amp=body.find('&', eq+1); std::string k=body.substr(i, eq-i); std::string v=body.substr(eq+1, amp==std::string::npos? std::string::npos : amp-eq-1); for(size_t j=0;j<v.size();j++){ if(v[j]=='+') v[j]=' '; } m[k]=v; if(amp==std::string::npos) break; i=amp+1; } return m; }
static void respond_json(int cfd, int code, const std::string& body){ char hdr[256]; snprintf(hdr, sizeof(hdr), "HTTP/1.1 %d OK\r\nContent-Type: application/json\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n", code, body.size()); write_all(cfd, hdr, strlen(hdr)); write_all(cfd, body.c_str(), body.size()); }

int main(int argc, char** argv){ int port=8080; if(argc>1){ port=atoi(argv[1]); if(port<=0) port=8080; } int sfd=::socket(AF_INET, SOCK_STREAM, 0); int opt=1; setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)); sockaddr_in addr; memset(&addr,0,sizeof(addr)); addr.sin_family=AF_INET; addr.sin_addr.s_addr=htonl(INADDR_ANY); addr.sin_port=htons(port); if(::bind(sfd, (sockaddr*)&addr, sizeof(addr))<0){ perror("bind"); return 1; } if(::listen(sfd, 16)<0){ perror("listen"); return 1; } printf("listening at http://localhost:%d\n", port);
  load_indices();
  while(true){ sockaddr_in caddr; socklen_t clen=sizeof(caddr); int cfd=::accept(sfd, (sockaddr*)&caddr, &clen); if(cfd<0) continue; std::string req=read_all(cfd); if(req.empty()){ ::close(cfd); continue; } std::string method,path; parse_request_line(req, method, path); size_t qpos=path.find('?'); std::string rpath = qpos==std::string::npos? path : path.substr(0,qpos); std::string qs = qpos==std::string::npos? std::string() : path.substr(qpos+1); if(rpath.size()>1 && rpath.back()=='/') rpath.pop_back(); size_t hdrEnd=req.find("\r\n\r\n"); std::string body= hdrEnd==std::string::npos? std::string() : req.substr(hdrEnd+4);
    if((method=="GET" || method=="HEAD") && rpath=="/health"){ respond_json(cfd, 200, std::string("{\"status\":\"ok\"}")); }
    else if((method=="POST" || method=="GET") && rpath=="/query"){ auto kv=parse_kv(body); auto qkv=parse_kv(qs); for(auto &it: qkv){ if(!kv.count(it.first)) kv[it.first]=it.second; } std::string index = kv.count("index")? kv["index"]: std::string(); std::string q= kv.count("q")? kv["q"]: std::string(); int limit= kv.count("limit")? atoi(kv["limit"].c_str()) : 100; if(index.empty()||q.empty()){ respond_json(cfd, 400, std::string("{\"error\":\"missing index or q\"}")); }
      else{
        auto it = g_index_map.find(index); if(it==g_index_map.end()){ respond_json(cfd, 400, std::string("{\"error\":\"unknown index\"}")); }
        else { std::string dir = it->second; LogDispatcher disp; int c=disp.Connect((char*)dir.c_str()); if(c<=0){ respond_json(cfd, 500, std::string("{\"error\":\"connect failed\"}")); }
        else{
          char* args[MAX_CMD_ARG_COUNT]; std::vector<std::string> mem; mem.push_back(q); args[0]=(char*)mem[0].c_str(); std::string json; disp.SearchByWildcard_JSON(args, 1, limit, json); respond_json(cfd, 200, json); disp.DisConnect(); }
        }
      }
    }
    else if(method=="POST" && rpath=="/_bulk"){ auto kv=parse_kv(body); auto qkv=parse_kv(qs); for(auto &it: qkv){ if(!kv.count(it.first)) kv[it.first]=it.second; } std::string index = kv.count("index")? kv["index"]: std::string(); if(index.empty()){ respond_json(cfd, 400, std::string("{\"error\":\"missing index\"}")); }
      else { RollingWriter* w=get_writer(index); if(!w){ respond_json(cfd, 400, std::string("{\"error\":\"unknown index\"}")); }
        else {
          std::vector<std::string> lines; lines.reserve(1024);
          size_t pos2=0; while(pos2<body.size()){ size_t nl=body.find('\n', pos2); std::string line = body.substr(pos2, nl==std::string::npos? std::string::npos : nl-pos2); pos2 = nl==std::string::npos? body.size() : nl+1; if(line.empty()) continue; std::string t=line; size_t i=0; while(i<t.size() && (t[i]==' '||t[i]=='\t'||t[i]=='\r')) i++; if(i<t.size() && t[i]=='{' && t.find("\"index\"")!=std::string::npos) { continue; } lines.push_back(line); }
          std::string seg; bool flushed=false; int n=w->bulk_append(lines, seg, flushed); std::string out; out.reserve(128); out.append("{"); out.append("\"ingested\":"); out.append(std::to_string(n)); out.append(",\"flushed\":"); out.append(flushed?"true":"false"); if(flushed){ out.append(",\"segment\":\""); out.append(seg); out.append("\""); } out.append("}"); respond_json(cfd, 200, out);
        }
      }
    }
    else if((method=="GET" || method=="HEAD") && rpath=="/indices"){ std::string out; out.append("{"); out.append("\"indices\":{"); bool first=true; for(auto &it: g_index_map){ if(!first) out.append(","); first=false; out.append("\""); out.append(it.first); out.append("\":\""); out.append(it.second); out.append("\""); } out.append("}}"); respond_json(cfd, 200, out); }
    else if(method=="POST" && rpath=="/indices"){ auto kv=parse_kv(body); auto qkv=parse_kv(qs); for(auto &it: qkv){ if(!kv.count(it.first)) kv[it.first]=it.second; } std::string index = kv.count("index")? kv["index"]: std::string(); std::string pathv = kv.count("path")? kv["path"]: std::string(); std::string del = kv.count("delete")? kv["delete"]: std::string(); if(index.empty()){ respond_json(cfd, 400, std::string("{\"error\":\"missing index\"}")); }
      else { if(!del.empty()){ auto it=g_index_map.find(index); if(it!=g_index_map.end()){ g_index_map.erase(it); save_indices(); } respond_json(cfd, 200, std::string("{\"ok\":true}")); }
        else if(pathv.empty()){ respond_json(cfd, 400, std::string("{\"error\":\"missing path\"}")); }
        else { g_index_map[index]=pathv; save_indices(); respond_json(cfd, 200, std::string("{\"ok\":true}")); } }
    }
    else{ respond_json(cfd, 404, std::string("{\"error\":\"not found\"}")); }
    ::close(cfd);
  }
  return 0;
}
