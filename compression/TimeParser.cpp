#include "TimeParser.h"
#include <cstring>
#include <cstdio>
#include <ctime>

static inline long long to_epoch_ms(struct tm& tmv, int millis, bool has_tz, int tz_offset_minutes)
{
    time_t sec;
#ifdef __APPLE__
    // On macOS, use timegm for UTC; if no TZ info, assume local then convert to UTC.
    char* oldtz = getenv("TZ");
    if(has_tz) {
        sec = timegm(&tmv);
        sec -= tz_offset_minutes * 60; // %z gives offset from UTC; adjust
    } else {
        // treat as local time
        sec = mktime(&tmv);
    }
#else
    if(has_tz) {
        sec = timegm(&tmv);
        sec -= tz_offset_minutes * 60;
    } else {
        sec = mktime(&tmv);
    }
#endif
    if(sec == (time_t)-1){
        // Fallback: try local time if GMT failed
        sec = mktime(&tmv);
        if(sec == (time_t)-1) return -1; // signal failure
        if(has_tz) sec -= tz_offset_minutes * 60;
    }
    return (long long)sec * 1000LL + (long long)millis;
}

// minimal strptime wrapper: returns true if parsed, and sets tz offset minutes if present in %z
static bool strptime_parse(const char* s, const char* fmt, struct tm& tmv, int& millis, bool& has_tz, int& tz_offset_minutes)
{
    millis = 0; has_tz = false; tz_offset_minutes = 0;
    memset(&tmv, 0, sizeof(tmv));
    const char* end = strptime(s, fmt, &tmv);
    if(!end) return false;
    // fractional milliseconds: detect ",SSS" or ".SSS" immediately after seconds
    if(*end == ',' || *end == '.') {
        int m = 0, count = 0; end++;
        while(*end >= '0' && *end <= '9' && count < 3) { m = m*10 + (*end - '0'); end++; count++; }
        while(count++ < 3) m *= 10; // normalize to ms
        millis = m;
    }
    // timezone numeric like +HHMM or -HHMM only when near end
    has_tz = false;
    int L = (int)strlen(s);
    for(int k=L-1; k>=0; --k){
        if(s[k]=='+' || s[k]=='-'){
            if(k+4 < L && isdigit(s[k+1]) && isdigit(s[k+2]) && isdigit(s[k+3]) && isdigit(s[k+4])){
                int sign = (s[k] == '-') ? -1 : 1;
                int hh = (s[k+1]-'0')*10 + (s[k+2]-'0');
                int mm = (s[k+3]-'0')*10 + (s[k+4]-'0');
                tz_offset_minutes = sign * (hh*60 + mm);
                has_tz = true;
            }
            break;
        }
    }
    return true;
}

bool parse_timestamp_ms(const char* s, int len, long long& out_ms)
{
    // Copy into buffer for strptime
    char buf[1024];
    int n = len > 1023 ? 1023 : len;
    memcpy(buf, s, n); buf[n] = '\0';
    struct tm tmv; int millis=0; bool has_tz=false; int tz_offset=0;
    // Try formats (order roughly common-first)
    const char* fmts[] = {
        "%Y-%m-%d %H:%M:%S", // baseline
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S", // with fractional handled
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%y-%m-%d %H:%M:%S",
        "%y-%m-%d %H:%M:%S",
        "%y/%m/%d %H:%M:%S",
        "%b %d %Y %H:%M:%S",
        "%b %d %H:%M:%S %Y",
        "%b %d, %Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %I:%M:%S %p",
        "%d/%b/%Y:%H:%M:%S %z",
        "%d/%b/%Y %H:%M:%S",
    };
    size_t fmt_count = sizeof(fmts)/sizeof(fmts[0]);
    for(size_t i=0;i<fmt_count;i++){
        if(strptime_parse(buf, fmts[i], tmv, millis, has_tz, tz_offset)){
            long long v = to_epoch_ms(tmv, millis, has_tz, tz_offset);
            if(v < 0) continue; // try next format
            out_ms = v;
            return true;
        }
    }
    return false;
}

std::pair<int,int> detect_timestamp_span(const char* s, int len)
{
    // Heuristics: search for digit-heavy span containing ':' and space or '/' or '-'
    int best_start=-1, best_len=0;
    for(int i=0;i<len;i++){
        if(isdigit(s[i]) || isalpha(s[i])){
            int j=i; int colon=0; int slashdash=0;
            while(j<len && (isalnum(s[j]) || s[j]=='/' || s[j]=='-' || s[j]==':' || s[j]==' ' || s[j]==',' || s[j]=='.' || s[j]=='+' || s[j]=='Z')){
                colon += (s[j]==':');
                slashdash += (s[j]=='/' || s[j]=='-');
                j++;
            }
            if(colon>=1 && slashdash>=1){
                best_start=i; best_len=j-i; break;
            }
            i=j;
        }
    }
    return {best_start, best_len};
}