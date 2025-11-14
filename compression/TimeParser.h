#ifndef TIMEPARSER_H
#define TIMEPARSER_H

#include <string>
#include <ctime>
#include <vector>

// Parse various timestamp formats into epoch milliseconds.
// Returns true on success, sets out_ms.
bool parse_timestamp_ms(const char* s, int len, long long& out_ms);

// Try to quickly locate a timestamp substring within a line.
// Returns offset and length of detected timestamp substring, or {-1,0} if not found.
std::pair<int,int> detect_timestamp_span(const char* s, int len);

#endif