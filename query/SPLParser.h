#pragma once
#include <string>

enum SPLCmdType { SPL_NONE=0, SPL_COUNT, SPL_COUNT_BY, SPL_SUM, SPL_AVG, SPL_MIN, SPL_MAX, SPL_TOP, SPL_DISTINCT, SPL_GROUP_BY, SPL_TIMECHART };

struct SPLCommand {
  SPLCmdType type;
  std::string field;
  int k;
  std::string group;
  std::string valueAlias;
  int op;
};

bool parse_spl(const std::string& right, SPLCommand& out);