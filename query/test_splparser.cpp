#include "SPLParser.h"
#include <iostream>

static void run(const std::string& r){ SPLCommand cmd; bool ok = parse_spl(r, cmd); std::cout << (ok? "ok":"fail") << " : " << r << "\n"; if(ok){ std::cout << "type=" << cmd.type << ", field=" << cmd.field << ", group=" << cmd.group << ", k=" << cmd.k << ", op=" << cmd.op << ", valAlias=" << cmd.valueAlias << "\n"; } }

int main(){
  run("stats count()");
  run("stats count by host");
  run("stats sum(bytes)");
  run("stats sum(bytes) by user");
  run("top(user,20)");
  run("distinct(ip)");
  run("by(host) avg(bytes)");
  run("timechart span=1m count() by host");
  return 0;
}