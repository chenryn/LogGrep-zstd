#include "SPLParser.h"
#include <cstdlib>
#ifdef USE_PEGTL
#include <tao/pegtl.hpp>
namespace pegtl = tao::pegtl;

struct grammar_state {
  SPLCommand* out;
  std::string func;
  std::string field;
  std::string group;
  int topk = 10;
};

struct sp_space : pegtl::star< pegtl::space > {};
struct ident_head : pegtl::sor< pegtl::alpha, pegtl::one<'_'> > {};
struct ident_tail : pegtl::sor< pegtl::alnum, pegtl::one<'_'> > {};
struct ident : pegtl::seq< ident_head, pegtl::star< ident_tail > > {};

struct lparen : pegtl::one<'('> {};
struct rparen : pegtl::one<')'> {};
struct comma  : pegtl::one<','> {};

struct kw_stats     : pegtl::string<'s','t','a','t','s'> {};
struct kw_count     : pegtl::string<'c','o','u','n','t'> {};
struct kw_sum       : pegtl::string<'s','u','m'> {};
struct kw_avg       : pegtl::string<'a','v','g'> {};
struct kw_min       : pegtl::string<'m','i','n'> {};
struct kw_max       : pegtl::string<'m','a','x'> {};
struct kw_top       : pegtl::string<'t','o','p'> {};
struct kw_distinct  : pegtl::string<'d','i','s','t','i','n','c','t'> {};
struct kw_by        : pegtl::string<'b','y'> {};
struct kw_timechart : pegtl::string<'t','i','m','e','c','h','a','r','t'> {};

struct count_paren : pegtl::seq< kw_count, sp_space, pegtl::opt< pegtl::seq< lparen, sp_space, rparen > > > {};
struct agg_func_name : pegtl::sor< kw_sum, kw_avg, kw_min, kw_max > {};
struct agg_func : pegtl::seq< agg_func_name, sp_space, lparen, sp_space, ident, sp_space, rparen > {};
struct by_opt : pegtl::seq< sp_space, kw_by, sp_space, ident > {};

struct top_clause : pegtl::seq< kw_top, sp_space, lparen, sp_space, ident, sp_space, pegtl::opt< pegtl::seq< comma, sp_space, pegtl::plus< pegtl::digit > > >, sp_space, rparen > {};
struct digits : pegtl::plus< pegtl::digit > {};
struct top_num_clause : pegtl::seq< kw_top, sp_space, digits, sp_space, ident > {};
struct distinct_clause : pegtl::seq< kw_distinct, sp_space, lparen, sp_space, ident, sp_space, rparen > {};
struct stats_distinct_by : pegtl::seq< kw_stats, sp_space, distinct_clause, sp_space, pegtl::opt< by_opt > > {};
struct stats_count_by : pegtl::seq< kw_stats, sp_space, count_paren, sp_space, pegtl::opt< by_opt > > {};
struct stats_agg_by   : pegtl::seq< kw_stats, sp_space, agg_func, sp_space, pegtl::opt< by_opt > > {};
struct stats_distinct : pegtl::seq< kw_stats, sp_space, distinct_clause > {};
struct groupby_clause : pegtl::seq< kw_by, sp_space, lparen, sp_space, ident, sp_space, rparen, sp_space, pegtl::sor< count_paren, agg_func, distinct_clause > > {};
struct timechart_clause : pegtl::seq< kw_timechart, pegtl::star< pegtl::any > > {};
struct right_clause : pegtl::sor< stats_count_by, stats_agg_by, stats_distinct, stats_distinct_by, top_clause, top_num_clause, distinct_clause, groupby_clause, timechart_clause > {};

template< typename Rule >
struct action : pegtl::nothing< Rule > {};

template<> struct action< kw_sum > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.func = "sum"; } };
template<> struct action< kw_avg > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.func = "avg"; } };
template<> struct action< kw_min > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.func = "min"; } };
template<> struct action< kw_max > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.func = "max"; } };
template<> struct action< kw_distinct > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.func = "distinct"; } };
template<> struct action< ident >  { template< typename Input > static void apply( const Input& in, grammar_state& st ){ if(st.func.empty()) st.field = in.string(); else if(st.func=="by") st.group = in.string(); } };
template<> struct action< kw_by >  { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.func = "by"; } };
template<> struct action< digits > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.topk = atoi(in.string().c_str()); if(st.topk<=0) st.topk=10; } };

template<> struct action< count_paren > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.out->type = st.group.empty()? SPL_COUNT : SPL_COUNT_BY; st.out->field = st.group.empty()? std::string() : st.group; } };
template<> struct action< agg_func > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ if(st.func=="sum") st.out->type=SPL_SUM; else if(st.func=="avg") st.out->type=SPL_AVG; else if(st.func=="min") st.out->type=SPL_MIN; else if(st.func=="max") st.out->type=SPL_MAX; st.out->field=st.field; } };
template<> struct action< stats_agg_by > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ if(!st.group.empty()){ st.out->type=SPL_GROUP_BY; st.out->group=st.group; if(st.func=="sum") st.out->op=11; else if(st.func=="avg") st.out->op=12; st.out->valueAlias=st.field; } } };
template<> struct action< top_clause > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.out->type=SPL_TOP; st.out->field=st.field; st.out->k=st.topk; } };
template<> struct action< top_num_clause > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.out->type=SPL_TOP; st.out->field=st.field; st.out->k=st.topk; } };
template<> struct action< distinct_clause > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.out->type=SPL_DISTINCT; st.out->field=st.field; } };
template<> struct action< stats_distinct_by > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ if(!st.group.empty()){ st.out->type=SPL_GROUP_BY; st.out->group=st.group; st.out->op=13; st.out->valueAlias=st.field; } } };
template<> struct action< groupby_clause > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ if(!st.group.empty()){ st.out->type=SPL_GROUP_BY; st.out->group=st.group; if(st.func=="sum") st.out->op=11; else if(st.func=="avg") st.out->op=12; else if(st.func=="distinct") st.out->op=13; else st.out->op=10; if(st.func=="sum"||st.func=="avg"||st.func=="distinct") st.out->valueAlias=st.field; } } };
template<> struct action< timechart_clause > { template< typename Input > static void apply( const Input& in, grammar_state& st ){ st.out->type=SPL_TIMECHART; } };

static bool parse_spl_pegtl(const std::string& right, SPLCommand& out){ std::string r=right; for(size_t i=0;i<r.size();i++){ if(r[i]>='A'&&r[i]<='Z') r[i]=r[i]-'A'+'a'; if(r[i]=='+') r[i]=' '; } size_t i=r.find_first_not_of(" \t"); if(i==std::string::npos){ r.clear(); } else { size_t j=r.find_last_not_of(" \t"); r=r.substr(i, j-i+1); } pegtl::memory_input in(r, "spl"); grammar_state st; st.out=&out; out.type=SPL_NONE; bool ok = pegtl::parse< right_clause, action >( in, st ); return ok; }
#endif

static std::string norm(std::string s){ for(size_t i=0;i<s.size();i++){ if(s[i]>='A'&&s[i]<='Z') s[i]=s[i]-'A'+'a'; if(s[i]=='+') s[i]=' '; } return s; }
static void trim(std::string& s){ size_t i=s.find_first_not_of(" \t"); if(i==std::string::npos){ s.clear(); return;} size_t j=s.find_last_not_of(" \t"); s=s.substr(i,j-i+1); }

bool parse_spl(const std::string& right, SPLCommand& out){
#ifdef USE_PEGTL
  return parse_spl_pegtl(right, out);
#else
  std::string r=norm(right); out.type=SPL_NONE; out.field.clear(); out.group.clear(); out.valueAlias.clear(); out.k=10; out.op=0; trim(r);
  if(r.find("timechart")==0){ out.type=SPL_TIMECHART; return true; }
  if(r.find("stats")==0){ std::string expr=r.substr(5); trim(expr);
    if(expr=="count" || expr=="count()"){ out.type=SPL_COUNT; return true; }
    if(expr.find("count by ")==0){ out.type=SPL_COUNT_BY; std::string field=expr.substr(9); trim(field); out.field=field; return true; }
    if(expr.find("count() by ")==0){ out.type=SPL_COUNT_BY; std::string field=expr.substr(11); trim(field); out.field=field; return true; }
    if(expr.find("distinct(")==0){ size_t l=expr.find('('); size_t r2=expr.find(')', l+1); if(l!=std::string::npos && r2!=std::string::npos){ std::string field=expr.substr(l+1, r2-l-1); trim(field); out.field=field; size_t byp = expr.find(" by ", r2+1); if(byp!=std::string::npos){ std::string grp=expr.substr(byp+4); trim(grp); out.group=grp; out.op=13; out.valueAlias=field; out.type=SPL_GROUP_BY; return true; } out.type=SPL_DISTINCT; return true; } }
    if(expr.find("sum(")==0 || expr.find("avg(")==0 || expr.find("min(")==0 || expr.find("max(")==0){ size_t l=expr.find('('); size_t r2=expr.find(')', l+1); if(l!=std::string::npos && r2!=std::string::npos){ std::string func=expr.substr(0,l); std::string field=expr.substr(l+1, r2-l-1); trim(field); out.field=field; size_t byp = expr.find(" by ", r2+1); if(byp!=std::string::npos){ std::string grp=expr.substr(byp+4); trim(grp); out.group=grp; if(func=="sum"){ out.op=11; } else if(func=="avg"){ out.op=12; } out.valueAlias=field; out.type=SPL_GROUP_BY; return true; } if(func=="sum") out.type=SPL_SUM; else if(func=="avg") out.type=SPL_AVG; else if(func=="min") out.type=SPL_MIN; else if(func=="max") out.type=SPL_MAX; return true; } }
  }
  if(r.find("top ")==0){ std::string rest=r.substr(4); trim(rest); size_t sp=rest.find(' '); if(sp!=std::string::npos){ std::string ks=rest.substr(0, sp); std::string field=rest.substr(sp+1); trim(field); int k=atoi(ks.c_str()); if(k<=0) k=10; out.k=k; out.field=field; out.type=SPL_TOP; return true; } }
  if(r.find("top(")==0){ size_t l=r.find('('); size_t r2=r.find(')', l+1); if(l!=std::string::npos && r2!=std::string::npos){ std::string inner=r.substr(l+1, r2-l-1); size_t comma=inner.find(','); std::string field= inner.substr(0, comma==std::string::npos? inner.size() : comma); trim(field); out.field=field; int k=10; if(comma!=std::string::npos){ std::string ks= inner.substr(comma+1); trim(ks); k=atoi(ks.c_str()); if(k<=0) k=10; } out.k=k; out.type=SPL_TOP; return true; } }
  if(r.find("distinct(")==0){ size_t l=r.find('('); size_t r2=r.find(')', l+1); if(l!=std::string::npos && r2!=std::string::npos){ std::string field=r.substr(l+1, r2-l-1); trim(field); out.field=field; out.type=SPL_DISTINCT; return true; } }
  if(r.find("by(")==0){ size_t l=r.find('('); size_t r2=r.find(')', l+1); if(l!=std::string::npos && r2!=std::string::npos){ std::string group=r.substr(l+1, r2-l-1); trim(group); std::string tail=r.substr(r2+1); trim(tail); int op=10; std::string valueAlias; if(tail=="count()") op=10; else if(tail.find("sum(")==0){ op=11; size_t l2=tail.find('('); size_t r3=tail.find(')', l2+1); if(l2!=std::string::npos && r3!=std::string::npos){ valueAlias=tail.substr(l2+1, r3-l2-1); trim(valueAlias); } } else if(tail.find("avg(")==0){ op=12; size_t l2=tail.find('('); size_t r3=tail.find(')', l2+1); if(l2!=std::string::npos && r3!=std::string::npos){ valueAlias=tail.substr(l2+1, r3-l2-1); trim(valueAlias); } } out.group=group; out.valueAlias=valueAlias; out.op=op; out.type=SPL_GROUP_BY; return true; } }
  if(r.find("by(")==0){ size_t l=r.find('('); size_t r2=r.find(')', l+1); if(l!=std::string::npos && r2!=std::string::npos){ std::string group=r.substr(l+1, r2-l-1); trim(group); std::string tail=r.substr(r2+1); trim(tail); if(tail.find("distinct(")==0){ size_t l2=tail.find('('); size_t r3=tail.find(')', l2+1); std::string valueAlias; if(l2!=std::string::npos && r3!=std::string::npos){ valueAlias=tail.substr(l2+1, r3-l2-1); trim(valueAlias); } out.group=group; out.valueAlias=valueAlias; out.op=13; out.type=SPL_GROUP_BY; return true; } } }
  return false;
#endif
}