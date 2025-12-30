#include <fstream>
#include <cstring> 
#include <sys/types.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <atomic>
#include <mutex>
#include "LogDispatcher.h"
#include "var_alias.h"
#include "StatisticsAPI.h"
#include "HLL.h"
#include <map>
#include <vector>
#include <climits>
#include <algorithm>

using namespace std;

/////////////////////////init/////////////////
LogDispatcher::LogDispatcher()
{
	m_nServerHandle = 0;
	m_fileCnt =0;
	for (int itor = 0; itor < MAX_FILE_CNT; itor++)
	{
		m_logStores[itor] = NULL;
	}
}
LogDispatcher::~LogDispatcher()
{
	DisConnect();
	m_nServerHandle = 0;
	m_fileCnt =0;
}

int LogDispatcher::Connect(char* dirPath)
{
    DIR *d;
    struct dirent *file;
	if(dirPath == NULL || strlen(dirPath) <=3)
	{
		dirPath = DIR_PATH_DEFAULT;
	}
    if(!(d = opendir(dirPath)))
    {
		SyslogError("error dir path. path:%s.\n", dirPath);
        return -1;//open failed
    }
	int loadNum =0;
	int totalFilesNum =0;
	m_fileCnt =0;
    auto has_suffix = [](const char* name, const char* suf){ size_t ln=strlen(name); size_t ls=strlen(suf); if(ls>ln) return false; return strncmp(name+ln-ls, suf, ls)==0; };
    while((file = readdir(d)) != NULL)
    {
        if(strncmp(file->d_name, ".", 1) == 0 || strlen(file->d_name) < 3) continue;
        if(!(has_suffix(file->d_name, ".zip"))) continue;
        if(has_suffix(file->d_name, ".zip.meta") || has_suffix(file->d_name, ".zip.variables") || has_suffix(file->d_name, ".zip.templates")) continue;
        SyslogDebug("%s %s\n", dirPath, file->d_name);
        
        LogStoreApi* logStore = new LogStoreApi();
        loadNum = logStore->Connect(dirPath, file->d_name);
		if(loadNum > 0)
		{
			m_nServerHandle = 1;
			m_logStores[m_fileCnt++] = logStore;
			SyslogDebug("%d --load patterns success,load num:%d, path:%s/%s.\n", m_fileCnt-1, loadNum, dirPath, file->d_name);
		}
		else
		{
			SyslogError("path:%s/%s load failed, skipped already!\n", dirPath, file->d_name);
		}
		totalFilesNum++;
    }
    closedir(d);
	if(m_nServerHandle == 0)
	{
		SyslogError("error load logStore. path:%s.\n", dirPath);
		return 0;
	}
	printf("load logStore success,load num:%d/%d, path:%s.\n", m_fileCnt, totalFilesNum, dirPath);
	//CalRunningTime();
	return m_fileCnt;
}

int LogDispatcher::TraveDir(char* dirPath)
{
    DIR *d;
    struct dirent *file;
	if(dirPath == NULL || strlen(dirPath) <=3)
	{
		dirPath = DIR_PATH_DEFAULT;
	}
    if(!(d = opendir(dirPath)))
    {
		SyslogError("error dir path. path:%s.\n", dirPath);
        return -1;//open failed
    }
	int loadNum = 0;
    while((file = readdir(d)) != NULL)
    {
		if(strncmp(file->d_name, ".", 1) == 0 || strlen(file->d_name) < 3) continue;
		m_filequeue.push(file->d_name);
    }
    closedir(d);
	
	return 1;
}

int LogDispatcher::IsConnect()
{
	return m_nServerHandle;
}

void LogDispatcher::DisConnect()
{
	for (int itor = 0; itor < m_fileCnt; itor++)
	{
		if(m_logStores[itor] != NULL)
		{
			int status = m_logStores[itor]->DisConnect();
			if(status == 0)
			{
				//SyslogDebug("release patterns successful. %d\n", itor);
			}
			else
			{
				SyslogError("release patterns failed.\n");
			}
			m_logStores[itor] = NULL;
		}
	}
}

int LogDispatcher::CalRunningTime()
{
	RunningStatus runt;
	Statistics glb_stat;
	for (int itor = 0; itor < m_fileCnt; itor++)
	{
		std::lock_guard<std::mutex> lock(m_runningStatusMutex);
		RunningStatus t = m_logStores[itor] ->RunStatus;
		Statistics ss = m_logStores[itor]->Statistic;
		runt.LogMetaTime += t.LogMetaTime;
		m_runt.LoadDeComLogTime += t.LoadDeComLogTime;
		m_runt.SearchTotalTime += t.SearchTotalTime;
		m_runt.SearchPatternTime += t.SearchPatternTime;
		m_runt.SearchOutlierTime += t.SearchOutlierTime;
		m_runt.MaterializFulTime += t.MaterializFulTime;
		m_runt.MaterializAlgTime += t.MaterializAlgTime;
		runt.SearchTotalEntriesNum += t.SearchTotalEntriesNum;
		//SysCount("%s:%d \n", m_logStores[itor]->FileName.c_str(), t.SearchTotalEntriesNum);
		//runt.SearchOutliersNum += t.SearchOutliersNum;
		glb_stat.total_capsule_cnt += ss.total_capsule_cnt;
		glb_stat.total_decom_capsule_cnt += ss.total_decom_capsule_cnt;
		glb_stat.total_decom_capsule_time += ss.total_decom_capsule_time;
		glb_stat.total_queried_cap_cnt += ss.total_queried_cap_cnt;
		glb_stat.valid_cap_filter_cnt += ss.valid_cap_filter_cnt;
		glb_stat.hit_at_mainpat_cnt += ss.hit_at_mainpat_cnt;
		glb_stat.hit_at_subpat_cnt += ss.hit_at_subpat_cnt;
	}
	SysTotCount("\nLogMetaTime: %lf s\n", runt.LogMetaTime);
	SysInfo("LoadDeComLogTime: %lf s\n", m_runt.LoadDeComLogTime);
	SysTotCount("SearchTotalTime: %lf s\n", m_runt.SearchTotalTime);
	SysTotCount("SearchPatternTime: %lf s\n", m_runt.SearchPatternTime);
	SysTotCount("SearchOutlierTime: %lf s\n", m_runt.SearchOutlierTime);
	SysTotCount("MaterializFulTime: %lf s\n", m_runt.MaterializFulTime);
	SysInfo("MaterializAlgTime: %lf s\n", m_runt.MaterializAlgTime);
	
	SysInfo("SearchOutliersNum: %d\n", runt.SearchOutliersNum);
	SysTotCount("Thulr_CmdlSearchTotalEntriesNum: %d\n", runt.SearchTotalEntriesNum);
	SysTotCount("tot_decom_cap: %d\n", glb_stat.total_decom_capsule_cnt);
	SysTotCount("tot_check_cap: %d\n", glb_stat.total_queried_cap_cnt);
	SysTotCount("tot_valid_cap: %d\n", glb_stat.valid_cap_filter_cnt);
	SysTotCount("tot_cap: %d\n", glb_stat.total_capsule_cnt);
	SysTotCount("tot_decom_time: %lf\n", glb_stat.total_decom_capsule_time);
	SysTotCount("P2P time:%lf s\n", runt.LogMetaTime + m_runt.SearchTotalTime + m_runt.MaterializFulTime);
}	

int LogDispatcher::ResetRunningTime()
{
    m_runt.LoadDeComLogTime = 0;
    m_runt.SearchTotalTime = 0;
	//m_runt.SearchPatternTime = 0;
	//m_runt.SearchOutlierTime = 0;
	m_runt.MaterializFulTime = 0;
	m_runt.MaterializAlgTime = 0;
	//runt.SearchOutliersNum = 0;
}

int LogDispatcher::GetRunningStatus(OUT RunningStatus& out)
{
    out = m_runt;
    return 0;
}

//////////////////////SearchByWildcard///////////////////////////////

int LogDispatcher::SearchByWildcard(char *args[MAX_CMD_ARG_COUNT], int argCount)
{
	if(MAX_THREAD_PARALLEL == 1)
	{
		SearchByWildcard_Seq(args, argCount);
	}
	else
	{
		SearchByWildcard_Thread(args, argCount);
	}
	//rest time statistic
	ResetRunningTime();
}

int LogDispatcher::SearchByWildcard_Seq(char *args[MAX_CMD_ARG_COUNT], int argCount)
{
	int totalMatNum = MAX_MATERIAL_SIZE;
	int matnum =0;
	//printf("$$$$$$$\n");
	for (int itor = 0; itor < m_fileCnt; itor++)
	{
		
		LogStoreApi* logStore = m_logStores[itor];
		matnum = logStore->SearchByWildcard_Token(args, argCount, totalMatNum);
		totalMatNum -= matnum;
		
		//printf("tt: %d %d\n", totalMatNum, matnum);
	}
	CalRunningTime();
}

static void* wildcard_worker(void* p){ struct WArg{ LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; std::atomic<int>* rem; }; WArg* a=(WArg*)p; while(true){ int i = a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; int allowed = a->rem->load(); if(allowed<=0) break; LogStoreApi* logStore = a->stores[i]; int got = logStore->SearchByWildcard_Token(a->args, a->ac, allowed); if(got>0) a->rem->fetch_sub(got); } return NULL; }

int LogDispatcher::SearchByWildcard_Thread(char *args[MAX_CMD_ARG_COUNT], int argCount)
{
    std::atomic<int> remaining(MAX_MATERIAL_SIZE);
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    std::vector<pthread_t> ths; ths.resize(n);
    struct WArg{ LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; std::atomic<int>* rem; } a; a.stores=m_logStores; a.fileCnt=m_fileCnt; a.nextIdx=&nextIdx; a.args=args; a.ac=argCount; a.rem=&remaining;
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, wildcard_worker, &a); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    CalRunningTime();
    return 0;
}

void * LogDispatcher::SearchByWildcard_pthread(void *ptr)
{
    LogDispatcher* self = (LogDispatcher*)ptr;
    return self->SearchByWildcard_pthread_exe();
}

void * LogDispatcher::SearchByWildcard_pthread_exe()
{
    SearchByWildcard_Seq(m_args, m_argCount);
    return NULL;
}


int LogDispatcher::SearchByWildcard_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, int matNum, std::string &json_out)
{
    std::atomic<int> remaining(matNum);
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    struct JArg{ LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; std::atomic<int>* rem; std::vector<std::string>* parts; std::vector<int>* gotv; } a;
    a.stores=m_logStores; a.fileCnt=m_fileCnt; a.nextIdx=&nextIdx; a.args=args; a.ac=argCount; a.rem=&remaining; std::vector<std::string> parts(m_fileCnt); std::vector<int> gotv(m_fileCnt,0); a.parts=&parts; a.gotv=&gotv;
    auto worker = [](void* p)->void*{ JArg* a=(JArg*)p; while(true){ int i=a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; int allowed=a->rem->load(); if(allowed<=0) break; std::string part; int got=a->stores[i]->SearchByWildcard_Token_JSON(a->args, a->ac, allowed, part); (*a->gotv)[i]=got; if(got>0) a->rem->fetch_sub(got); (*a->parts)[i].swap(part);} return NULL; };
    std::vector<pthread_t> ths; ths.resize(n);
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, worker, &a); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    json_out.clear(); json_out.append("["); bool first=true; for(int i=0;i<m_fileCnt;i++){ if(gotv[i]>0){ const std::string& part=parts[i]; if(part.size()>=2){ if(!first) json_out.append(","); first=false; json_out.append(part.substr(1, part.size()-2)); } } }
    json_out.append("]");
    CalRunningTime();
    ResetRunningTime();
    return matNum - remaining.load();
}

int LogDispatcher::CountByWildcard(char *args[MAX_CMD_ARG_COUNT], int argCount)
{
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    struct CArg{ LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; std::atomic<int>* total; } a;
    a.stores=m_logStores; a.fileCnt=m_fileCnt; a.nextIdx=&nextIdx; a.args=args; a.ac=argCount; std::atomic<int> total(0); a.total=&total;
    auto worker = [](void* p)->void*{ CArg* a=(CArg*)p; while(true){ int i=a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; int c=a->stores[i]->CountByWildcard_Token(a->args, a->ac); if(c>0) a->total->fetch_add(c);} return NULL; };
    std::vector<pthread_t> ths; ths.resize(n);
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, worker, &a); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    return total.load();
}

int LogDispatcher::Aggregate_Scalar(char *args[MAX_CMD_ARG_COUNT], int argCount, int opType, const std::string& alias, double& value_out)
{
    value_out = 0.0;
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    double gsum = 0.0; long long gcount = 0; double gmin = 0.0; double gmax = 0.0; bool haveInit=false;
    struct SArg{ LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; int op; const std::string* alias; double gsum; long long gcount; double gmin; double gmax; bool haveInit; } a;
    a.stores=m_logStores; a.fileCnt=m_fileCnt; a.nextIdx=&nextIdx; a.args=args; a.ac=argCount; a.op=opType; a.alias=&alias; a.gsum=0.0; a.gcount=0; a.gmin=0.0; a.gmax=0.0; a.haveInit=false;
    auto worker = [](void* p)->void*{ SArg* a=(SArg*)p; bool isEmpty = (a->ac == 1 && (a->args[0] == NULL || a->args[0][0] == '\0')); while(true){ int i=a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; LogStoreApi* logStore=a->stores[i]; LISTBITMAPS bitmaps; logStore->BuildBitmapsForQuery(a->args, a->ac, bitmaps); if(!isEmpty && bitmaps.empty()) continue; VarAliasManager* mgr=VarAliasManager::getInstance(); std::vector<int> vids=mgr->getVarIds(*a->alias); StatisticsAPI stats(logStore); for(size_t k=0;k<vids.size();k++){ int varId=vids[k]; int pid=(varId & 0xFFFF0000); LISTBITMAPS::iterator ib=bitmaps.find(pid); BitMap* filter = NULL; if(ib == bitmaps.end()){ if(isEmpty) filter = NULL; else continue; } else { filter = ib->second; if(!isEmpty && filter == NULL) continue; } if(a->op==0){ double s=stats.GetVarSum(varId + VAR_TYPE_VAR, filter); a->gsum += s; } else if(a->op==1){ double s=stats.GetVarSum(varId + VAR_TYPE_VAR, filter); int c=stats.GetVarCount(varId + VAR_TYPE_VAR, filter); a->gsum += s; a->gcount += c; } else if(a->op==2){ double v=stats.GetVarMin(varId + VAR_TYPE_VAR, filter); if(!a->haveInit){ a->gmin=v; a->haveInit=true; } else { if(v<a->gmin) a->gmin=v; } } else if(a->op==3){ double v=stats.GetVarMax(varId + VAR_TYPE_VAR, filter); if(!a->haveInit){ a->gmax=v; a->haveInit=true; } else { if(v>a->gmax) a->gmax=v; } } } for(LISTBITMAPS::iterator it=bitmaps.begin(); it!=bitmaps.end(); ++it){ if(it->second) delete it->second; } } return NULL; };
    std::vector<pthread_t> ths; ths.resize(n);
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, worker, &a); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    if(opType==0){ value_out=a.gsum; return 0; }
    if(opType==1){ value_out=a.gcount>0? (a.gsum / (double)a.gcount) : 0.0; return 0; }
    if(opType==2){ value_out=a.haveInit? a.gmin : 0.0; return 0; }
    if(opType==3){ value_out=a.haveInit? a.gmax : 0.0; return 0; }
    return -1;
}

int LogDispatcher::Aggregate_Distinct(char *args[MAX_CMD_ARG_COUNT], int argCount, const std::string& alias, int& value_out)
{
    value_out = 0;
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    std::vector<HyperLogLog> locals(m_fileCnt, HyperLogLog(12));
    struct DArg{ LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; const std::string* alias; std::vector<HyperLogLog>* locals; } a;
    a.stores=m_logStores; a.fileCnt=m_fileCnt; a.nextIdx=&nextIdx; a.args=args; a.ac=argCount; a.alias=&alias; a.locals=&locals;
    auto worker = [](void* p)->void*{ DArg* a=(DArg*)p; bool isEmpty = (a->ac == 1 && (a->args[0] == NULL || a->args[0][0] == '\0')); while(true){ int i=a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; LogStoreApi* logStore=a->stores[i]; LISTBITMAPS bitmaps; logStore->BuildBitmapsForQuery(a->args, a->ac, bitmaps); if(!isEmpty && bitmaps.empty()) continue; VarAliasManager* mgr=VarAliasManager::getInstance(); std::vector<int> vids=mgr->getVarIds(*a->alias); StatisticsAPI stats(logStore); for(size_t k=0;k<vids.size();k++){ int varId=vids[k]; int pid=(varId & 0xFFFF0000); LISTBITMAPS::iterator ib=bitmaps.find(pid); BitMap* filter = NULL; if(ib == bitmaps.end()){ if(isEmpty) filter = NULL; else continue; } else { filter = ib->second; if(!isEmpty && filter == NULL) continue; } stats.BuildHLL(varId + VAR_TYPE_VAR, filter, (*a->locals)[i]); } for(LISTBITMAPS::iterator it=bitmaps.begin(); it!=bitmaps.end(); ++it){ if(it->second) delete it->second; } } return NULL; };
    std::vector<pthread_t> ths; ths.resize(n);
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, worker, &a); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    HyperLogLog global(12);
    for(int i=0;i<m_fileCnt;i++){ global.merge(locals[i]); }
    value_out = (int)std::llround(global.estimate());
    return 0;
}

int LogDispatcher::Aggregate_TopK_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, const std::string& alias, int k, std::string& json_out)
{
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    std::vector< std::map<std::string,int> > locals(m_fileCnt);
    struct TArg{ LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; const std::string* alias; int k; std::vector< std::map<std::string,int> >* locals; } a;
    a.stores=m_logStores; a.fileCnt=m_fileCnt; a.nextIdx=&nextIdx; a.args=args; a.ac=argCount; a.alias=&alias; a.k=k; a.locals=&locals;
    auto worker = [](void* p)->void*{ TArg* a=(TArg*)p; bool isEmpty = (a->ac == 1 && (a->args[0] == NULL || a->args[0][0] == '\0')); while(true){ int i=a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; LogStoreApi* logStore=a->stores[i]; LISTBITMAPS bitmaps; logStore->BuildBitmapsForQuery(a->args, a->ac, bitmaps); if(!isEmpty && bitmaps.empty()) continue; VarAliasManager* mgr=VarAliasManager::getInstance(); std::vector<int> vids=mgr->getVarIds(*a->alias); StatisticsAPI stats(logStore); std::map<std::string,int>& loc=(*a->locals)[i]; for(size_t k2=0;k2<vids.size();k2++){ int varId=vids[k2]; int pid=(varId & 0xFFFF0000); LISTBITMAPS::iterator ib=bitmaps.find(pid); BitMap* filter = NULL; if(ib == bitmaps.end()){ if(isEmpty) filter = NULL; else continue; } else { filter = ib->second; if(!isEmpty && filter == NULL) continue; } std::map<std::string,int> freq=stats.GetVarFrequency(varId + VAR_TYPE_VAR, a->k, filter); for(std::map<std::string,int>::iterator it=freq.begin(); it!=freq.end(); ++it){ loc[it->first] += it->second; } } for(LISTBITMAPS::iterator it=bitmaps.begin(); it!=bitmaps.end(); ++it){ if(it->second) delete it->second; } } return NULL; };
    std::vector<pthread_t> ths; ths.resize(n);
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, worker, &a); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    std::map<std::string,int> freqAll; for(int i=0;i<m_fileCnt;i++){ for(std::map<std::string,int>::iterator it=locals[i].begin(); it!=locals[i].end(); ++it){ freqAll[it->first] += it->second; } }
    std::vector<std::pair<std::string,int> > vec; vec.reserve(freqAll.size()); for(std::map<std::string,int>::iterator it=freqAll.begin(); it!=freqAll.end(); ++it){ vec.push_back(*it); }
    std::sort(vec.begin(), vec.end(), [](const std::pair<std::string,int>& a, const std::pair<std::string,int>& b){ return a.second > b.second; }); if((int)vec.size()>k) vec.resize(k);
    json_out.clear(); json_out.append("["); bool first=true; for(size_t i=0;i<vec.size();i++){ if(!first) json_out.append(","); first=false; json_out.append("{\"value\":\""); const std::string& v=vec[i].first; for(size_t j=0;j<v.size();j++){ char c=v[j]; if(c=='\"'||c=='\\'){ json_out.push_back('\\'); json_out.push_back(c);} else { json_out.push_back(c);} } json_out.append("\",\"count\":"); json_out.append(std::to_string(vec[i].second)); json_out.append("}"); }
    json_out.append("]");
    return (int)vec.size();
}

int LogDispatcher::Aggregate_Group_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, const std::string& groupAlias, int opType, const std::string& valueAlias, std::string& json_out)
{
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    std::vector< std::map<std::string,double> > gsumLoc(m_fileCnt);
    std::vector< std::map<std::string,long long> > gcountLoc(m_fileCnt);
    std::vector< std::map<std::string,int> > gdistinctLoc(m_fileCnt);
    struct GArg{ LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; const std::string* group; int op; const std::string* valAlias; std::vector< std::map<std::string,double> >* gsumLoc; std::vector< std::map<std::string,long long> >* gcountLoc; std::vector< std::map<std::string,int> >* gdistinctLoc; } a;
    a.stores=m_logStores; a.fileCnt=m_fileCnt; a.nextIdx=&nextIdx; a.args=args; a.ac=argCount; a.group=&groupAlias; a.op=opType; a.valAlias=&valueAlias; a.gsumLoc=&gsumLoc; a.gcountLoc=&gcountLoc; a.gdistinctLoc=&gdistinctLoc;
    auto worker = [](void* p)->void*{ GArg* a=(GArg*)p; bool isEmpty = (a->ac == 1 && (a->args[0] == NULL || a->args[0][0] == '\0')); while(true){ int i=a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; LogStoreApi* logStore=a->stores[i]; LISTBITMAPS bitmaps; logStore->BuildBitmapsForQuery(a->args, a->ac, bitmaps); if(!isEmpty && bitmaps.empty()) continue; VarAliasManager* mgr=VarAliasManager::getInstance(); std::vector<int> gvids=mgr->getVarIds(*a->group); StatisticsAPI stats(logStore); std::map<std::string,double>& gsum=(*a->gsumLoc)[i]; std::map<std::string,long long>& gcount=(*a->gcountLoc)[i]; std::map<std::string,int>& gdistinct=(*a->gdistinctLoc)[i]; for(size_t gi=0; gi<gvids.size(); gi++){ int gvar=gvids[gi]; int pid=(gvar & 0xFFFF0000); LISTBITMAPS::iterator ib=bitmaps.find(pid); BitMap* filter = NULL; if(ib == bitmaps.end()){ if(isEmpty) filter = NULL; else continue; } else { filter = ib->second; if(!isEmpty && filter == NULL) continue; } if(a->op==10){ std::map<std::string,int> c=stats.GetVarGroupByCount(gvar + VAR_TYPE_VAR, filter); for(std::map<std::string,int>::iterator it=c.begin(); it!=c.end(); ++it){ gcount[it->first] += it->second; } } else if(a->op==11 || a->op==12){ std::vector<int> vvids=mgr->getVarIds(*a->valAlias); for(size_t vi=0; vi<vvids.size(); vi++){ int vvar=vvids[vi]; if((vvar & 0xFFFF0000) != pid) continue; std::map<std::string,double> s=stats.GetVarGroupBySum(gvar + VAR_TYPE_VAR, vvar + VAR_TYPE_VAR, filter); for(std::map<std::string,double>::iterator it=s.begin(); it!=s.end(); ++it){ gsum[it->first] += it->second; } std::map<std::string,int> c=stats.GetVarGroupByCount(gvar + VAR_TYPE_VAR, filter); for(std::map<std::string,int>::iterator it=c.begin(); it!=c.end(); ++it){ gcount[it->first] += it->second; } } } else if(a->op==13){ std::vector<int> vvids=mgr->getVarIds(*a->valAlias); for(size_t vi=0; vi<vvids.size(); vi++){ int vvar=vvids[vi]; if((vvar & 0xFFFF0000) != pid) continue; std::map<std::string,int> d=stats.GetVarGroupByDistinctCount(gvar + VAR_TYPE_VAR, vvar + VAR_TYPE_VAR, filter); for(std::map<std::string,int>::iterator it=d.begin(); it!=d.end(); ++it){ gdistinct[it->first] += it->second; } } } } for(LISTBITMAPS::iterator it=bitmaps.begin(); it!=bitmaps.end(); ++it){ if(it->second) delete it->second; } } return NULL; };
    std::vector<pthread_t> ths; ths.resize(n);
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, worker, &a); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    std::map<std::string,double> gsumMap; std::map<std::string,long long> gcountMap; std::map<std::string,int> gdistinctMap; for(int i=0;i<m_fileCnt;i++){ for(std::map<std::string,double>::iterator it=gsumLoc[i].begin(); it!=gsumLoc[i].end(); ++it){ gsumMap[it->first] += it->second; } for(std::map<std::string,long long>::iterator it2=gcountLoc[i].begin(); it2!=gcountLoc[i].end(); ++it2){ gcountMap[it2->first] += it2->second; } for(std::map<std::string,int>::iterator it3=gdistinctLoc[i].begin(); it3!=gdistinctLoc[i].end(); ++it3){ gdistinctMap[it3->first] += it3->second; } }
    json_out.clear(); json_out.append("["); bool first=true; if(opType==10){ for(std::map<std::string,long long>::iterator it=gcountMap.begin(); it!=gcountMap.end(); ++it){ if(!first) json_out.append(","); first=false; json_out.append("{\"key\":\""); const std::string& k=it->first; for(size_t j=0;j<k.size();j++){ char c=k[j]; if(c=='\"'||c=='\\'){ json_out.push_back('\\'); json_out.push_back(c);} else { json_out.push_back(c);} } json_out.append("\",\"value\":"); json_out.append(std::to_string((long long)it->second)); json_out.append("}"); } }
    if(opType==11){ for(std::map<std::string,double>::iterator it=gsumMap.begin(); it!=gsumMap.end(); ++it){ if(!first) json_out.append(","); first=false; json_out.append("{\"key\":\""); const std::string& k=it->first; for(size_t j=0;j<k.size();j++){ char c=k[j]; if(c=='\"'||c=='\\'){ json_out.push_back('\\'); json_out.push_back(c);} else { json_out.push_back(c);} } json_out.append("\",\"value\":"); json_out.append(std::to_string(it->second)); json_out.append("}"); } }
    if(opType==12){ for(std::map<std::string,double>::iterator it=gsumMap.begin(); it!=gsumMap.end(); ++it){ long long cc=gcountMap[it->first]; double v=cc>0? (it->second / (double)cc) : 0.0; if(!first) json_out.append(","); first=false; json_out.append("{\"key\":\""); const std::string& k=it->first; for(size_t j=0;j<k.size();j++){ char c=k[j]; if(c=='\"'||c=='\\'){ json_out.push_back('\\'); json_out.push_back(c);} else { json_out.push_back(c);} } json_out.append("\",\"value\":"); json_out.append(std::to_string(v)); json_out.append("}"); } }
    if(opType==13){ for(std::map<std::string,int>::iterator it=gdistinctMap.begin(); it!=gdistinctMap.end(); ++it){ if(!first) json_out.append(","); first=false; json_out.append("{\"key\":\""); const std::string& k=it->first; for(size_t j=0;j<k.size();j++){ char c=k[j]; if(c=='\"'||c=='\\'){ json_out.push_back('\\'); json_out.push_back(c);} else { json_out.push_back(c);} } json_out.append("\",\"value\":"); json_out.append(std::to_string(it->second)); json_out.append("}"); } }
    json_out.append("]");
    return 0;
}

int LogDispatcher::Timechart_Count_BySpan_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, long long span_ms, std::string& json_out)
{
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    std::vector< std::map<long long,int> > locals(m_fileCnt);
    struct A { LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; long long span; std::vector< std::map<long long,int> >* locals; } a;
    a.stores=m_logStores; a.fileCnt=m_fileCnt; a.nextIdx=&nextIdx; a.args=args; a.ac=argCount; a.span=span_ms; a.locals=&locals;
    auto worker = [](void* p)->void*{ A* a=(A*)p; while(true){ int i=a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; LogStoreApi* ls=a->stores[i]; std::map<long long,int> buckets; ls->Timechart_Count_BySpan(a->args, a->ac, a->span, buckets); (*a->locals)[i].swap(buckets); } return NULL; };
    std::vector<pthread_t> ths; ths.resize(n);
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, worker, &a); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    std::map<long long,int> merged; for(int i=0;i<m_fileCnt;i++){ for(auto &kv: locals[i]){ merged[kv.first] += kv.second; } }
    std::vector<std::pair<long long,int> > vec; vec.reserve(merged.size()); for(auto &kv: merged){ vec.push_back(kv); }
    std::sort(vec.begin(), vec.end(), [](const std::pair<long long,int>& a, const std::pair<long long,int>& b){ return a.first < b.first; });
    json_out.clear(); json_out.append("["); bool first=true; for(size_t i=0;i<vec.size();i++){ if(!first) json_out.append(","); first=false; json_out.append("{\"ts\":"); json_out.append(std::to_string(vec[i].first)); json_out.append(",\"count\":"); json_out.append(std::to_string(vec[i].second)); json_out.append("}"); }
    json_out.append("]");
    return (int)vec.size();
}

int LogDispatcher::Timechart_Count_ByBins_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, long long start_ms, long long end_ms, int bins, std::string& json_out)
{
    if(end_ms <= start_ms || bins <= 0){ json_out = "[]"; return 0; }
    long long width = (end_ms - start_ms) / bins; if(width <= 0) width = 1;
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    std::vector< std::vector<int> > locals(m_fileCnt);
    struct B { LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; long long s; long long e; int bins; std::vector< std::vector<int> >* locals; } b;
    b.stores=m_logStores; b.fileCnt=m_fileCnt; b.nextIdx=&nextIdx; b.args=args; b.ac=argCount; b.s=start_ms; b.e=end_ms; b.bins=bins; b.locals=&locals;
    auto worker2 = [](void* p)->void*{ B* a=(B*)p; while(true){ int i=a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; LogStoreApi* ls=a->stores[i]; std::vector<int> counts; ls->Timechart_Count_ByBins(a->args, a->ac, a->s, a->e, a->bins, counts); (*a->locals)[i].swap(counts); } return NULL; };
    std::vector<pthread_t> ths; ths.resize(n);
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, worker2, &b); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    std::vector<int> merged(bins, 0); for(int i=0;i<m_fileCnt;i++){ const std::vector<int>& loc=locals[i]; for(int j=0;j<(int)loc.size() && j<bins; j++){ merged[j] += loc[j]; } }
    json_out.clear(); json_out.append("["); bool first=true; for(int i=0;i<bins; i++){ if(!first) json_out.append(","); first=false; long long ts = start_ms + (long long)i * width; json_out.append("{\"ts\":"); json_out.append(std::to_string(ts)); json_out.append(",\"count\":"); json_out.append(std::to_string(merged[i])); json_out.append("}"); }
    json_out.append("]");
    return bins;
}

int LogDispatcher::GetMatchedTimeRange(char *args[MAX_CMD_ARG_COUNT], int argCount, long long& tmin, long long& tmax)
{
    tmin = LLONG_MAX; tmax = LLONG_MIN;
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    struct C { LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; long long* tmin; long long* tmax; } c;
    c.stores=m_logStores; c.fileCnt=m_fileCnt; c.nextIdx=&nextIdx; c.args=args; c.ac=argCount; c.tmin=&tmin; c.tmax=&tmax;
    auto worker3 = [](void* p)->void*{ C* a=(C*)p; while(true){ int i=a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; LogStoreApi* ls=a->stores[i]; long long lmin=LLONG_MAX, lmax=LLONG_MIN; int r=ls->GetMatchedTimeRange(a->args, a->ac, lmin, lmax); if(r>0){ if(lmin < *(a->tmin)) *(a->tmin) = lmin; if(lmax > *(a->tmax)) *(a->tmax) = lmax; } } return NULL; };
    std::vector<pthread_t> ths; ths.resize(n);
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, worker3, &c); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    if(tmin==LLONG_MAX) return 0; return 1;
}

int LogDispatcher::Timechart_BySpan_Group_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, long long span_ms, const std::string& groupAlias, std::string& json_out)
{
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    std::vector< std::map<std::string, std::map<long long,int> > > locals(m_fileCnt);
    struct A{ LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; long long span; const std::string* grp; std::vector< std::map<std::string, std::map<long long,int> > >* locals; } a;
    a.stores=m_logStores; a.fileCnt=m_fileCnt; a.nextIdx=&nextIdx; a.args=args; a.ac=argCount; a.span=span_ms; a.grp=&groupAlias; a.locals=&locals;
    auto worker = [](void* p)->void*{ A* a=(A*)p; while(true){ int i=a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; LogStoreApi* ls=a->stores[i]; std::map<std::string, std::map<long long,int> > gm; ls->Timechart_Count_BySpan_Group(a->args, a->ac, a->span, *a->grp, gm); (*a->locals)[i].swap(gm); } return NULL; };
    std::vector<pthread_t> ths; ths.resize(n);
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, worker, &a); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    std::map<std::string, std::map<long long,int> > merged; for(int i=0;i<m_fileCnt;i++){ for(auto &kv: locals[i]){ std::map<long long,int>& dst=merged[kv.first]; for(auto &kv2: kv.second){ dst[kv2.first] += kv2.second; } } }
    json_out.clear(); json_out.append("["); bool firstG=true; for(auto &gkv: merged){ if(!firstG) json_out.append(","); firstG=false; json_out.append("{\"key\":\""); const std::string& k=gkv.first; for(size_t j=0;j<k.size();j++){ char c=k[j]; if(c=='\"'||c=='\\'){ json_out.push_back('\\'); json_out.push_back(c);} else { json_out.push_back(c);} } json_out.append("\",\"points\":["); bool first=false; std::vector<std::pair<long long,int> > vec; vec.reserve(gkv.second.size()); for(auto &kv: gkv.second){ vec.push_back(kv);} std::sort(vec.begin(), vec.end(), [](const std::pair<long long,int>& a, const std::pair<long long,int>& b){ return a.first < b.first; }); for(size_t i2=0;i2<vec.size();i2++){ if(first){ json_out.append(","); } first=true; json_out.append("{\"ts\":"); json_out.append(std::to_string(vec[i2].first)); json_out.append(",\"count\":"); json_out.append(std::to_string(vec[i2].second)); json_out.append("}"); } json_out.append("]}"); }
    json_out.append("]");
    return (int)merged.size();
}

int LogDispatcher::Timechart_ByBins_Group_JSON(char *args[MAX_CMD_ARG_COUNT], int argCount, long long start_ms, long long end_ms, int bins, const std::string& groupAlias, std::string& json_out)
{
    if(end_ms<=start_ms || bins<=0){ json_out = "[]"; return 0; }
    long long width=(end_ms-start_ms)/bins; if(width<=0) width=1;
    std::atomic<int> nextIdx(0);
    int n = m_fileCnt < MAX_THREAD_PARALLEL? m_fileCnt : MAX_THREAD_PARALLEL;
    std::vector< std::map<std::string, std::vector<int> > > locals(m_fileCnt);
    struct B{ LogStoreApi** stores; int fileCnt; std::atomic<int>* nextIdx; char** args; int ac; long long s; long long e; int bins; const std::string* grp; std::vector< std::map<std::string, std::vector<int> > >* locals; } b;
    b.stores=m_logStores; b.fileCnt=m_fileCnt; b.nextIdx=&nextIdx; b.args=args; b.ac=argCount; b.s=start_ms; b.e=end_ms; b.bins=bins; b.grp=&groupAlias; b.locals=&locals;
    auto worker2 = [](void* p)->void*{ B* a=(B*)p; while(true){ int i=a->nextIdx->fetch_add(1); if(i>=a->fileCnt) break; LogStoreApi* ls=a->stores[i]; std::map<std::string, std::vector<int> > gm; ls->Timechart_Count_ByBins_Group(a->args, a->ac, a->s, a->e, a->bins, *a->grp, gm); (*a->locals)[i].swap(gm); } return NULL; };
    std::vector<pthread_t> ths; ths.resize(n);
    for(int i=0;i<n;i++){ pthread_create(&ths[i], NULL, worker2, &b); }
    for(int i=0;i<n;i++){ void* rv=NULL; pthread_join(ths[i], &rv); }
    std::map<std::string, std::vector<int> > merged; for(int i=0;i<m_fileCnt;i++){ for(auto &kv: locals[i]){ std::vector<int>& dst=merged[kv.first]; if((int)dst.size()<bins) dst.resize(bins,0); const std::vector<int>& src=kv.second; for(int j=0;j<bins && j<(int)src.size(); j++){ dst[j] += src[j]; } } }
    json_out.clear(); json_out.append("["); bool firstG=true; for(auto &gkv: merged){ if(!firstG) json_out.append(","); firstG=false; json_out.append("{\"key\":\""); const std::string& k=gkv.first; for(size_t j=0;j<k.size();j++){ char c=k[j]; if(c=='\"'||c=='\\'){ json_out.push_back('\\'); json_out.push_back(c);} else { json_out.push_back(c);} } json_out.append("\",\"points\":["); bool first=false; for(int bi=0; bi<bins; bi++){ if(first){ json_out.append(","); } first=true; long long ts = start_ms + (long long)bi * width; json_out.append("{\"ts\":"); json_out.append(std::to_string(ts)); json_out.append(",\"count\":"); json_out.append(std::to_string(gkv.second[bi])); json_out.append("}"); } json_out.append("]}"); }
    json_out.append("]");
    return (int)merged.size();
}



