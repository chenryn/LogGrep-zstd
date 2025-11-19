#include "LogStore_API.h"
#include <iostream>
#include <map>
#include <string>

using namespace std;

int main()
{
    LogStoreApi api;
    api.Test_AddPattern("alpha , gamma", 1, 5);
    api.Test_AddPattern("delta value", 2, 3);
    api.Test_InitOutliersMeta(1);

    LISTBITMAPS bm1;
    api.Test_QuerySingle("alpha*gamma", bm1);
    BitMap* e1 = bm1[(1<<16)];
    BitMap* e2 = bm1[(2<<16)];
    cout << "A*B single: E1=" << (e1 && (e1->BeSizeFul() || e1->GetSize()>0)) << ", E2=" << (e2 && (e2->BeSizeFul() || e2->GetSize()>0)) << endl;

    char* args[MAX_CMD_ARG_COUNT];
    for(int i=0;i<MAX_CMD_ARG_COUNT;i++){ args[i]=new char[MAX_CMD_ARGSTR_LENGTH]; args[i][0]='\0'; }
    strcpy(args[0], "(");
    strcpy(args[1], "alpha*gamma");
    strcpy(args[2], ")");
    int argc = 3;

    LISTBITMAPS bm2;
    api.Test_QueryLogic(args, argc, bm2);
    BitMap* r1 = bm2[(1<<16)];
    BitMap* r2 = bm2[(2<<16)];
    cout << "Paren logic: E1=" << (r1 && (r1->BeSizeFul() || r1->GetSize()>0)) << ", E2=" << (r2 && (r2->BeSizeFul() || r2->GetSize()>0)) << endl;

    for(int i=0;i<MAX_CMD_ARG_COUNT;i++){ delete[] args[i]; }
    return 0;
}