####### Output directory
FILE_DIR = 
TEMP_DIR = 
REFER_DIR = ../compression/
#REFER2_DIR = ../LogGrep_compression_zstd/LZMA/
LIBDIR =../zstd-dev/lib
LIB = $(LIBDIR)/libzstd.a

EXE_DIR = ../output/
#######################
RM	= rm -fr
DEL_FILE	= rm -f

CXX	= g++ -arch x86_64
CPPFLAGS = -std=c++11 -w -c -g -Wall -ggdb -arch x86_64
#CPPFLAGS = -O2 -w -c -g
LANGUAGE = -D	ENGLISH
READLINE = -D	READLINE_LIBRARY
##LANGUAGE = -D	CHINESE

PEGTL_INC =
PEGTL_FLAGS =
ifneq ($(PEGTL_INC),)
PEGTL_FLAGS = -DUSE_PEGTL -I$(PEGTL_INC)
endif

BIN_OBJECT	= thulr_cmdline
#LZMALIB_OBJECT = liblzma.a

AR	= ar -rc

####### Files

SRC_OBJECTS	=	$(FILE_DIR)BaseCmd.cpp \
	$(FILE_DIR)CmdDefine.cpp \
	$(FILE_DIR)CmdInvoker.cpp \
	$(FILE_DIR)CmdManager.cpp \
	$(FILE_DIR)CmdOperator.cpp \
	$(FILE_DIR)ConcreteCmd.cpp \
	$(FILE_DIR)CmdLineTool.cpp \
	$(FILE_DIR)LogStructure.cpp\
	$(FILE_DIR)SearchAlgorithm.cpp\
	$(FILE_DIR)LogStore_API.cpp\
	$(FILE_DIR)LogDispatcher.cpp\
	$(FILE_DIR)var_alias.cpp\
	$(FILE_DIR)StatisticsAPI.cpp\
	$(REFER_DIR)Coffer.cpp\
	$(REFER_DIR)TimeParser.cpp\

OBJECTS=	$(TEMP_DIR)BaseCmd.o \
	$(TEMP_DIR)CmdDefine.o \
	$(TEMP_DIR)CmdInvoker.o \
	$(TEMP_DIR)CmdManager.o \
	$(TEMP_DIR)CmdOperator.o \
	$(TEMP_DIR)ConcreteCmd.o \
	$(TEMP_DIR)CmdLineTool.o \
	$(TEMP_DIR)LogStructure.o\
	$(TEMP_DIR)SearchAlgorithm.o \
	$(TEMP_DIR)LogStore_API.o\
	$(TEMP_DIR)LogDispatcher.o\
	$(TEMP_DIR)var_alias.o\
	$(TEMP_DIR)StatisticsAPI.o\
	$(TEMP_DIR)Coffer.o\
	$(TEMP_DIR)TimeParser.o\
		
H_OBJECTS	=	$(FILE_DIR)BaseCmd.h \
	$(FILE_DIR)CmdDefine.h \
	$(FILE_DIR)CmdInvoker.h \
	$(FILE_DIR)CmdManager.h \
	$(FILE_DIR)CmdOperator.h \
	$(FILE_DIR)ConcreteCmd.h \
	$(FILE_DIR)LogStructure.h\
	$(FILE_DIR)SearchAlgorithm.h\
	$(FILE_DIR)LogStore_API.h\
	$(FILE_DIR)LogDispatcher.h\
	$(REFER_DIR)Coffer.h\


####### Compile
#######-l dl: dlopen dlerror dlclose dlsym
######-l pthread: pthread_create
######-l rt:realtime

all: APP

APP: $(BIN_OBJECT)

$(BIN_OBJECT):$(OBJECTS)
	$(CXX) -o  $(BIN_OBJECT) $(OBJECTS) $(LIB) -l dl
	cp $(BIN_OBJECT) $(EXE_DIR)

$(OBJECTS):$(SRC_OBJECTS) $(H_OBJECTS)
	$(CXX) $(CPPFLAGS) $(SRC_OBJECTS)

server_stub: thulr_server_stub
server: thulr_server
server_real: thulr_server

thulr_server_stub: $(TEMP_DIR)LogStructure.o $(TEMP_DIR)SearchAlgorithm.o $(TEMP_DIR)LogStore_API.o \
    $(TEMP_DIR)LogDispatcher.o $(TEMP_DIR)StatisticsAPI.o $(TEMP_DIR)var_alias.o $(TEMP_DIR)CmdDefine.o $(TEMP_DIR)Coffer.o ServerMain.cpp Ingestor.cpp \
    ../compression/Encoder.cpp ../compression/LengthParser.cpp ../compression/template.cpp \
    ../compression/union.cpp ../compression/SubPattern.cpp ../compression/util.cpp \
    ../compression/sampler.cpp ../compression/TimeParser.cpp
	$(CXX) -std=c++11 $(PEGTL_FLAGS) -DLOGGREP_NO_MAIN -DLOGGREP_LOCAL_STUB -o thulr_server_stub ServerMain.cpp SPLParser.cpp Ingestor.cpp \
        $(TEMP_DIR)LogStructure.o $(TEMP_DIR)SearchAlgorithm.o $(TEMP_DIR)LogStore_API.o \
        $(TEMP_DIR)LogDispatcher.o $(TEMP_DIR)StatisticsAPI.o $(TEMP_DIR)var_alias.o $(TEMP_DIR)CmdDefine.o \
        $(TEMP_DIR)Coffer.o ../compression/Encoder.cpp ../compression/LengthParser.cpp \
        ../compression/template.cpp ../compression/union.cpp ../compression/SubPattern.cpp \
        ../compression/util.cpp ../compression/sampler.cpp \
        ../compression/TimeParser.cpp -I. -I../compression -I../zstd-dev/lib \
        $(LIB) -l dl

thulr_server: $(TEMP_DIR)LogStructure.o $(TEMP_DIR)SearchAlgorithm.o $(TEMP_DIR)LogStore_API.o \
    $(TEMP_DIR)LogDispatcher.o $(TEMP_DIR)StatisticsAPI.o $(TEMP_DIR)var_alias.o $(TEMP_DIR)CmdDefine.o $(TEMP_DIR)Coffer.o ServerMain.cpp Ingestor.cpp \
    ../compression/Encoder.cpp ../compression/LengthParser.cpp ../compression/template.cpp \
    ../compression/union.cpp ../compression/SubPattern.cpp ../compression/util.cpp \
    ../compression/sampler.cpp ../compression/TimeParser.cpp \
    ../compression/main.cpp
	$(CXX) -std=c++11 $(PEGTL_FLAGS) -DLOGGREP_NO_MAIN -o thulr_server ServerMain.cpp SPLParser.cpp Ingestor.cpp \
        $(TEMP_DIR)LogStructure.o $(TEMP_DIR)SearchAlgorithm.o $(TEMP_DIR)LogStore_API.o \
        $(TEMP_DIR)LogDispatcher.o $(TEMP_DIR)StatisticsAPI.o $(TEMP_DIR)var_alias.o $(TEMP_DIR)CmdDefine.o \
        $(TEMP_DIR)Coffer.o ../compression/Encoder.cpp ../compression/LengthParser.cpp \
        ../compression/template.cpp ../compression/union.cpp ../compression/SubPattern.cpp \
        ../compression/util.cpp ../compression/sampler.cpp \
        ../compression/TimeParser.cpp ../compression/main.cpp -I. -I../compression -I../zstd-dev/lib \
        $(LIB) -l dl

tests: test_ssh_simple test_ssh_statistics

test_ssh_simple: $(OBJECTS) test_ssh_simple.cpp
	$(CXX) -std=c++11 -o test_ssh_simple test_ssh_simple.cpp \
		$(TEMP_DIR)StatisticsAPI.o $(TEMP_DIR)LogStore_API.o \
		$(TEMP_DIR)LogStructure.o $(TEMP_DIR)SearchAlgorithm.o \
		$(TEMP_DIR)LogDispatcher.o $(TEMP_DIR)var_alias.o \
		$(TEMP_DIR)CmdDefine.o \
		$(TEMP_DIR)Coffer.o -I. -I../compression -I../zstd-dev/lib \
		$(LIB) -l dl

test_ssh_statistics: $(OBJECTS) test_ssh_statistics.cpp
	$(CXX) -std=c++11 -o test_ssh_statistics test_ssh_statistics.cpp \
		$(TEMP_DIR)StatisticsAPI.o $(TEMP_DIR)LogStore_API.o \
		$(TEMP_DIR)LogStructure.o $(TEMP_DIR)SearchAlgorithm.o \
		$(TEMP_DIR)LogDispatcher.o $(TEMP_DIR)var_alias.o \
		$(TEMP_DIR)CmdDefine.o \
		$(TEMP_DIR)Coffer.o -I. -I../compression -I../zstd-dev/lib \
		$(LIB) -l dl

test_logic_axb: $(OBJECTS) test_logic_axb.cpp
	$(CXX) -std=c++11 -o test_logic_axb test_logic_axb.cpp \
		$(TEMP_DIR)LogStore_API.o $(TEMP_DIR)LogStructure.o \
		$(TEMP_DIR)SearchAlgorithm.o $(TEMP_DIR)CmdDefine.o \
		$(TEMP_DIR)var_alias.o $(TEMP_DIR)Coffer.o \
		-I. -I../compression -I../zstd-dev/lib $(LIB) -l dl

.PHONY:clean

clean:
	$(RM) $(OBJECTS)
	
test_splparser: SPLParser.cpp SPLParser.h test_splparser.cpp
	$(CXX) -std=c++11 $(PEGTL_FLAGS) -o test_splparser test_splparser.cpp SPLParser.cpp
	
