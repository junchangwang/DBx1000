CC=g++
CFLAGS= -g -std=c++17

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/ ./storage/bwtree/
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./storage/bwtree/ -I./storage/ARTOLC/
INCLUDE += -I./NB-UpBit/src

CFLAGS += $(INCLUDE) -D NOGRAPHITE=1
LDFLAGS = -L. -pthread -lrt -ljemalloc 
LDFLAGS += -L./NB-UpBit/build -lbitmap
LDFLAGS += -L/usr/lib/x86_64-linux-gnu -lboost_filesystem -lboost_program_options -lboost_system -lurcu -latomic -ltbb
LIBBITMAP = NB-UpBit/build/libbitmap.a

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
CPPS += ./storage/ARTOLC/Tree.cpp
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)


all: rundb

rundb: $(OBJS) $(LIBBITMAP)
	$(CC) -o $@ $^ $(LDFLAGS)

$(LIBBITMAP):
	git clone -b multi-threaded git@github.com:junchangwang/NB-UpBit.git
	cd NB-UpBit && ./build.sh

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp
	$(CC) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb $(OBJS) $(DEPS)