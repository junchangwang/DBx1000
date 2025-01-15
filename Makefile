CC=g++
CFLAGS= -g -O3 -std=gnu++17 -Wno-invalid-offsetof

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/ ./storage/bwtree/
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./storage/bwtree/ -I./storage/ARTOLC/
INCLUDE += -I../src

CFLAGS += $(INCLUDE) -D NOGRAPHITE=1
LDFLAGS = -L. -pthread -lrt -ljemalloc 
LDFLAGS += -L../build -lbitmap
LDFLAGS += -L/usr/lib/x86_64-linux-gnu -lboost_filesystem -lboost_program_options -lboost_system -lurcu -latomic -ltbb
LIBBITMAP = ../build/libbitmap.a

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
CPPS += ./storage/ARTOLC/Tree.cpp
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)


all: rundb

rundb: $(OBJS) $(LIBBITMAP)
	$(CC) -o $@ $^ $(LDFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp
	$(CC) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb $(OBJS) $(DEPS)
