CXX      = g++
CXXFLAGS = -std=c++20 -Wall -Wextra -O3 -DNDEBUG -mcx16 -Iinclude
SRCS     = src/crc32.cpp src/wal.cpp src/vlog.cpp src/sstable.cpp src/arena.cpp src/skiplist.cpp src/memtable.cpp src/version_edit.cpp src/version_set.cpp src/compaction.cpp src/vlog_gc.cpp src/bloom.cpp src/benchmark.cpp src/kvstore.cpp src/thread_pool.cpp src/resp_server.cpp src/fault_injection.cpp main.cpp
OBJS     = $(SRCS:.cpp=.o)
TARGET   = acdb

ifeq ($(OS),Windows_NT)
	TARGET  := $(TARGET).exe
	RM       = del /Q
	RMDIR    = rmdir /S /Q
	LDFLAGS  = -lws2_32 -latomic
else
	RM       = rm -f
	RMDIR    = rm -rf
	LDFLAGS  = -latomic
endif

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

redis: $(TARGET)
	./$(TARGET) redis

clean:
	-$(RM) $(TARGET) test_wal.bin $(subst /,\,$(OBJS)) 2>nul
	-$(RMDIR) test_acdb 2>nul

.PHONY: all clean redis
