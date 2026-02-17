CXX      = g++
CXXFLAGS = -std=c++20 -Wall -Wextra -Iinclude
SRCS     = src/crc32.cpp src/wal.cpp src/vlog.cpp src/sstable.cpp src/memtable.cpp src/manifest.cpp src/compaction.cpp src/vlog_gc.cpp src/bloom.cpp src/benchmark.cpp src/cli.cpp src/kvstore.cpp main.cpp
TARGET   = stdb

ifeq ($(OS),Windows_NT)
	TARGET := $(TARGET).exe
	RM = del /Q
	RMDIR = rmdir /S /Q
else
	RM = rm -f
	RMDIR = rm -rf
endif

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	$(RM) $(TARGET) test_wal.bin
	$(RMDIR) test_stdb 2>nul || true

.PHONY: all clean
