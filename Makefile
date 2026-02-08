CXX      = g++
CXXFLAGS = -std=c++20 -Wall -Wextra -Iinclude
SRCS     = src/crc32.cpp src/wal.cpp src/memtable.cpp src/kvstore.cpp main.cpp
TARGET   = stdb

ifeq ($(OS),Windows_NT)
	TARGET := $(TARGET).exe
	RM = del /Q
else
	RM = rm -f
endif

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	$(RM) $(TARGET) test_wal.bin

.PHONY: all clean

// partial state 8097
