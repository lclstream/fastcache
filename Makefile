#PREFIX := /sdf/group/lcls/ds/ana/sw/conda2/inst/envs/ps_20241122
PREFIX := /sdf/group/lcls/ds/ana/sw/conda2/inst/envs/xpp_drp_cpu_311_dev

LIBS := $(PREFIX)/lib
INC := $(PREFIX)/include

CXX := g++
CXXFLAGS := -std=c++20 -Wall -Wextra -O2 -I$(INC) -g

LDFLAGS := -L$(LIBS)
LDLIBS := -lzmq -pthread

SRC_DIR := src
BUILD_DIR := build

SRCS := $(wildcard $(SRC_DIR)/*.cpp)
OBJS := $(SRCS:src/%.cpp=build/%.o)

TARGET := fastcache

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(OBJS) $(LDFLAGS) $(LDLIBS) -o $(TARGET)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR):
	mkdir -p $@

clean:
	rm -rf $(BUILD_DIR) $(TARGET)

.PHONY: all clean
