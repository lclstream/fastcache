#PREFIX := /sdf/group/lcls/ds/ana/sw/conda2/inst/envs/ps_20241122
PREFIX := /sdf/group/lcls/ds/ana/sw/conda2/inst/envs/xpp_drp_cpu_311_dev

LIBS := $(PREFIX)/lib
INC := $(PREFIX)/include

CXX := g++
CXXFLAGS := -std=c++20 -Wall -Wextra -O3 -I$(INC) -march=native

LDFLAGS := -L$(LIBS)
LDLIBS := -lzmq -pthread

SRC_DIR := src
TEST_DIR := tests
BUILD_DIR := build

SRCS := $(wildcard $(SRC_DIR)/*.cpp)
OBJS := $(SRCS:src/%.cpp=build/%.o)

TEST_SRCS := $(wildcard $(TEST_DIR)/*.cpp)
TEST_OBJS := $(TEST_SRCS:$(TEST_DIR)/%.cpp=$(BUILD_DIR)/tests/%.o)

APP_LOGIC_OBJS := $(filter-out $(BUILD_DIR)/fastcache.o, $(OBJS))

TARGET := lclstream-fastcache
TEST_TARGETS := $(TEST_SRCS:$(TEST_DIR)/%.cpp=%)

all: $(TARGET)

test: $(TEST_TARGETS)
	@for test in $(TEST_TARGETS); do \
		echo "=== Running $$test ==="; \
		./$$test || exit 1; \
	done
	@echo "=== All tests done! ==="

$(TARGET): $(OBJS)
	$(CXX) $(OBJS) $(LDFLAGS) $(LDLIBS) -o $(TARGET)

%: $(APP_LOGIC_OBJS) $(BUILD_DIR)/tests/%.o
	$(CXX) $^ $(LDFLAGS) $(LDLIBS) -o $@

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR)/tests/%.o: $(TEST_DIR)/%.cpp | $(BUILD_DIR)/tests
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR) $(BUILD_DIR)/tests:
	mkdir -p $@

clean:
	rm -rf $(BUILD_DIR) $(TARGET) $(TEST_TARGET)

.PHONY: all clean test
