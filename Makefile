#CONDA_PREFIX := /sdf/group/lcls/ds/ana/sw/conda2/inst/envs/xpp_drp_cpu_311_dev
CONDA_PREFIX := /sdf/group/lcls/ds/ana/sw/conda_bld/kmecseki/.conda/envs/ejfat-dev
EJFAT_PREFIX := /sdf/home/k/kmecseki/projects/EJFat
GRPC_PREFIX := /sdf/home/k/kmecseki/opt/grpc2

#BOOST_PREFIX := /sdf/scratch/users/k/kmecseki/boost

CONDA_LIBS := $(CONDA_PREFIX)/lib
#BOOST_LIBS := $(BOOST_PREFIX)/lib
GRPC_LIBS := $(GRPC_PREFIX)/lib
GRPC_LIBS2 := $(GRPC_PREFIX)/lib64
EJFAT_LIBS := $(EJFAT_PREFIX)/builddir/src

CONDA_INC := $(CONDA_PREFIX)/include
#BOOST_INC := $(BOOST_PREFIX)/include
GRPC_INC := $(GRPC_PREFIX)/include
EJFAT_INC := $(EJFAT_PREFIX)/include
RECEIVER_INC := $(EJFAT_PREFIX)/builddir/src

CXX := g++
CXXFLAGS := -std=c++20 -Wall -Wextra -Wl,-rpath -g -O3 -MMD -MP -I$(GRPC_INC) -I$(EJFAT_INC) -I$(CONDA_INC) -I$(RECEIVER_INC)

HAS_LIBURING := $(shell PKG_CONFIG_PATH=$(CONDA_PREFIX)/lib/pkgconfig:$$PKG_CONFIG_PATH pkg-config --exists liburing && echo yes)

ifeq ($(HAS_LIBURING),yes)
    $(info Adding liburing)
	CXXFLAGS += -DLIBURING_AVAILABLE
endif

LDFLAGS := -L$(GRPC_LIBS) -L$(GRPC_LIBS2) -L$(CONDA_LIBS) -L$(EJFAT_PREFIX)/lib -L$(EJFAT_LIBS)
LDLIBS := -lzmq -pthread -le2sar -lgrpc -lgpr -lprotobuf -lgrpc++ -laddress_sorting -labsl_synchronization -lboost_thread -lboost_chrono -lboost_system -lboost_log -lboost_log_setup -lboost_url -luring

SRC_DIR := src
TEST_DIR := tests
BUILD_DIR := build

SRCS := $(wildcard $(SRC_DIR)/*.cpp)
OBJS := $(SRCS:src/%.cpp=build/%.o)

TEST_SRCS := $(wildcard $(TEST_DIR)/*.cpp)
TEST_OBJS := $(TEST_SRCS:$(TEST_DIR)/%.cpp=$(BUILD_DIR)/tests/%.o)

TARGET := lclstream-fastcache
TEST_TARGETS := $(TEST_SRCS:$(TEST_DIR)/%.cpp=%)
RECEIVER := receiver

FCOBJS := $(filter-out $(BUILD_DIR)/fastcache.o, $(OBJS))

all: $(TARGET) $(RECEIVER)

test: $(TEST_TARGETS)
	@for test in $(TEST_TARGETS); do \
		echo "=== Running $$test ==="; \
		./$$test || exit 1; \
	done
	@echo "=== All tests passed OK! ==="

build/receiver.o: src/receiver/receiver.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(RECEIVER): build/receiver.o
	$(CXX) $< $(LDFLAGS) $(LDLIBS) -lgpr -labsl_cord -labsl_cordz_info -labsl_log_internal_check_op -labsl_log_internal_message -labsl_log_internal_nullguard -labsl_strings -labsl_cordz_functions -o $@

$(TARGET): $(OBJS)
	$(CXX) $(OBJS) $(LDFLAGS) $(LDLIBS) -o $(TARGET)

%: $(FCOBJS) $(BUILD_DIR)/tests/%.o
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
