CXX = g++
CXXFLAGS = -std=c++11 -Wall -MMD -fno-omit-frame-pointer
LDFLAGS = -lpthread -lboost_system -lboost_thread -lgtest
LDTESTFLAG = -lcppunit 
OPTFLAG = -O2 -funroll-loops
BUILD_DIR = bin
OBJ_DIR = $(BUILD_DIR)/objs
SRC_DIR = src
BASIC_OBJS := $(patsubst $(SRC_DIR)/%.cc, $(OBJ_DIR)/%.o, $(wildcard $(SRC_DIR)/*/*.cc))
BENCHMARK_SRC = $(SRC_DIR)/benchmarks
BENCHMARK_OBJS := $(patsubst $(BENCHMARK_SRC)/%.cc, $(OBJ_DIR)/benchmarks/%.o, $(wildcard $(BENCHMARK_SRC)/*/*.cc))
BENCHMARK_DIR = experiment_benchmark
BENCHMARK_EXECS := $(patsubst $(BENCHMARK_DIR)/%.cc, $(BUILD_DIR)/$(BENCHMARK_DIR)/%, $(wildcard $(BENCHMARK_DIR)/*.cc))
TEST_DIR = test
TEST_EXECS := $(patsubst $(TEST_DIR)/%.cc, $(BUILD_DIR)/test/%, $(wildcard $(TEST_DIR)/*.cc))

all : $(BENCHMARK_OBJS) $(TEST_EXECS)

benchmark : $(BENCHMARK_OBJS) $(BENCHMARK_EXECS) 
	for i in `find $(BUILD_DIR)/$(BENCHMARK_DIR)/* -not -name "*.d"`; do chmod 755 $$i ; done

rebuild : clean all

$(OBJ_DIR)/%/%.o : $(SRC_DIR)/%/%.cc
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(OBJ_DIR)/%.o : $(SRC_DIR)/%.cc
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR)/benchmarks/%.o : $(BENCHMARK_SRC)/%.cc $(BASIC_OBJS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR)/$(BENCHMARK_DIR)/% : $(BENCHMARK_DIR)/%.cc $(BASIC_OBJS) $(BENCHMARK_OBJS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $< -o $@ $(BASIC_OBJS) $(BENCHMARK_OBJS) $(LDFLAGS) $(OPTFLAG)

$(BUILD_DIR)/$(TEST_DIR)/% : $(TEST_DIR)/%.cc $(BASIC_OBJS) $(BENCHMARK_OBJS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) $< -o $@ $(BASIC_OBJS) $(BENCHMARK_OBJS) $(LDFLAGS) $(LDTESTFLAG) $(OPTFLAG)

.PHONY : check clean rebuild

check : $(TEST_EXECS)
	for i in `find $(BUILD_DIR)/test/* -not -name "*.d"`; do ./$$i ; done

clean:
	rm -rf $(BUILD_DIR)/*

DEPFILES := $(wildcard $(BUILD_DIR)/*.d $(BUILD_DIR)/*/*.d $(BUILD_DIR)/*/*/*.d $(BUILD_DIR)/*/*/*/*.d)
ifneq ($(DEPFILES),)
	-include $(DEPFILES)
endif
