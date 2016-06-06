NUM_TABLES     = -1
NUM_THREADS    = -1

CXX = g++
CXXFLAGS = -std=c++11 -Wall -MMD -fno-omit-frame-pointer -g 
LDFLAGS = -lpthread -lboost_system -lboost_thread -lgtest
LDTESTFLAG = 
OPTFLAG = -03 -funroll-loops

BUILD_DIR = bin
OBJ_DIR = $(BUILD_DIR)/objs
SRC_DIR = src
BASIC_OBJS := $(patsubst $(SRC_DIR)/%.cc, $(OBJ_DIR)/%.o, $(wildcard $(SRC_DIR)/*/*.cc))
BENCHMARK_SRC = $(SRC_DIR)/benchmarks
BENCHMARK_OBJS := $(patsubst $(BENCHMARK_SRC)/%.cc, $(OBJ_DIR)/benchmarks/%.o, $(wildcard $(BENCHMARK_SRC)/*/*.cc))
TEST_DIR = test
TEST_EXECS := $(patsubst $(TEST_DIR)/%.cc, $(BUILD_DIR)/test/%, $(wildcard $(TEST_DIR)/*.cc))

all: $(BENCHMARK_OBJS) $(TEST_EXECS)

rebuild : clean all

$(OBJ_DIR)/%.o : $(SRC_DIR)/%.cc
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(OBJ_DIR)/%/%.o : $(SRC_DIR)/%/%.cc
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR)/benchmarks/%.o : $(BENCHMARK_SRC)/%.cc $(BASIC_OBJS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@ 

$(BUILD_DIR)/test/% : $(TEST_DIR)/%.cc $(BASIC_OBJS) $(BENCHMARK_OBJS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -DNUM_TABLES_DEF=$(NUM_TABLES) -DNUM_THREADS_DEF=$(NUM_THREADS) $< -o $@ $(LDFLAGS) $(LDTESTFLAG) $(BASIC_OBJS) $(BENCHMARK_OBJS)

.PHONY : check clean rebuild

check : $(TEST_EXECS)
	for i in `find $(BUILD_DIR)/test/* -not -name "*.d"`; do ./$$i ; done

clean:
	rm -rf $(BUILD_DIR)/*

DEPFILES := $(wildcard $(BUILD_DIR)/*.d $(BUILD_DIR)/*/*.d $(BUILD_DIR)/*/*/*.d $(BUILD_DIR)/*/*/*/*.d)
ifneq ($(DEPFILES),)
	-include $(DEPFILES)
endif
