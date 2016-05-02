NUM_TABLES     = -1
NUM_THREADS    = -1

CXX = g++
CXXFLAGS = -std=c++14 -Wall -MMD -fno-omit-frame-pointer -g 
LDFLAGS = -lpthread -lboost_system -lboost_thread
LDTESTFLAG = -lcppunit
OPTFLAG = -03 -funroll-loops

BUILD_DIR = bin
OBJ_DIR = $(BUILD_DIR)/objs
SRC_DIR = src
BENCHMARK_SRC = $(SRC_DIR)/benchmarks
BASIC_OBJS = $(pathsubst $(SRC_DIR)/%/%.cc, $(OBJ_DIR)/%/%.o $(wildcard $(SRC_DIR)/*/*.cc))
TEST_DIR = test
TEST_EXECS = $(patsubst $(TEST_DIR)/%.cc, $(BUILD_DIR)/test/%, $(wildcard $(TEST_DIR)/*.cc))

$(BUILD_DIR)/%/%.o : $(SRC_DIR)/%/%.cc
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD_DIR)/test/% : $(TEST_DIR)/%.cc $(BASIC_OBJS)
	@mkdir -p $(@D)
	$(CXX) $(CXXFLAGS) -DNUM_TABLES_DEF=$(NUM_TABLES) -DNUM_THREADS_DEF=$(NUM_THREADS) $< -o $@ $(LDFLAGS) $(LDTESTFLAG)

.PHONY : check clean

check : $(TEST_EXECS)
	for i in `find $(BUILD_DIR)/test/* -not -name "*.d"`; do ./$$i ; done

clean:
	rm -rf $(BUILD_DIR)/*

DEPFILES := $(wildcard $(BUILD_DIR)/*.d $(BUILD_DIR)/*/*.d $(BUILD_DIR)/*/*/*.d $(BUILD_DIR)/*/*/*/*.d)
ifneq ($(DEPFILES),)
	-include $(DEPFILES)
endif
