CXX = g++
CXXFLAGS = -std=c++20 -Wall -Wextra -O2
SRC = main.cpp
OUT = a.out
ARGS ?=

.PHONY: run fmt clean

run: $(OUT)
	./$(OUT) $(ARGS)

$(OUT): $(SRC)
	$(CXX) $(CXXFLAGS) $(SRC) -o $(OUT)

fmt:
	clang-format -i $(SRC)

clean:
	@rm -f $(OUT)
