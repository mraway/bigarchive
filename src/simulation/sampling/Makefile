.PHONY : all
all: sampling freq

sampling: sampling.o lru_cache.o
	g++ -o $@ $^

freq: freq.o lru_cache.o
	g++ -o $@ $^

%.o: %.cpp %.hpp
	g++ -c $<

%.o: %.cpp
	g++ -c $<

.PHONY : clean
clean:
	rm -f sampling freq *.o *~
