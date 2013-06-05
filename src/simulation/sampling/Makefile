.PHONY : all
all: sampling

sampling: sampling.o lru_cache.o
	g++ -o $@ $^

%.o: %.cpp %.hpp
	g++ -c $<

%.o: %.cpp
	g++ -c $<

.PHONY : clean
clean:
	rm -f sampling *.o *~
