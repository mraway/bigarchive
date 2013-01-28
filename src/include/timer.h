#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
using namespace std;

class Timer {
private:
    timeval startTime;

public:

    void start(){
	//startTime 
        gettimeofday(&startTime, NULL);
    }

    double stop(){
        timeval endTime;
        long seconds, useconds;
        double duration;
        gettimeofday(&endTime, NULL);
        seconds  = endTime.tv_sec  - startTime.tv_sec;
        useconds = endTime.tv_usec - startTime.tv_usec;
        duration = seconds + useconds/1000000.0;
        return duration * 1000;
    }

    static void printTime(double d){
        printf("%5.6f milliseconds\n", d);
    }
};
