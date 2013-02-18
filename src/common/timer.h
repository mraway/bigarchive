#ifndef _TIMER_H_
#define _TIMER_H_

#include <stdlib.h>
#include <sys/time.h>
#include <map>
#include <log4cxx/logger.h>
#include <log4cxx/xml/domconfigurator.h>

using namespace std;
using namespace log4cxx;
using namespace log4cxx::xml;
using namespace log4cxx::helpers;

class Timer {
private:
    timeval start_time_;
    double duration_;	 // in milliseconds
    bool is_running_;

public:
    Timer();

    void Start();

    // stop and reset timer, return the total duration
    double Reset();

    // stop the timer and return the last duration
    double Stop();

    // return the current total duration
    double GetDuration();
};


class TimerPool {
private:
    static map<string, Timer> timer_map_;
    static LoggerPtr logger_;

public:
    static void Start(const string& timer_name);

    static double Stop(const string& timer_name);

    static double Reset(const string& time_name);

    // stop the timer, print the last duration and total duration
    static void Print(const string& timer_name);
    
    static void PrintAll();
};

#define TIMER_START()	\
    do {	\
    	string timer_name = __FILE__;	\
        timer_name += "::";	\
    	timer_name += __PRETTY_FUNCTION__;    \
    	TimerPool::Start(timer_name);               \
    } while (0)

#define TIMER_STOP()	\
    do {	\
    	string timer_name = __FILE__;	\
        timer_name += "::";	\
    	timer_name += __PRETTY_FUNCTION__;    \
        TimerPool::Stop(timer_name);	\
    } while (0)

#define TIMER_PRINT()	\
    do {	\
    	string timer_name = __FILE__;	\
        timer_name += "::";	\
    	timer_name += __PRETTY_FUNCTION__;    \
    	TimerPool::Print(timer_name);	\
    } while (0)

#define TIMER_PRINT_ALL()	\
    do {	\
    	TimerPool::PrintAll();	\
    } while (0)

#define MEASURE(statement)	\
    do {	\
    	string timer_name = __FILE__;	\
        timer_name += "::";	\
    	timer_name += #statement;	\
        TimerPool::Start(timer_name);	\
        statement;	\
        TimerPool::Stop(timer_name);	\
    } while (0)
        
#define MEASURE_AND_PRINT(statement)	\
    do {	\
    	string timer_name = __FILE__;	\
        timer_name += "::";	\
    	timer_name += #statement;	\
        TimerPool::Start(timer_name);	\
        statement;	\
        TimerPool::Print(timer_name);	\
    } while (0)

#endif	// _TIMER_H_
