#ifndef __CY_TIMER__
#define __CY_TIMER__

#pragma once

#include <iostream>
#include <sys/time.h>

#include <chrono>
#include <thread>

namespace cy {

    struct Timer_t {
        inline uint64_t getChronoMicro() {      
            struct timeval start;
            gettimeofday(&start, NULL);
            // tv_sec = seconds | tv_usecs = microseconds
            return (start.tv_sec * 1000000LL) + start.tv_usec;
        }
        inline uint64_t getChronoMicro(uint64_t start) {      
            return getChronoMicro() - start;
        }
        inline uint64_t getChrono() {
            return getChronoMicro();
        }
        inline uint64_t getChrono(uint64_t start) {
            return getChronoMicro(start);
        } 

        inline void sleep_for(size_t seconds) {
            std::this_thread::sleep_for(std::chrono::seconds(seconds));
        }
    };

    /*
    std::ostream& operator<< (std::ostream& os, const Timer_t& t) {
        os << "Timer: " << &t << std::endl;
        return os;
    }
    */

}

#endif
