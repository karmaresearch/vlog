#ifndef VLOG_HI_RES_TIMER_H__
#define VLOG_HI_RES_TIMER_H__

#include <string>
#include <sstream>
#include <chrono>

class HiResTimer {
    typedef std::chrono::duration<int, std::micro> us_type;
    typedef std::chrono::high_resolution_clock::time_point hi_res_time;
    typedef std::chrono::high_resolution_clock::duration hi_res_duration;

    std::string name;
    size_t ticks = 0;
    hi_res_time t_start;
    hi_res_duration t_accu = std::chrono::high_resolution_clock::duration::zero();

public:
    HiResTimer(const std::string &name) : name(name) {
    }

    void start() {
        t_start = std::chrono::high_resolution_clock::now();
    }

    void stop() {
        auto t_stop = std::chrono::high_resolution_clock::now();
        t_accu += t_stop - t_start;
        ticks++;
    }

    void reset() {
        ticks = 0;
        t_accu = std::chrono::high_resolution_clock::duration::zero();
    }

    std::ostream &tout(std::ostream &os) {
        os << name << " calls " << ticks;
        us_type u_accu(std::chrono::duration_cast<us_type>(t_accu));
        os << " total time " << u_accu.count() << "us";
        return os;
    }

    std::string tostring() {
        std::ostringstream oss;
        tout(oss);
        return oss.str();
    }
};

#endif  // ndef VLOG_HI_RES_TIMER_H__

