#ifndef _TIMINGS_H
#define _TIMINGS_H

#include <boost/chrono.hpp>
#include <string>

#define T_SPO 0
#define T_OPS 1
#define T_POS 2
#define T_SOP 3
#define T_OSP 4
#define T_PSO 5

class Timings {
private:
    std::string queryfile;

public:
    Timings(std::string filequeries) : queryfile(filequeries) {}

    virtual void init() = 0;

    virtual boost::chrono::duration<double> launchQuery(const int perm,
            const long s,
            const long p,
            const long o,
            const int countIgnores,
            long &c,
            long &junk) = 0;

    void launchTests();
};

#endif
