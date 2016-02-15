#include <trident/tests/common.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>

#include <boost/chrono.hpp>

TridentTimings::TridentTimings(string inputfile, string filequeries) :
    Timings(filequeries), q(NULL), inputfile(inputfile) {}

void TridentTimings::init() {
    kb = std::unique_ptr<KB>(new KB(inputfile.c_str(), true, false, true, config));
    q = kb->query();
}

boost::chrono::duration<double> TridentTimings::launchQuery(
    const int perm,
    const long s,
    const long p,
    const long o,
    const int countIgnores,
    long & c,
    long & junk) {

    PairItr *itr;
    timens::system_clock::time_point start = timens::system_clock::now();
    if (countIgnores == 2) {
        itr = q->getTermList(perm);
    } else {
        itr = q->get(perm, s, p, o);
        if (countIgnores == 1) {
            itr->ignoreSecondColumn();
        }
    }
    c = 0;
    while (itr->hasNext()) {
        itr->next();
        c++;
        junk += itr->getValue1();
        if (countIgnores == 0)
            junk += itr->getValue2();
    }
    boost::chrono::duration<double> dur = boost::chrono::system_clock::now()
                                          - start;
    q->releaseItr(itr);
    return dur;
}
