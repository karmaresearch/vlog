#ifndef _COMMON_TESTS_H
#define _COMMON_TESTS_H

#include <trident/tests/timings.h>

#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <trident/iterators/pairitr.h>

struct _Triple {
    long s, p, o;
    _Triple(long s, long p, long o) {
        this->s = s;
        this->p = p;
        this->o = o;
    }
};

void _reorderTriple(int perm, PairItr *itr, long triple[3]);

bool _less_spo(const _Triple &p1, const _Triple &p2);
bool _less_sop(const _Triple &p1, const _Triple &p2);
bool _less_ops(const _Triple &p1, const _Triple &p2);
bool _less_osp(const _Triple &p1, const _Triple &p2);
bool _less_pso(const _Triple &p1, const _Triple &p2);
bool _less_pos(const _Triple &p1, const _Triple &p2);

void _copyCurrentFirst(int perm, long triple[3], long v);
void _copyCurrentFirstSecond(int perm, long triple[3], long v1, long v2);

void _testKB(string inputfile, KB *kb);
void _test_createqueries(string inputfile, string queryfile);


class TridentTimings : public Timings {
private:
    Querier *q;
    KBConfig config;
    std::unique_ptr<KB> kb;
    string inputfile;

public:
    TridentTimings(string inputfile, string filequeries);

    void init();

    boost::chrono::duration<double> launchQuery(const int perm,
            const long s,
            const long p,
            const long o,
            const int countIgnores,
            long &c,
            long &junk);

    ~TridentTimings() {
        if (q != NULL)
            delete q;
    }
};


#endif
