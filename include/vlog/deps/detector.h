#ifndef _DETECTOR_H
#define _DETECTOR_H

#include <vlog/deps/equivalence.h>
#include <vlog/deps/mapping.h>

#include <vlog/edb.h>
#include <vlog/concepts.h>

#include <vector>

class Detector {
    private:
        void getAllTerminals(
                std::vector<Literal> &out,
                const Literal &l,
                Program &p,
                EDBLayer &layer);

    public:
        std::vector<Equivalence> getDatabaseDependencies(Program &prog, EDBLayer &layer);

        void printDatabaseDependencies(Program &prog, EDBLayer &layer);
};

#endif
