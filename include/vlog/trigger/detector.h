#ifndef _DETECTOR_H
#define _DETECTOR_H

#include <vlog/trigger/equivalence.h>
#include <vlog/trigger/mapping.h>

#include <vlog/edb.h>
#include <vlog/concepts.h>

#include <vector>

class Detector {
    private:
        void getAllTerminals(
                std::vector<Literal> &out,
                const Literal &l,
                Program &p,
                EDBLayer &layer,
                std::vector<Literal> &cache,
                std::vector<Rule> &rules);

    public:
        std::vector<Equivalence> getDatabaseDependencies(Program &prog, EDBLayer &layer);

		VLIBEXP void printDatabaseDependencies(Program &prog, EDBLayer &layer);
};

#endif
