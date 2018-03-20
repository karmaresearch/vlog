#ifndef _VLOG_LAUNCHER_H
#define _VLOG_LAUNCHER_H

#include <unordered_map>

#include <vlog/edb.h>
#include <vlog/reasoner.h>
#include <vlog/consts.h>

#include <dblayer.hpp>

class VLogLayer : public DBLayer {
    private:
        EDBLayer &edb;
        Program &p;
        Reasoner reasoner;
        const Predicate predQueries;
        const Predicate edbPredName;
        char tmpText[MAX_TERM_SIZE];
        unordered_map<VTuple, double, hash_VTuple> edbCardinalities;
        unordered_map<VTuple, double, hash_VTuple> idbCardinalities;
        VLIBEXP void init();

    public:
        VLogLayer(EDBLayer &edb, Program &p, uint64_t threshold,
                string predname, string edbpredname) : edb(edb), p(p),
        reasoner(threshold), predQueries(p.getPredicate(predname)),
        edbPredName(p.getPredicate(edbpredname)) {
            init();
        }

        VLIBEXP bool lookup(const std::string& text,
                ::Type::ID type,
                unsigned subType,
                uint64_t& id);

        VLIBEXP bool lookupById(uint64_t id,
                const char*& start,
                const char*& stop,
                ::Type::ID& type,
                unsigned& subType);

        VLIBEXP bool lookupById(uint64_t id,
                char* start,
                size_t& len,
                ::Type::ID& type,
                unsigned& subType);

        VLIBEXP uint64_t getNextId();

        VLIBEXP double getScanCost(DBLayer::DataOrder order,
                uint64_t value1,
                uint64_t value1C,
                uint64_t value2,
                uint64_t value2C,
                uint64_t value3,
                uint64_t value3C);

        VLIBEXP double getScanCost(DBLayer::DataOrder order,
                uint64_t value1,
                uint64_t value1C,
                uint64_t value2,
                uint64_t value2C);

        VLIBEXP double getScanCost(DBLayer::DataOrder order,
                uint64_t value1,
                uint64_t value1C);

        VLIBEXP double getJoinSelectivity(bool valueL1,
                uint64_t value1CL,
                bool value2L,
                uint64_t value2CL,
                bool value3L,
                uint64_t value3CL,
                bool value1R,
                uint64_t value1CR,
                bool value2R,
                uint64_t value2CR,
                bool value3R,
                uint64_t value3CR);

        VLIBEXP uint64_t getCardinality(uint64_t c1,
                uint64_t c2,
                uint64_t c3);

        VLIBEXP uint64_t getCardinality(VTuple tuple);

        VLIBEXP uint64_t getCardinality();

        VLIBEXP std::unique_ptr<DBLayer::Scan> getScan(const DBLayer::DataOrder order,
                const DBLayer::Aggr_t aggr,
                DBLayer::Hint *hint);
};

#endif
