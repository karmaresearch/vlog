#ifndef _VLOG_LAUNCHER_H
#define _VLOG_LAUNCHER_H

#include <unordered_map>

#include <vlog/edb.h>
#include <vlog/reasoner.h>

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
        void init();

    public:
        VLogLayer(EDBLayer &edb, Program &p, uint64_t threshold,
                string predname, string edbpredname) : edb(edb), p(p),
        reasoner(threshold), predQueries(p.getPredicate(predname)),
        edbPredName(p.getPredicate(edbpredname)) {
            init();
        }

        bool lookup(const std::string& text,
                ::Type::ID type,
                unsigned subType,
                uint64_t& id);

        bool lookupById(uint64_t id,
                const char*& start,
                const char*& stop,
                ::Type::ID& type,
                unsigned& subType);

        bool lookupById(uint64_t id,
                char* start,
                size_t& len,
                ::Type::ID& type,
                unsigned& subType);


        uint64_t getNextId();

        double getScanCost(DBLayer::DataOrder order,
                uint64_t value1,
                uint64_t value1C,
                uint64_t value2,
                uint64_t value2C,
                uint64_t value3,
                uint64_t value3C);

        double getScanCost(DBLayer::DataOrder order,
                uint64_t value1,
                uint64_t value1C,
                uint64_t value2,
                uint64_t value2C);

        double getScanCost(DBLayer::DataOrder order,
                uint64_t value1,
                uint64_t value1C);

        double getJoinSelectivity(bool valueL1,
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

        uint64_t getCardinality(uint64_t c1,
                uint64_t c2,
                uint64_t c3);

        uint64_t getCardinality(VTuple tuple);

        uint64_t getCardinality();

        std::unique_ptr<DBLayer::Scan> getScan(const DBLayer::DataOrder order,
                const DBLayer::Aggr_t aggr,
                DBLayer::Hint *hint);
};

#endif
