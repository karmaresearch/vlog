#ifndef _VLOG_SCAN_H
#define _VLOG_SCAN_H

#include <dblayer.hpp>

#include <vlog/edb.h>
#include <vlog/concepts.h>
#include <vlog/reasoner.h>

class VLogScan : public DBLayer::Scan {
private:
    const DBLayer::DataOrder order;
    const DBLayer::Aggr_t aggr;
    DBLayer::Hint *hint;
    EDBLayer &layer;
    Program &p;
    Reasoner *r;
    Predicate predQuery;
    uint8_t value1_index;
    uint8_t value2_index;
    uint8_t value3_index;

    std::unique_ptr<TupleIterator> iterator;

    Literal getLiteral(DBLayer::DataOrder order, uint64_t first, bool constrained1,
                       uint64_t second, bool constrained2, uint64_t third,
                       bool constrained3);

public:
    VLogScan(const DBLayer::DataOrder order,
             const DBLayer::Aggr_t aggr,
             DBLayer::Hint *hint,
             Predicate predQuery,
             EDBLayer &layer,
             Program &p,
             Reasoner *r) : order(order), aggr(aggr),
        hint(hint), layer(layer),
        p(p), r(r), predQuery(predQuery) {
        switch (order) {
        case DBLayer::Order_No_Order_SPO:
        case DBLayer::Order_Subject_Predicate_Object:
            value1_index = 0;
            value2_index = 1;
            value3_index = 2;
            break;
        case DBLayer::Order_No_Order_SOP:
        case DBLayer::Order_Subject_Object_Predicate:
            value1_index = 0;
            value2_index = 2;
            value3_index = 1;
            break;
        case DBLayer::Order_No_Order_PSO:
        case DBLayer::Order_Predicate_Subject_Object:
            value1_index = 1;
            value2_index = 0;
            value3_index = 2;
            break;
        case DBLayer::Order_No_Order_OSP:
        case DBLayer::Order_Object_Subject_Predicate:
            value1_index = 2;
            value2_index = 0;
            value3_index = 1;
            break;
        case DBLayer::Order_No_Order_POS:
        case DBLayer::Order_Predicate_Object_Subject:
            value1_index = 1;
            value2_index = 2;
            value3_index = 0;
            break;
        case DBLayer::Order_No_Order_OPS:
        case DBLayer::Order_Object_Predicate_Subject:
            value1_index = 2;
            value2_index = 1;
            value3_index = 0;
            break;
        default:
            throw 10;
        }
    }

    uint64_t getValue1();

    uint64_t getValue2();

    uint64_t getValue3();

    uint64_t getCount();

    bool next();

    bool first();

    bool first(uint64_t, bool);

    bool first(uint64_t, bool, uint64_t, bool);

    bool first(uint64_t, bool, uint64_t, bool, uint64_t, bool);

};

#endif
