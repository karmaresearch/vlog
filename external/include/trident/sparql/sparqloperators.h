#ifndef SPARQL_OPERATOR_H
#define SPARQL_OPERATOR_H


#include <trident/sparql/joins.h>
#include <trident/iterators/tupleiterators.h>
#include <trident/model/table.h>
#include <trident/model/tuple.h>

#include <trident/kb/kb.h>

#include <cctype>
#include <inttypes.h>
#include <vector>

typedef enum { SCAN, NESTEDMERGEJOIN, HASHJOIN } Op;
class SPARQLOperator {
public:

    virtual Op getType() = 0;

    virtual TupleIterator *getIterator() = 0;

    virtual void releaseIterator(TupleIterator *itr) = 0;

    virtual size_t getOutputTupleSize() = 0;

    virtual std::vector<string> getTupleFieldsIDs() = 0;

    virtual void print(int indent) = 0;

    virtual bool doesSupportsSideways() {
        return false;
    }

    virtual void optimize(std::vector<uint8_t> *posBindings,
                          std::vector<uint64_t> *valueBindings) {
    }

    virtual TupleIterator *getIterator(
        std::vector<uint8_t> &positions, std::vector<uint64_t> &values) {
        BOOST_LOG_TRIVIAL(error) << "Not supported";
        throw 10;
    }

    virtual ~SPARQLOperator() {}
};

class Join : public SPARQLOperator {
private:
    std::vector<string> fields;

protected:
    std::vector<std::shared_ptr<SPARQLOperator>> children;
    std::shared_ptr<JoinPlan> plan;

public:
    Join(std::vector<std::shared_ptr<SPARQLOperator>> children);

    Join(std::vector<std::shared_ptr<SPARQLOperator>> children,
         std::vector<string> &projections);

    size_t getOutputTupleSize() {
        return fields.size();
    }

    std::vector<string> getTupleFieldsIDs() {
        return fields;
    }

    std::vector<std::shared_ptr<SPARQLOperator>> getChildren() {
        return children;
    }
};

class NestedMergeJoin : public Join {
private:
    Querier *q;
    std::shared_ptr<NestedJoinPlan> nestedPlan;

public:
    NestedMergeJoin(Querier *q,
                    std::vector<std::shared_ptr<SPARQLOperator>> children);

    NestedMergeJoin(Querier *q,
                    std::vector<std::shared_ptr<SPARQLOperator>> children,
                    std::vector<string> &projections);

    NestedMergeJoin(NestedMergeJoin &exiting,
                    std::vector<string> &projections);

    Op getType() {
        return NESTEDMERGEJOIN;
    }

    TupleIterator *getIterator();

    void releaseIterator(TupleIterator *itr);

    void print(int indent);
};

class TridentHashJoin : public Join {
public:
    TridentHashJoin(std::vector<std::shared_ptr<SPARQLOperator>> children);

    TridentHashJoin(std::vector<std::shared_ptr<SPARQLOperator>> children,
             std::vector<string> &projections);

    Op getType() {
        return HASHJOIN;
    }

    TupleIterator *getIterator();

    void releaseIterator(TupleIterator *itr);

    void print(int indent);
};

class Scan : public SPARQLOperator {
private:
    std::vector<string> fields;
    Pattern *pattern;

public:
    virtual TupleIterator *getIterator() = 0;

    virtual long estimateCost() = 0;

    Scan(Pattern *pattern);

    Pattern *getPattern() {
        return pattern;
    }

    Op getType() {
        return SCAN;
    }

    size_t getOutputTupleSize() {
        return fields.size();
    }

    std::vector<string> getTupleFieldsIDs() {
        return fields;
    }

    void print(int indent);
};

class KBScan : public Scan {
private:
    Querier *q;
    Tuple t;

public:
    KBScan(Querier *q, Pattern *p);

    TupleIterator *getIterator();

    long estimateCost();

    TupleIterator *getSampleIterator();

    void releaseIterator(TupleIterator *itr);
};

/*class MaterializedScan : public Scan {
private:

    std::shared_ptr<TupleTable> table;

public:
    MaterializedScan(std::shared_ptr<SemiNaiver> sn, Pattern *pattern,
                     Program *program);

    TupleIterator *getIterator();

    long estimateCost();

    TupleIterator *getSampleIterator();

    void releaseIterator(TupleIterator *itr);
};


class EDBLayer;
class DictMgmt;
class ReasoningScan : public Scan {
protected:
    Pattern *pattern;
    EDBLayer *layer;
    Program *program;
    DictMgmt *dict;

    Reasoner reasoner;
    ReasoningMode mode;

public:
    ReasoningScan(Pattern *pattern, EDBLayer *layer, Program *program,
                  DictMgmt *dict, const uint64_t reasoningThreshold);

    virtual TupleIterator *getIterator();

    void optimize(std::vector<uint8_t> *posBindings,
                  std::vector<uint64_t> *valueBindings);

    long estimateCost();

    virtual TupleIterator *getIterator(std::vector<uint8_t> &positions,
                                       std::vector<uint64_t> &values);

    bool doesSupportsSideways() {
        return true;
    }

    void releaseIterator(TupleIterator *itr);
};*/
#endif
