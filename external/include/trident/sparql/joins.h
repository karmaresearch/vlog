#ifndef JOINS_H_
#define JOINS_H_

#include <trident/sparql/joinplan.h>
#include <trident/sparql/resultprinter.h>

#include <trident/kb/consts.h>
#include <trident/iterators/pairitr.h>
#include <trident/kb/querier.h>
#include <trident/model/table.h>

#include <boost/chrono.hpp>

#include <iostream>
#include <algorithm>

#define JOIN_FAILED 0
#define JOIN_SUCCESSFUL 1
#define NOMORE_JOIN 3

namespace timens = boost::chrono;

class NestedMergeJoinItr : public TupleIterator {
private:
    Querier *q;
    std::shared_ptr<NestedJoinPlan> plan;

    long compressedRow[MAX_N_PATTERNS];
    PairItr *iterators[MAX_N_PATTERNS];

    PairItr *getFirstIterator(Pattern p);

    void cleanup();

    int executeJoin(long *row, Pattern *patterns, int idxPattern,
                    PairItr **iterators, JoinPoint *joins, const int nJoins);

    static int try_merge_join(PairItr **iterators, int idxCurrentPattern,
                              const JoinPoint *joins, const int nJoins);

    void init(PairItr *firstIterator, TupleTable *outputR, long limitOutputTuple);

    long executePlan();

    //Fields used during the execution of the query
    PairItr *currentItr;

    //long nElements;
    JoinPoint **allJoins;
    int *nJoins;
    int idxCurrentPattern;
    int *currentVarsPos;
    int nCurrentVarsPos;

    const int *startingVarPerPattern;
    int idxLastPattern;
    int idxCurrentRow;

    int *varsToReturn;
    int sVarsToReturn;
    int maxTuplesInBuffer;
    int outputTuples;

    //Used to scroll through the results
    TupleTable *outputResults;
    int remainingInBuffer;
    const uint64_t *currentBuffer;
    bool deleteOutputResults;

    //const Timeout * const timeout;

public:
    NestedMergeJoinItr(Querier *q, std::shared_ptr<NestedJoinPlan> plan) {
        this->q = q;
        this->plan = plan;
        init(getFirstIterator(plan->patterns[0]), NULL, 0);
    }

    NestedMergeJoinItr(Querier *q, std::shared_ptr<NestedJoinPlan> plan,
                       PairItr *firstIterator,
                       TupleTable *outputR, long limitOutputTuple) {
        this->q = q;
        this->plan = plan;
        init(firstIterator, outputR, limitOutputTuple);
    }

    bool hasNext();

    void next();

    size_t getTupleSize();

    uint64_t getElementAt(const int pos);

    virtual ~NestedMergeJoinItr() {
        if (deleteOutputResults) {
            delete outputResults;
        }
    }
};

class SPARQLOperator;
typedef google::dense_hash_map<uint64_t, size_t, std::hash<uint64_t>, std::equal_to<uint64_t>> HashJoinMap;
typedef google::dense_hash_map<std::pair<uint64_t, uint64_t>, size_t, boost::hash<std::pair<uint64_t, uint64_t>>,
        std::equal_to<std::pair<uint64_t, uint64_t>>> DoubleHashJoinMap;
class HashJoinItr : public TupleIterator {
private:
    std::vector<std::shared_ptr<SPARQLOperator>> children;
    std::shared_ptr<JoinPlan> plan;

    TupleTable *output;
    int rowIdx;
    bool isComputed;

    void execJoin();

    void fillNextMap(const int i, std::vector<uint64_t> &currentMapValues,
                     int &currentMapRowSize,
                     std::unique_ptr<HashJoinMap> &currentMap1,
                     std::unique_ptr<DoubleHashJoinMap> &currentMap2,
                     std::vector<uint64_t> &tmpContainer,
                     const int tmpContainerRowSize,
                     std::vector<size_t> &idxRows);

public:
    HashJoinItr(std::vector<std::shared_ptr<SPARQLOperator>> children,
                std::shared_ptr<JoinPlan> plan) {
        assert(children.size() >= 2);
        output = NULL;
        rowIdx = -1;
        isComputed = false;
        this->children = children;
        this->plan = plan;
    }

    bool hasNext();

    void next();

    size_t getTupleSize();

    uint64_t getElementAt(const int pos);

    ~HashJoinItr();
};
#endif
