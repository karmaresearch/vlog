#ifndef RULEEXECUTOR_H
#define RULEEXECUTOR_H

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/qsqr.h>

class BindingsTable;
class QSQR;
class DictMgmt;
class TupleTable;

//#define LINEAGE 1

#ifdef LINEAGE
struct LineageInfo {
    Rule *adornedRule;
    int bodyAtomId;
    size_t nQueries;

    uint8_t sizeQuery;
    std::vector<Term_t> queries;

    size_t offset;
    void *pointerToInput;

    Literal *query;
};
#endif

class RuleExecutor {
private:
    const Rule adornedRule;

    Program *program;
    EDBLayer &layer;

    //Used to create the supplementary relations
    std::vector<size_t> sizeSupplRelations;
    std::vector<int> posFromHeadToFirstSupplRelation;
    std::vector<std::vector<int>> posToCopyFromPreviousStep;

    //Used during unification
    std::vector<std::pair<int, int>> repeatedBoundVarsInHead;
    std::vector<std::pair<uint8_t, std::pair<uint8_t, uint8_t>>> headVarsInEDB;

    //Used to add tuples to the inputs when sub-idb queries are invoked
    std::vector<std::vector<std::pair<uint8_t, uint8_t>>> posFromLiteral;
    std::vector<std::vector<std::pair<uint8_t, uint8_t>>> posFromSupplRelation;

    //Used to perform the joins
    std::vector<uint8_t> njoins;
    std::vector<int16_t> startJoins;
    std::vector<std::pair<uint8_t, uint8_t>> joins;

    //Used to populate the answer set
    std::vector<uint8_t> projectionLastSuppl;

#ifdef LINEAGE
    std::vector<LineageInfo> lineage;
#endif

    BindingsTable **createSupplRelations();

    void deleteSupplRelations(BindingsTable **supplRelations);

    void calculateJoinsSizeIntermediateRelations();

    bool isUnifiable(const Term_t * const value, const size_t sizeTuple,
                     const size_t *posInAdorment, const EDBLayer &layer);

    size_t estimateRule(const int depth, const uint8_t bodyAtom,
                          BindingsTable **supplRelations,
                          QSQR *qsqr, EDBLayer &layer);

    void evaluateRule(const uint8_t bodyAtom, BindingsTable **supplRelations,
                      QSQR *qsqr, EDBLayer &layer
#ifdef LINEAGE
                      , std::vector<LineageInfo> &lineage
#endif
                     );

    void join(TupleTable *r1, TupleTable *r2, std::pair<uint8_t, uint8_t> *joins, uint8_t njoins, BindingsTable *output);

    void copyLastRelInAnswers(QSQR *qsqr,
                              size_t nTuples,
                              BindingsTable **supplRelations,
                              BindingsTable *lastSupplRelation);

    static char cmp(const uint64_t *row1, const uint64_t *row2, const std::pair<uint8_t, uint8_t> *joins, const uint8_t njoins);

#ifdef LINEAGE
    void printLineage(std::vector<LineageInfo> &lineage);
#endif

public:
    RuleExecutor(const Rule &rule, uint8_t headAdornment,
                 Program *program, EDBLayer &layer
                );

    size_t estimate(const int depth, BindingsTable *input/*, size_t offsetInput*/, QSQR *qsqr,
                      EDBLayer &edbLayer);

    void evaluate(BindingsTable *input, size_t offsetInput, QSQR *qsqr,
                  EDBLayer &edbLayer
#ifdef LINEAGE
                  , std::vector<LineageInfo> &lineage
#endif

                 );

#ifndef RECURSIVE_QSQR
    void processTask(QSQR_Task *task);
#endif

    string tostring() {
        return adornedRule.tostring(NULL, NULL);
    }

    ~RuleExecutor();
};

#endif
