#ifndef _JOINPROC_H
#define _JOINPROC_H

#include <trident/model/table.h>
#include <vlog/concepts.h>
#include <vlog/fctable.h>
#include <vlog/seminaiver.h>
#include <vlog/filterer.h>
#include <vlog/resultjoinproc.h>

#include <inttypes.h>

typedef google::dense_hash_map<Term_t, std::pair<size_t, size_t>, std::hash<Term_t>, std::equal_to<Term_t>> JoinHashMap;
typedef google::dense_hash_map<std::pair<Term_t, Term_t>,
        std::pair<size_t, size_t>, boost::hash<std::pair<Term_t, Term_t>>,
        std::equal_to<std::pair<Term_t, Term_t>>> DoubleJoinHashMap;

struct LessTwoTuples {
    const uint8_t sizeTuple;
    const std::vector<Term_t> &values;

    LessTwoTuples(const uint8_t sizeTuple, const std::vector<Term_t> &values) : sizeTuple(sizeTuple), values(values) {}

    bool operator() (const size_t r1, const size_t r2) const {
        for (uint8_t i = 0; i < sizeTuple; ++i)
            if (values[r1 + i] != values[r2 + i])
                return values[r1 + i] < values[r2 + i];
        return false;
    }
};

//If the previous table has less than these lines, then it executes an hash join
#define THRESHOLD_HASHJOIN 100

#define THRESHOLD_HASHJOIN_SCAN 1000

class SemiNaiver;
class JoinExecutor {
private:

    //Map that contains new tuples that are too large
    //std::vector<Term_t> tooLargeInferenceValues;
    //std::vector<size_t> tooLargeInferencePR;
    //std::set<size_t, LessTwoTuples> tooLargeInference;

    //Map that contains tuples that are unique but that cannot be outputted until they are out of the window
    //std::vector<Term_t> uniqueInferenceValues;
    //std::vector<size_t> uniqueInferencePR;
    //std::set<size_t, LessTwoTuples> uniqueInference;


    //long stats;

    static bool isJoinVerificative(
        const FCInternalTable *t1,
        const RuleExecutionPlan &plan,
        const int currentLiteral);

    static bool isJoinTwoToOneJoin(const RuleExecutionPlan &plan,
                                   const int currentLiteral);

    static void verificativeJoin(
        SemiNaiver *naiver,
        const FCInternalTable *intermediateResults,
        const Literal &literal,
        const size_t min,
        const size_t max,
        ResultJoinProcessor *output,
        const RuleExecutionPlan &plan,
        const int currentLiteral);

    static void verificativeJoinOneColumn(
        SemiNaiver *naiver,
        const FCInternalTable *intermediateResults,
        const Literal &literal,
        const size_t min,
        const size_t max,
        ResultJoinProcessor *output,
        const RuleExecutionPlan &plan,
        const int currentLiteral);

    static void verificativeJoinOneColumnSameOutput(
        SemiNaiver *naiver,
        const FCInternalTable *intermediateResults,
        const Literal &literal,
        const size_t min,
        const size_t max,
        ResultJoinProcessor *output,
        const RuleExecutionPlan &plan,
        const int currentLiteral);

    static void joinTwoToOne(
        SemiNaiver *naiver,
        const FCInternalTable *intermediateResults,
        const Literal &literal,
        const size_t min,
        const size_t max,
        ResultJoinProcessor *output,
        const RuleExecutionPlan &plan,
        const int currentLiteral);

    //static bool same(const Segment *segment, const uint32_t idx1, const uint32_t idx2, const std::vector<uint8_t> &fields);

    static int cmp(const Term_t *r1, const Term_t *r2, const std::vector<uint8_t> &fields1,
                   const std::vector<uint8_t> &fields2);

    static int cmp(FCInternalTableItr *r1, FCInternalTableItr *r2,
                   const std::vector<uint8_t> &fields1, const std::vector<uint8_t> &fields2);

    static int cmp(const std::vector<Term_t> &r1, FCInternalTableItr * r2,
                   const std::vector<uint8_t> &fields1,
                   const std::vector<uint8_t> &fields2);

    static void doPhysicalHashJoin(FCIterator &itr2, JoinHashMap &map,
                                   std::vector<Term_t> &mapValues, const uint8_t joinIdx2,
                                   const uint8_t rowSize, const uint8_t s2,
                                   ResultJoinProcessor *output);

    static bool isJoinSelective(JoinHashMap &map, const Literal &literal,
                                const size_t minIteration, const size_t maxIteration,
                                SemiNaiver *naiver, const uint8_t joinPos);

    static void execSelectiveHashJoin(const RuleExecutionDetails &currentRule,
                                      SemiNaiver *naiver, const JoinHashMap &map,
                                      const DoubleJoinHashMap &doublemap,
                                      ResultJoinProcessor *out, const uint8_t njoinfields,
                                      const uint8_t idxJoinField1, const uint8_t idxJoinField2,
                                      const Literal *outputLiteral,
                                      const Literal &literal, const uint8_t rowSize,
                                      const std::vector<uint8_t> &posToSort, std::vector<Term_t> &values,
                                      const bool literalSharesVarsWithHead,
                                      const size_t min, const size_t max,
                                      const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
                                      int &processedTables);

    static void do_merge_join_classicalgo(InmemoryFCInternalTableItr *sortedItr1,
                                          FCInternalTableItr *sortedItr2,
                                          const std::vector<uint8_t> &fields1,
                                          const std::vector<uint8_t> &fields2,
                                          const uint8_t posBlocks,
                                          const Term_t *valBlocks,
                                          ResultJoinProcessor * output);

    static void do_merge_join_fasteralgo(InmemoryFCInternalTableItr *sortedItr1,
                                         FCInternalTableItr *sortedItr2,
                                         const std::vector<uint8_t> &fields1,
                                         const std::vector<uint8_t> &fields2,
                                         const uint8_t posBlocks,
                                         const uint8_t nValBlocks,
                                         const Term_t *valBlocks,
                                         ResultJoinProcessor * output);

    static void do_mergejoin(const FCInternalTable *filteredT1, std::vector<uint8_t> &fieldsToSortInMap,
                             std::vector<std::shared_ptr<const FCInternalTable>> &tables2,
                             const std::vector<uint8_t> &fields1, const uint8_t *posOtherVars, const std::vector<Term_t> *valuesOtherVars,
                             const std::vector<uint8_t> &fields2, ResultJoinProcessor *output);

public:
    static void join(SemiNaiver *naiver, const FCInternalTable * t1, const Literal *outputLiteral, const Literal &literal,
                     const size_t min, const size_t max,
                     const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
                     std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
                     ResultJoinProcessor * output, const bool lastLiteral,
                     const RuleExecutionDetails &ruleDetails,
                     const RuleExecutionPlan &plan, int &processedTables,
                     const int currentLiteral);

    static void mergejoin(const FCInternalTable * t1, SemiNaiver *naiver,
                          const Literal *outputLiteral,
                          const Literal &literalToQuery,
                          const uint32_t min, const uint32_t max,
                          std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
                          ResultJoinProcessor * output);

    static void hashjoin(const FCInternalTable * t1, SemiNaiver *naiver, const Literal *outputLiteral,
                         const Literal &literal, const size_t min, const size_t max,
                         const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
                         std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
                         ResultJoinProcessor * output, const bool lastLiteral,
                         const RuleExecutionDetails &ruleDetails, const RuleExecutionPlan &plan,
                         int &processedTables);

    static int cmp(const Term_t *r1, const Term_t *r2, const uint8_t s);
};

class DuplicateContainers {
private:
    const uint8_t nfields;
    const size_t ntables;
    uint8_t fields[SIZETUPLE];
    bool empty;

    const FCInternalTable *firstTable;
    FCInternalTableItr *firstItr;

    const FCInternalTable **tables;
    FCInternalTableItr **itrs;

public:
    DuplicateContainers() : nfields(0), ntables(0), empty(true), firstTable(NULL),
        firstItr(NULL), tables(NULL), itrs(NULL) {
    }

    DuplicateContainers(FCIterator &itr, const uint8_t sizerow);

    bool isEmpty() const {
        return empty;
    }

    bool exists(const Term_t *v);

    void clear();

    int cmp(FCInternalTableItr *firstItr, const Term_t *v) const;

    ~DuplicateContainers() {
        /*if (tables != NULL)
            delete[] tables;

        if (itrs != NULL)
            delete[] itrs;*/ //I cannot clean them here because this object is often copied. The deallocation must be done explicitly in clear()
    }
};

#endif
