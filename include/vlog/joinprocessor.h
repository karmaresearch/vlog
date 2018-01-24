#ifndef _JOINPROC_H
#define _JOINPROC_H

#include <trident/model/table.h>
#include <vlog/concepts.h>
#include <vlog/fctable.h>
#include <vlog/seminaiver.h>
#include <vlog/filterer.h>
#include <vlog/resultjoinproc.h>

#include <inttypes.h>
#include <mutex>

typedef google::dense_hash_map<Term_t, std::pair<size_t, size_t>, std::hash<Term_t>, std::equal_to<Term_t>> JoinHashMap;
typedef google::dense_hash_map<std::pair<Term_t, Term_t>,
        std::pair<size_t, size_t>, std::hash<std::pair<Term_t, Term_t>>,
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

#define FLUSH_SIZE (1 << 20)

class Output {
    private:

        ResultJoinProcessor *output;
        std::mutex *m;
        const uint8_t nCopyFromFirst;
        const uint8_t nCopyFromSecond;
        const uint8_t rowsize;
        const std::pair<uint8_t, uint8_t> *posFromFirst;
        const std::pair<uint8_t, uint8_t> *posFromSecond;
        std::vector<Term_t> resultTerms;
        std::vector<int> resultBlockId;
        std::vector<bool> resultUnique;
        const bool mustFlush;

    public:
        Output(ResultJoinProcessor *output, std::mutex *m) :
            output(output), m(m),
            nCopyFromFirst(output->getNCopyFromFirst()),
            nCopyFromSecond(output->getNCopyFromSecond()),
            rowsize(output->getRowSize()),
            posFromFirst(output->getPosFromFirst()),
            posFromSecond(output->getPosFromSecond()), mustFlush(true) {
            }

        Output(ResultJoinProcessor *output, std::mutex *m, bool mustFlush) :
            output(output), m(m),
            nCopyFromFirst(output->getNCopyFromFirst()),
            nCopyFromSecond(output->getNCopyFromSecond()),
            rowsize(output->getRowSize()),
            posFromFirst(output->getPosFromFirst()),
            posFromSecond(output->getPosFromSecond()), mustFlush(mustFlush) {
            }

        void processResults(const int blockid, const Term_t *first,
                FCInternalTableItr* second, const bool unique) {
            if (m == NULL) {
                output->processResults(blockid, first, second, unique);
                return;
            }
            for (int i = 0; i < nCopyFromFirst; i++) {
                resultTerms.push_back(first[posFromFirst[i].second]);
            }
            for (int i = 0; i < nCopyFromSecond; i++) {
                resultTerms.push_back(second->getCurrentValue(posFromSecond[i].second));
            }
            resultBlockId.push_back(blockid);
            resultUnique.push_back(unique);
            if (mustFlush && resultBlockId.size() >= FLUSH_SIZE) {
                flush();
            }
        }

        void processResults(const int blockid,
                const std::vector<const std::vector<Term_t> *> &vectors1, size_t i1,
                const std::vector<const std::vector<Term_t> *> &vectors2, size_t i2,
                const bool unique) {
            if (m == NULL) {
                output->processResults(blockid, vectors1, i1, vectors2, i2, unique);
                return;
            }
            for (int i = 0; i < nCopyFromFirst; i++) {
                resultTerms.push_back((*vectors1[posFromFirst[i].second])[i1]);
            }
            for (int i = 0; i < nCopyFromSecond; i++) {
                resultTerms.push_back((*vectors2[posFromSecond[i].second])[i2]);
            }
            resultBlockId.push_back(blockid);
            resultUnique.push_back(unique);
            if (mustFlush && resultBlockId.size() >= FLUSH_SIZE) {
                flush();
            }
        }

        void processResults(const int blockid, FCInternalTableItr *first,
                FCInternalTableItr* second, const bool unique) {
            if (m == NULL) {
                output->processResults(blockid, first, second, unique);
                return;
            }
            for (int i = 0; i < nCopyFromFirst; i++) {
                resultTerms.push_back(first->getCurrentValue(posFromFirst[i].second));
            }
            for (int i = 0; i < nCopyFromSecond; i++) {
                resultTerms.push_back(second->getCurrentValue(posFromSecond[i].second));
            }
            resultBlockId.push_back(blockid);
            resultUnique.push_back(unique);
            if (mustFlush && resultBlockId.size() >= FLUSH_SIZE) {
                flush();
            }
        }

        void flush() {
            if (m == NULL) {
                return;
            }
            Term_t *p = &resultTerms[0];
            std::lock_guard<std::mutex> lock(*m);
            output->processResults(resultBlockId, p, resultUnique, m);
            resultUnique.clear();
            resultBlockId.clear();
            resultTerms.clear();
        }

        ~Output() {
        }
};

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
                const RuleExecutionPlan &vars,
                const int currentLiteral);

        static bool isJoinTwoToOneJoin(const RuleExecutionPlan &hv,
                const int currentLiteral);

        static void verificativeJoin(
                SemiNaiver *naiver,
                const FCInternalTable *intermediateResults,
                const Literal &literal,
                const size_t min,
                const size_t max,
                ResultJoinProcessor *output,
                const RuleExecutionPlan &hv,
                const int currentLiteral,
                int nthreads);

        static void verificativeJoinOneColumn(
                SemiNaiver *naiver,
                const FCInternalTable *intermediateResults,
                const Literal &literal,
                const size_t min,
                const size_t max,
                ResultJoinProcessor *output,
                const RuleExecutionPlan &hv,
                const int currentLiteral,
                int nthreads);

        static void verificativeJoinOneColumnSameOutput(
                SemiNaiver *naiver,
                const FCInternalTable *intermediateResults,
                const Literal &literal,
                const size_t min,
                const size_t max,
                ResultJoinProcessor *output,
                const RuleExecutionPlan &hv,
                const int currentLiteral,
                int nthreads);

        static void joinTwoToOne(
                SemiNaiver *naiver,
                const FCInternalTable *intermediateResults,
                const Literal &literal,
                const size_t min,
                const size_t max,
                ResultJoinProcessor *output,
                const RuleExecutionPlan &hv,
                const int currentLiteral,
                const int nthreads);

        //static bool same(const Segment *segment, const uint32_t idx1, const uint32_t idx2, const std::vector<uint8_t> &fields);

        static int cmp(const Term_t *r1, const Term_t *r2, const std::vector<uint8_t> &fields1,
                const std::vector<uint8_t> &fields2);

        static int cmp(FCInternalTableItr *r1, FCInternalTableItr *r2,
                const std::vector<uint8_t> &fields1, const std::vector<uint8_t> &fields2);

        static int cmp(const std::vector<Term_t> &r1, FCInternalTableItr * r2,
                const std::vector<uint8_t> &fields1,
                const std::vector<uint8_t> &fields2);

        static int cmp(const std::vector<const std::vector<Term_t> *> &vectors1, size_t i1,
                const std::vector<const std::vector<Term_t> *> &vectors2, size_t i2,
                const std::vector<uint8_t> &fields1,
                const std::vector<uint8_t> &fields2);

        static bool sameAs(const std::vector<const std::vector<Term_t> *> &vectors,
                size_t i1,  size_t i2,
                const std::vector<uint8_t> &fields1);

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
                const std::vector<Literal> *outputLiterals,
                const Literal &literal, const uint8_t rowSize,
                const std::vector<uint8_t> &posToSort, std::vector<Term_t> &values,
                const bool literalSharesVarsWithHead,
                const size_t min, const size_t max,
                const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
                int &processedTables);

        static void do_merge_join_fasteralgo(FCInternalTableItr *sortedItr1,
                FCInternalTableItr *sortedItr2,
                const std::vector<uint8_t> &fields1,
                const std::vector<uint8_t> &fields2,
                const uint8_t posBlocks,
                const uint8_t nValBlocks,
                const Term_t *valBlocks,
                ResultJoinProcessor *output);

        static void do_mergejoin(const FCInternalTable *filteredT1, std::vector<uint8_t> &fieldsToSortInMap,
                std::vector<std::shared_ptr<const FCInternalTable>> &tables2,
                const std::vector<uint8_t> &fields1, const uint8_t *posOtherVars, const std::vector<Term_t> *valuesOtherVars,
                const std::vector<uint8_t> &fields2, ResultJoinProcessor *output, int nthreads);

    public:
        static void do_merge_join_classicalgo(FCInternalTableItr *sortedItr1,
                FCInternalTableItr *sortedItr2,
                const std::vector<uint8_t> &fields1,
                const std::vector<uint8_t> &fields2,
                const uint8_t posBlocks,
                const Term_t *valBlocks,
                Output *output);

        static void do_merge_join_classicalgo(const std::vector<const std::vector<Term_t> *> &vectors1,
                size_t l1, size_t u1,
                const std::vector<const std::vector<Term_t> *> &vectors2,
                size_t l2, size_t u2,
                const std::vector<uint8_t> &fields1,
                const std::vector<uint8_t> &fields2,
                const uint8_t posBlocks,
                const Term_t *valBlocks,
                Output * output);

        static void join(SemiNaiver *naiver, const FCInternalTable * t1,
                const std::vector<Literal> *outputLiterals, const Literal &literal,
                const size_t min, const size_t max,
                const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
                std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
                ResultJoinProcessor * output, const bool lastLiteral,
                const RuleExecutionDetails &ruleDetails,
                const RuleExecutionPlan &hv, int &processedTables,
                const int currentLiteral,
                const int nthreads);

        static void mergejoin(const FCInternalTable * t1, SemiNaiver *naiver,
                const std::vector<Literal> *outputLiterals,
                const Literal &literalToQuery,
                const uint32_t min, const uint32_t max,
                std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
                ResultJoinProcessor * output, int nthreads);

        static void hashjoin(const FCInternalTable * t1,
                SemiNaiver *naiver, const std::vector<Literal> *outputLiterals,
                const Literal &literal, const size_t min, const size_t max,
                const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
                std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
                ResultJoinProcessor *output, const int lastLiteral,
                const RuleExecutionDetails &ruleDetails,
                const RuleExecutionPlan &hv,
                int &processedTables, int nthreads);

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
