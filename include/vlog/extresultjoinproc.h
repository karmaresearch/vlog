#ifndef _EXT_RESULT_H
#define _EXT_RESULT_Hresultjoinproc

#include <vlog/finalresultjoinproc.h>
#include <vlog/resultjoinproc.h>

class ExistentialRuleProcessor : public FinalRuleProcessor {
    private:
        std::shared_ptr<ChaseMgmt> chaseMgmt;
        SemiNaiver *sn;

        //If the data is added row by row, then I set the following flag to true
        bool replaceExtColumns;
        uint8_t nConstantColumns;
        uint8_t posConstantColumns[SIZETUPLE * 3];
        uint8_t nKnownColumns;
        uint8_t posKnownColumns[SIZETUPLE * 3];
        std::map<uint8_t, std::vector<uint8_t>> posExtColumns;
        std::unique_ptr<SegmentInserter> tmpRelation;
        //In the above case, I store the data in a temporary segment, and assign
        //existential IDs when I consolidate
	std::vector<uint8_t> varsUsedForExt;
	std::vector<int> colsForExt;

        static void filterDerivations(FCTable *t,
                std::vector<std::shared_ptr<Column>> &tobeRetained,
                std::vector<uint8_t> &columnsToCheck,
                std::vector<uint64_t> &outputProc);

        static void filterDerivations(
                const Literal &literal,
                FCTable *t,
                Term_t *row,
                uint8_t initialCount,
                const RuleExecutionDetails *detailsRule,
                uint8_t nCopyFromSecond,
                std::pair<uint8_t, uint8_t> *posFromSecond,
                std::vector<std::shared_ptr<Column>> c,
                uint64_t sizecolumns,
                std::vector<uint64_t> &output);

        void retainNonExisting(
                std::vector<uint64_t> &filterRows,
                uint64_t &sizecolumns,
                std::vector<std::shared_ptr<Column>> &c);

    public:
        ExistentialRuleProcessor(
                std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
                std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
                std::vector<FCBlock> &listDerivations,
                std::vector<Literal> &heads,
                const RuleExecutionDetails *detailsRule,
                const uint8_t ruleExecOrder,
                const size_t iteration,
                const bool addToEndTable,
                const int nthreads,
                SemiNaiver *sn,
                std::shared_ptr<ChaseMgmt> chaseMgmt);

        void addColumns(const int blockid, FCInternalTableItr *itr,
                const bool unique, const bool sorted,
                const bool lastInsert);

        void addColumns(const int blockid,
                std::vector<std::shared_ptr<Column>> &columns,
                const bool unique, const bool sorted);

        void addColumn(const int blockid, const uint8_t pos,
                std::shared_ptr<Column> column, const bool unique,
                const bool sorted);

        void processResults(const int blockid, const Term_t *first,
                FCInternalTableItr* second, const bool unique) {
            //TODO: Chase...
            LOG(ERRORL) << "Not implemented yet";
            throw 10;
        }

        void processResults(const int blockid,
                const std::vector<const std::vector<Term_t> *> &vectors1, size_t i1,
                const std::vector<const std::vector<Term_t> *> &vectors2, size_t i2,
                const bool unique);

        void processResults(std::vector<int> &blockid, Term_t *p, std::vector<bool> &unique, std::mutex *m) {
            //TODO: Chase...
            LOG(ERRORL) << "Not implemented yet";
            throw 10;
        }

        void processResults(const int blockid, FCInternalTableItr *first,
                FCInternalTableItr* second, const bool unique) {
            //TODO: Chase...
            LOG(ERRORL) << "Not implemented yet";
            throw 10;
        }

        void consolidate(const bool isFinished);
};

#endif
