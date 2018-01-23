#ifndef _EXT_RESULT_H
#define _EXT_RESULT_Hresultjoinproc

#include <vlog/finalresultjoinproc.h>
#include <vlog/resultjoinproc.h>

class ExistentialRuleProcessor : public FinalRuleProcessor {
    private:
        std::shared_ptr<ChaseMgmt> chaseMgmt;
        SemiNaiver *sn;

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

        void processResults(const int blockid, const bool unique, std::mutex *m);

        void addColumns(const int blockid, FCInternalTableItr *itr,
                const bool unique, const bool sorted,
                const bool lastInsert);

        void addColumns(const int blockid,
                std::vector<std::shared_ptr<Column>> &columns,
                const bool unique, const bool sorted);

        void addColumn(const int blockid, const uint8_t pos,
                std::shared_ptr<Column> column, const bool unique,
                const bool sorted);

};

#endif
