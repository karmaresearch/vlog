#ifndef _EGD_RESULT_JOIN_PROC
#define _EGD_RESULT_JOIN_PROC

#include <vlog/resultjoinproc.h>

class EGDRuleProcessor: public ResultJoinProcessor {
    private:
        std::vector<std::pair<uint64_t, uint64_t>> termsToReplace;
        SemiNaiver *sn;

        std::vector<FCBlock> &listDerivations;
        const uint8_t ruleExecOrder;
        const size_t iteration;

        FCTable *t;
        const RuleExecutionDetails *ruleDetails;
        const Literal literal;
        const uint8_t posLiteralInRule;

        const bool UNA;
        const bool emfa;

    protected:
        void processResults(const int blockid,
                const bool unique, std::mutex *m);
    public:
        EGDRuleProcessor(SemiNaiver *sn,
                std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
                std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
                std::vector<FCBlock> &listDerivations,
                FCTable *table,
                Literal &head, const uint8_t posHeadInRule,
                const RuleExecutionDetails *detailsRule,
                const uint8_t ruleExecOrder,
                const size_t iteration,
                const bool addToEndTable,
                const int nthreads,
                const bool ignoreDuplicatesElimination,
                const bool UNA,
                const bool emfa);

        void processResults(const int blockid, const Term_t *first,
                FCInternalTableItr* second, const bool unique);

        void processResults(const int blockid,
                const std::vector<
                    const std::vector<Term_t> *> &vectors1, size_t i1,
                const std::vector<
                    const std::vector<Term_t> *> &vectors2, size_t i2,
                const bool unique);

        void processResults(std::vector<int> &blockid,
                Term_t *p,
                std::vector<bool> &unique, std::mutex *m);

        void processResults(const int blockid, FCInternalTableItr *first,
                FCInternalTableItr* second, const bool unique);

        void processResultsAtPos(const int blockid, const uint8_t pos,
                const Term_t v, const bool unique);

        bool isBlockEmpty(const int blockId, const bool unique) const;

        void addColumns(const int blockid,
                std::vector<std::shared_ptr<Column>> &columns,
                const bool unique, const bool sorted);

        void addColumns(const int blockid, FCInternalTableItr *itr,
                const bool unique, const bool sorted,
                const bool lastInsert);

        void addColumn(const int blockid, const uint8_t pos,
                std::shared_ptr<Column> column,
                const bool unique, const bool sorted);

        bool consolidate(const bool isFinished);

        bool isEmpty() const;

        ~EGDRuleProcessor() {}
};

#endif
