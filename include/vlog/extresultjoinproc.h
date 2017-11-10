#ifndef _EXT_RESULT_H
#define _EXT_RESULT_H

class ExistentialRuleProcessor : public FinalRuleProcessor {
    private:
        std::shared_ptr<ChaseMgmt> chaseMgmt;

        void filterDerivations(std::vector<std::shared_ptr<Column>> c,
                uint64_t sizecolumns,
                std::vector<uint64_t> &output);

    public:
        ExistentialRuleProcessor(std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
                std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
                std::vector<FCBlock> &listDerivations,
                FCTable *t,
                Literal &head,
                const uint8_t posHeadInRule,
                const RuleExecutionDetails *detailsRule,
                const uint8_t ruleExecOrder,
                const size_t iteration,
                const bool addToEndTable,
                const int nthreads,
                std::shared_ptr<ChaseMgmt> chaseMgmt);

        void processResults(const int blockid, const bool unique, std::mutex *m);

        void addColumns(const int blockid, FCInternalTableItr *itr,
                const bool unique, const bool sorted,
                const bool lastInsert);
};

#endif
