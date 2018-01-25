#ifndef _FINAL_RESULT_H
#define _FINAL_RESULT_H

#include <vlog/resultjoinproc.h>

class SingleHeadFinalRuleProcessor: public ResultJoinProcessor {
    private:
        std::vector<FCBlock> &listDerivations;
        const uint8_t ruleExecOrder;

        const size_t iteration;

        int nbuffers;
        SegmentInserter **utmpt;
        std::shared_ptr<const Segment> *tmptseg;

        bool newDerivation;
        bool addToEndTable;

#if USE_DUPLICATE_DETECTION
#else
        void mergeTmpt(const int blockid, const bool unique, std::mutex *m);
#endif
    protected:
        FCTable *t;
        const RuleExecutionDetails *ruleDetails;
        const Literal literal;
        const uint8_t posLiteralInRule;

        SegmentInserter **tmpt;

        void enlargeBuffers(const int newsize);

    public:
        SingleHeadFinalRuleProcessor(
                std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
                std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
                std::vector<FCBlock> &listDerivations,
                FCTable *t,
                Literal &head, const uint8_t posHeadInRule,
                const RuleExecutionDetails *detailsRule,
                const uint8_t ruleExecOrder,
                const size_t iteration,
                const bool addToEndTable,
                const int nthreads);

        SingleHeadFinalRuleProcessor(
                Term_t *row,
                bool deleteRow,
                std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
                std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
                std::vector<FCBlock> &listDerivations,
                FCTable *t,
                Literal &head, const uint8_t posHeadInRule,
                const RuleExecutionDetails *detailsRule,
                const uint8_t ruleExecOrder,
                const size_t iteration,
                const bool addToEndTable,
                const int nthreads);

#if DEBUG
        void checkSizes() const {
            for (int i = 0; i < nbuffers; i++) {
                if (tmpt[i] != NULL) {
                    tmpt[i]->checkSizes();
                }
                if (tmptseg[i] != NULL) {
                    tmptseg[i]->checkSizes();
                }
                if (utmpt[i] != NULL) {
                    utmpt[i]->checkSizes();
                }
            }
        }
#endif

        bool shouldAddToEndTable() {
            return addToEndTable;
        }

        bool hasNewDerivation() const {
            return newDerivation;
        }

        Literal getLiteral() const {
            return literal;
        }

        size_t getIteration() const {
            return iteration;
        }

        FCTable *getTable() const {
            return t;
        }

        void addColumns(const int blockid,
                std::vector<std::shared_ptr<Column>> &columns,
                const bool unique, const bool sorted);

        void addColumn(const int blockid, const uint8_t pos,
                std::shared_ptr<Column> column, const bool unique,
                const bool sorted);

        void addColumns(const int blockid, FCInternalTableItr *itr,
                const bool unique, const bool sorted,
                const bool lastInsert);

        bool isEmpty() const;

        void processResults(std::vector<int> &blockid, Term_t *p,
                std::vector<bool> &unique, std::mutex *m);

        void processResults(const int blockid, const Term_t *first,
                FCInternalTableItr* second, const bool unique);

        void processResults(const int blockid, const bool unique,
                std::mutex *m);

        void processResults(const int blockid,
                const std::vector<const std::vector<Term_t> *> &vectors1, size_t i1,
                const std::vector<const std::vector<Term_t> *> &vectors2, size_t i2,
                const bool unique);

        void processResults(const int blockid, FCInternalTableItr *first,
                FCInternalTableItr* second, const bool unique);

        void processResultsAtPos(const int blockid, const uint8_t pos,
                const Term_t v, const bool unique);

        bool containsUnfilteredDerivation() const {
            if (tmpt != NULL) {
                for (int i = 0; i < nbuffers; ++i) {
                    if (tmpt[i] != NULL && !tmpt[i]->isEmpty())
                        return true;
                }
            }
            if (tmptseg != NULL) {
                for (int i = 0; i < nbuffers; ++i) {
                    if (tmptseg[i] != NULL && !tmptseg[i]->isEmpty())
                        return true;
                }
            }
            return false;
        }

        bool isBlockEmpty(const int blockId, const bool unique) const;

        //uint32_t getRowsInBlock(const int blockId, const bool unique) const;

        void consolidate(const bool isFinished) {
            consolidate(isFinished, false);
        }

        void consolidate(const bool isFinished, const bool forceCheck);

        void consolidateSegment(std::shared_ptr<const Segment> seg);

        std::vector<std::shared_ptr<const Segment>> getAllSegments();

        ~SingleHeadFinalRuleProcessor();
};

class SemiNaiver;
class FinalRuleProcessor: public ResultJoinProcessor {
    private:
        std::vector<FCBlock> &listDerivations;
        const uint8_t ruleExecOrder;
        const size_t iteration;
        bool newDerivation;
        std::vector<Literal> &heads;

    protected:
        std::vector<std::unique_ptr<SingleHeadFinalRuleProcessor>> atomTables;
        const RuleExecutionDetails *ruleDetails;
        bool addToEndTable;

    public:
        FinalRuleProcessor(
                std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
                std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
                std::vector<FCBlock> &listDerivations,
                std::vector<Literal> &heads,
                const RuleExecutionDetails *detailsRule,
                const uint8_t ruleExecOrder,
                const size_t iteration,
                const bool addToEndTable,
                const int nthreads,
                SemiNaiver *sn);

        bool shouldAddToEndTable() {
            return addToEndTable;
        }

        bool isEmpty() const;

        virtual void processResults(const int blockid, const bool unique,
                std::mutex *m);

        virtual void processResults(std::vector<int> &blockid, Term_t *p,
                std::vector<bool> &unique, std::mutex *m);

        virtual void processResults(const int blockid, const Term_t *first,
                FCInternalTableItr* second, const bool unique);

        virtual void processResults(const int blockid,
                const std::vector<const std::vector<Term_t> *> &vectors1, size_t i1,
                const std::vector<const std::vector<Term_t> *> &vectors2, size_t i2,
                const bool unique);

        virtual void processResults(const int blockid, FCInternalTableItr *first,
                FCInternalTableItr* second, const bool unique);

        virtual void processResultsAtPos(const int blockid, const uint8_t pos,
                const Term_t v, const bool unique);

        virtual void addColumns(const int blockid,
                std::vector<std::shared_ptr<Column>> &columns,
                const bool unique, const bool sorted);

        virtual void addColumn(const int blockid, const uint8_t pos,
                std::shared_ptr<Column> column, const bool unique,
                const bool sorted);

        virtual void addColumns(const int blockid, FCInternalTableItr *itr,
                const bool unique, const bool sorted,
                const bool lastInsert);

        bool isBlockEmpty(const int blockId, const bool unique) const;

        virtual void consolidate(const bool isFinished);

        ~FinalRuleProcessor() {}
};

#endif
