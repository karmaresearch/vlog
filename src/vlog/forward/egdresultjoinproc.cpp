#include <vlog/egdresultjoinproc.h>

EGDRuleProcessor::EGDRuleProcessor(
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
        const bool ignoreDuplicatesElimination) :
    ResultJoinProcessor(head.getPredicate().getCardinality(),
            new Term_t[head.getPredicate().getCardinality()], true,
            (uint8_t) posFromFirst.size(),
            (uint8_t) posFromSecond.size(),
            posFromFirst.size() > 0 ? & (posFromFirst[0]) : NULL,
            posFromSecond.size() > 0 ? & (posFromSecond[0]) : NULL, nthreads,
            ignoreDuplicatesElimination) {
    }



//Protected
void EGDRuleProcessor::processResults(const int blockid,
        const bool unique, std::mutex *m) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

//Public
void EGDRuleProcessor::processResults(const int blockid, const Term_t *first,
        FCInternalTableItr* second, const bool unique) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

void EGDRuleProcessor::processResults(const int blockid,
        const std::vector<
        const std::vector<Term_t> *> &vectors1, size_t i1,
        const std::vector<
        const std::vector<Term_t> *> &vectors2, size_t i2,
        const bool unique) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

void EGDRuleProcessor::processResults(std::vector<int> &blockid,
        Term_t *p,
        std::vector<bool> &unique, std::mutex *m) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

void EGDRuleProcessor::processResults(const int blockid, FCInternalTableItr *first,
        FCInternalTableItr* second, const bool unique) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

void EGDRuleProcessor::processResultsAtPos(const int blockid, const uint8_t pos,
        const Term_t v, const bool unique) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

bool EGDRuleProcessor::isBlockEmpty(const int blockId, const bool unique) const {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

void EGDRuleProcessor::addColumns(const int blockid,
        std::vector<std::shared_ptr<Column>> &columns,
        const bool unique, const bool sorted) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

void EGDRuleProcessor::addColumns(const int blockid, FCInternalTableItr *itr,
        const bool unique, const bool sorted,
        const bool lastInsert) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

void EGDRuleProcessor::addColumn(const int blockid, const uint8_t pos,
        std::shared_ptr<Column> column,
        const bool unique, const bool sorted) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

bool EGDRuleProcessor::isEmpty() const {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}
