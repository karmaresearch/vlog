#include <vlog/egdresultjoinproc.h>

#include <google/dense_hash_map>

EGDRuleProcessor::EGDRuleProcessor(
        SemiNaiver *sn,
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
            ignoreDuplicatesElimination),
    nreplacements(0),
    sn(sn),
    listDerivations(listDerivations),
    ruleExecOrder(ruleExecOrder),
    iteration(iteration),
    t(table),
    ruleDetails(detailsRule),
    literal(head),
    posLiteralInRule(posHeadInRule ){
        assert(addToEndTable);
        assert(!ignoreDuplicatesElimination); //Not sure about what would happen
    }

//Protected -- main method
void EGDRuleProcessor::processResults(const int blockid,
        const bool unique, std::mutex *m) {
    assert(rowsize == 2);
    termsToReplace.push_back(std::make_pair(row[0], row[1]));
}

//Public
void EGDRuleProcessor::processResults(const int blockid, const Term_t *first,
        FCInternalTableItr* second, const bool unique) {
    copyRawRow(first, second);
    processResults(blockid, unique, NULL);
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

void EGDRuleProcessor::processResults(const int blockid,
        FCInternalTableItr *first,
        FCInternalTableItr* second, const bool unique) {
    for (uint32_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first->getCurrentValue(posFromFirst[i].second);
    }
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second->getCurrentValue(posFromSecond[i].second);
    }
    processResults(blockid, unique, NULL);
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
    return nreplacements == 0;
}

void EGDRuleProcessor::consolidate(const bool isFinished) {
    if (termsToReplace.size() > 0) {
        //Remove duplicates
        std::sort(termsToReplace.begin(), termsToReplace.end());
        auto it = std::unique (termsToReplace.begin(), termsToReplace.end());
        termsToReplace.resize(std::distance(termsToReplace.begin(),it));

        //Create a map
        EGDTermMap map;
        map.set_empty_key((Term_t) -1);
        for(auto &pair : termsToReplace) {
            uint64_t key = pair.first;
            uint64_t value = pair.second;
            if (key == value)
                continue;
            if (!map.count(key)) {
                map.insert(std::make_pair(key,std::vector<uint64_t>()));
            }
            map[key].push_back(value);
        }

        //Replace all the terms in the database
        if (map.size() > 0) {
            //Go through all the derivations in listDerivations.
            for(auto &block : listDerivations) {
                assert(block.isCompleted);
                auto table = block.table;
                table->replaceAllTermsWithMap(map);
            }
        }
        termsToReplace.clear();

        //Add a table with zero elements
        std::shared_ptr<const FCInternalTable> ptrTable(
                //The table has zero columns. So it is non-empty even if there
                //are no subs in it
                new InmemoryFCInternalTable(0,
                    iteration, true, NULL));
        t->add(ptrTable, literal,
                posLiteralInRule, ruleDetails, ruleExecOrder,
                iteration, isFinished, nthreads);
    }
}
