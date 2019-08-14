#include <vlog/egdresultjoinproc.h>
#include <vlog/seminaiver.h>

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

    for (int i = 0; i < nCopyFromFirst; i++) {
        row[posFromFirst[i].first] = (*vectors1[posFromFirst[i].second])[i1];
    }
    for (int i = 0; i < nCopyFromSecond; i++) {
        row[posFromSecond[i].first] = (*vectors2[posFromSecond[i].second])[i2];
    }
    processResults(blockid, unique || ignoreDupElimin, NULL);
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
    assert(columns.size() == 2);
    uint64_t nrows = 0;
    nrows = columns[0]->size();

    std::vector<std::unique_ptr<ColumnReader>> columnReaders;
    for(uint8_t i = 0; i < columns.size(); ++i) {
        columnReaders.push_back(columns[i]->getReader());
    }
    //Fill the row
    for (size_t rowid = 0; rowid < nrows; ++rowid) {
        for (uint8_t j = 0; j < 2; ++j) {
            if (!columnReaders[j]->hasNext()) {
                LOG(ERRORL) << "This should not happen";
            }
            row[j] = columnReaders[j]->next();
        }
        processResults(blockid, unique, NULL);
    }
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

bool EGDRuleProcessor::consolidate(const bool isFinished) {
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

            if (((key & RULEVARMASK) == 0) && ((value & RULEVARMASK) == 0)) {
                LOG(ERRORL) << "Due to UNA, the chase does not exist (" <<
                    key << "," << value << ")";
                throw 10;
            }
            uint64_t tmp;
            if ((key & RULEVARMASK) == 0) {
                //Swap the elements
                tmp = key;
                key = value;
                value = tmp;
            }

            if (!map.count(key)) {
                map.insert(std::make_pair(key,value));
            }
            map[key] = value;
        }

        //Replace all the terms in the database
        bool replaced = false;
        if (map.size() > 0) {
            //Go through all the derivations in listDerivations.
            for(auto &block : listDerivations) {
                assert(block.isCompleted);
                auto table = block.table;
                auto newSegment = table->replaceAllTermsWithMap(map);
                if (newSegment.get() != NULL && !newSegment->isEmpty()) {
                    auto predId = block.query.getPredicate().getId();
                    auto fctable = sn->getTable(predId, block.query.getTupleSize());
                    //Some of the replaced rows might be duplicates ...
                    //Retain up to iteration;
                    auto filteredSegment = fctable->retainFrom(newSegment,
                            false, nthreads, block.iteration);
                    std::shared_ptr<const FCInternalTable> newtable =
                        std::shared_ptr<const FCInternalTable>(
                                new InmemoryFCInternalTable(table->getRowSize(),
                                    block.iteration,
                                    true,
                                    filteredSegment));
                    block.table = newtable;
                    //Replace it also in fctable
                    fctable->replaceInternalTable(block.iteration, newtable);
                    replaced = true;
                }
            }
        }
        termsToReplace.clear();
        return replaced;
    }
    return false;
}
