#include <vlog/egdresultjoinproc.h>
#include <vlog/seminaiver.h>

#include <google/dense_hash_map>
#include <climits>

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
        const bool ignoreDuplicatesElimination,
        const bool UNA,
        const bool emfa):
    ResultJoinProcessor(head.getPredicate().getCardinality(),
            new Term_t[head.getPredicate().getCardinality()], true,
            (uint8_t) posFromFirst.size(),
            (uint8_t) posFromSecond.size(),
            posFromFirst.size() > 0 ? & (posFromFirst[0]) : NULL,
            posFromSecond.size() > 0 ? & (posFromSecond[0]) : NULL, nthreads,
            ignoreDuplicatesElimination),
    sn(sn),
    listDerivations(listDerivations),
    ruleExecOrder(ruleExecOrder),
    iteration(iteration),
    t(table),
    ruleDetails(detailsRule),
    literal(head),
    posLiteralInRule(posHeadInRule),
    UNA(UNA),
    emfa(emfa) {
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

    assert(itr->getNColumns() == 2);
    assert(rowsize == 2);

    uint8_t columns[256];
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        columns[i] = posFromSecond[i].second;
    }
    std::vector<std::shared_ptr<Column>> c = itr->getColumn(nCopyFromSecond, columns);
    assert(c.size() == rowsize);

    if (nCopyFromSecond > 1) {
        std::vector<std::shared_ptr<Column>> c2;
        int rowID = 0;
        int largest = -1;
        for (int i = 0; i < nCopyFromSecond; ++i) {
            //Get the row with the smallest ID
            int minID = INT_MAX;
            for (int j = 0; j <  nCopyFromSecond; ++j) {
                if (posFromSecond[j].first > largest &&
                        posFromSecond[j].first < minID) {
                    rowID = j;
                    minID = posFromSecond[j].first;
                }
            }
            c2.push_back(c[rowID]);
            largest = posFromSecond[rowID].first;
        }
        assert(c2.size() == c.size());
        c = c2;
    }
    addColumns(blockid, c, unique, sorted);
}

void EGDRuleProcessor::addColumn(const int blockid, const uint8_t pos,
        std::shared_ptr<Column> column,
        const bool unique, const bool sorted) {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

bool EGDRuleProcessor::isEmpty() const {
    return termsToReplace.empty();
}

bool EGDRuleProcessor::consolidate(const bool isFinished) {
    if (!isFinished)
        return false;

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

            if (UNA && ((key & RULEVARMASK) == 0) && ((value & RULEVARMASK) == 0)) {
                LOG(ERRORL) << "Due to UNA, the chase does not exist (" <<
                    key << "," << value << ")";
                throw 10;
            }

            //The swap depends on the depth of the terms
            uint64_t depthKey = 0;
            if ((key & RULEVARMASK) != 0) {
                depthKey = sn->getChaseManager()->countDepth(key);
            }
            uint64_t depthValue = 0;
            if ((value & RULEVARMASK) != 0) {
                depthValue = sn->getChaseManager()->countDepth(value);
            }
            bool keySmallerThanValue = true;
            if (depthKey > depthValue) {
                keySmallerThanValue = false;
            } else if (depthKey == depthValue) {
                keySmallerThanValue = key < value;
            }

            if (!keySmallerThanValue) {
                if (!map.count(key)) {
                    map.insert(std::make_pair(key,std::make_pair(value, depthValue)));
                } else {
                    auto prevValue = map[key];
                    auto depthPrevValue = prevValue.second;
                    bool prevValueSmallerThanValue = true;
                    if (depthValue < depthPrevValue) {
                        prevValueSmallerThanValue = false;
                    } else if (depthValue == depthPrevValue) {
                        prevValueSmallerThanValue = prevValue.first < value;
                    }
                    if (!prevValueSmallerThanValue) {
                        map[key] = std::make_pair(value, depthValue);
                    }
                }
            } else {
                if (!map.count(value)) {
                    map.insert(std::make_pair(value, std::make_pair(key, depthKey)));
                } else {
                    auto prevKey = map[value];
                    auto depthPrevKey = prevKey.second;
                    bool prevKeySmallerThanKey = true;
                    if (depthKey < depthPrevKey) {
                        prevKeySmallerThanKey = false;
                    } else if (depthKey == depthPrevKey) {
                        prevKeySmallerThanKey = prevKey.first < key;
                    }
                    if (!prevKeySmallerThanKey) {
                        map[value] = std::make_pair(key, depthKey);
                    }

                }
            }
        }

        while (true) {
            bool replacedEntry = false;
            for(auto &pair : map) {
                if (map.count(pair.second.first)) {
                    //Replace the value with the current one
                    auto &v = map[pair.second.first];
                    pair.second = v;
                    replacedEntry = true;
                }
            }
            if (!replacedEntry) {
                break;
            }
        }

        //Replace all the terms in the database
        bool replaced = false;
        if (map.size() > 0) {
            //Go through all the derivations in listDerivations.
            int idx = 0;
            for(auto &block : listDerivations) {
                assert(block.isCompleted);
                auto table = block.table;
                //Remove replaced facts?
                bool removedReplaced = !emfa;
                std::pair<std::shared_ptr<const Segment>,
                    std::shared_ptr<const Segment>> out =
                        table->replaceAllTermsWithMap(map, removedReplaced);
                auto oldSegment = out.first;
                auto newSegment = out.second;

                auto predId = block.query.getPredicate().getId();
                auto fctable
                    = sn->getTable(predId, block.query.getTupleSize());
                if (newSegment.get() != NULL && !newSegment->isEmpty()) {
                    //Some of the replaced rows might be duplicates ...
                    auto filteredSegment = fctable->retainFrom(newSegment,
                            false, nthreads);
                    if (filteredSegment->getNRows() > 0) {
                        //Create a new block
                        auto rowsize = table->getRowSize();
                        std::shared_ptr<const FCInternalTable> newtable =
                            std::shared_ptr<const FCInternalTable>(
                                    new InmemoryFCInternalTable(rowsize,
                                        iteration,
                                        true,
                                        newSegment));

                        fctable->add(newtable, block.query, 0, NULL, 0,
                                iteration, true, nthreads);
                        replaced = true;
                    }
                }

                if (removedReplaced && replaced) {
                    if (oldSegment.get() == NULL || oldSegment->isEmpty()) {
                        LOG(ERRORL) << "Substituting a table with NULL or empty table. Not tested!";
                        throw 10;
                    }
                    std::shared_ptr<const FCInternalTable> newtable =
                        std::shared_ptr<const FCInternalTable>(
                                new InmemoryFCInternalTable(table->getRowSize(),
                                    block.iteration,
                                    true,
                                    oldSegment));
                    block.table = newtable;
                    //Replace it also in fctable
                    fctable->replaceInternalTable(block.iteration, newtable);
                }
            }
        }
        termsToReplace.clear();
        return replaced;
    }
    return false;
}
