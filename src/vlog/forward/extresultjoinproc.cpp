#include <vlog/resultjoinproc.h>
#include <vlog/ruleexecdetails.h>

ExistentialRuleProcessor::ExistentialRuleProcessor(
        std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
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
        std::shared_ptr<ChaseMgmt> chaseMgmt) :
    FinalRuleProcessor(posFromFirst, posFromSecond, listDerivations,
            t, head, posHeadInRule, detailsRule, ruleExecOrder, iteration,
            addToEndTable, nthreads), chaseMgmt(chaseMgmt) {
    }

void ExistentialRuleProcessor::processResults(const int blockid,
        const bool unique, std::mutex *m) {
    //TODO: Chase...
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
    FinalRuleProcessor::processResults(blockid, unique, m);
}

int _compare(std::unique_ptr<SegmentIterator> &itr1,
        FCInternalTableItr *itr2,
        uint8_t rowsize) {
    for(uint8_t i = 0; i < rowsize; ++i) {
        auto t1 = itr1->get(i);
        auto t2 = itr2->getCurrentValue(i);
        if (t1 < t2) {
            return -1;
        } else if (t1 > t2) {
            return 1;
        }
    }
    return 0;
}

void ExistentialRuleProcessor::filterDerivations(
        std::vector<std::shared_ptr<Column>> c,
        std::vector<uint64_t> &output) {
    //Filter out all substitutions are are already existing...
    std::vector<std::shared_ptr<Column>> tobeRetained;
    std::vector<uint8_t> columnsToCheck;
    for (int i = 0; i < rowsize; ++i) {
        auto t = literal.getTermAtPos(i);
        if (!t.isVariable()) {
            tobeRetained.push_back(std::shared_ptr<Column>(
                        new CompressedColumn(row[i],
                            c[0]->size())));
            columnsToCheck.push_back(i);
        } else {
            bool found = false;
            uint64_t posToCopy;
            for(int j = 0; j < nCopyFromSecond; ++j) {
                if (posFromSecond[j].first == i) {
                    found = true;
                    posToCopy = posFromSecond[j].second;
                    break;
                }
            }
            if (!found) {
                //Add a dummy column since this is an existential variable
                tobeRetained.push_back(std::shared_ptr<Column>());
            } else {
                tobeRetained.push_back(std::shared_ptr<Column>(c[posToCopy]));
                columnsToCheck.push_back(i);
            }
        }
    }
    //now tobeRetained contained a copy of the head without the existential
    //replacements. I restrict it to only substitutions that are not in the KG
    std::shared_ptr<const Segment> seg = std::shared_ptr<const Segment>(
            new Segment(rowsize, tobeRetained));

    //do the filtering
    auto sortedSeg = seg->sortBy(NULL);
    auto tableItr = t->read(0);
    while (!tableItr.isEmpty()) {
        auto table = tableItr.getCurrentTable();
        auto itr1 = sortedSeg->iterator();
        auto itr2 = table->getSortedIterator();
        uint64_t idx = 0;
        bool itr1Ok = itr1->hasNext();
        if (itr1Ok) itr1->next();
        bool itr2Ok = itr2->hasNext();
        if (itr2Ok) itr2->next();
        while (itr1Ok && itr2Ok) {
            int cmp = _compare(itr1, itr2, rowsize);
            if (cmp > 0) {
                //the table must move to the next one
                itr2Ok = itr2->hasNext();
                if (itr2Ok)
                    itr2->next();
            } else if (cmp >= 0) {
                if (cmp == 0)
                    output.push_back(idx);
                itr1Ok = itr1->hasNext();
                if (itr1Ok)
                    itr1->next();
            }
        }
        tableItr.moveNextCount();
    }
    std::sort(output.begin(), output.end());
}

void ExistentialRuleProcessor::addColumns(const int blockid,
        FCInternalTableItr *itr, const bool unique,
        const bool sorted, const bool lastInsert) {
    enlargeBuffers(blockid + 1);

    if (tmpt[blockid] == NULL) {
        tmpt[blockid] = new SegmentInserter(rowsize);
    }

    uint8_t columns[128];
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        columns[i] = posFromSecond[i].second;
    }
    std::vector<std::shared_ptr<Column>> c = itr->getColumn(nCopyFromSecond, columns);
    uint64_t sizecolumn = 0;
    if (c.size() > 0) {
        sizecolumn = c[0]->size();
    }

    std::vector<uint64_t> filterRows; //The restricted chase might remove some IDs
    if (chaseMgmt->isRestricted()) {
        filterDerivations(c, filterRows);
    }

    if (!filterRows.empty()) {
        //Filter out the potential values for the derivation
        std::vector<ColumnWriter> writers;
        std::vector<std::unique_ptr<ColumnReader>> readers;
        for(uint8_t i = 0; i < rowsize; ++i) {
            readers.push_back(c[i]->getReader());
        }
        writers.resize(c.size());
        uint64_t idxs = 0;
        uint64_t nextid = filterRows[idxs];
        for(uint64_t i = 0; i < sizecolumn; ++i) {
            if (i < nextid) {
                //Copy
                for(uint8_t j = 0; j < rowsize; ++j) {
                    if (!readers[j]->hasNext()) {
                        throw 10;
                    }
                    writers[j].add(readers[j]->next());
                }
            } else {
                //Move to the next ID if any
                if (idxs < filterRows.size()) {
                    nextid = filterRows[++idxs];
                } else {
                    nextid = ~0lu; //highest value -- copy the rest
                }
            }
        }
        //Copy back the retricted columns
        for(uint8_t i = 0; i < rowsize; ++i) {
            c[i] = writers[i].getColumn();
        }
    }

    //Create existential columns store them in a vector with the corresponding var ID
    std::vector<std::pair<uint8_t, std::shared_ptr<Column>>> extvars;
    for (int i = 0; i < rowsize; ++i) {
        auto t = literal.getTermAtPos(i);
        if (t.isVariable()) {
            bool found = false;
            for(int j = 0; j < nCopyFromSecond; ++j) {
                if (posFromSecond[j].first == i) {
                    found = true;
                    break;
                }
            }
            if (!found) { //Must be existential
                auto extcolumn = chaseMgmt->getNewOrExistingIDs(
                        ruleDetails->rule.getId(),
                        t.getId(),
                        c,
                        sizecolumn);
                extvars.push_back(std::make_pair(t.getId(), extcolumn));
            }
        }
    }

    if (nCopyFromSecond > 1) { //Rearrange the columns depending on the order
        //in the head
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

    //Remove from c all columns that do not appear in the head
    uint8_t nonheadcolumns = 0;
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        if (posFromSecond[i].first == (uint8_t)~0) {
            nonheadcolumns++;
        }
    }
    c.resize(c.size() - nonheadcolumns);

    if (c.size() < rowsize) {
        //The head contains also constants or ext vars. We must add fields in the vector
        // of created columns.
        std::vector<std::shared_ptr<Column>> newc;
        int idxVar = 0;
        for (int i = 0; i < rowsize; ++i) {
            auto t = literal.getTermAtPos(i);
            if (!t.isVariable()) {
                newc.push_back(std::shared_ptr<Column>(
                            new CompressedColumn(row[i],
                                c[0]->size())));
            } else {
                //Does the variable appear in the body? Then copy it
                if (idxVar < nCopyFromSecond &&
                        posFromSecond[idxVar].first == i) {
                    newc.push_back(c[idxVar++]);
                } else {
                    //Existential variable
                    uint8_t idx = 0;
                    bool found = false;
                    for(; idx < extvars.size(); ++idx) {
                        if (extvars[idx].first == t.getId()) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        LOG(ERRORL) << "Should not happen";
                        throw 10;
                    }
                    newc.push_back(extvars[idx].second);
                }
            }
        }
        c = newc;
    }
    tmpt[blockid]->addColumns(c, sorted, lastInsert);
}
