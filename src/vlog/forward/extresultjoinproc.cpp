#include <vlog/extresultjoinproc.h>
#include <vlog/ruleexecdetails.h>
#include <vlog/seminaiver.h>

ExistentialRuleProcessor::ExistentialRuleProcessor(
        std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
        std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
        std::vector<Literal> &heads,
        const RuleExecutionDetails *detailsRule,
        const uint8_t ruleExecOrder,
        const size_t iteration,
        const bool addToEndTable,
        const int nthreads,
        SemiNaiver *sn,
        std::shared_ptr<ChaseMgmt> chaseMgmt) :
    FinalRuleProcessor(posFromFirst, posFromSecond,
            heads, detailsRule, ruleExecOrder, iteration,
            addToEndTable, nthreads, sn), chaseMgmt(chaseMgmt) {
        this->sn = sn;
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
        const uint8_t *colsCheck,
        const uint8_t ncolsCheck) {
    for(uint8_t i = 0; i < ncolsCheck; ++i) {
        auto t1 = itr1->get(colsCheck[i]);
        auto t2 = itr2->getCurrentValue(colsCheck[i]);
        if (t1 < t2) {
            return -1;
        } else if (t1 > t2) {
            return 1;
        }
    }
    return 0;
}

void ExistentialRuleProcessor::filterDerivations(const Literal &literal,
        FCTable *t,
        Term_t *row,
        uint8_t initialCount,
        const RuleExecutionDetails *detailsRule,
        uint8_t nCopyFromSecond,
        std::pair<uint8_t, uint8_t> *posFromSecond,
        std::vector<std::shared_ptr<Column>> c,
        uint64_t sizecolumns,
        std::vector<uint64_t> &output) {
    //Filter out all substitutions are are already existing...
    std::vector<std::shared_ptr<Column>> tobeRetained;
    std::vector<uint8_t> columnsToCheck;
    const uint8_t rowsize = literal.getTupleSize();
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
                tobeRetained.push_back(std::shared_ptr<Column>(
                            new CompressedColumn(0, sizecolumns)));
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
    const uint8_t *colsCheck = columnsToCheck.data();
    const uint8_t nColsCheck = columnsToCheck.size();
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
            int cmp = _compare(itr1, itr2, colsCheck, nColsCheck);
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
    auto last = std::unique(output.begin(), output.end());
    output.resize(last - output.begin());
}

void ExistentialRuleProcessor::addColumns(const int blockid,
        FCInternalTableItr *itr, const bool unique,
        const bool sorted, const bool lastInsert) {
    uint8_t columns[128];
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        columns[i] = posFromSecond[i].second;
    }
    std::vector<std::shared_ptr<Column>> c = itr->getColumn(nCopyFromSecond, columns);
    uint64_t sizecolumns = 0;
    if (c.size() > 0) {
        sizecolumns = c[0]->size();
    }

    std::vector<uint64_t> filterRows; //The restricted chase might remove some IDs
    if (chaseMgmt->isRestricted()) {
        uint8_t count = 0;
        for(const auto &at : atomTables) {
            const auto &h = at->getLiteral();
            FCTable *t = sn->getTable(h.getPredicate().getId(),
                    h.getPredicate().getCardinality());
            filterDerivations(h, t, row, count, ruleDetails, nCopyFromSecond,
                    posFromSecond, c, sizecolumns, filterRows);
            count += h.getTupleSize();
        }
    }

    if (filterRows.size() == sizecolumns) {
        return; //no new info
    }

    //Filter out the potential values for the derivation
    if (!filterRows.empty()) {
        std::vector<ColumnWriter> writers;
        std::vector<std::unique_ptr<ColumnReader>> readers;
        for(uint8_t i = 0; i < rowsize; ++i) {
            readers.push_back(c[i]->getReader());
        }
        writers.resize(c.size());
        uint64_t idxs = 0;
        uint64_t nextid = filterRows[idxs];
        for(uint64_t i = 0; i < sizecolumns; ++i) {
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
        sizecolumns = 0;
        if (rowsize > 0) {
            sizecolumns = c[0]->size();
        }
    }

    //Create existential columns store them in a vector with the corresponding var ID
    std::map<uint8_t, std::shared_ptr<Column>> extvars;
    uint8_t count = 0;
    for(const auto &at : atomTables) {
        const auto &literal = at->getLiteral();
        for(uint8_t i = 0; i < literal.getTupleSize(); ++i) {
            auto t = literal.getTermAtPos(i);
            if (t.isVariable()) {
                bool found = false;
                for(int j = 0; j < nCopyFromSecond; ++j) { //Does it exist in the body?
                    if (posFromSecond[j].first == i) {
                        found = true;
                        break;
                    }
                }
                if (!found && !extvars.count(t.getId())) { //Must be existential
                    auto extcolumn = chaseMgmt->getNewOrExistingIDs(
                            ruleDetails->rule.getId(),
                            t.getId(),
                            c,
                            sizecolumns);
                    extvars.insert(std::make_pair(t.getId(), extcolumn));
                }
            }
        }
        count += literal.getTupleSize();
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

    if (c.size() < rowsize) {
        //The heads contain also constants or ext vars.
        std::vector<std::shared_ptr<Column>> newc;
        int idxVar = 0;
        uint8_t count = 0;
        for(const auto &at : atomTables) {
            const auto &literal = at->getLiteral();
            for (int i = 0; i < literal.getTupleSize(); ++i) {
                auto t = literal.getTermAtPos(i);
                if (!t.isVariable()) {
                    newc.push_back(std::shared_ptr<Column>(
                                new CompressedColumn(row[count + i],
                                    c[0]->size())));
                } else {
                    //Does the variable appear in the body? Then copy it
                    if (idxVar < nCopyFromSecond &&
                            posFromSecond[idxVar].first == i) {
                        newc.push_back(c[idxVar++]);
                    } else {
                        newc.push_back(extvars[t.getId()]);
                    }
                }
            }
            count += literal.getTupleSize();
        }
        c = newc;
    }

    count = 0;
    for(auto &t : atomTables) {
        uint8_t sizeTuple = t->getLiteral().getTupleSize();
        std::vector<std::shared_ptr<Column>> c2;
        for(uint8_t i = 0; i < sizeTuple; ++i) {
            c2.push_back(c[count + i]);
        }
        t->addColumns(blockid, c2, unique, sorted);
        count += sizeTuple;
    }
}
