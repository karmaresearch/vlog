#include <vlog/extresultjoinproc.h>
#include <vlog/ruleexecdetails.h>
#include <vlog/seminaiver.h>

ExistentialRuleProcessor::ExistentialRuleProcessor(
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
        std::shared_ptr<ChaseMgmt> chaseMgmt) :
    FinalRuleProcessor(posFromFirst, posFromSecond, listDerivations,
            heads, detailsRule, ruleExecOrder, iteration,
            addToEndTable, nthreads, sn), chaseMgmt(chaseMgmt) {
        this->sn = sn;
        //if (posFromFirst.size() > 0)
        //    throw 10;
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
        std::vector<uint64_t> &outputProc) {
    //Filter out all substitutions are are already existing...
    std::vector<std::shared_ptr<Column>> tobeRetained;
    std::vector<uint8_t> columnsToCheck;
    const uint8_t rowsize = literal.getTupleSize();
    std::vector<uint64_t> output;
    for (int i = 0; i < rowsize; ++i) {
        auto t = literal.getTermAtPos(i);
        if (!t.isVariable()) {
            tobeRetained.push_back(std::shared_ptr<Column>(
                        new CompressedColumn(row[initialCount + i],
                            c[0]->size())));
            columnsToCheck.push_back(i);
        } else {
            bool found = false;
            uint64_t posToCopy;
            for(int j = 0; j < nCopyFromSecond; ++j) {
                if (posFromSecond[j].first == initialCount + i) {
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
            } else if (cmp <= 0) {
                if (cmp == 0)
                    output.push_back(idx);
                itr1Ok = itr1->hasNext();
                if (itr1Ok)
                    itr1->next();
                // Should we not increment idx here??? Added. --Ceriel
                idx++;
            }
        }
        tableItr.moveNextCount();
    }
    std::sort(output.begin(), output.end());
    auto end = std::unique(output.begin(), output.end());
    output.resize(end - output.begin());
    for(auto el : output)
        outputProc.push_back(el);

}

void ExistentialRuleProcessor::retainNonExisting(
        std::vector<uint64_t> &filterRows,
        uint64_t &sizecolumns,
        std::vector<std::shared_ptr<Column>> &c) {

    std::sort(filterRows.begin(), filterRows.end());
    std::vector<uint64_t> newFilterRows; //Remember only the rows where all
    //atoms were found
    uint8_t count = 0;
    uint64_t prevIdx = ~0lu;
    for(uint64_t i = 0; i < filterRows.size(); ++i) {
        if (filterRows[i] == prevIdx) {
            count++;
        } else {
            if (prevIdx != ~0lu && count == atomTables.size()) {
                newFilterRows.push_back(prevIdx);
            }
            prevIdx = filterRows[i];
            count = 1;
        }
    }
    if (prevIdx != ~0lu && count == atomTables.size()) {
        newFilterRows.push_back(prevIdx);
    }
    filterRows = newFilterRows;

    if (!filterRows.empty()) {
        //Now I can filter the columns
        std::vector<ColumnWriter> writers;
        std::vector<std::unique_ptr<ColumnReader>> readers;
        for(uint8_t i = 0; i < c.size(); ++i) {
            readers.push_back(c[i]->getReader());
        }
        writers.resize(c.size());
        uint64_t idxs = 0;
        uint64_t nextid = filterRows[idxs];
        for(uint64_t i = 0; i < sizecolumns; ++i) {
            if (i < nextid) {
                //Copy
                for(uint8_t j = 0; j < c.size(); ++j) {
                    if (!readers[j]->hasNext()) {
                        throw 10;
                    }
                    writers[j].add(readers[j]->next());
                }
            } else {
                //Move to the next ID if any
                idxs++;
                if (idxs < filterRows.size()) {
                    nextid = filterRows[idxs];
                } else {
                    nextid = ~0lu; //highest value -- copy the rest
                }
            }
        }
        //Copy back the retricted columns
        for(uint8_t i = 0; i < c.size(); ++i) {
            c[i] = writers[i].getColumn();
        }
        sizecolumns = 0;
        if (rowsize > 0) {
            sizecolumns = c[0]->size();
        }
    }
}

void ExistentialRuleProcessor::addColumns(const int blockid,
        std::vector<std::shared_ptr<Column>> &c,
        const bool unique, const bool sorted) {
    //Even though the code below is designed to handle multiple head atoms
    //this works only if there is only head atom.
    if (atomTables.size() > 1) {
        LOG(ERRORL) << "Calling untested procedure. Aborting ...";
        throw 10;
    }

    uint64_t sizecolumns = 0;
    if (c.size() > 0) {
        sizecolumns = c[0]->size();
    }

    std::vector<std::shared_ptr<Column>> knownColumns;
    uint8_t nKnownColumns = 0;
    std::pair<uint8_t,uint8_t> posKnownColumns[SIZETUPLE];
    for(uint8_t j = 0; j < c.size(); ++j) {
        bool found = false;
        for(uint8_t i = 0; i < nCopyFromFirst; ++i) {
            if (posFromFirst[i].first == j) {
                found = true;
                posKnownColumns[nKnownColumns++] = std::make_pair(0, posFromFirst[i].first);
                break;
            }
        }
        for(uint8_t i = 0; i < nCopyFromSecond && !found; ++i) {
            if (posFromSecond[i].first == j) {
                found = true;
                posKnownColumns[nKnownColumns++] = std::make_pair(0, posFromSecond[i].first);
                break;
            }
        }
        if (found) {
            knownColumns.push_back(c[j]);
        }
    }

    std::vector<uint64_t> filterRows; //The restricted chase might remove some IDs
    if (chaseMgmt->isRestricted()) {
        uint8_t count = 0;
        for(const auto &at : atomTables) {
            const auto &h = at->getLiteral();
            FCTable *t = sn->getTable(h.getPredicate().getId(),
                    h.getPredicate().getCardinality());
            filterDerivations(h, t, row, count, ruleDetails, nKnownColumns,
                    posKnownColumns, c, sizecolumns, filterRows);
            count += h.getTupleSize();
        }
    }

    if (filterRows.size() == sizecolumns * atomTables.size()) {
        return; //every substitution already exists in the database. Nothing
        //new can be derived.
    }

    //Filter out the potential values for the derivation
    //(only restricted chase can do it)
    if (!filterRows.empty()) {
        retainNonExisting(filterRows, sizecolumns, c);
    }

    //Create existential columns
    const auto &at = atomTables[0];
    const auto &literal = at->getLiteral();
    assert(literal.getTupleSize() == c.size());
    std::map<uint8_t, std::shared_ptr<Column>> extvars;
    for(uint8_t i = 0; i < c.size(); ++i) {
        auto t = literal.getTermAtPos(i);
        bool found = false;
        for(int j = 0; j < nKnownColumns; ++j) {
            if (posKnownColumns[j].second == i) {
                found = true;
                break;
            }
        }
        //The column is existential
        if (!found) {
            if (!extvars.count(t.getId())) { //Must be existential
                //The body might contain more variables than what
                //is needed to create existential columns
                //First I copy the columns from the body of the rule
                auto extcolumn = chaseMgmt->getNewOrExistingIDs(
                        ruleDetails->rule.getId(),
                        t.getId(),
                        knownColumns,
                        sizecolumns);
                extvars.insert(std::make_pair(t.getId(), extcolumn));
            }
            c[i] = extvars[t.getId()];
        }
    }

    //Output the columns
    for(auto &t : atomTables) {
        t->addColumns(blockid, c, unique, sorted);
    }
}

void ExistentialRuleProcessor::addColumn(const int blockid, const uint8_t pos,
        std::shared_ptr<Column> column, const bool unique,
        const bool sorted) {
    //TODO: Chase...
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

void ExistentialRuleProcessor::addColumns(const int blockid,
        FCInternalTableItr *itr, const bool unique,
        const bool sorted, const bool lastInsert) {
    if (nCopyFromFirst > 0) {
        LOG(ERRORL) << "This method is not supposed to work if other columns"
            " from outside the itr";
        throw 10;
    }
    std::vector<std::shared_ptr<Column>> c = itr->getAllColumns();
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

    if (filterRows.size() == sizecolumns * atomTables.size()) {
        return; //every substitution already exists in the database. Nothing
        //new can be derived.
    }

    //Filter out the potential values for the derivation
    //(only restricted chase can do it)
    if (!filterRows.empty()) {
        retainNonExisting(filterRows, sizecolumns, c);
    }

    //Create existential columns store them in a vector with the corresponding
    //var ID
    std::map<uint8_t, std::shared_ptr<Column>> extvars;
    uint8_t count = 0;
    for(const auto &at : atomTables) {
        const auto &literal = at->getLiteral();
        for(uint8_t i = 0; i < literal.getTupleSize(); ++i) {
            auto t = literal.getTermAtPos(i);
            if (t.isVariable()) {
                bool found = false;
                for(int j = 0; j < nCopyFromSecond; ++j) { //Does it exist in the body?
                    if (posFromSecond[j].first == count + i) {
                        found = true;
                        break;
                    }
                }
                if (!found && !extvars.count(t.getId())) { //Must be existential
                    //The body might contain more variables than what
                    //is needed to create existential columns
                    //First I copy the columns from the body of the rule
                    std::vector<
                        std::pair<uint8_t, std::shared_ptr<Column>>> depc_t;
                    for(uint8_t i = 0; i < nCopyFromSecond; ++i) {
                        depc_t.push_back(std::make_pair(posFromSecond[i].first,
                                    c[posFromSecond[i].second]));
                    }
                    //I sort the columns according the order where they appear
                    //in the head
                    sort(depc_t.begin(), depc_t.end(),
                            [](const std::pair<uint8_t, std::shared_ptr<Column>>& a,
                                const std::pair<uint8_t, std::shared_ptr<Column>>& b) -> bool {
                            return a.first < b.first;
                            });
                    //Now that the columns are sorted, I no longer care
                    //about the indices
                    std::vector<std::shared_ptr<Column>> depc;
                    for(auto &el : depc_t) {
                        depc.push_back(el.second);
                    }
                    auto extcolumn = chaseMgmt->getNewOrExistingIDs(
                            ruleDetails->rule.getId(),
                            t.getId(),
                            depc,
                            sizecolumns);
                    extvars.insert(std::make_pair(t.getId(), extcolumn));
                }
            }
        }
        count += literal.getTupleSize();
    }

    //The heads might also contain constants or ext vars.
    std::vector<std::shared_ptr<Column>> newc;
    count = 0;
    for(const auto &at : atomTables) {
        const auto &literal = at->getLiteral();
        for (int i = 0; i < literal.getTupleSize(); ++i) {
            auto t = literal.getTermAtPos(i);
            if (!t.isVariable()) {
                newc.push_back(std::shared_ptr<Column>(
                            new CompressedColumn(row[count + i],
                                sizecolumns)));
            } else {
                //Does the variable appear in the body? Then copy it
                bool found = false;
                for(uint8_t j = 0; j < nCopyFromSecond; ++j) {
                    if (posFromSecond[j].first == count + i) {
                        found = true;
                        uint8_t pos = posFromSecond[j].second;
                        newc.push_back(c[pos]);
                        break;
                    }
                }
                if (!found) {
                    newc.push_back(extvars[t.getId()]);
                }
            }
        }
        count += literal.getTupleSize();
    }
    c = newc;

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
