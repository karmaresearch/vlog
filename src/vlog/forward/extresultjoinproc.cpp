#include <vlog/extresultjoinproc.h>
#include <vlog/ruleexecdetails.h>
#include <vlog/seminaiver.h>

static bool isPresent(uint8_t el, std::vector<uint8_t> &v) {
    for (int i = 0; i < v.size(); i++) {
        if (el == v[i]) {
            return true;
        }
    }
    return false;
}

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
        std::shared_ptr<ChaseMgmt> chaseMgmt,
        bool filterRecursive,
        const bool ignoreDupElimin) :
    FinalRuleProcessor(posFromFirst, posFromSecond, listDerivations,
            heads, detailsRule, ruleExecOrder, iteration,
            addToEndTable, nthreads, sn,
            ignoreDupElimin), chaseMgmt(chaseMgmt), filterRecursive(filterRecursive) {
        this->sn = sn;

        //When I consolidate, should I assign values to the existential columns?
        //Default is no
        replaceExtColumns = false;
        auto &extmap = detailsRule->
            orderExecutions[ruleExecOrder].extvars2posFromSecond;

        //Remember the positions of the constants, of known columns, and
        //of ext vars
        nConstantColumns = 0;
        nKnownColumns = 0;
        int count = 0;
        for(const auto head : heads) {
            for(uint8_t i = 0; i < head.getTupleSize(); ++i) {
                if (!head.getTermAtPos(i).isVariable()) {
                    posConstantColumns[nConstantColumns++] = count;
                } else {
                    auto term = head.getTermAtPos(i);
                    if (extmap.count(term.getId())) {
                        //It is an ext var
                        if (!posExtColumns.count(term.getId())) {
                            posExtColumns.insert(std::make_pair(term.getId(),
                                        std::vector<uint8_t>()));
                        }
                        posExtColumns[term.getId()].push_back(count);
                    } else {
                        if (! isPresent(term.getId(),varsUsedForExt)) {
                            varsUsedForExt.push_back(term.getId());
                            colsForExt.push_back(count);
                        }
                        posKnownColumns[nKnownColumns++] = count;
                    }
                }
                count++;
            }
        }
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
        uint8_t nCopyColumns,
        std::pair<uint8_t, uint8_t> *posCopyColumns,
        std::vector<std::shared_ptr<Column>> c,
        uint64_t sizecolumns,
        std::vector<uint64_t> &outputProc) {
    std::vector<std::shared_ptr<Column>> tobeRetained;
    std::vector<uint8_t> columnsToCheck;
    const uint8_t rowsize = literal.getTupleSize();
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
            for(int j = 0; j < nCopyColumns; ++j) {
                if (posCopyColumns[j].first == initialCount + i) {
                    found = true;
                    posToCopy = posCopyColumns[j].second;
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

    filterDerivations(t, tobeRetained, columnsToCheck, outputProc);
}

void ExistentialRuleProcessor::filterDerivations(FCTable *t,
        std::vector<std::shared_ptr<Column>> &tobeRetained,
        std::vector<uint8_t> &columnsToCheck,
        std::vector<uint64_t> &outputProc) {
    std::vector<uint64_t> output;

    //tobeRetained contained a copy of the head without the existential
    //replacements. I restrict it to only substitutions that are not in the KG
    std::shared_ptr<const Segment> seg = std::shared_ptr<const Segment>(
            new Segment(tobeRetained.size(), tobeRetained));

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

// Filters out rows with recursive terms
void ExistentialRuleProcessor::retainNonRecursive(
        uint64_t &sizecolumns,
        std::vector<std::shared_ptr<Column>> &c) {
    std::vector<ColumnWriter> writers;
    std::vector<std::unique_ptr<ColumnReader>> readers;
    for(uint8_t i = 0; i < c.size(); ++i) {
        readers.push_back(c[i]->getReader());
    }
    writers.resize(c.size());
    uint64_t idxs = 0;
    uint64_t cols[256];
    for(uint64_t i = 0; i < sizecolumns; ++i) {
        bool copy = true;
        for(uint8_t j = 0; copy && j < c.size(); ++j) {
            if (!readers[j]->hasNext()) {
                throw 10;
            }
            cols[j] = readers[j]->next();
            if (chaseMgmt->checkRecursive(cols[j])) {
                copy = false;
                break;
            }
        }
        if (copy) {
            for(uint8_t j = 0; j < c.size(); ++j) {
                writers[j].add(cols[j]);
            }
        }
    }
    //Copy back the retricted columns
    for(uint8_t i = 0; i < c.size(); ++i) {
        c[i] = writers[i].getColumn();
    }
    sizecolumns = 0;
    if (c.size() > 0) {
        sizecolumns = c[0]->size();
    }
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
                for(uint8_t j = 0; j < c.size(); ++j) {
                    if (!readers[j]->hasNext()) {
                        throw 10;
                    }
                    readers[j]->next();
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

    uint64_t sizecolumns = 0;
    if (c.size() > 0) {
        sizecolumns = c[0]->size();
    }

    uint8_t nKnownColumns = 0;
    std::pair<uint8_t,uint8_t> posKnownColumns[256];
    for(uint8_t j = 0; j < c.size(); ++j) {
        bool found = false;
        for(uint8_t i = 0; i < nCopyFromFirst; ++i) {
            if (posFromFirst[i].first == j) {
                found = true;
                posKnownColumns[nKnownColumns++] = std::make_pair(j, j);
                break;
            }
        }
        for(uint8_t i = 0; i < nCopyFromSecond && !found; ++i) {
            if (posFromSecond[i].first == j) {
                found = true;
                posKnownColumns[nKnownColumns++] = std::make_pair(j, j);
                break;
            }
        }
    }

    if (chaseMgmt->isRestricted()) {
        std::vector<uint64_t> filterRows; //The restricted chase might remove some IDs
        uint8_t count = 0;
        std::vector<bool> blocked;
        size_t blockedCount = 0;
        if (chaseMgmt->isCheckCyclicMode()) {
            for (size_t i = 0; i < sizecolumns; i++) {
                blocked.push_back(false);
            }
        }
        for(const auto &at : atomTables) {
            const auto &h = at->getLiteral();
            if (chaseMgmt->isCheckCyclicMode()) {
                //Init
                std::unique_ptr<uint64_t[]> tmprow(
                        new uint64_t[c.size()]);
                std::unique_ptr<uint64_t[]> headrow(
                        new uint64_t[h.getTupleSize()]);

                std::vector<std::unique_ptr<ColumnReader>> columnReaders;
                for(uint8_t i = 0; i < c.size(); ++i) {
                    columnReaders.push_back(c[i]->getReader());
                }

                //Check in the head the positions that should be checked
                std::vector<uint8_t> posToCheck; //These are positions in the head.
                std::vector<std::pair<uint8_t, uint8_t>> posToCopy;
                for(uint8_t j = 0; j < h.getTupleSize(); ++j) {
                    const auto t = h.getTermAtPos(j);
                    if (t.isVariable()) {
                        bool found = false;
                        for(uint8_t i = 0; i < nCopyFromFirst; ++i) {
                            if (posFromFirst[i].first == j) {
                                found = true;
                                posToCheck.push_back(j);
                                break;
                            }
                        }
                        for(uint8_t i = 0; i < nCopyFromSecond && !found; ++i) {
                            if (posFromSecond[i].first == j) {
                                posToCheck.push_back(j);
                                break;
                            }
                        }
                    } else {
                        posToCheck.push_back(j); //constants should be checked as well
                        headrow[j] = t.getValue();
                    }
                }

                for(size_t i = 0; i < sizecolumns; ++i) {
                    if (blocked[i]) {
                        continue;
                    }
                    //Fill the row
                    for(uint8_t j = 0; j < c.size(); ++j) {
                        if (!columnReaders[j]->hasNext()) {
                            LOG(ERRORL) << "This should not happen";
                        }
                        tmprow[j] = columnReaders[j]->next();
                    }
                    //Fill the headrow with the values from row
                    for(const auto &p : posToCopy) {
                        headrow[p.first] = tmprow[p.second];
                    }
                    if (RMFA_check(tmprow.get(), h, headrow.get(), posToCheck)) {
                        //It is blocked
                        blocked[i] = true;
                        LOG(DEBUGL) << "Blocking row " << i;
                        blockedCount++;
                    }
                }
            } else {
                FCTable *t = sn->getTable(h.getPredicate().getId(),
                        h.getPredicate().getCardinality());
                filterDerivations(h, t, row, count, ruleDetails, nKnownColumns,
                        posKnownColumns, c, sizecolumns, filterRows);
                count += h.getTupleSize();
            }
        }

        if (chaseMgmt->isCheckCyclicMode()) {
            if (blockedCount == sizecolumns) {
                return;
            }
            for (size_t i = 0; i < sizecolumns; i++) {
                if (blocked[i]) {
                    for (int j = 0; j < atomTables.size(); j++) {
                        filterRows.push_back(i);
                    }
                }
            }
        }

        if (filterRows.size() == sizecolumns * atomTables.size()) {
            return; //every substitution already exists in the database. Nothing
            //new can be derived.
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
    }

    std::vector<std::shared_ptr<Column>> knownColumns;
    for(int i = 0; i < colsForExt.size(); ++i) {
        knownColumns.push_back(c[colsForExt[i]]);
    }

    //Create existential columns
    // assert(literal.getTupleSize() == c.size()); ???? not correct, I think.
    std::map<uint8_t, std::shared_ptr<Column>> extvars;
    uint8_t count = 0;
    for(const auto &at : atomTables) {
        std::vector<std::shared_ptr<Column>> cols;
        const auto &literal = at->getLiteral();
        for(uint8_t i = 0; i < literal.getTupleSize(); ++i) {
            // TODO: deal with possible constant fields?
            auto t = literal.getTermAtPos(i);
            bool found = false;
            for(int j = 0; j < nKnownColumns; ++j) {
                if (posKnownColumns[j].second == i + count) {
                    found = true;
                    cols.push_back(c[posKnownColumns[j].second]);
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
                cols.push_back(extvars[t.getId()]);
            }
        }
        at->addColumns(blockid, cols, unique, sorted);
        count += literal.getTupleSize();
    }
}

void ExistentialRuleProcessor::processResults(const int blockid, const Term_t *first,
        FCInternalTableItr* second, const bool unique) {
    copyRawRow(first, second);
    replaceExtColumns = true;
    if (!tmpRelation) {
        tmpRelation = std::unique_ptr<SegmentInserter>(
                new SegmentInserter(rowsize));
    }
    tmpRelation->addRow(row);
}

void ExistentialRuleProcessor::processResults(const int blockid,
        const std::vector<const std::vector<Term_t> *> &vectors1, size_t i1,
        const std::vector<const std::vector<Term_t> *> &vectors2, size_t i2,
        const bool unique) {
    for (int i = 0; i < nCopyFromFirst; i++) {
        row[posFromFirst[i].first] = (*vectors1[posFromFirst[i].second])[i1];
    }
    for (int i = 0; i < nCopyFromSecond; i++) {
        row[posFromSecond[i].first] = (*vectors2[posFromSecond[i].second])[i2];
    }

    replaceExtColumns = true;
    if (!tmpRelation) {
        tmpRelation = std::unique_ptr<SegmentInserter>(
                new SegmentInserter(rowsize));
    }
    tmpRelation->addRow(row);
}

void ExistentialRuleProcessor::processResults(std::vector<int> &blockid, Term_t *p,
        std::vector<bool> &unique, std::mutex *m) {
    //TODO: Chase...
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}

void ExistentialRuleProcessor::processResults(const int blockid, FCInternalTableItr *first,
        FCInternalTableItr* second, const bool unique) {
    for (uint32_t i = 0; i < nCopyFromFirst; ++i) {
        row[posFromFirst[i].first] = first->getCurrentValue(posFromFirst[i].second);
    }
    for (uint32_t i = 0; i < nCopyFromSecond; ++i) {
        row[posFromSecond[i].first] = second->getCurrentValue(posFromSecond[i].second);
    }
    replaceExtColumns = true;
    if (!tmpRelation) {
        tmpRelation = std::unique_ptr<SegmentInserter>(
                new SegmentInserter(rowsize));
    }
    tmpRelation->addRow(row);
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

    if (chaseMgmt->isRestricted()) {
        std::vector<uint64_t> filterRows; //The restricted chase might remove some IDs
        uint8_t count = 0;
        std::vector<bool> blocked;
        size_t blockedCount = 0;
        if (chaseMgmt->isCheckCyclicMode()) {
            for (size_t i = 0; i < sizecolumns; i++) {
                blocked.push_back(false);
            }
        }
        for(const auto &at : atomTables) {
            const auto &h = at->getLiteral();

            if (chaseMgmt->isCheckCyclicMode()) {
                std::unique_ptr<uint64_t[]> tmprow(new uint64_t[c.size()]);
                std::unique_ptr<uint64_t[]> headrow(new uint64_t[h.getTupleSize()]);
                std::vector<std::unique_ptr<ColumnReader>> columnReaders;
                for(uint8_t i = 0; i < c.size(); ++i) {
                    columnReaders.push_back(c[i]->getReader());
                }

                std::vector<uint8_t> columnsToCheck;
                std::vector<std::pair<uint8_t,uint8_t>> columnsToCopy;
                for(uint8_t j = 0; j < h.getTupleSize(); ++j) {
                    VTerm t = h.getTermAtPos(j);
                    if (t.isVariable()) {
                        bool found = false;
                        for(uint8_t m = 0; m < nCopyFromSecond && !found; ++m) {
                            if (posFromSecond[m].first == count + j) {
                                found = true;
                                columnsToCopy.push_back(std::make_pair(
                                            j, posFromSecond[m].second));
                                columnsToCheck.push_back(j);
                            }
                        }
                    } else {
                        headrow[j] = t.getValue();
                        columnsToCheck.push_back(j);
                    }
                }
                for(size_t i = 0; i < sizecolumns; ++i) {
                    if (blocked[i]) {
                        continue;
                    }
                    //Fill the row
                    for(uint8_t j = 0; j < c.size(); ++j) {
                        if (!columnReaders[j]->hasNext()) {
                            LOG(ERRORL) << "This should not happen";
                        }
                        tmprow[j] = columnReaders[j]->next();
                    }
                    for(const auto &p : columnsToCopy) {
                        headrow[p.first] = tmprow[p.second];
                    }
                    if (RMFA_check(tmprow.get(), h, headrow.get(),
                                columnsToCheck)) { //Is blocked
                        blocked[i] = true;
                        LOG(DEBUGL) << "Blocking row " << i;
                        blockedCount++;
                    }
                }
            } else {
                FCTable *t = sn->getTable(h.getPredicate().getId(),
                        h.getPredicate().getCardinality());
                filterDerivations(h, t, row, count, ruleDetails, nCopyFromSecond,
                        posFromSecond, c, sizecolumns, filterRows);
            }
            count += h.getTupleSize();
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
                for(int j = 0; j < nCopyFromSecond; ++j) { //Does it exist
                    //in the body?
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
                    //A column may be used more than once in the head. Filter duplicates out.
                    //Otherwise, an assert goes off in getNewOrExistingIDs --Ceriel
                    for(auto &el : depc_t) {
                        bool present = false;
                        for (auto &l : depc) {
                            if (el.second == l) {
                                present = true;
                                break;
                            }
                        }
                        if (! present) {
                            depc.push_back(el.second);
                        }
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

void ExistentialRuleProcessor::RMFA_computeBodyAtoms(
        std::vector<Literal> &output,
        uint64_t *row) {
    const RuleExecutionPlan &plan = ruleDetails->orderExecutions[ruleExecOrder];
    auto bodyAtoms = plan.plan;
    int idx = 0;
    for(const auto &atom : bodyAtoms) {
        VTuple tuple(atom->getTuple());
        auto v2p = plan.vars2pos[idx];
        for(int i = 0; i < v2p.size(); ++i) {
            auto pair = v2p[i];
            tuple.set(VTerm(0, row[pair.second]), pair.first);
        }
        output.push_back(Literal(atom->getPredicate(), tuple));
        idx++;
    }
}

std::unique_ptr<SemiNaiver> ExistentialRuleProcessor::RMFA_saturateInput(
        std::vector<Literal> &input) {
    //Populate the EDB layer
    EDBLayer layer(sn->getEDBLayer());
    std::map<PredId_t, std::vector<uint64_t>> edbPredicates;
    std::map<PredId_t, std::vector<uint64_t>> idbPredicates;
    for(const auto &literal : input) {
        auto predid = literal.getPredicate().getId();
        if (literal.getPredicate().getType() != EDB) {
            if (!idbPredicates.count(predid)) {
                idbPredicates.insert(std::make_pair(predid, std::vector<uint64_t>()));
            }
            for(uint8_t i = 0; i < literal.getTupleSize(); ++i) {
                auto t = literal.getTermAtPos(i);
                idbPredicates[predid].push_back(t.getValue());
            }
        } else {
            if (!edbPredicates.count(predid)) {
                edbPredicates.insert(std::make_pair(predid, std::vector<uint64_t>()));
            }
            for(uint8_t i = 0; i < literal.getTupleSize(); ++i) {
                auto t = literal.getTermAtPos(i);
                edbPredicates[predid].push_back(t.getValue());
            }
        }
    }
    for(auto &pair : edbPredicates) {
        uint8_t arity = sn->getEDBLayer().getPredArity(pair.first);
        layer.addInmemoryTable(pair.first, arity, pair.second);
    }

    //Launch the semi-naive evaluation
    Program *program = sn->getProgram();
    std::unique_ptr<SemiNaiver> lsn(new SemiNaiver(program->getAllRules(),
                layer, program, true, true, false, 1, false, true));

    //Populate the IDB layer
    for(auto &pair : idbPredicates) {
        Predicate pred = sn->getProgram()->getPredicate(pair.first);
        const uint8_t card = pred.getCardinality();

        //Construct the table
        SegmentInserter inserter(card);
        auto els = pair.second.data();
        for(uint64_t i = 0; i < pair.second.size(); i += card) {
            inserter.addRow(els + i);
        }

        //Populate the table
        std::shared_ptr<const FCInternalTable> ltable(
                new InmemoryFCInternalTable(card,
                    0, false, inserter.getSegment()));

        //Define a generic query
        VTuple tuple(card);
        for(uint8_t i = 0; i < card; ++i) {
            // tuple.set(VTerm(i, 0), i);
            tuple.set(VTerm(i+1, 0), i);	// I suppose these should all be variables ... --Ceriel
        }
        Literal query(pred, tuple);
        FCBlock block(0, ltable, query, 0, NULL, 0, true);
        FCTable *table = lsn->getTable(pair.first, card);
        table->addBlock(block);
    }

    //Launch the materialization
    lsn->run();
    return lsn;
}

void _addIfNotExist(std::vector<Literal> &output, Literal l) {
    bool found = false;
    for(const auto &ol : output) {
        if (ol.getPredicate().getId() == l.getPredicate().getId()) {
            found = true;
            for(uint8_t j = 0; j < ol.getTupleSize(); ++j) {
                if (ol.getTermAtPos(j).getValue() != l.getTermAtPos(j).getValue()) {
                    found = false;
                    break;
                }
            }
            if (found) break;
        }
    }
    if (!found)
        output.push_back(l);
}

void ExistentialRuleProcessor::RMFA_enhanceFunctionTerms(
        std::vector<Literal> &output,
        uint64_t &startFreshIDs,
        size_t startOutput) {
    size_t oldsize = output.size();
    //Check every fact until oldsize. If there is a function term, get also
    //all related facts
    LOG(DEBUGL) << "RMFA_enhanceFuntionTerms, startOutput = " << startOutput << ", oldsize = " << oldsize;
    for(size_t i = startOutput; i < oldsize; ++i) {
        const auto literal = output[i];
        for(uint8_t j = 0; j < literal.getTupleSize(); ++j) {
            const auto &term = literal.getTermAtPos(j);
            if (term.getValue() != COUNTER(term.getValue())) {
                //This is a function term. Get rule ID
                const uint64_t ruleID = GET_RULE(term.getValue());
                auto *ruleContainer = chaseMgmt->getRuleContainer(ruleID);
                assert(ruleContainer != NULL);
                //Get var ID
                const uint64_t varID = GET_VAR(term.getValue());
                //Get the arguments of the function term from the chase mgmt
                auto *rows = ruleContainer->getRows(varID);
                const uint64_t localCounter = COUNTER(term.getValue());
                const uint64_t *values = rows->getRow(localCounter);
                const uint64_t nvalues = rows->getSizeRow();
                //Map them to variables
                const auto &nameVars = rows->getNameArgVars();
                std::map<uint8_t, uint64_t> mappings;
                for(uint8_t i = 0; i < nvalues; ++i) {
                    mappings.insert(std::make_pair(nameVars[i], values[i]));
                }
                //Materialize the remaining facts giving fresh IDs to
                //the rem. variables
                auto const *rule = ruleContainer->getRule();
                for(const auto &bLiteral : rule->getBody()) {
                    VTuple t(bLiteral.getTupleSize());
                    for(uint8_t m = 0; m < bLiteral.getTupleSize(); ++m) {
                        const VTerm term = bLiteral.getTermAtPos(m);
                        if (term.isVariable()) {
                            uint8_t varID = term.getId();
                            if (!mappings.count(varID)) {
                                mappings.insert(
                                        std::make_pair(varID,
                                            startFreshIDs++));
                            }
                            t.set(VTerm(0, mappings[varID]), m);
                        } else {
                            t.set(term, m);
                        }
                    }
                    _addIfNotExist(output, Literal(bLiteral.getPredicate(), t));
                }
                //Also add the original variable
                assert(!mappings.count(varID));
                mappings.insert(std::make_pair(varID, term.getValue()));

                //Also consider the possible other head atoms
                for(const auto &hLiteral : rule->getHeads()) {
                    if (hLiteral.getPredicate().getId() ==
                            literal.getPredicate().getId()) {
                        continue;
                    }
                    VTuple t(hLiteral.getTupleSize());
                    for(uint8_t m = 0; m < hLiteral.getTupleSize(); ++m) {
                        const VTerm term = hLiteral.getTermAtPos(m);
                        if (term.isVariable()) {
                            uint8_t varID = term.getId();
                            if (!mappings.count(varID)) {
                                LOG(ERRORL) << "There are existenatial variables not defined. Must implement their retrievals";
                                throw 10;
                            }
                            t.set(VTerm(0, mappings[varID]), m);
                        } else {
                            t.set(term, m);
                        }
                    }
                    _addIfNotExist(output, Literal(hLiteral.getPredicate(), t));
                }
            }
        }
    }
    //Recursively apply the function if there are new literals
    if (output.size() > oldsize) {
        RMFA_enhanceFunctionTerms(output, startFreshIDs, oldsize);
    }
}

bool ExistentialRuleProcessor::RMFA_check(uint64_t *row,
        const Literal &headLiteral,
        uint64_t *headrow, std::vector<uint8_t> &columnsToCheck) {
    LOG(DEBUGL) << "RMFA_check, headLiteral = " << headLiteral.tostring(NULL, NULL);
#if DEBUG
    for (int i = 0; i < columnsToCheck.size(); i++) {
	LOG(TRACEL) << "columnsToCheck[" << i << "] = " << (int) columnsToCheck[i];
	LOG(TRACEL) << "headrow[" << (int) columnsToCheck[i] << "] = " << headrow[columnsToCheck[i]];
    }
#endif
    //Get a starting value for the fresh IDs
    uint64_t freshIDs = 1; //0 is star

    //In this case I invoke the code needed for the RMFA
    std::vector<Literal> input; //"input" corresponds to B_\rho,\sigma in the paper
    //First I need to add to body atoms
    RMFA_computeBodyAtoms(input, row);

#if DEBUG
    for (int i = 0; i < input.size(); i++) {
	LOG(TRACEL) << "input[" << i << "] = " << input[i].tostring(NULL, NULL);
    }
#endif

    //Then I need to add all facts relevant to produce the function terms
    RMFA_enhanceFunctionTerms(input, freshIDs);

    //Finally I need to saturate "input" with the datalog rules
    std::unique_ptr<SemiNaiver> n = RMFA_saturateInput(input);

    //Check if the head is blocked in this set
    bool found = false; //If found = true, then the rule application is blocked
    auto itr = n->getTable(headLiteral.getPredicate().getId());
    while(!itr.isEmpty()) {
        auto table = itr.getCurrentTable();
        //Iterate over the content of the table
        auto tbItr = table->getIterator();
        while (tbItr->hasNext()) {
            tbItr->next();
            found = true;
            for(uint8_t j = 0; j < columnsToCheck.size(); ++j) {
                auto cId = columnsToCheck[j];
#if DEBUG
		LOG(TRACEL) << "cId = " << (int) cId << ", currentValue = " << tbItr->getCurrentValue(cId);
#endif
                if (tbItr->getCurrentValue(cId) != headrow[cId]) {
                    found = false;
                    break;
                }
            }
            if (found) {
                break;
            }
        }
        if (found)
            break;
        itr.moveNextCount();
    }
    return found;
}

void ExistentialRuleProcessor::consolidate(const bool isFinished) {
    if (replaceExtColumns && tmpRelation != NULL) {
        //Populate the allColumns vector with known columns and constants
        std::vector<std::shared_ptr<Column>> allColumns;
        allColumns.resize(rowsize);
        auto unfilterdSegment = tmpRelation->getSegment();
        uint64_t nrows = unfilterdSegment->getNRows();
        for(uint8_t i = 0; i < nKnownColumns; ++i) {
            allColumns[posKnownColumns[i]] = unfilterdSegment->getColumn(
                    posKnownColumns[i]);
        }
        //Add the constants in the form of columns
        for(int i = 0; i < nConstantColumns; ++i) {
            allColumns[posConstantColumns[i]] = std::shared_ptr<Column>(
                    new CompressedColumn(row[posConstantColumns[i]],
                        nrows));
        }

        //If the chase is restricted, we must first remove data
        if (chaseMgmt->isRestricted()) {
            std::vector<uint64_t> filterRows;
            uint8_t count = 0;
            std::vector<bool> blocked;
            size_t blockedCount = 0;
            if (chaseMgmt->isCheckCyclicMode()) {
                for (size_t i = 0; i < nrows; i++) {
                    blocked.push_back(false);
                }
            }
            for(const auto &at : atomTables) {
                const auto &h = at->getLiteral();
                std::vector<std::shared_ptr<Column>> tobeRetained;
                std::vector<uint8_t> columnsToCheck;
                for(int i = 0; i < h.getTupleSize(); ++i) {
                    tobeRetained.push_back(allColumns[count + i]);
                    if (h.getTermAtPos(i).isVariable()) {
                        //It is not a existential variable
                        if (!posExtColumns.count(h.getTermAtPos(i).getId())) {
                            columnsToCheck.push_back(i);
                        } else {
                            assert(allColumns[count + i] == NULL);
                            //Add a dummy column
                            tobeRetained[tobeRetained.size() - 1] =
                                std::shared_ptr<Column>(
                                        new CompressedColumn(0,
                                            nrows));
                        }
                    }
                }
                if (chaseMgmt->isCheckCyclicMode()) {
                    //Init
                    const uint8_t segmentSize = unfilterdSegment->getNColumns();
                    std::unique_ptr<uint64_t[]> tmprow(
                            new uint64_t[segmentSize]);
                    std::unique_ptr<uint64_t[]> headrow(
                            new uint64_t[rowsize]);
                    std::vector<std::shared_ptr<Column>> segmentColumns;
                    std::vector<std::unique_ptr<ColumnReader>> segmentReaders;
                    for(uint8_t i = 0; i < segmentSize; ++i) {
                        segmentColumns.push_back(unfilterdSegment->getColumn(i));
                        segmentReaders.push_back(segmentColumns.back()->getReader());
                    }
                    std::vector<std::unique_ptr<ColumnReader>> headReaders;
                    for(uint8_t i = 0; i < allColumns.size(); ++i) {
                        headReaders.push_back(allColumns[i]->getReader());
                    }
                    //Check the rows, one by one
                    for(size_t i = 0; i < nrows; ++i) {
                        if (blocked[i]) {
                            continue;
                        }
                        //Fill the row
                        for(uint8_t j = 0; j < segmentSize; ++j) {
                            if (!segmentReaders[j]->hasNext()) {
                                LOG(ERRORL) << "This should not happen";
                            }
                            tmprow[j] = segmentReaders[j]->next();
                        }
                        //Fill the potential head
                        for(uint8_t j = 0; j < allColumns.size(); ++j) {
                            if (!headReaders[j]->hasNext()) {
                                LOG(ERRORL) << "This should not happen";
                            }
                            headrow[j] = headReaders[j]->next();
                        }
                        if (RMFA_check(tmprow.get(), h, headrow.get(),
                                    columnsToCheck)) { //Is it blocked?
                            blocked[i] = true;
                            LOG(DEBUGL) << "Blocking row " << i;
                            blockedCount++;
                        }
                    }
                } else {
                    FCTable *t = sn->getTable(h.getPredicate().getId(),
                            h.getPredicate().getCardinality());
                    filterDerivations(t, tobeRetained,
                            columnsToCheck,
                            filterRows);
                }
                count += h.getTupleSize();
            }

            if (chaseMgmt->isCheckCyclicMode()) {
                if (blockedCount == nrows) {
                    tmpRelation = std::unique_ptr<SegmentInserter>();
                    return;
                }
                for (size_t i = 0; i < nrows; i++) {
                    if (blocked[i]) {
                        for (int j = 0; j < atomTables.size(); j++) {
                            filterRows.push_back(i);
                        }
                    }
                }
            }

            if (filterRows.size() == nrows * atomTables.size()) {
                tmpRelation = std::unique_ptr<SegmentInserter>();
                return; //every substitution already exists in the database.
                // Nothing new can be derived.
            }
            //Filter out only valid subs
            if (!filterRows.empty()) {
                retainNonExisting(filterRows, nrows, allColumns);
            }
        }

        //Populate the known columns (they will be the arguments to get the
        //fresh IDs)
        std::vector<std::shared_ptr<Column>> knownColumns;
        for(int i = 0; i < colsForExt.size(); ++i) {
            knownColumns.push_back(allColumns[colsForExt[i]]);
        }

        if (filterRecursive) {
            retainNonRecursive(nrows, knownColumns);
        }

        if (nrows == 0) {
            return;
        }

        for(auto &el : posExtColumns) {
            auto extcolumn = chaseMgmt->getNewOrExistingIDs(
                    ruleDetails->rule.getId(),
                    el.first, //ID of the variable
                    knownColumns,
                    nrows);
            for(uint8_t pos : el.second) { //Add the existential columns to the
                //final list of columns
                allColumns[pos] = extcolumn;
            }
        }
        /*        LOG(DEBUGL) << "N. rows " << nrows;
                  for(int i = 0; i < allColumns.size(); ++i) {
                  auto reader = allColumns[i]->getReader();
                  for(int j = 0; j < 10; ++j) {
                  reader->hasNext();
                  auto v = reader->next();
                  cout << v << "\t";
                  }
                  cout << std::endl;
                  }*/

        //Add the columns to the head atoms
        int count = 0;
        for(auto &t : atomTables) {
            uint8_t sizeTuple = t->getLiteral().getTupleSize();
            std::vector<std::shared_ptr<Column>> c2;
            for(uint8_t i = 0; i < sizeTuple; ++i) {
                c2.push_back(allColumns[count + i]);
            }
            t->addColumns(0, c2, false, false);
            count += sizeTuple;
        }

        replaceExtColumns = false;
        tmpRelation = std::unique_ptr<SegmentInserter>();
    }
    FinalRuleProcessor::consolidate(isFinished);
}
