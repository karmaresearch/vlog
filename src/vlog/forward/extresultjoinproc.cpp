#include <vlog/extresultjoinproc.h>
#include <vlog/ruleexecdetails.h>
#include <vlog/seminaiver.h>

static bool isPresent(Var_t el, std::vector<Var_t> &v) {
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
    std::vector<std::pair<int,int>> duplicateExts;
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
                for (int j = 0; j < i; j++) {
                    auto t1 = literal.getTermAtPos(j);
                    if (t1.isVariable() && t.getId() == t1.getId()) {
                        // Duplicate existential variable. We have to check those as well.
                        duplicateExts.push_back(std::pair<int, int>(i, j));
                        break;
                    }
                }
            } else {
                tobeRetained.push_back(std::shared_ptr<Column>(c[posToCopy]));
                columnsToCheck.push_back(i);
            }
        }
    }

    filterDerivations(t, tobeRetained, columnsToCheck, duplicateExts, outputProc);
}

void ExistentialRuleProcessor::filterDerivations(FCTable *t,
        std::vector<std::shared_ptr<Column>> &tobeRetained,
        std::vector<uint8_t> &columnsToCheck,
        std::vector<std::pair<int,int>> &duplicateExts,
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
                if (cmp == 0) {
                    // Check duplicate existentials
                    if (duplicateExts.size() > 0) {
                        bool diff = false;
                        for (int i = 0; i < duplicateExts.size(); i++) {
                            auto t1 = itr2->getCurrentValue(duplicateExts[i].first);
                            auto t2 = itr2->getCurrentValue(duplicateExts[i].second);
                            if (t1 != t2) {
                                diff = true;
                                break;
                            }
                        }
                        if (diff) {
                            // Shift the table.
                            itr2Ok = itr2->hasNext();
                            if (itr2Ok)
                                itr2->next();
                            continue;
                        }
                    }
                    output.push_back(idx);
                }
                itr1Ok = itr1->hasNext();
                if (itr1Ok)
                    itr1->next();
                // Should we not increment idx here??? Added. --Ceriel
                idx++;
            }
        }
        table->releaseIterator(itr2);
        itr1->clear();
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
            if (c[i]) {
                readers.push_back(c[i]->getReader());
            } else {
                readers.emplace_back();
            }
        }
        writers.resize(c.size());
        uint64_t idxs = 0;
        uint64_t nextid = filterRows[idxs];
        for(uint64_t i = 0; i < sizecolumns; ++i) {
            if (i < nextid) {
                //Copy
                for(uint8_t j = 0; j < c.size(); ++j) {
                    if (readers[j]) {
                        if (!readers[j]->hasNext()) {
                            throw 10;
                        }
                        writers[j].add(readers[j]->next());
                    }
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
                    if (readers[j]) {
                        if (!readers[j]->hasNext()) {
                            throw 10;
                        }
                        readers[j]->next();
                    }
                }

            }
        }
        //Copy back the retricted columns
        for(uint8_t i = 0; i < c.size(); ++i) {
            if (c[i]) {
                c[i] = writers[i].getColumn();
            }
        }
        sizecolumns = 0;
        if (rowsize > 0) {
            bool found = false;
            for(uint8_t i = 0; i < c.size(); ++i) {
                if (c[i]) {
                    sizecolumns = c[i]->size();
                    found = true;
                    break;
                }
            }
            if (!found) {
                LOG(ERRORL) << "No atom without non-existential columns. I don't now how to get the size";
                throw 10;
            }
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
        size_t nAtomsToCheck = atomTables.size();
        PredId_t headPredicateToIgnore = -1;
        if (chaseMgmt->getChaseType() == TypeChase::SUM_RESTRICTED_CHASE) {
            headPredicateToIgnore = chaseMgmt->getPredicateIgnoreBlocking();
        }

        std::vector<uint64_t> filterRows; //The restricted chase might remove some IDs
        int count = 0;
        if (chaseMgmt->isCheckCyclicMode()) {
            std::vector<bool> blocked(sizecolumns);
            size_t blockedCount = 0;
            uint64_t tmprow[256];
            std::vector<std::unique_ptr<ColumnReader>> columnReaders;
            for(uint8_t i = 0; i < c.size(); ++i) {
                columnReaders.push_back(c[i]->getReader());
            }

            for (size_t i = 0; i < sizecolumns; ++i) {
                //Fill the row
                for (int j = 0; j < c.size(); ++j) {
                    if (!columnReaders[j]->hasNext()) {
                        LOG(ERRORL) << "This should not happen";
                    }
                    tmprow[j] = columnReaders[j]->next();
                }

                if (blocked_check(tmprow, c.size(), headPredicateToIgnore)) {
                    blocked[i] = true;
                    blockedCount++;
                }
            }

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
        } else {
            for(const auto &at : atomTables) {
                const auto &h = at->getLiteral();
                auto headPredicate = h.getPredicate().getId();
                FCTable *t = sn->getTable(headPredicate,
                        h.getPredicate().getCardinality());
                filterDerivations(h, t, row, count, ruleDetails, nKnownColumns,
                        posKnownColumns, c, sizecolumns, filterRows);
                count += h.getTupleSize();
            }
        }

        if (filterRows.size() == sizecolumns * nAtomsToCheck) {
            return; //every substitution already exists in the database. Nothing
            //new can be derived.
        }

        //Filter out the potential values for the derivation
        //(only restricted chase can do it)
        if (!filterRows.empty()) {
            retainNonExisting(filterRows, sizecolumns, c);
        }
    }

    if (filterRecursive) {
        retainNonRecursive(sizecolumns, c);
    }

    if (sizecolumns == 0) {
        return;
    }

    std::vector<std::shared_ptr<Column>> knownColumns;
    for(int i = 0; i < colsForExt.size(); ++i) {
        knownColumns.push_back(c[colsForExt[i]]);
    }

    //Create existential columns
    // assert(literal.getTupleSize() == c.size()); ???? not correct, I think.
    std::map<Var_t, std::shared_ptr<Column>> extvars;
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

    if (sizecolumns == 0) {
        return;
    }

    if (chaseMgmt->isRestricted()) {
        size_t nAtomsToCheck = atomTables.size();
        PredId_t headPredicateToIgnore = -1;
        if (chaseMgmt->getChaseType() == TypeChase::SUM_RESTRICTED_CHASE) {
            headPredicateToIgnore = chaseMgmt->getPredicateIgnoreBlocking();
        }

        std::vector<uint64_t> filterRows; //The restricted chase might remove some IDs
        int count = 0;
        if (chaseMgmt->isCheckCyclicMode()) {
            std::vector<bool> blocked(sizecolumns);
            size_t blockedCount = 0;
            uint64_t tmprow[256];
            std::vector<std::unique_ptr<ColumnReader>> columnReaders;
            for(uint8_t i = 0; i < c.size(); ++i) {
                columnReaders.push_back(c[i]->getReader());
            }

            for (size_t i = 0; i < sizecolumns; ++i) {
                //Fill the row
                for (int j = 0; j < c.size(); ++j) {
                    if (!columnReaders[j]->hasNext()) {
                        LOG(ERRORL) << "This should not happen";
                    }
                    tmprow[j] = columnReaders[j]->next();
                }

                if (blocked_check(tmprow, c.size(), headPredicateToIgnore)) {
                    //It is blocked
                    blocked[i] = true;
                    blockedCount++;
                }
            }

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
        } else {
            for(const auto &at : atomTables) {
                const auto &h = at->getLiteral();
                auto headPredicate = h.getPredicate().getId();
                FCTable *t = sn->getTable(headPredicate,
                        h.getPredicate().getCardinality());
                filterDerivations(h, t, row, count, ruleDetails, nCopyFromSecond,
                        posFromSecond, c, sizecolumns, filterRows);
                count += h.getTupleSize();
            }
        }

        if (filterRows.size() == sizecolumns * nAtomsToCheck) {
            return; //every substitution already exists in the database. Nothing
            //new can be derived.
        }

        //Filter out the potential values for the derivation
        //(only restricted chase can do it)
        if (!filterRows.empty()) {
            retainNonExisting(filterRows, sizecolumns, c);
        }
    }

    if (filterRecursive) {
        retainNonRecursive(sizecolumns, c);
    }

    if (sizecolumns == 0) {
        return;
    }

    //Create existential columns store them in a vector with the corresponding
    //var ID
    std::map<Var_t, std::shared_ptr<Column>> extvars;
    int count = 0;
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
                            [](const std::pair<Var_t, std::shared_ptr<Column>>& a,
                                const std::pair<Var_t, std::shared_ptr<Column>>& b) -> bool {
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

std::vector<uint64_t> ExistentialRuleProcessor::blocked_check_computeBodyAtoms(
        std::vector<Literal> &output,
        uint64_t *row, PredId_t headPredicateToIgnore) {
    const RuleExecutionPlan &plan = ruleDetails->orderExecutions[ruleExecOrder];
#if DEBUG
    LOG(TRACEL) << "Adding body atoms for rule " << ruleDetails->rule.tostring(NULL, NULL);
#endif
    auto bodyAtoms = plan.plan;
    int idx = 0;
    std::vector<Var_t> vars = ruleDetails->rule.getFrontierVariables(headPredicateToIgnore);
    std::vector<uint64_t> toMatch(vars.size());
    std::vector<bool> found(vars.size());

    for(const auto &atom : bodyAtoms) {
        VTuple tuple(atom->getTuple());
        auto v2p = plan.vars2pos[idx];
        for(int i = 0; i < v2p.size(); ++i) {
            auto pair = v2p[i];
            tuple.set(VTerm(0, row[pair.second]), pair.first);
            Var_t varNo = atom->getTermAtPos(pair.first).getId();
            for (int j = 0; j < vars.size(); j++) {
                if (vars[j] == varNo) {
                    if (! found[j]) {
                        found[j] = true;
                        toMatch[j] = row[pair.second];
                    }
                    break;
                }
            }
        }
        output.push_back(Literal(atom->getPredicate(), tuple));
#if DEBUG
        LOG(TRACEL) << "computeBodyAtoms: adding literal " << output[output.size()-1].tostring(NULL, NULL);
#endif
        idx++;
    }
    return toMatch;
}

std::unique_ptr<SemiNaiver> ExistentialRuleProcessor::saturateInput(
        std::vector<Literal> &input, Program *program, EDBLayer *layer) {

    std::map<PredId_t, std::vector<uint64_t>> edbPredicates;
    std::map<PredId_t, std::vector<uint64_t>> idbPredicates;

    for(const auto &literal : input) {
#if DEBUG
        LOG(TRACEL) << "literal: " << literal.tostring(program, program->getKB());
#endif

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

    std::vector<PredId_t> predicates = sn->getEDBLayer().getAllPredicateIDs();
    for (auto pred : predicates) {
        if (!edbPredicates.count(pred)) {
            edbPredicates.insert(std::make_pair(pred, std::vector<uint64_t>()));
        }
    }
    for(auto &pair : edbPredicates) {
        uint8_t arity = sn->getEDBLayer().getPredArity(pair.first);
        layer->addInmemoryTable(pair.first, arity, pair.second);
    }

    //Launch the semi-naive evaluation
    std::unique_ptr<SemiNaiver> lsn(new SemiNaiver(
                *layer, program, true, true, false, 1, false, true));

    //Populate the IDB layer
    for(auto &pair : idbPredicates) {
        Predicate pred = program->getPredicate(pair.first);
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
                    0, true, inserter.getSortedAndUniqueSegment()));

        //Define a generic query
        VTuple tuple(card);
        for(uint8_t i = 0; i < card; ++i) {
            // tuple.set(VTerm(i, 0), i);
            tuple.set(VTerm(i+1, 0), i);    // I suppose these should all be variables ... --Ceriel
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

void ExistentialRuleProcessor::enhanceFunctionTerms(
        std::vector<Literal> &output,
        uint64_t &startFreshIDs,
        bool rmfa) {
    //If there is a function term, get also all related facts
#if DEBUG
    LOG(DEBUGL) << "enhanceFuntionTerms, startFreshIDs = " << startFreshIDs;
#endif
    for(size_t i = 0; i < output.size(); ++i) {
        const auto literal = output[i];
#if DEBUG
        LOG(TRACEL) << "Processing literal " << literal.tostring(NULL, NULL);
#endif
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
                uint64_t *values = rows->getRow(localCounter);
                const uint64_t nvalues = rows->getSizeRow();
                //Map them to variables
                const auto &nameVars = rows->getNameArgVars();
                std::map<Var_t, uint64_t> mappings;
                for(int i = 0; i < nvalues; ++i) {
                    mappings.insert(std::make_pair(nameVars[i], values[i]));
                }
                //Materialize the remaining facts giving fresh IDs to
                //the rem. variables (only if RMFA, for RMFC, we use *).
                auto const *rule = ruleContainer->getRule();
                for(const auto &bLiteral : rule->getBody()) {
                    if (! bLiteral.isNegated()) {
                        VTuple t(bLiteral.getTupleSize());
                        for(uint8_t m = 0; m < bLiteral.getTupleSize(); ++m) {
                            const VTerm term = bLiteral.getTermAtPos(m);
                            if (term.isVariable()) {
                                Var_t varID = term.getId();
                                if (!mappings.count(varID)) {
                                    if (rmfa) {
                                        mappings.insert(
                                                std::make_pair(varID, startFreshIDs++));
                                    } else {
                                        Program *p = sn->get_RMFC_program();
                                        assert(p != NULL);
                                        uint64_t id = 0;
                                        p->getKB()->getOrAddDictNumber("*", 1, id);
                                        mappings.insert(
                                                std::make_pair(varID, id));
                                    }
                                }
                                t.set(VTerm(0, mappings[varID]), m);
                            } else {
                                t.set(term, m);
                            }
                        }
                        _addIfNotExist(output, Literal(bLiteral.getPredicate(), t));
                    }
                }
                //Also add the original variable, and consider other head atoms
                assert(!mappings.count(varID));
                mappings.insert(std::make_pair(varID, term.getValue()));

                for(const auto &hLiteral : rule->getHeads()) {
                    /*
                       if (hLiteral.getPredicate().getId() ==
                       literal.getPredicate().getId()) {
                       continue;
                       }
                       */
                    VTuple t(hLiteral.getTupleSize());
                    for(uint8_t m = 0; m < hLiteral.getTupleSize(); ++m) {
                        const VTerm term = hLiteral.getTermAtPos(m);
                        if (term.isVariable()) {
                            Var_t varID = term.getId();
                            if (!mappings.count(varID)) {
                                // Here, we must create a value for the existential variable, using the same row (I think ...)
                                auto *vrows = ruleContainer->getRows(varID);
                                uint64_t value;
                                bool v = vrows->existingRow(values, value);
                                assert(v);
                                uint64_t rulevar = RULE_SHIFT(ruleID) + VAR_SHIFT(varID);
                                mappings.insert(std::make_pair(varID, rulevar | value));
                                // LOG(ERRORL) << "There are existential variables not defined. Must implement their retrievals";
                                // throw 10;
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
}

bool ExistentialRuleProcessor::blocked_check(uint64_t *row,
        size_t sizeRow, PredId_t headPredicateToIgnore) {
    //For RMFC, we need to replace all non-skolem constants with *.
    uint64_t newrow[256];
    Program *rmfc = sn->get_RMFC_program();
    if (rmfc != NULL) {
        if (chaseMgmt->getRuleToCheck() == ruleDetails->rule.getId() && ruleDetails->lastExecution <= 0) {
            // We need one execution of this rule to introduce a skolem constant.
            // Otherwise, it would immediately be blocked by the critical instance.
            LOG(DEBUGL) << "blocked_check returns false";
            return false;
        }
        for (int i = 0; i < sizeRow; i++) {
            newrow[i] = (row[i] & RULEVARMASK) != 0 ? row[i] : 0;
        }
        row = newrow;
    }

    std::vector<Literal> input; //"input" corresponds to B_\rho,\sigma in the paper
    //First I need to add to body atoms
    std::vector<uint64_t> toMatch = blocked_check_computeBodyAtoms(input, row, headPredicateToIgnore);

    std::unique_ptr<SemiNaiver> saturation;

    //Get a starting value for the fresh IDs
    uint64_t freshIDs = 1; //0 is star

    //Then I need to add all facts relevant to produce the function terms
    enhanceFunctionTerms(input, freshIDs, sn->get_RMFC_program() == NULL);
    if (rmfc == NULL) {
        //Finally I need to saturate "input" with the datalog rules
        saturation = saturateInput(input, sn->getProgram(), new EDBLayer(sn->getEDBLayer(), false));
    } else {
        EDBLayer *layer = new EDBLayer(*(rmfc->getKB()), true);
        // Exclusion of rule ρ⋆ under substitution σ⋆
        // we have to provide a binding for the added __EXCLUDE_DUMMY__.
        const Rule &rule = rmfc->getRule(ruleDetails->rule.getId());
        const std::vector<Literal> &body = rule.getBody();
        Literal lastLit = body.back();
        VTuple tupl(lastLit.getTupleSize());
        for (int i = 0; i < lastLit.getTupleSize(); i++) {
            tupl.set(VTerm(0, row[i]), i);
        }
        input.push_back(Literal(lastLit.getPredicate(), tupl));
#if DEBUG
        LOG(DEBUGL) << "Adding exclusion info for rule " << rule.tostring(rmfc, layer) << ": " << input.back().tostring(rmfc, layer);
#endif

        saturation = saturateInput(input, rmfc, layer);
    }

    // Now, check our special rule (see cycles/checker.cpp).
    std::string specialPredicate = "__GENERATED_PRED__" + std::to_string(ruleDetails->rule.getId());
    PredId_t pred = saturation->getProgram()->getPredicate(specialPredicate).getId();

    //Check if the head is blocked in this set, i.e., if this predicate has matching derivations.
    auto itr = saturation->getTable(pred);
    bool found = false;
    while ( ! found && !itr.isEmpty()) {
        auto table = itr.getCurrentTable();
        //Iterate over the content of the table
        auto tbItr = table->getIterator();
        assert(tbItr->getNColumns() == toMatch.size());
        while (tbItr->hasNext()) {
            tbItr->next();
            found = true;
            for (int i = 0; i < toMatch.size(); i++) {
                auto value = tbItr->getCurrentValue(i);
                if (toMatch[i] != value) {
                    found = false;
                    break;
                }
            }
            if (found) {
                break;
            }
        }
        table->releaseIterator(tbItr);
        itr.moveNextCount();
    }

    EDBLayer &l = saturation->getEDBLayer();
    delete &l;

    LOG(DEBUGL) << "blocked_check returns " << found;

    return found;
}

bool ExistentialRuleProcessor::consolidate(const bool isFinished) {
    if (replaceExtColumns && tmpRelation != NULL) {
        //Populate the allColumns vector with known columns and constants
        std::vector<std::shared_ptr<Column>> allColumns;
        allColumns.resize(rowsize);
        auto unfilterdSegment = tmpRelation->getSegment();
        uint64_t nrows = unfilterdSegment->getNRows();
        if (nrows == 0) {
            return false;
        }
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
            size_t nAtomsToCheck = atomTables.size();
            PredId_t headPredicateToIgnore = -1;
            if (chaseMgmt->getChaseType() == TypeChase::SUM_RESTRICTED_CHASE) {
                headPredicateToIgnore = chaseMgmt->getPredicateIgnoreBlocking();
            }

            std::vector<uint64_t> filterRows;
            int count = 0;
            if (chaseMgmt->isCheckCyclicMode()) {
                size_t blockedCount = 0;
                const uint8_t segmentSize = unfilterdSegment->getNColumns();
                std::vector<bool> blocked(nrows);
                uint64_t tmprow[256];
                std::vector<std::shared_ptr<Column>> segmentColumns;
                std::vector<std::unique_ptr<ColumnReader>> segmentReaders;
                for(uint8_t i = 0; i < segmentSize; ++i) {
                    segmentColumns.push_back(unfilterdSegment->getColumn(i));
                    segmentReaders.push_back(segmentColumns.back()->getReader());
                }

                for (size_t i = 0; i < nrows; ++i) {
                    //Fill the row
                    for(uint8_t j = 0; j < segmentSize; ++j) {
                        if (!segmentReaders[j]->hasNext()) {
                            LOG(ERRORL) << "This should not happen";
                        }
                        tmprow[j] = segmentReaders[j]->next();
                    }
                    if (blocked_check(tmprow, segmentSize, headPredicateToIgnore)) { //Is it blocked?
                        blocked[i] = true;
                        blockedCount++;
                    }
                }

                if (blockedCount == nrows) {
                    tmpRelation = std::unique_ptr<SegmentInserter>();
                    return false;
                }
                for (size_t i = 0; i < nrows; i++) {
                    if (blocked[i]) {
                        for (int j = 0; j < atomTables.size(); j++) {
                            filterRows.push_back(i);
                        }
                    }
                }
            } else {
                for(const auto &at : atomTables) {
                    const auto &h = at->getLiteral();
                    std::vector<std::shared_ptr<Column>> tobeRetained;
                    std::vector<uint8_t> columnsToCheck;
                    std::vector<std::pair<int,int>> duplicateExts;
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
                                for (int j = 0; j < i; j++) {
                                    auto t1 = h.getTermAtPos(j);
                                    if (t1.isVariable() && t1.getId() == h.getTermAtPos(i).getId()) {
                                        // Duplicate existential variable. We have to check those as well.
                                        duplicateExts.push_back(std::pair<int, int>(i, j));
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    auto headPredicate = h.getPredicate().getId();
                    FCTable *t = sn->getTable(h.getPredicate().getId(),
                            h.getPredicate().getCardinality());
                    filterDerivations(t, tobeRetained,
                            columnsToCheck,
                            duplicateExts,
                            filterRows);

                    count += h.getTupleSize();
                }
            }

            if (filterRows.size() == nrows * nAtomsToCheck) {
                tmpRelation = std::unique_ptr<SegmentInserter>();
                return false; //every substitution already exists in the database.
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
            return false;
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
    return FinalRuleProcessor::consolidate(isFinished);
}
