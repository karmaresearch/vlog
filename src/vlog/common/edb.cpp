#include <vlog/edb.h>
#include <vlog/concepts.h>
#include <vlog/idxtupletable.h>
#include <vlog/column.h>

#include <vlog/trident/tridenttable.h>
#ifdef MYSQL
#include <vlog/mysql/mysqltable.h>
#endif
#ifdef ODBC
#include <vlog/odbc/odbctable.h>
#endif
#ifdef MAPI
#include <vlog/mapi/mapitable.h>
#endif
#ifdef MDLITE
#include <vlog/mdlite/mdlitetable.h>
#endif
#include <vlog/inmemory/inmemorytable.h>

#include <unordered_map>
#include <climits>

void EDBLayer::addTridentTable(const EDBConf::Table &tableConf, bool multithreaded) {
    EDBInfoTable infot;
    const string pn = tableConf.predname;
    const string kbpath = tableConf.params[0];
    if (!Utils::exists(kbpath) || !Utils::exists(kbpath + "/p0")) {
        LOG(ERRORL) << "The KB at " << kbpath << " does not exist. Check the edb.conf file.";
        throw 10;
    }
    infot.id = (PredId_t) predDictionary.getOrAdd(pn);
    infot.arity = 3;
    infot.type = tableConf.type;
    infot.manager = std::shared_ptr<EDBTable>(new TridentTable(kbpath, multithreaded));
    dbPredicates.insert(make_pair(infot.id, infot));
    LOG(DEBUGL) << "Inserted " << pn << " with number " << infot.id;
}

#ifdef MYSQL
void EDBLayer::addMySQLTable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const string pn = tableConf.predname;
    infot.id = (PredId_t) predDictionary.getOrAdd(pn);
    infot.arity = 3;
    infot.type = tableConf.type;
    infot.manager = std::shared_ptr<EDBTable>(new MySQLTable(tableConf.params[0],
                tableConf.params[1], tableConf.params[2], tableConf.params[3],
                tableConf.params[4], tableConf.params[5]));
    dbPredicates.insert(make_pair(infot.id, infot));
}
#endif

#ifdef ODBC
void EDBLayer::addODBCTable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const string pn = tableConf.predname;
    infot.id = (PredId_t) predDictionary.getOrAdd(pn);
    infot.arity = 3;
    infot.type = tableConf.type;
    infot.manager = std::shared_ptr<EDBTable>(new ODBCTable(tableConf.params[0],
                tableConf.params[1], tableConf.params[2], tableConf.params[3],
                tableConf.params[4]));
    dbPredicates.insert(make_pair(infot.id, infot));
}
#endif

#ifdef MAPI
void EDBLayer::addMAPITable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const string pn = tableConf.predname;
    infot.id = (PredId_t) predDictionary.getOrAdd(pn);
    infot.arity = 3;
    infot.type = tableConf.type;
    infot.manager = std::shared_ptr<EDBTable>(new MAPITable(tableConf.params[0],
                (int) strtol(tableConf.params[1].c_str(), NULL, 10), tableConf.params[2], tableConf.params[3],
                tableConf.params[4], tableConf.params[5], tableConf.params[6]));
    dbPredicates.insert(make_pair(infot.id, infot));
}
#endif

#ifdef MDLITE
void EDBLayer::addMDLiteTable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const string pn = tableConf.predname;
    infot.id = (PredId_t) predDictionary.getOrAdd(pn);
    infot.type = tableConf.type;
    MDLiteTable *table = new MDLiteTable(tableConf.params[0], tableConf.params[1]);
    infot.manager = std::shared_ptr<EDBTable>(table);
    infot.arity = table->getArity();
    dbPredicates.insert(make_pair(infot.id, infot));
}
#endif

void EDBLayer::addInmemoryTable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const string pn = tableConf.predname;
    infot.id = (PredId_t) predDictionary.getOrAdd(pn);
    infot.type = tableConf.type;
    InmemoryTable *table = new InmemoryTable(tableConf.params[0],
            tableConf.params[1], infot.id);
    infot.manager = std::shared_ptr<EDBTable>(table);
    infot.arity = table->getArity();
    dbPredicates.insert(make_pair(infot.id, infot));
}

bool EDBLayer::doesPredExists(PredId_t id) const {
    return dbPredicates.count(id);
}

void EDBLayer::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    PredId_t predid = query->getLiteral()->getPredicate().getId();


    if (dbPredicates.count(predid)) {
        // LOG(DEBUGL) << "EDB: go into manager, posToFilter size = " << (posToFilter == NULL ? 0 : posToFilter->size())
        //     << ", valuesToFilter size = " << (valuesToFilter == NULL ? 0 : valuesToFilter->size());
        auto el = dbPredicates.find(predid);
        el->second.manager->query(query, outputTable, posToFilter, valuesToFilter);
    } else {
        IndexedTupleTable *rel = tmpRelations[predid];
        uint8_t size = rel->getSizeTuple();

        /*
           LOG(DEBUGL) << "Query = " << query->tostring();
           if (posToFilter != NULL) {
           LOG(DEBUGL) << "posToFilter.size() = " << posToFilter->size();
           }
           */
        switch (size) {
            case 1: {
                        uint64_t row[1];
                        if (posToFilter != NULL) {
                            assert(posToFilter->size() == 1 &&
                                    posToFilter->at(0) == (uint8_t) 0);
                            for (std::vector<Term_t>::iterator
                                    itr = valuesToFilter->begin();
                                    itr != valuesToFilter->end(); ++itr) {
                                if (rel->exists(*itr)) {
                                    row[0] = *itr;
                                    outputTable->addRow(row);
                                }
                            }
                        } else {
                            //Copy all values
                            for (std::vector <Term_t>::iterator itr = rel->getSingleColumn()->begin();
                                    itr != rel->getSingleColumn()->end(); ++itr) {
                                row[0] = *itr;
                                outputTable->addRow(row);
                            }
                        }
                        break;
                    }
            case 2: {
                        const uint8_t nRepeatedVars = query->getNRepeatedVars();
                        uint64_t row[2];
                        if (posToFilter == NULL || posToFilter->size() == 0) {
                            for (std::vector<std::pair<Term_t, Term_t>>::iterator
                                    itr = rel->getTwoColumn1()->begin();
                                    itr != rel->getTwoColumn1()->end(); ++itr) {
                                bool valid = true;
                                row[0] = itr->first;
                                row[1] = itr->second;
                                if (nRepeatedVars > 0) {
                                    for (uint8_t i = 0; i < nRepeatedVars; ++i) {
                                        std::pair<uint8_t, uint8_t> rp = query->getRepeatedVar(i);
                                        if (row[rp.first] != row[rp.second]) {
                                            valid = false;
                                            break;
                                        }
                                    }
                                }
                                if (valid) {
                                    outputTable->addRow(row);
                                }
                            }
                        } else if (posToFilter->size() == 1) {
                            std::vector<Term_t> filterValues;
                            std::vector<Term_t>::iterator itr2 = valuesToFilter->begin();
                            bool sorted = true;
                            if (itr2 < valuesToFilter->end()) {
                                Term_t prev = (Term_t) *itr2;
                                filterValues.push_back(prev);
                                itr2++;
                                while (itr2 != valuesToFilter->end()) {
                                    if (*itr2 < prev) {
                                        sorted = false;
                                        filterValues.push_back(*itr2);
                                    } else if (*itr2 > prev) {
                                        filterValues.push_back(*itr2);
                                    }
                                    prev = *itr2;
                                    itr2++;
                                }
                            }
                            if (! sorted) {
                                std::sort(filterValues.begin(), filterValues.end());
                            }
                            std::vector<std::pair<Term_t, Term_t>> *pairs;
                            bool inverted = posToFilter->at(0) != 0;
                            if (!inverted) {
                                pairs = rel->getTwoColumn1();
                                std::vector<std::pair<Term_t, Term_t>>::iterator itr1 = pairs->begin();
                                std::vector<Term_t>::iterator itr2 = filterValues.begin();
                                while (itr1 != pairs->end() && itr2 != filterValues.end()) {
                                    while (itr1 != pairs->end() && itr1->first < *itr2) {
                                        itr1++;
                                    }
                                    if (itr1 == pairs->end())
                                        continue;

                                    while (itr2 != filterValues.end() && itr1->first > *itr2) {
                                        itr2++;
                                    }
                                    if (itr1 != pairs->end() && itr2 != filterValues.end()) {
                                        if (itr1->first == *itr2) {
                                            bool valid = true;
                                            row[0] = itr1->first;
                                            row[1] = itr1->second;
                                            if (nRepeatedVars > 0) {
                                                for (uint8_t i = 0; i < nRepeatedVars; ++i) {
                                                    std::pair<uint8_t, uint8_t> rp = query->getRepeatedVar(i);
                                                    if (row[rp.first] != row[rp.second]) {
                                                        valid = false;
                                                        break;
                                                    }
                                                }
                                            }

                                            if (valid) {
                                                outputTable->addRow(row);
                                            }
                                        }
                                        itr1++;
                                    }
                                }
                            } else {
                                pairs = rel->getTwoColumn2();
                                std::vector<std::pair<Term_t, Term_t>>::iterator itr1 = pairs->begin();
                                std::vector<Term_t>::iterator itr2 = filterValues.begin();
                                while (itr1 != pairs->end() && itr2 != filterValues.end()) {
                                    while (itr1 != pairs->end() && itr1->second < *itr2) {
                                        itr1++;
                                    }
                                    if (itr1 == pairs->end())
                                        continue;

                                    while (itr2 != filterValues.end() && itr1->second > *itr2) {
                                        itr2++;
                                    }
                                    if (itr1 != pairs->end() && itr2 != filterValues.end()) {
                                        if (itr1->second == *itr2) {
                                            bool valid = true;
                                            row[0] = itr1->first;
                                            row[1] = itr1->second;
                                            if (nRepeatedVars > 0) {
                                                for (uint8_t i = 0; i < nRepeatedVars; ++i) {
                                                    std::pair<uint8_t, uint8_t> rp = query->getRepeatedVar(i);
                                                    if (row[rp.first] != row[rp.second]) {
                                                        valid = false;
                                                        break;
                                                    }
                                                }
                                            }

                                            if (valid) {
                                                outputTable->addRow(row);
                                            }
                                        }
                                        itr1++;
                                    }
                                }
                            }
                        } else {
                            //posToFilter==2
                            std::vector<std::pair<Term_t, Term_t>> filterValues;
                            std::vector<Term_t>::iterator itr2 = valuesToFilter->begin();
                            bool sorted = true;
                            if (itr2 < valuesToFilter->end()) {
                                Term_t prev1 = *itr2;
                                itr2++;
                                Term_t prev2 = *itr2;
                                itr2++;
                                filterValues.push_back(std::make_pair(prev1, prev2));
                                while (itr2 != valuesToFilter->end()) {
                                    Term_t v1 = *itr2;
                                    itr2++;
                                    Term_t v2 = *itr2;
                                    itr2++;
                                    filterValues.push_back(std::make_pair(v1, v2));
                                    if (sorted && (v1 < prev1 || (v1 == prev1 && v2 < prev2))) {
                                        sorted = false;
                                    } else {
                                        prev1 = v1;
                                        prev2 = v2;
                                    }
                                }
                            }
                            if (! sorted) {
                                std::sort(filterValues.begin(), filterValues.end());
                            }
                            std::vector<std::pair<Term_t, Term_t>> *pairs;
                            bool inverted = posToFilter->at(0) != 0;
                            if (!inverted) {
                                pairs = rel->getTwoColumn1();
                            } else {
                                pairs = rel->getTwoColumn2();
                            }

                            for (std::vector<std::pair<Term_t, Term_t>>::iterator itr = filterValues.begin();
                                    itr != filterValues.end(); itr++) {
                                //Binary search
                                if (std::binary_search(pairs->begin(), pairs->end(),
                                            *itr)) {
                                    bool valid = true;
                                    row[0] = itr->first;
                                    row[1] = itr->second;
                                    if (nRepeatedVars > 0) {
                                        for (uint8_t i = 0; i < nRepeatedVars; ++i) {
                                            std::pair<uint8_t, uint8_t> rp = query->getRepeatedVar(i);
                                            if (row[rp.first] != row[rp.second]) {
                                                valid = false;
                                                break;
                                            }
                                        }
                                    }

                                    if (valid) {
                                        outputTable->addRow(row);
                                    }
                                }
                            }
                        }
                        break;
                    }
            default:
                    LOG(ERRORL) << "This should not happen";
                    throw 10;
        }
    }
    // LOG(DEBUGL) << "result size = " << outputTable->getNRows();
}

EDBIterator *EDBLayer::getIterator(const Literal &query) {
    const Literal *literal = &query;
    PredId_t predid = literal->getPredicate().getId();

    if (dbPredicates.count(predid)) {
        auto p = dbPredicates.find(predid);
        return p->second.manager->getIterator(query);
    } else {
        bool equalFields = query.hasRepeatedVars();
        IndexedTupleTable *rel = tmpRelations[predid];
        uint8_t size = rel->getSizeTuple();

        bool c1 = !literal->getTermAtPos(0).isVariable();
        bool c2 = literal->getTupleSize() == 2 && !literal->getTermAtPos(1).isVariable();
        Term_t vc1, vc2 = 0;
        if (c1)
            vc1 = literal->getTermAtPos(0).getValue();
        if (c2)
            vc2 = literal->getTermAtPos(1).getValue();

        // LOG(DEBUGL) << "getIterator, equalFields = " << equalFields << ", c1 = " << c1 << ", c2 = " << c2 << ", size = " << size;
        EDBMemIterator *itr;
        switch (size) {
            case 1:
                itr = memItrFactory.get();
                itr->init1(predid, rel->getSingleColumn(), c1, vc1);
                return itr;
            case 2:
                itr = memItrFactory.get();
                itr->init2(predid, c1, rel->getTwoColumn1(), c1, vc1, c2, vc2, equalFields);
                return itr;
        }
    }
    throw 10;
}

EDBIterator *EDBLayer::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    const Literal *literal = &query;
    PredId_t predid = literal->getPredicate().getId();

    if (dbPredicates.count(predid)) {
        auto p = dbPredicates.find(predid);
        return p->second.manager->getSortedIterator(query, fields);
    } else {
        assert(literal->getTupleSize() <= 2);
        bool equalFields = false;
        if (query.hasRepeatedVars()) {
            equalFields = true;
        }
        bool c1 = !literal->getTermAtPos(0).isVariable();
        bool c2 = literal->getTupleSize() == 2 && !literal->getTermAtPos(1).isVariable();
        Term_t vc1, vc2 = 0;
        if (c1)
            vc1 = literal->getTermAtPos(0).getValue();
        if (c2)
            vc2 = literal->getTermAtPos(1).getValue();

        IndexedTupleTable *rel = tmpRelations[predid];
        uint8_t size = rel->getSizeTuple();
        // LOG(DEBUGL) << "getSortedIterator, equalFields = " << equalFields << ", c1 = " << c1 << ", c2 = " << c2 << ", size = " << (int) size << ", fields.size() = " << fields.size();
        for (int i = 0; i < fields.size(); i++) {
            // LOG(DEBUGL) << "fields[" << i << "] = " << (int) fields[i];
        }
        EDBMemIterator *itr;
        switch (size) {
            case 1:
                itr = memItrFactory.get();
                itr->init1(predid, rel->getSingleColumn(), c1, vc1);
                return itr;
            case 2:
                itr = memItrFactory.get();
                if (c1) {
                    itr->init2(predid, true, rel->getTwoColumn1(), c1, vc1, c2, vc2, equalFields);
                } else {
                    if (c2) {
                        itr->init2(predid, false, rel->getTwoColumn2(), c1, vc1, c2, vc2, equalFields);
                    } else {
                        //No constraints
                        if (fields.size() != 0 && fields[0] == 0) {
                            itr->init2(predid, true, rel->getTwoColumn1(), c1, vc1, c2, vc2, equalFields);
                        } else {
                            itr->init2(predid, false, rel->getTwoColumn2(), c1, vc1, c2, vc2, equalFields);
                        }
                    }
                }
                return itr;
        }
    }
    throw 10;
}

size_t EDBLayer::getCardinalityColumn(const Literal &query,
        uint8_t posColumn) {
    const Literal *literal = &query;
    PredId_t predid = literal->getPredicate().getId();
    if (dbPredicates.count(predid)) {
        auto p = dbPredicates.find(predid);
        return p->second.manager->getCardinalityColumn(query, posColumn);
    } else {
        // throw 10;
        IndexedTupleTable *rel = tmpRelations[predid];
        return rel->size(posColumn);
    }
}

size_t EDBLayer::getCardinality(const Literal &query) {
    const Literal *literal = &query;
    PredId_t predid = literal->getPredicate().getId();
    if (dbPredicates.count(predid)) {
        auto p = dbPredicates.find(predid);
        return p->second.manager->getCardinality(query);
    } else {
        IndexedTupleTable *rel = tmpRelations[predid];
        if (literal->getNVars() == literal->getTupleSize()) {
            return rel->getNTuples();
        }
        // TODO: Can we optimize this?
        bool equalFields = false;
        if (query.hasRepeatedVars()) {
            equalFields = true;
        }
        uint8_t size = rel->getSizeTuple();

        bool c1 = !literal->getTermAtPos(0).isVariable();
        bool c2 = literal->getTupleSize() == 2 && !literal->getTermAtPos(1).isVariable();
        Term_t vc1, vc2 = 0;
        if (c1)
            vc1 = literal->getTermAtPos(0).getValue();
        if (c2)
            vc2 = literal->getTermAtPos(1).getValue();

        EDBMemIterator *itr = NULL;
        switch (size) {
            case 1:
                itr = memItrFactory.get();
                itr->init1(predid, rel->getSingleColumn(), c1, vc1);
                break;
            case 2:
                itr = memItrFactory.get();
                if (c1 || ! c2) {
                    itr->init2(predid, c1, rel->getTwoColumn1(), c1, vc1, c2, vc2, equalFields);
                } else {
                    itr->init2(predid, c1, rel->getTwoColumn2(), c1, vc1, c2, vc2, equalFields);
                }
                break;
        }
        size_t count = 0;
        while (itr->hasNext()) {
            count++;
            itr->next();
        }
        memItrFactory.release(itr);
        return count;
    }
}

size_t EDBLayer::estimateCardinality(const Literal &query) {
    const Literal *literal = &query;
    PredId_t predid = literal->getPredicate().getId();
    if (dbPredicates.count(predid)) {
        auto p = dbPredicates.find(predid);
        return p->second.manager->estimateCardinality(query);
    } else {
        // if (literal->getNVars() != literal->getTupleSize()) {
        //     LOG(DEBUGL) << "Estimate is not very precise";
        // }
        IndexedTupleTable *rel = tmpRelations[predid];
        return rel->getNTuples();
    }
}

bool EDBLayer::isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    const Literal *literal = &query;
    PredId_t predid = literal->getPredicate().getId();
    if (dbPredicates.count(predid)) {
        auto p = dbPredicates.find(predid);
        return p->second.manager->isEmpty(query, posToFilter, valuesToFilter);
    } else {
        IndexedTupleTable *rel = tmpRelations[predid];
        assert(literal->getTupleSize() <= 2);
        /*
           if (posToFilter != NULL) {
           LOG(DEBUGL) << "isEmpty literal = " << literal->tostring()
           << ", posToFilter->size() = " << posToFilter->size()
           << ", valuesToFilter->size() = " << valuesToFilter->size();
           }
           */

        std::unique_ptr<Literal> rewrittenLiteral;
        if (posToFilter != NULL) {
            //Create a new literal where the var are replaced by the constants
            VTuple t = literal->getTuple();
            //TODO: this cannot be right, since valuesToFilter may contain a list of multiple
            //patterns. --Ceriel
            for (int i = 0; i < posToFilter->size(); ++i) {
                uint8_t pos = posToFilter->at(i);
                Term_t value = valuesToFilter->at(i);
                t.set(VTerm(0, value), pos);
                rewrittenLiteral = std::unique_ptr<Literal>(new Literal(literal->getPredicate(), t));
                literal = rewrittenLiteral.get();
            }
        }

        int diff = literal->getNUniqueVars() - literal->getTupleSize();
        if (diff == 0) {
            return rel->getNTuples() == 0;
        } else if (diff == -1) {
            //The difference could be a duplicated variable or a constant
            bool foundConstant = false;
            uint8_t idxVar = 0;
            Term_t valConst = 0;
            for (uint8_t i = 0; i < literal->getTupleSize(); ++i) {
                if (!literal->getTermAtPos(i).isVariable()) {
                    idxVar = i;
                    valConst = literal->getTermAtPos(i).getValue();
                    foundConstant = true;
                }
            }
            if (foundConstant) {
                return !rel->exists(idxVar, valConst);
            } else {
                //Check all rows where two columns are equal
                assert(literal->getTupleSize() == 2);
                for (std::vector<std::pair<Term_t, Term_t>>::iterator itr =
                        rel->getTwoColumn1()->begin(); itr != rel->getTwoColumn1()->end();
                        ++itr) {
                    if (itr->first == itr->second) {
                        return false;
                    }
                }
                return true;
            }
        } else {
            if (literal->getNUniqueVars() == 0) {
                //Need to check whether a particular row exists
                assert(literal->getTupleSize() == 2);
                if (std::binary_search(rel->getTwoColumn1()->begin(),
                            rel->getTwoColumn1()->end(),
                            std::make_pair((Term_t) literal->getTermAtPos(0).getValue(),
                                (Term_t) literal->getTermAtPos(1).getValue())))
                    return false;
                else
                    return  true;
            } else {
                LOG(ERRORL) << "Not supported";
                throw 10;
            }
        }
    }
}

// Only used in prematerialization
void EDBLayer::addTmpRelation(Predicate & pred, IndexedTupleTable * table) {
    tmpRelations[pred.getId()] = table;
}

// Only used in prematerialization
bool EDBLayer::checkValueInTmpRelation(const uint8_t relId, const uint8_t posInRelation,
        const Term_t value) const {
    if (tmpRelations[relId] != NULL) {
        return tmpRelations[relId]->exists(posInRelation, value);
    } else {
        return true;
    }
}

void EDBLayer::releaseIterator(EDBIterator * itr) {
    if (dbPredicates.count(itr->getPredicateID())) {
        auto p = dbPredicates.find(itr->getPredicateID());
        return p->second.manager->releaseIterator(itr);
    } else {
        memItrFactory.release((EDBMemIterator*)itr);
    }
}

std::vector<std::shared_ptr<Column>>  EDBLayer::checkNewIn(
        std::vector <
        std::shared_ptr<Column >> &valuesToCheck,
        const Literal &l,
        std::vector<uint8_t> &posInL) {
    if (!dbPredicates.count(l.getPredicate().getId())) {
        LOG(ERRORL) << "Not supported";
        throw 10;
    }
    auto p = dbPredicates.find(l.getPredicate().getId());
    return p->second.manager->checkNewIn(valuesToCheck, l, posInL);
}

static std::vector<std::shared_ptr<Column>> checkNewInGeneric(const Literal &l1,
        std::vector<uint8_t> &posInL1,
        const Literal &l2,
        std::vector<uint8_t> &posInL2, EDBTable *p, EDBTable *p2) {
    std::vector<uint8_t> posVars1 = l1.getPosVars();
    std::vector<uint8_t> fieldsToSort1;
    for (int i = 0; i < posInL1.size(); i++) {
	fieldsToSort1.push_back(posVars1[posInL1[i]]);
    }
    std::vector<uint8_t> posVars2 = l2.getPosVars();
    std::vector<uint8_t> fieldsToSort2;
    std::vector<uint64_t> savedVal;
    for (int i = 0; i < posInL2.size(); i++) {
	fieldsToSort2.push_back(posVars2[posInL2[i]]);
    }
    EDBIterator *itr1 = p->getSortedIterator(l1, fieldsToSort1);
    EDBIterator *itr2 = p2->getSortedIterator(l2, fieldsToSort2);

    std::vector<std::shared_ptr<ColumnWriter>> cols;
    for (int i = 0; i < fieldsToSort1.size(); i++) {
	cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));
    }

    bool more = false;
    if (itr1->hasNext() && itr2->hasNext()) {
	itr1->next();
	itr2->next();
	while (true) {
	    bool equal = true;
	    bool lt = false;
	    for (int i = 0; i < fieldsToSort1.size(); i++) {
		if (itr1->getElementAt(fieldsToSort1[i]) != itr2->getElementAt(fieldsToSort2[i])) {
		    equal = false;
		    lt = itr1->getElementAt(fieldsToSort1[i]) < itr2->getElementAt(fieldsToSort2[i]);
		    break;
		}
	    }
	    if (equal) {
		if (itr1->hasNext()) {
		    itr1->next();
		} else {
		    break;
		}
	    } else if (lt) {
		if (savedVal.size() == 0) {
		    for (int i = 0; i < fieldsToSort1.size(); i++) {
			savedVal.push_back(itr1->getElementAt(fieldsToSort1[i]));
			cols[i]->add(savedVal[i]);
		    }
		} else {
		    bool present = true;
		    for (int i = 0; i < fieldsToSort1.size(); i++) {
			if (savedVal[i] != itr1->getElementAt(fieldsToSort1[i])) {
			    present = false;
			    savedVal[i] = itr1->getElementAt(fieldsToSort1[i]);
			}
		    }
		    if (! present) {
			for (int i = 0; i < fieldsToSort1.size(); i++) {
			    cols[i]->add(savedVal[i]);
			}
		    }
		}
		if (itr1->hasNext()) {
		    itr1->next();
		} else {
		    break;
		}
	    } else {
		if (itr2->hasNext()) {
		    itr2->next();
		} else {
		    more = true;
		    break;
		}
	    }
	}
    } else {
	more = itr1->hasNext();
    }

    while (more) {
	if (savedVal.size() == 0) {
	    for (int i = 0; i < fieldsToSort1.size(); i++) {
		savedVal.push_back(itr1->getElementAt(fieldsToSort1[i]));
		cols[i]->add(savedVal[i]);
	    }
	} else {
	    bool present = true;
	    for (int i = 0; i < fieldsToSort1.size(); i++) {
		if (savedVal[i] != itr1->getElementAt(fieldsToSort1[i])) {
		    present = false;
		    savedVal[i] = itr1->getElementAt(fieldsToSort1[i]);
		}
	    }
	    if (! present) {
		for (int i = 0; i < fieldsToSort1.size(); i++) {
		    cols[i]->add(savedVal[i]);
		}
	    }
	}
	more = itr1->hasNext();
	if (more) {
	    itr1->next();
	}
    }

    std::vector<std::shared_ptr<Column>> output;
    for (auto &writer : cols) {
	output.push_back(writer->getColumn());
    }

    itr1->clear();
    itr2->clear();
    delete itr1;
    delete itr2;
    return output;
}

std::vector<std::shared_ptr<Column>> EDBLayer::checkNewIn(const Literal &l1,
        std::vector<uint8_t> &posInL1,
        const Literal &l2,
        std::vector<uint8_t> &posInL2) {

    if (!dbPredicates.count(l1.getPredicate().getId()) || ! dbPredicates.count(l2.getPredicate().getId())) {
        LOG(ERRORL) << "Not supported";
        throw 10;
    }

    auto p = dbPredicates.find(l1.getPredicate().getId());
    auto p2 = dbPredicates.find(l2.getPredicate().getId());

    if (p->second.manager != p2->second.manager) {
	// We have to do it ourselves.
	return checkNewInGeneric(l1, posInL1, l2, posInL2, p->second.manager.get(), p2->second.manager.get());
    }

    return p->second.manager->checkNewIn(l1, posInL1, l2, posInL2);
}

bool EDBLayer::supportsCheckIn(const Literal &l) {
    return dbPredicates.count(l.getPredicate().getId());
}

std::shared_ptr<Column> EDBLayer::checkIn(
        std::vector<Term_t> &values,
        const Literal &l,
        uint8_t posInL,
        size_t &sizeOutput) {
    if (!dbPredicates.count(l.getPredicate().getId())) {
        LOG(ERRORL) << "Not supported: literal = " << l.tostring();
        throw 10;
    }
    auto p = dbPredicates.find(l.getPredicate().getId());
    return p->second.manager->checkIn(values, l, posInL, sizeOutput);

}

bool EDBLayer::getDictNumber(const char *text, const size_t sizeText, uint64_t &id) {
    if (dbPredicates.size() > 0) {
        //Get the number from the first edb table
        return dbPredicates.begin()->second.manager->
            getDictNumber(text, sizeText, id);
    }
    return false;
}

bool EDBLayer::getDictText(const uint64_t id, char *text) {
    if (dbPredicates.size() > 0) {
        //Get the number from the first edb table
        return dbPredicates.begin()->second.manager->getDictText(id, text);
    }
    return false;
}

uint64_t EDBLayer::getNTerms() {
    if (dbPredicates.size() > 0) {
        //Get the number from the first edb table
        return dbPredicates.begin()->second.manager->getNTerms();
    }
    return 0;
}

Predicate EDBLayer::getDBPredicate(int idPredicate) {
    if (!dbPredicates.count(idPredicate)) {
        throw 10; //cannot happen
    }
    EDBInfoTable &info = dbPredicates.find(idPredicate)->second;
    return Predicate(idPredicate, 0, EDB, info.arity);
}

std::vector<PredId_t> EDBLayer::getAllPredicateIDs() {
    std::vector<PredId_t> out;
    for(auto &t : dbPredicates) {
        out.push_back(t.first);
    }
    return out;
}

uint64_t EDBLayer::getPredSize(PredId_t id) {
    if (dbPredicates.count(id)) {
        return dbPredicates[id].manager->getSize();
    }
    return 0;
}

string EDBLayer::getPredType(PredId_t id) {
    if (dbPredicates.count(id)) {
        return dbPredicates[id].type;
    }
    return 0;
}

string EDBLayer::getPredName(PredId_t id) {
    if (dbPredicates.count(id)) {
        return predDictionary.getRawValue(id);
    }
    return 0;
}

uint8_t EDBLayer::getPredArity(PredId_t id) {
    if (dbPredicates.count(id)) {
        return dbPredicates[id].arity;
    }
    return 0;
}

void EDBMemIterator::init1(PredId_t id, std::vector<Term_t>* v, const bool c1, const Term_t vc1) {
    predid = id;
    nfields = 1;
    oneColumn = v->begin();
    endOneColumn = v->end();

    if (c1) {
        //Search for the value.
        std::pair<std::vector<Term_t>::iterator, std::vector<Term_t>::iterator> bounds
            = std::equal_range(v->begin(), v->end(), vc1);
        oneColumn = bounds.first;
        endOneColumn = bounds.second;
    }

    isFirst = true;
    hasFirst = oneColumn != endOneColumn;
    ignoreSecondColumn = false;
    isIgnoreAllowed = false;
}

void EDBMemIterator::init2(PredId_t id, const bool defaultSorting, std::vector<std::pair<Term_t, Term_t>>* v, const bool c1,
        const Term_t vc1, const bool c2, const Term_t vc2,
        const bool equalFields) {
    predid = id;
    ignoreSecondColumn = false;
    isIgnoreAllowed = true;
    this->equalFields = equalFields;
    nfields = 2;
    twoColumns = v->begin();
    endTwoColumns = v->end();
    if (c1) {
        isIgnoreAllowed = false;
        assert(defaultSorting);
        bool lowerOk = false;
        if (c2) {
            std::pair<Term_t, Term_t> pair = std::make_pair(vc1, vc2);
            twoColumns = std::lower_bound(v->begin(), v->end(), pair);
            lowerOk = twoColumns != endTwoColumns && twoColumns->first == vc1 && twoColumns->second == vc2;
        } else {
            std::pair<Term_t, Term_t> pair = std::make_pair(vc1, 0);
            twoColumns = std::lower_bound(v->begin(), v->end(), pair);
            lowerOk = twoColumns != endTwoColumns && twoColumns->first == vc1;
        }
        if (!lowerOk) {
            twoColumns = endTwoColumns;
        } else {
            //Get the upper bound
            if (c2) {
                std::pair<Term_t, Term_t> pair = std::make_pair(vc1, vc2);
                endTwoColumns = std::upper_bound(twoColumns, v->end(), pair);
            } else {
                std::pair<Term_t, Term_t> pair = std::make_pair(vc1, std::numeric_limits<Term_t>::max());
                endTwoColumns = std::upper_bound(twoColumns, v->end(), pair);
            }
        }
    } else {
        if (c2) {
            assert(!defaultSorting);
            std::pair<Term_t, Term_t> pair = std::make_pair(0, vc2);
            twoColumns = std::lower_bound(v->begin(), v->end(), pair, [](const std::pair<Term_t, Term_t>& lhs, const std::pair<Term_t, Term_t>& rhs) {
                    return lhs.second < rhs.second || (lhs.second == rhs.second && lhs.first < rhs.first);
                    } );
            bool lowerOk = twoColumns != endTwoColumns && twoColumns->second == vc2;
            if (!lowerOk) {
                twoColumns = endTwoColumns;
            } else {
                std::pair<Term_t, Term_t> pair = std::make_pair(std::numeric_limits<Term_t>::max(), vc2);
                endTwoColumns = std::upper_bound(twoColumns, v->end(), pair, [](const std::pair<Term_t, Term_t>& lhs, const std::pair<Term_t, Term_t>& rhs) {
                        return lhs.second < rhs.second || (lhs.second == rhs.second && lhs.first < rhs.first);
                        });
            }
        }
    }

    isFirst = true;
    hasFirst = twoColumns != endTwoColumns;
}

void EDBMemIterator::skipDuplicatedFirstColumn() {
    if (isIgnoreAllowed)
        ignoreSecondColumn = true;
    // LOG(DEBUGL) << "isIgnoreAllowed = " << isIgnoreAllowed << ", ignoreSecondColumn = " << ignoreSecondColumn;
}

bool EDBMemIterator::hasNext() {
    if (equalFields) {
        //Move to the first line where both columns are equal
        if (!isNextCheck) {
            isNext = false;

            if (isFirst) {
                pointerEqualFieldsNext = twoColumns;
            } else {
                pointerEqualFieldsNext = twoColumns + 1;
            }

            while (pointerEqualFieldsNext != endTwoColumns) {
                if (pointerEqualFieldsNext->first == pointerEqualFieldsNext->second) {
                    isNext = true;
                    break;
                }
                pointerEqualFieldsNext++;
            }
            isNextCheck = true;
        }
        return isNext;
    }

    if (isFirst) {
        return hasFirst;
    }

    if (nfields == 1) {
        return (oneColumn + 1) != endOneColumn;
    } else {
        if (ignoreSecondColumn) {
            //Go through the next value in the first column
            //Make hasNext callable multiple times before calling next. --Ceriel
            if (isNextCheck) {
                return isNext;
            }
            isNextCheck = true;
            do {
                Term_t prevel = twoColumns->first;
                twoColumns++;
                if (twoColumns != endTwoColumns) {
                    if (twoColumns->first != prevel) {
                        isNext = true;
                        return true;
                    }
                } else {
                    isNext = false;
                    return false;
                }

            } while (true);

        } else {
            return (twoColumns + 1) != endTwoColumns;
        }
    }
}

void EDBMemIterator::next() {
    if (equalFields) {
        isFirst = false;
        twoColumns = pointerEqualFieldsNext;
        isNextCheck = false;
        return;
    } else if (ignoreSecondColumn) {
        isFirst = false;
        isNextCheck = false;
        return;
    }

    if (isFirst) {
        isFirst = false;
    } else {
        if (nfields == 1) {
            oneColumn++;
        } else {
            twoColumns++;
        }
    }
}

Term_t EDBMemIterator::getElementAt(const uint8_t p) {
    if (nfields == 1) {
        return *oneColumn;
    } else {
        if (p == 0) {
            return twoColumns->first;
        } else {
            return twoColumns->second;
        }
    }
}

std::vector<std::shared_ptr<Column>> EDBTable::checkNewIn(const Literal &l1,
                                  std::vector<uint8_t> &posInL1,
                                  const Literal &l2,
std::vector<uint8_t> &posInL2) {
    return checkNewInGeneric(l1, posInL1, l2, posInL2, this, this);
}

std::vector<std::shared_ptr<Column>> EDBTable::checkNewIn(
                                      std::vector <
                                      std::shared_ptr<Column >> &checkValues,
                                      const Literal &l,
std::vector<uint8_t> &posInL) {

    LOG(DEBUGL) << "checkNewIn version 2";

    std::vector<uint8_t> posVars = l.getPosVars();
    std::vector<uint8_t> fieldsToSort;
    for (int i = 0; i < posInL.size(); i++) {
	fieldsToSort.push_back(posVars[posInL[i]]);
    }

    EDBIterator *iter = getSortedIterator(l, fieldsToSort);

    int sz = checkValues.size();

    std::vector<std::shared_ptr<ColumnWriter>> cols;
    for (int i = 0; i < fieldsToSort.size(); i++) {
	cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));
    }

    std::vector<std::unique_ptr<ColumnReader>> valuesToChReader;

    for (int i = 0; i < checkValues.size(); i++) {
	std::shared_ptr<Column> valuesToCh = checkValues[i];
	valuesToChReader.push_back(valuesToCh->getReader());
    }

    std::vector<Term_t> prevcv;
    std::vector<Term_t> cv;
    std::vector<Term_t> vi;

    for (int i = 0; i < sz; i++) {
	prevcv.push_back((Term_t) -1);
	cv.push_back((Term_t) -1);
	vi.push_back((Term_t) -1);
    }

    if (iter->hasNext()) {
	iter->next();
	for (int i = 0; i < sz; i++) {
	    if (! valuesToChReader[i]->hasNext()) {
		throw 10;
	    }
	    cv[i] = valuesToChReader[i]->next();
	}

	bool equal;

	while (true) {
	    for (int i = 0; i < sz; i++) {
		vi[i] = iter->getElementAt(fieldsToSort[i]);
	    }
	    equal = true;
	    bool lt = false;
	    for (int i = 0; i < sz; i++) {
		if (vi[i] == cv[i]) {
		} else {
		    if (vi[i] < cv[i]) {
			lt = true;
		    }
		    equal = false;
		    break;
		}
	    }
	    if (equal) {
		for (int i = 0; i < sz; i++) {
		    prevcv[i] = cv[i];
		}
		if (iter->hasNext()) {
		    iter->next();
		    for (int i = 0; i < sz; i++) {
                        if (!valuesToChReader[i]->hasNext()) {
                            cv[0] = (Term_t) - 1;
                            break;
                        } else {
                            cv[i] = valuesToChReader[i]->next();
                        }
                    }
		    if (cv[0] == (Term_t) -1) {
			break;
		    }
		} else {
		    break;
		}
	    } else if (lt) {
		if (iter->hasNext()) {
		    iter->next();
		} else {
		    break;
		}
	    } else {
		equal = true;
		for (int i = 0; i < sz; i++) {
		    if (cv[i] != prevcv[i]) {
			equal = false;
			break;
		    }
		}
		if (! equal) {
		    for (int i = 0; i < sz; i++) {
			cols[i]->add(cv[i]);
                        prevcv[i] = cv[i];
                    }
		}
		for (int i = 0; i < sz; i++) {
		    if (!valuesToChReader[i]->hasNext()) {
			cv[0] = (Term_t) - 1;
			break;
		    } else {
			cv[i] = valuesToChReader[i]->next();
		    }
		}
		if (cv[0] == (Term_t) -1) {
		    break;
		}
            }
        }

        while (cv[0] != (Term_t) - 1) {
	    equal = true;
	    for (int i = 0; i < sz; i++) {
		if (cv[i] != prevcv[i]) {
		    equal = false;
		    break;
		}
	    }
	    if (! equal) {
		for (int i = 0; i < sz; i++) {
		    cols[i]->add(cv[i]);
		    prevcv[i] = cv[i];
		}
	    }
	    for (int i = 0; i < sz; i++) {
		if (!valuesToChReader[i]->hasNext()) {
		    cv[0] = (Term_t) - 1;
		    break;
		} else {
		    cv[i] = valuesToChReader[i]->next();
		}
	    }
	}
    }

    std::vector<std::shared_ptr<Column>> output;
    for (auto &el : cols)
        output.push_back(el->getColumn());
    iter->clear();
    delete iter;
    return output;
}

std::shared_ptr<Column> EDBTable::checkIn(
    std::vector<Term_t> &values,
    const Literal &l,
    uint8_t posInL,
    size_t &sizeOutput) {

//    if (l.getNVars() == 0 || l.getNVars() == l.getTupleSize()) {
//	LOG(ERRORL) << "EDBTable::checkIn() with getNVars() == " << l.getNVars() << " is not supported.";
//	throw 10;
//    }

    LOG(DEBUGL) << "EDBTable::checkIn";
    std::vector<uint8_t> posVars = l.getPosVars();
    std::vector<uint8_t> fieldsToSort;
    fieldsToSort.push_back(posVars[posInL]);
    EDBIterator *iter = getSortedIterator(l, fieldsToSort);

    //Output
    std::unique_ptr<ColumnWriter> col(new ColumnWriter());
    size_t idx1 = 0;
    const uint8_t varIndex = posVars[posInL];
    sizeOutput = 0;
    while (iter->hasNext()) {
	iter->next();
	const Term_t v2 = iter->getElementAt(varIndex);
	while (values[idx1] < v2) {
	    idx1++;
	    if (idx1 == values.size()) {
		break;
	    }
	}
	if (idx1 == values.size()) {
	    break;
	}
	if (values[idx1] == v2) {
	    col->add(v2);
	    sizeOutput++;
	    idx1++;
	    if (idx1 == values.size()) {
		break;
	    }
	}
    }
    iter->clear();
    delete iter;
    return col->getColumn();
}
