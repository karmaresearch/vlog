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
#ifdef SPARQL
#include <vlog/sparql/sparqltable.h>
#endif
#include <vlog/inmemory/inmemorytable.h>
#include <vlog/incremental/edb-table-from-idb.h>
#include <vlog/incremental/edb-table-importer.h>

#include <climits>


EDBLayer::EDBLayer(EDBLayer &db, bool copyTables) : conf(db.conf) {
    this->predDictionary = db.predDictionary;
    this->termsDictionary = db.termsDictionary;
    if (copyTables) {
        this->dbPredicates = db.dbPredicates;
    }
}

std::vector<PredId_t> EDBLayer::getAllEDBPredicates() {
    std::vector<PredId_t> out;
    for(const auto &pair : dbPredicates) {
        out.push_back(pair.first);
    }
    return out;
}

void EDBLayer::addTridentTable(const EDBConf::Table &tableConf, bool multithreaded) {
    EDBInfoTable infot;
    const std::string pn = tableConf.predname;
    const std::string kbpath = tableConf.params[0];
    if (!Utils::exists(kbpath) || !Utils::exists(kbpath + DIR_SEP + "p0")) {
        LOG(ERRORL) << "The KB at " << kbpath << " does not exist. Check the edb.conf file.";
        throw 10;
    }
    infot.id = (PredId_t) predDictionary->getOrAdd(pn);
    infot.type = tableConf.type;
    infot.manager = std::shared_ptr<EDBTable>(new TridentTable(kbpath, multithreaded, this));
    infot.arity = infot.manager->getArity();
    dbPredicates.insert(make_pair(infot.id, infot));
    LOG(INFOL) << "Inserted " << pn << " with number " << infot.id;
    LOG(DEBUGL) << "Inserted " << pn << " with number " << infot.id;
}

#ifdef MYSQL
void EDBLayer::addMySQLTable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const std::string pn = tableConf.predname;
    infot.id = (PredId_t) predDictionary->getOrAdd(pn);
    infot.arity = 3;
    infot.type = tableConf.type;
    infot.manager = std::shared_ptr<EDBTable>(new MySQLTable(infot.id, tableConf.params[0],
                tableConf.params[1], tableConf.params[2], tableConf.params[3],
                tableConf.params[4], tableConf.params[5], this));
    dbPredicates.insert(make_pair(infot.id, infot));
}
#endif

#ifdef ODBC
void EDBLayer::addODBCTable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const std::string pn = tableConf.predname;
    infot.id = (PredId_t) predDictionary->getOrAdd(pn);
    infot.arity = 3;
    infot.type = tableConf.type;
    infot.manager = std::shared_ptr<EDBTable>(new ODBCTable(infot.id, tableConf.params[0],
                tableConf.params[1], tableConf.params[2], tableConf.params[3],
                tableConf.params[4], this));
    dbPredicates.insert(make_pair(infot.id, infot));
}
#endif

#ifdef MAPI
void EDBLayer::addMAPITable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const std::string pn = tableConf.predname;
    infot.id = (PredId_t) predDictionary->getOrAdd(pn);
    infot.arity = 3;
    infot.type = tableConf.type;
    infot.manager = std::shared_ptr<EDBTable>(new MAPITable(infot.id, tableConf.params[0],
                (int) strtol(tableConf.params[1].c_str(), NULL, 10), tableConf.params[2], tableConf.params[3],
                tableConf.params[4], tableConf.params[5], tableConf.params[6], this));
    dbPredicates.insert(make_pair(infot.id, infot));
}
#endif

#ifdef MDLITE
void EDBLayer::addMDLiteTable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const std::string pn = tableConf.predname;
    infot.id = (PredId_t) predDictionary->getOrAdd(pn);
    infot.type = tableConf.type;
    MDLiteTable *table = new MDLiteTable(infot.id, tableConf.params[0], tableConf.params[1], this);
    infot.manager = std::shared_ptr<EDBTable>(table);
    infot.arity = table->getArity();
    dbPredicates.insert(make_pair(infot.id, infot));
}
#endif

void EDBLayer::addInmemoryTable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const std::string pn = tableConf.predname;
    infot.id = (PredId_t) predDictionary->getOrAdd(pn);
    infot.type = tableConf.type;
    string repository = tableConf.params[0];
    if (repository.size() > 0 && repository[0] != '/') {
        //Relative path. Add the root path
        repository = rootPath + "/" + repository;
    }
    InmemoryTable *table;
    if (tableConf.params.size() == 2) {
        table = new InmemoryTable(repository,
                tableConf.params[1], infot.id, this);
    } else {
        char sep = tableConf.params[2][0];
        if (sep == 't')
            sep = '\t';
        table = new InmemoryTable(repository,
                tableConf.params[1], infot.id, this, sep, loadAllData);
    }
    infot.manager = std::shared_ptr<EDBTable>(table);
    infot.arity = table->getArity();
    dbPredicates.insert(make_pair(infot.id, infot));

    LOG(DEBUGL) << "Imported InmemoryTable " << pn << " id " << infot.id << " size " << table->getSize();
    // table->dump(std::cerr);
}

void EDBLayer::addInmemoryTable(std::string predicate, std::vector<std::vector<std::string>> &rows) {
    PredId_t id = (PredId_t) predDictionary->getOrAdd(predicate);
    addInmemoryTable(predicate, id, rows);
}

void EDBLayer::addInmemoryTable(std::string predicate, PredId_t id, std::vector<std::vector<std::string>> &rows) {
    EDBInfoTable infot;
    infot.id = id;
    if (doesPredExists(infot.id)) {
        LOG(INFOL) << "Rewriting table for predicate " << predicate;
        dbPredicates.erase(infot.id);
    }
    infot.type = "INMEMORY";
    InmemoryTable *table = new InmemoryTable(infot.id, rows, this);
    infot.arity = table->getArity();
    infot.manager = std::shared_ptr<EDBTable>(table);
    dbPredicates.insert(make_pair(infot.id, infot));
    LOG(DEBUGL) << "Added table for " << predicate << ":" << infot.id << ", arity = " << (int) table->getArity() << ", size = " << table->getSize();
    //LOG(INFOL) << "Imported InmemoryTable id " << infot.id << " predicate " << predicate;
    // table->dump(std::cerr);
}


void EDBLayer::addInmemoryTable(PredId_t id,
        uint8_t arity,
        std::vector<uint64_t> &rows) {
    EDBInfoTable infot;
    infot.id = id;
    if (doesPredExists(infot.id)) {
        LOG(INFOL) << "Rewriting table for predicate " << id;
        dbPredicates.erase(infot.id);
    }
    infot.type = "INMEMORY";
    InmemoryTable *table = new InmemoryTable(infot.id, arity, rows, this);
    infot.arity = table->getArity();
    infot.manager = std::shared_ptr<EDBTable>(table);
    dbPredicates.insert(make_pair(infot.id, infot));
}

#ifdef SPARQL
void EDBLayer::addSparqlTable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const std::string predicate = tableConf.predname;
    infot.id = (PredId_t) predDictionary->getOrAdd(predicate);
    if (doesPredExists(infot.id)) {
        LOG(WARNL) << "Rewriting table for predicate " << predicate;
        dbPredicates.erase(infot.id);
    }
    infot.type = "SPARQL";
    SparqlTable *table = new SparqlTable(infot.id, tableConf.params[0], this, tableConf.params[1], tableConf.params[2]);
    infot.arity = table->getArity();
    infot.manager = std::shared_ptr<EDBTable>(table);
    dbPredicates.insert(make_pair(infot.id, infot));
}
#endif

void EDBLayer::addEDBonIDBTable(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const string pn = tableConf.predname;

    Term_t tid;
    if (! predDictionary->get(pn, tid)) {
        LOG(ERRORL) << "predicate should have been pre-registered in EDB";
        throw "predicate should have been pre-registered in EDB";
    }
    PredId_t pid = (PredId_t)tid;
    infot.id = pid;
    infot.type = tableConf.type;
    EDBonIDBTable *table = new EDBonIDBTable(infot.id, this,
            prevSemiNaiver[tableConf.params[0]]);
    infot.manager = std::shared_ptr<EDBTable>(table);
    infot.arity = table->getArity();
    dbPredicates.insert(make_pair(infot.id, infot));

    LOG(INFOL) << "Inserted EDBonIDB table id " << infot.id << " predicate " << pn;
    // table->dump(std::cout);
}

void EDBLayer::addEDBimporter(const EDBConf::Table &tableConf) {
    EDBInfoTable infot;
    const string pn = tableConf.predname;

    Term_t tid;
    if (! predDictionary->get(pn, tid)) {
        LOG(ERRORL) << "predicate should have been pre-registered in EDB";
        throw "predicate should have been pre-registered in EDB";
    }
    PredId_t pid = (PredId_t)tid;
    infot.id = pid;
    infot.type = tableConf.type;
    EDBimporter *table = new EDBimporter(infot.id, this,
            prevSemiNaiver[tableConf.params[0]]);
    infot.manager = std::shared_ptr<EDBTable>(table);
    infot.arity = table->getArity();
    dbPredicates.insert(make_pair(infot.id, infot));

    LOG(INFOL) << "Inserted EDBimporter table id " << infot.id << " predicate " << pn;
    // table->dump(std::cout);
}

bool EDBLayer::doesPredExists(PredId_t id) const {
    LOG(DEBUGL) << "doesPredExists for: " << id;
    return dbPredicates.count(id);
}

void EDBLayer::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    PredId_t predid = query->getLiteral()->getPredicate().getId();


    if (dbPredicates.count(predid)) {
        auto el = dbPredicates.find(predid);
        el->second.manager->query(query, outputTable, posToFilter, valuesToFilter);
    } else if (tmpRelations.size() <= predid) {
        // nothing
    } else {
        IndexedTupleTable *rel = tmpRelations[predid];
        uint8_t size = rel->getSizeTuple();
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
}

EDBIterator *EDBLayer::getIterator(const Literal &query) {
    const Literal *literal = &query;
    PredId_t predid = literal->getPredicate().getId();

    if (dbPredicates.count(predid)) {
        auto p = dbPredicates.find(predid);
        auto itr = p->second.manager->getIterator(query);
        if (hasRemoveLiterals(predid)) {
            LOG(DEBUGL) << "HERE " << __func__ << ":" << __LINE__ << "=" << query.tostring(NULL, this) << " inject RemoveIterator";
            EDBIterator *ritr = new EDBRemovalIterator(query, *removals[predid], itr);
            return ritr;
        } else {
            return itr;
        }

    } else if (tmpRelations.size() <= predid) {
        return new EmptyEDBIterator(predid);
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

        EDBMemIterator *itr;
        switch (size) {
            case 1:
                itr = memItrFactory.get();
                itr->init1(predid, rel->getSingleColumn(), c1, vc1);
                break;
            case 2:
                itr = memItrFactory.get();
                itr->init2(predid, c1, rel->getTwoColumn1(), c1, vc1, c2, vc2, equalFields);
                break;
            default:
                throw 10;
        }

        if (hasRemoveLiterals(predid)) {
            LOG(DEBUGL) << "HERE " << __func__ << ":" << __LINE__ << "=" << query.tostring(NULL, this) << " inject RemoveIterator";
            EDBIterator *ritr = new EDBRemovalIterator(query, *removals[predid], itr);
            return ritr;
        } else {
            return itr;
        }
    }
}


EDBIterator *EDBLayer::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    const Literal *literal = &query;
    PredId_t predid = literal->getPredicate().getId();
    EDBIterator *itr;

    if (dbPredicates.count(predid)) {
        auto p = dbPredicates.find(predid);
        itr = p->second.manager->getSortedIterator(query, fields);
        if (hasRemoveLiterals(predid)) {
            LOG(DEBUGL) << "EDBLayer=" << name << " Wrap an EDBRemovalIterator for " << literal->tostring();
            itr = new EDBRemovalIterator(query, fields, *removals[predid], itr);
        } else {
            LOG(DEBUGL) << "EDBLayer=" << name << " No wrap of an EDBRemovalIterator for " << literal->tostring();
        }
        return itr;
    } else if (tmpRelations.size() <= predid) {
        return new EmptyEDBIterator(predid);
    } else {
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
        EDBMemIterator *mitr;
        switch (size) {
            case 1:
                mitr = memItrFactory.get();
                mitr->init1(predid, rel->getSingleColumn(), c1, vc1);
                break;
            case 2:
                mitr = memItrFactory.get();
                if (c1) {
                    mitr->init2(predid, true, rel->getTwoColumn1(), c1, vc1, c2, vc2, equalFields);
                } else {
                    if (c2) {
                        mitr->init2(predid, false, rel->getTwoColumn2(), c1, vc1, c2, vc2, equalFields);
                    } else {
                        //No constraints
                        if (fields.size() != 0 && fields[0] == 0) {
                            mitr->init2(predid, true, rel->getTwoColumn1(), c1, vc1, c2, vc2, equalFields);
                        } else {
                            mitr->init2(predid, false, rel->getTwoColumn2(), c1, vc1, c2, vc2, equalFields);
                        }
                    }
                }
                break;
            default:
                throw 10;
        }
        if (hasRemoveLiterals(predid)) {
            LOG(DEBUGL) << "EDBLayer=" << name << " Wrap an EDBRemovalIterator on EDBMemIterator for " << literal->tostring();
            itr = new EDBRemovalIterator(query, fields, *removals[predid], mitr);
        } else {
            itr = mitr;
        }
    }

    return itr;
}

size_t EDBLayer::getCardinalityColumn(const Literal &query,
        uint8_t posColumn) {
    const Literal *literal = &query;
    PredId_t predid = literal->getPredicate().getId();
    if (dbPredicates.count(predid)) {
        auto p = dbPredicates.find(predid);
        return p->second.manager->getCardinalityColumn(query, posColumn);
    } else if (tmpRelations.size() <= predid) {
        return 0;
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
    } else if (tmpRelations.size() <= predid) {
        return 0;
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
    } else if (tmpRelations.size() <= predid) {
        return 0;
    } else {
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
    } else if (tmpRelations.size() <= predid) {
        return true;
    } else {
        IndexedTupleTable *rel = tmpRelations[predid];
        if (!rel) {
            return true;
        }
        assert(literal->getTupleSize() <= 2);

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
    if (pred.getId() >= tmpRelations.size()) {
        tmpRelations.resize(2*pred.getId()+1);
    }
    tmpRelations[pred.getId()] = table;
}

// Only used in prematerialization
bool EDBLayer::checkValueInTmpRelation(const uint8_t relId, const uint8_t posInRelation,
        const Term_t value) const {
    if (relId < tmpRelations.size() && tmpRelations[relId] != NULL) {
        return tmpRelations[relId]->exists(posInRelation, value);
    } else {
        return true;
    }
}

void EDBLayer::releaseIterator(EDBIterator * itr) {
    PredId_t pred_id = itr->getPredicateID();
    if (dbPredicates.count(pred_id)) {
        auto p = dbPredicates.find(pred_id);
        if (hasRemoveLiterals(pred_id)) {
            auto ritr = dynamic_cast<EDBRemovalIterator *>(itr);
            p->second.manager->releaseIterator(ritr->getUnderlyingIterator());
            delete itr;
        } else {
            p->second.manager->releaseIterator(itr);
        }
    } else {
        memItrFactory.release((EDBMemIterator*)itr);
    }
}

std::vector<std::shared_ptr<Column>> EDBLayer::checkNewIn(
        std::vector<std::shared_ptr<Column>> &valuesToCheck,
        const Literal &l,
        std::vector<uint8_t> &posInL) {
    if (!dbPredicates.count(l.getPredicate().getId())) {
        LOG(ERRORL) << "Not supported";
        throw 10;
    }
    auto p = dbPredicates.find(l.getPredicate().getId());
    return p->second.manager->checkNewIn(valuesToCheck, l, posInL);
}

template <typename VAR>
static std::vector<VAR> range(size_t size) {
    std::vector<VAR> r(size);
    for (size_t i = 0; i < size; ++i) {
        r[i] = static_cast<VAR>(i);
    }

    return r;
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
    EDBIterator *itr1 = p->getSortedIterator(l1, posInL1);
    EDBIterator *itr2 = p2->getSortedIterator(l2, posInL2);

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

bool EDBLayer::getDictNumber(const char *text, const size_t sizeText, uint64_t &id) const {
    bool resp = false;
    if (dbPredicates.size() > 0) {
        resp = dbPredicates.begin()->second.manager->
            getDictNumber(text, sizeText, id);
    }
    if (!resp && termsDictionary.get()) {
        std::string t(text, sizeText);
        resp = termsDictionary->get(t, id);
    }
    return resp;
}

bool EDBLayer::getOrAddDictNumber(const char *text, const size_t sizeText,
        uint64_t &id) {
    bool resp = false;
    if (dbPredicates.size() > 0) {
        resp = dbPredicates.begin()->second.manager->
            getDictNumber(text, sizeText, id);
    }
    if (!resp) {
        if (!termsDictionary.get()) {
            LOG(DEBUGL) << "The additional terms will start from " << getNTerms();
            termsDictionary = std::shared_ptr<Dictionary>(
                    new Dictionary(getNTerms()));
        }
        std::string t(text, sizeText);
        id = termsDictionary->getOrAdd(t);
        LOG(TRACEL) << "getOrAddDictNumber \"" << t << "\" returns " << id;
        resp = true;
    }
    return resp;
}

bool EDBLayer::getDictText(const uint64_t id, char *text) const {
    bool resp = false;
    if (dbPredicates.size() > 0) {
        resp = dbPredicates.begin()->second.manager->getDictText(id, text);
    }
    if (!resp && termsDictionary.get()) {
        std::string t = termsDictionary->getRawValue(id);
        if (t != "") {
            memcpy(text, t.c_str(), t.size());
            text[t.size()] = '\0';
            return true;
        }
    }
    return resp;
}

std::string EDBLayer::getDictText(const uint64_t id) const {
    std::string t = "";
    bool resp = false;
    if (dbPredicates.size() > 0) {
        resp = dbPredicates.begin()->second.manager->getDictText(id, t);
    }
    if (!resp && termsDictionary.get()) {
        t = termsDictionary->getRawValue(id);
    }
    return t;
}

uint64_t EDBLayer::getNTerms() const {
    uint64_t size = 0;
    if (dbPredicates.size() > 0) {
        size = dbPredicates.begin()->second.manager->getNTerms();
    }
    if (termsDictionary.get()) {
        size += termsDictionary->size();
    }
    return size;
}

Predicate EDBLayer::getDBPredicate(int idPredicate) const {
    if (!dbPredicates.count(idPredicate)) {
        throw 10; //cannot happen
    }
    const EDBInfoTable &info = dbPredicates.find(idPredicate)->second;
    return Predicate(idPredicate, 0, EDB, info.arity);
}

std::vector<PredId_t> EDBLayer::getAllPredicateIDs() const {
    std::vector<PredId_t> out;
    for(const auto &t : dbPredicates) {
        out.push_back(t.first);
    }
    return out;
}

uint64_t EDBLayer::getPredSize(PredId_t id) const {
    if (dbPredicates.count(id)) {
        return dbPredicates.at(id).manager->getSize();
    }
    return 0;
}

std::string EDBLayer::getPredType(PredId_t id) const {
    if (dbPredicates.count(id)) {
        return dbPredicates.at(id).type;
    }
    return 0;
}

std::string EDBLayer::getPredName(PredId_t id) const {
    if (dbPredicates.count(id)) {
        return predDictionary->getRawValue(id);
    }
    return 0;
}

uint8_t EDBLayer::getPredArity(PredId_t id) const {
    if (dbPredicates.count(id)) {
        return dbPredicates.at(id).arity;
    }
    return 0;
}

void EDBLayer::setPredArity(PredId_t id, uint8_t arity) {
    if (dbPredicates.count(id)) {
        dbPredicates[id].arity = arity;
    }
}

PredId_t EDBLayer::getPredID(const std::string &name) const {
    for (const auto &p : dbPredicates) {
        if (predDictionary->getRawValue(p.first) == name) {
            return p.first;
        }
    }
    return -1;
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
        std::shared_ptr<Column>> &checkValues,
        const Literal &l,
        std::vector<uint8_t> &posInL) {

    LOG(DEBUGL) << "checkNewIn version 2";

    std::vector<uint8_t> posVars = l.getPosVars();
    std::vector<uint8_t> fieldsToSort;
    for (int i = 0; i < posInL.size(); i++) {
        fieldsToSort.push_back(posVars[posInL[i]]);
    }

    EDBIterator *iter = getSortedIterator(l, posInL);

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
    //  LOG(ERRORL) << "EDBTable::checkIn() with getNVars() == " << l.getNVars() << " is not supported.";
    //  throw 10;
    //    }

    LOG(DEBUGL) << "EDBTable::checkIn, literal = " << l.tostring() << ", posInL = " << (int) posInL;
    std::vector<uint8_t> posVars = l.getPosVars();
    uint8_t pos = posVars[posInL];
    std::vector<uint8_t> fieldsToSort;
    fieldsToSort.push_back(posInL);
    EDBIterator *iter = getSortedIterator(l, fieldsToSort);

    //Output
    HiResTimer t_merge("checkIn merge");
    t_merge.start();
    std::unique_ptr<ColumnWriter> col(new ColumnWriter());
    size_t idx1 = 0;
    sizeOutput = 0;
    while (iter->hasNext()) {
        iter->next();
        const Term_t v2 = iter->getElementAt(pos);
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
    t_merge.stop();
    LOG(INFOL) << t_merge.tostring();

    return col->getColumn();
}


// Import all predicates from pre-existing prevSemiNaiver-s ->Program into this
// EDB layer: they are all needed as EDB predicates. Thingy: I want them
// to have the same PredId_t so lookup in the EDBonIDB is simple.
void EDBLayer::handlePrevSemiNaiver() {

    struct NamedPredicate {
        NamedPredicate(Predicate pred, const std::string &name) :
            pred(pred), name(name) {
            };

        Predicate pred;
        std::string name;
    };

    const uint64_t dictStart = 1;

    for (auto pSN : prevSemiNaiver) {
        // const
        Program *program = pSN.second->getProgram();
        // Program has no method to expose all PredId's, look them up through
        // the predicate name
        const std::vector<std::string> pred_names = program->getAllPredicateStrings();
        // Need to unique_ptr<> the thingy because Predicate has no default
        // constructor
        auto pred = std::vector<std::unique_ptr<NamedPredicate>>(
                pred_names.size() + dictStart);
        for (const std::string &n : pred_names) {
            Predicate p = program->getPredicate(n);
            if (p.getId() >= pred.size()) {
                LOG(ERRORL) << "Predicates are not contiguous. Cannot import to new EDBLayer!";
                throw("Importing predicates: they are not contiguous");
            }
            pred[p.getId()] = std::unique_ptr<NamedPredicate>(
                    new NamedPredicate(p, n));
        }

        for (::size_t i = dictStart; i < pred.size(); ++i) {
            const auto &p = pred[i];
            PredId_t id = predDictionary->getOrAdd(p->name);
            if (id != p->pred.getId()) {
                LOG(ERRORL) << "Predicates not contiguous in insert, how can that be?";
                throw("Copy previous SN predicates: not contiguous after all");
            }
        }
    }
}
