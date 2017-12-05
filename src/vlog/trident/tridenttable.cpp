#include <vlog/trident/tridenttable.h>

#include <trident/sparql/sparqloperators.h>
#include <trident/binarytables/newcolumntable.h>

void antiJoinOneColumn(int posJoin1, int posJoin2,
                       NewColumnTable *pitr1, NewColumnTable *pitr2,
                       std::shared_ptr<ColumnWriter> col) {
    SeqColumnWriter cw(col.get());
    if (posJoin1 == 1) {
        if (posJoin2 == 2) {
            pitr1->columnNotIn(1, pitr2, 2, &cw);
        } else { //posJoin2 == 1
            pitr1->columnNotIn(1, pitr2, 1, &cw);

        }
    } else {
        if (posJoin2 == 2) {
            pitr1->columnNotIn(2, pitr2, 2, &cw);
        } else {
            pitr1->columnNotIn(2, pitr2, 1, &cw);
        }
    }
}

void antiJoinTwoColumns(NewColumnTable *pitr1, NewColumnTable *pitr2,
                        std::shared_ptr<ColumnWriter> col1,
                        std::shared_ptr<ColumnWriter> col2) {

    bool more = false;
    if (pitr1->hasNext() && pitr2->hasNext()) {
        pitr1->next();
        pitr2->next();
        while (true) {
            if (pitr1->getValue1() == pitr2->getValue1() &&
                    pitr1->getValue2() == pitr2->getValue2()) {
                if (pitr1->hasNext()) {
                    pitr1->next();
                    if (pitr2->hasNext()) {
                        pitr2->next();
                    } else {
                        more = true;
                        break;
                    }
                } else {
                    break;
                }
            } else if (pitr1->getValue1() <  pitr2->getValue1() ||
                       (pitr1->getValue1() == pitr2->getValue1() &&
                        pitr1->getValue2() < pitr2->getValue2())) {

                col1->add(pitr1->getValue1());
                col2->add(pitr1->getValue2());

                if (pitr1->hasNext())
                    pitr1->next();
                else
                    break;
            } else {
                if (pitr2->hasNext())
                    pitr2->next();
                else {
                    more = true;
                    break;
                }
            }
        }

        if (more) {
            col1->add(pitr1->getValue1());
            col2->add(pitr1->getValue2());
            while (pitr1->hasNext()) {
                pitr1->next();
                col1->add(pitr1->getValue1());
                col2->add(pitr1->getValue2());
            }
        }
    }
}

std::vector<std::shared_ptr<Column>> TridentTable::performAntiJoin(
                                      const Literal &l1,
                                      std::vector<uint8_t> &pos1,
                                      const Literal &l2,
std::vector<uint8_t> &pos2) {

    TridentTupleItr itr1, itr2;

    //Prepare the iterators
    VTuple t1 = l1.getTuple();
    std::vector<uint8_t> fieldToSort;
    fieldToSort.push_back(l1.getPosVars()[pos1[0]]);
    if (pos1.size() == 2) {
        fieldToSort.push_back(l1.getPosVars()[pos1[1]]);
    }
    itr1.init(q, &t1, &fieldToSort, true, multithreaded ? &mutex : NULL);
    VTuple t2 = l2.getTuple();
    fieldToSort.clear();
    fieldToSort.push_back(l2.getPosVars()[pos2[0]]);
    if (pos2.size() == 2)
        fieldToSort.push_back(l2.getPosVars()[pos2[1]]);
    itr2.init(q, &t2, &fieldToSort, true, multithreaded ? &mutex : NULL);

    //Output
    std::vector<std::shared_ptr<ColumnWriter>> cols;
    cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));
    if (pos1.size() == 2)
        cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));


    //Do some checks
    if (l1.getNVars() == 0 || l1.getNVars() == 3) {
        LOG(ERRORL) << "These cases are not supported.";
        throw 10;
    }

    //Work with the physical iterators... a dirty hack but much faster
    PairItr *pitr1 = itr1.getPhysicalIterator();
    PairItr *pitr2 = itr2.getPhysicalIterator();
    if (pos1.size() == 1) {
        //Join on one column
        if (l1.getNVars() == 1) {
            if (l2.getNVars() == 1) {
                antiJoinOneColumn(2, 2, (NewColumnTable*) pitr1, (NewColumnTable*) pitr2, cols[0]);
            } else {
                antiJoinOneColumn(2, 1, (NewColumnTable*) pitr1, (NewColumnTable*) pitr2, cols[0]);
            }
        } else {
            if (l2.getNVars() == 1) {
                antiJoinOneColumn(1, 2, (NewColumnTable*) pitr1, (NewColumnTable*) pitr2, cols[0]);
            } else {
                antiJoinOneColumn(1, 1, (NewColumnTable*) pitr1, (NewColumnTable*) pitr2, cols[0]);
            }
        }
    } else {
        //Join on two columns
        antiJoinTwoColumns((NewColumnTable*) pitr1, (NewColumnTable*) pitr2, cols[0], cols[1]);
    }

    std::vector<std::shared_ptr<Column>> output;
    for (auto &writer : cols) {
        output.push_back(writer->getColumn());
    }
    return output;
}

std::vector<std::shared_ptr<Column>> TridentTable::performAntiJoin(
                                      std::vector <
                                      std::shared_ptr<Column >> &valuesToCheck,
                                      const Literal &l,
std::vector<uint8_t> &pos) {

    VTuple t = l.getTuple();
    assert(l.getNVars() < l.getTupleSize());
    std::vector<uint8_t> fieldToSort;
    fieldToSort.push_back(l.getPosVars()[pos[0]]);
    if (pos.size() == 2)
        fieldToSort.push_back(l.getPosVars()[pos[1]]);
    TridentTupleItr itr;
    itr.init(q, &t, &fieldToSort, true, multithreaded ? &mutex : NULL);

    //Output
    std::vector<std::shared_ptr<ColumnWriter>> cols;
    cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));
    if (pos.size() == 2)
        cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));

    //Do the antijoin
    if (valuesToCheck.size() == 1) {
        PairItr *pitrO = itr.getPhysicalIterator();
        NewColumnTable *pitr = (NewColumnTable*) pitrO;
        //size_t idx = 0;
        const bool firstCol = pos[0] == 1 || (pos[0] == 0 && l.getNVars() == 2);
        std::shared_ptr<Column> valuesToCh = valuesToCheck[0];
        std::unique_ptr<ColumnReader> valuesToChReader = valuesToCh->getReader();


        if (!valuesToChReader->hasNext()) {
            throw 10; //Can it happen?
        }
        Term_t prevV2 = (Term_t) - 1;
        Term_t v2 = valuesToChReader->next();
        if (firstCol)
            pitr->ignoreSecondColumn();

        if (pitr->hasNext()) {
            pitr->next();

            while (true) {
                const Term_t v1 = firstCol ? pitr->getValue1() : pitr->getValue2();
                if (v1 == v2) {
                    //Move one side
                    if (!valuesToChReader->hasNext()) {
                        v2 = (Term_t) - 1;
                        break;
                    } else {
                        prevV2 = v2;
                        v2 = valuesToChReader->next();

                        //Same as the previous one?
                        if (v2 == prevV2) {
                            do {
                                if (valuesToChReader->hasNext()) {
                                    v2 = valuesToChReader->next();
                                } else {
                                    v2 = (Term_t) - 1;
                                    break;
                                }
                            } while (v2 == prevV2);

                            if (v2 == (Term_t) - 1)
                                break;
                        }
                    }

                    //Move also the other one
                    if (pitr->hasNext()) {
                        pitr->next();
                    } else {
                        break;
                    }
                } else if (v1 < v2) {
                    if (pitr->hasNext())
                        pitr->next();
                    else
                        break;
                } else {
                    cols[0]->add(v2);
                    if (!valuesToChReader->hasNext()) {
                        v2 = (Term_t) - 1;
                        break;
                    } else {
                        prevV2 = v2;
                        v2 = valuesToChReader->next();

                        //Same as the previous one?
                        if (v2 == prevV2) {
                            do {
                                if (valuesToChReader->hasNext()) {
                                    v2 = valuesToChReader->next();
                                } else {
                                    v2 = (Term_t) - 1;
                                    break;
                                }
                            } while (v2 == prevV2);

                            if (v2 == (Term_t) - 1)
                                break;
                        }
                    }
                }
            }
        }

        if (v2 != (Term_t) - 1) {
            cols[0]->add(v2);
            prevV2 = v2;
            while (valuesToChReader->hasNext()) {
                const Term_t v2 = valuesToChReader->next();
                if (v2 != prevV2) {
                    cols[0]->add(v2);
                    prevV2 = v2;
                }
            }
        }
    } else { //nfields == 2
        PairItr *pitrO = itr.getPhysicalIterator();
        NewColumnTable *pitr = (NewColumnTable*) pitrO;
        //size_t idx = 0;
        //const size_t sizeToCheck = valuesToCheck[0]->size();
        //

        std::unique_ptr<ColumnReader> valuesToCheck1 = valuesToCheck[0]->getReader();
        std::unique_ptr<ColumnReader> valuesToCheck2 = valuesToCheck[1]->getReader();

        Term_t prevcv1 = (Term_t) - 1;
        Term_t prevcv2 = (Term_t) - 1;

        Term_t cv1 = (Term_t) - 1;
        Term_t cv2 = (Term_t) - 1;
        if (pitr->hasNext()) {
            pitr->next();
            if (!valuesToCheck1->hasNext())
                throw 10;
            cv1 = valuesToCheck1->next();

            if (!valuesToCheck2->hasNext())
                throw 10;
            cv2 = valuesToCheck2->next();

            while (true) {
                const Term_t v1 = pitr->getValue1();
                const Term_t v2 = pitr->getValue2();

                if (v1 == cv1 && v2 == cv2) {
                    prevcv1 = cv1;
                    prevcv2 = cv2;
                    if (pitr->hasNext()) {
                        pitr->next();
                        if (!valuesToCheck1->hasNext()) {
                            cv1 = (Term_t) - 1;
                            break;
                        } else if (!valuesToCheck2->hasNext()) {
                            cv1 = (Term_t) - 1;
                            break;
                        } else {
                            cv1 = valuesToCheck1->next();
                            cv2 = valuesToCheck2->next();
                        }
                    } else {
                        break;
                    }
                } else if (v1 < cv1 || (v1 == cv1 && v2 < cv2)) {
                    if (pitr->hasNext())
                        pitr->next();
                    else
                        break;
                } else {
                    if (cv1 != prevcv1 || cv2 != prevcv2) {
                        cols[0]->add(cv1);
                        cols[1]->add(cv2);
                        prevcv1 = cv1;
                        prevcv2 = cv2;
                    }

                    if (!valuesToCheck1->hasNext()) {
                        cv1 = (Term_t) - 1;
                        break;
                    } else if (!valuesToCheck2->hasNext()) {
                        cv1 = (Term_t) - 1;
                        break;
                    } else {
                        cv1 = valuesToCheck1->next();
                        cv2 = valuesToCheck2->next();
                    }
                }
            }
        }

        if (cv1 != (Term_t) - 1) {
            if (cv1 != prevcv1 || cv2 != prevcv2) {
                cols[0]->add(cv1);
                cols[1]->add(cv2);
                prevcv1 = cv1;
                prevcv2 = cv2;
            }
            while (valuesToCheck1->hasNext() && valuesToCheck2->hasNext()) {
                cv1 = valuesToCheck1->next();
                cv2 = valuesToCheck2->next();
                //cols[0]->add(valuesToCheck1->next());
                //cols[1]->add(valuesToCheck2->next());
                if (cv1 != prevcv1 || cv2 != prevcv2) {
                    cols[0]->add(cv1);
                    cols[1]->add(cv2);
                    prevcv1 = cv1;
                    prevcv2 = cv2;
                }
            }
        }
    }

    std::vector<std::shared_ptr<Column>> output;
    for (auto &el : cols)
        output.push_back(el->getColumn());
    return output;
}

// Local
void TridentTable::getQueryFromEDBRelation0(QSQQuery *query,
        TupleTable *outputTable) {
    //No join to perform. Simply execute the query using TupleKBIterator
    VTuple tuple = query->getLiteral()->getTuple();
    TridentTupleItr itr;
    itr.init(q, &tuple, NULL, multithreaded ? &mutex : NULL);
    uint64_t row[3];
    uint8_t *pos = query->getPosToCopy();
    const uint8_t npos = query->getNPosToCopy();
    while (itr.hasNext()) {
        itr.next();
        for (uint8_t i = 0; i < npos; ++i) {
            row[i] = itr.getElementAt(pos[i]);
        }
        outputTable->addRow(row);
    }
}

// Local
void TridentTable::getQueryFromEDBRelation3(QSQQuery *query,
        TupleTable *outputTable,
        std::vector<Term_t> *valuesToFilter) {
    //Group by predicate
    std::unordered_map<uint64_t, std::vector<std::pair<uint64_t, uint64_t>>*> map;
    for (std::vector<Term_t>::iterator itr = valuesToFilter->begin();
            itr != valuesToFilter->end(); ++itr) {
        //RAISE_IF_EXPIRED(timeout);
        uint64_t s = *itr;
        itr++;
        uint64_t p = *itr;
        itr++;
        uint64_t o = *itr;

        std::unordered_map<uint64_t, std::vector<std::pair<uint64_t, uint64_t>>*>::iterator mapItr = map.find(p);
        if (mapItr == map.end()) {
            std::vector<std::pair<uint64_t, uint64_t>> *pairs =
                    new std::vector<std::pair<uint64_t, uint64_t>>();
            pairs->push_back(std::make_pair(o, s));
            map.insert(std::make_pair(p, pairs));
        } else {
            mapItr->second->push_back(std::make_pair(o, s));
        }
    }

    //Calculate joins, posToCopy, posToFilter, and varToReturn
    std::vector<int> posVarsToReturn;
    posVarsToReturn.push_back(2);
    posVarsToReturn.push_back(0);
    posVarsToReturn.push_back(1);

    std::vector<std::vector<int>> posToCopy;
    std::vector<int> pos1;
    pos1.push_back(0);
    pos1.push_back(1);
    pos1.push_back(2);
    posToCopy.push_back(pos1);
    std::vector<int> pos2;
    posToCopy.push_back(pos2);

    std::vector<std::pair<int, int>> joins;
    joins.push_back(std::make_pair(1, 2));
    joins.push_back(std::make_pair(2, 0));

    //Ask multiple queries
    Pattern p2;
    p2.subject(-3);
    p2.object(-2);
    for (std::unordered_map<uint64_t, std::vector<std::pair<uint64_t, uint64_t>>*>::iterator itr = map.begin();
            itr != map.end(); ++itr) {
        std::vector<std::pair<uint64_t, uint64_t>> *pairs = itr->second;
        std::sort(pairs->begin(), pairs->end());
        if (multithreaded) {
            mutex.lock();
        }
        ArrayItr *firstItr = q->getArrayIterator();
        std::shared_ptr<std::vector<std::pair<uint64_t, uint64_t>>> spairs = std::shared_ptr<std::vector<std::pair<uint64_t, uint64_t>>>(pairs);
        firstItr->init(spairs, -1, -1);
        firstItr->setKey(itr->first);
        p2.predicate(itr->first);

        std::shared_ptr<NestedJoinPlan> plan(new NestedJoinPlan(p2, q,
                                             posToCopy, joins, posVarsToReturn));
        NestedMergeJoinItr join(q, plan, firstItr, outputTable, LONG_MAX);
        if (multithreaded) {
            mutex.unlock();
        }
        if (join.hasNext()) {
            //Calling hasNext() of NWayJoin should populate the entire outputTable
            if (outputTable->getNRows() == 0) {
                throw 10; //This should not happen
            }
        } else {
            assert(outputTable->getNRows() == 0);
        }
        //delete pairs;
    }
}

// Local
void TridentTable::getQueryFromEDBRelation12(QSQQuery *query,
        TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    int sizePosToFilter = posToFilter->size();

    std::vector<std::pair<uint64_t, uint64_t>> pairs;

    bool sorted = true;
    if (sizePosToFilter == 1) {
        std::vector<Term_t>::iterator itr = valuesToFilter->begin();
        Term_t prevEl = *itr;
        pairs.push_back(std::make_pair(0, *itr));
        itr++;
        for (;
                itr != valuesToFilter->end(); ++itr) {
            if (*itr < prevEl) {
                sorted = false;
                pairs.push_back(std::make_pair(0, *itr));
            } else if (*itr > prevEl) {
                pairs.push_back(std::make_pair(0, *itr));
            }
            prevEl = *itr;
        }
    } else {
        uint64_t preV1 = valuesToFilter->at(0);
        uint64_t preV2 = valuesToFilter->at(1);
        for (std::vector<Term_t>::iterator itr = valuesToFilter->begin();
                itr != valuesToFilter->end(); ++itr) {
            uint64_t v1 = *itr;
            itr++;
            uint64_t v2 = *itr;
            pairs.push_back(std::make_pair(v1, v2));
            if (sorted && (v1 < preV1 || (preV1 == v1 && v2 < preV2))) {
                sorted = false;
            } else {
                preV1 = v1;
                preV2 = v2;
            }
        }
    }

    if (!sorted) {
        std::sort(pairs.begin(), pairs.end());
    }

    std::vector<std::pair<int, int>> joins;
    std::vector<std::vector<int>> posToCopy;

    //-->First pattern
    std::vector<int> pos;
    if (sizePosToFilter == 1) {
        pos.push_back(2);
        joins.push_back(std::make_pair(0, posToFilter->at(0)));
    } else {
        pos.push_back(1);
        pos.push_back(2);
        joins.push_back(std::make_pair(0, posToFilter->at(0)));
        joins.push_back(std::make_pair(1, posToFilter->at(1)));
    }
    posToCopy.push_back(pos);

    //-->Second pattern
    std::vector<int> pos2;
    for (int i = 0; i < 3; ++i) {
        if (query->getLiteral()->getTermAtPos(i).isVariable()) {
            pos2.push_back(i);
        }
    }
    posToCopy.push_back(pos2);

    int nVarsToReturn = pos2.size();
    std::vector<int> posVarsToReturn;
    for (int i = 0; i < nVarsToReturn; ++i) {
        posVarsToReturn.push_back(sizePosToFilter + i);
    }

    const Literal *l = query->getLiteral();
    assert(pairs.size() > 0);
    getQueryFromEDBRelation12(l->getTermAtPos(0), l->getTermAtPos(1),
                              l->getTermAtPos(2), outputTable, posToFilter,
                              &pairs, posVarsToReturn,
                              joins, posToCopy/*, timeout*/);
}

void dummydeleter(std::vector<std::pair<uint64_t, uint64_t>> *t) {
    //Do nothing. The object will be deleted later on
}

// Local
void TridentTable::getQueryFromEDBRelation12(VTerm s, VTerm p, VTerm o,
        TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<std::pair<uint64_t, uint64_t>> *pairs,
        std::vector<int> &posVarsToReturn,
        std::vector<std::pair<int, int>> &joins,
        std::vector<std::vector<int>> &posToCopy) {
    //Sort pairs
    std::sort(pairs->begin(), pairs->end());

    Pattern p2;
    int sizePosToFilter = posToFilter->size();
    //Subject
    if (s.isVariable()) {
        if (posToFilter->at(0) == 0) {
            p2.subject(-2);
        } else if (sizePosToFilter == 2 && posToFilter->at(1) == 0) {
            p2.subject(-3);
        } else {
            p2.subject(-1);
        }
    } else {
        p2.subject(s.getValue());
    }
    if (p.isVariable()) {
        if (posToFilter->at(0) == 1) {
            p2.predicate(-2);
        } else if (sizePosToFilter == 2 && posToFilter->at(1) == 1) {
            p2.predicate(-3);
        } else {
            p2.predicate(-1);
        }
    } else {
        p2.predicate(p.getValue());
    }
    if (o.isVariable()) {
        if (posToFilter->at(0) == 2) {
            p2.object(-2);
        } else if (sizePosToFilter == 2 && posToFilter->at(1) == 2) {
            p2.object(-3);
        } else {
            p2.object(-1);
        }
    } else {
        p2.object(o.getValue());
    }
    if (multithreaded) {
        mutex.lock();
    }
    ArrayItr *firstItr = q->getArrayIterator();
    std::shared_ptr<std::vector<std::pair<uint64_t, uint64_t>>> spairs =
        std::shared_ptr <
        std::vector<std::pair<uint64_t, uint64_t> >> (pairs, dummydeleter);
    firstItr->init(spairs, -1, -1);
    std::shared_ptr<NestedJoinPlan> plan(new NestedJoinPlan(p2, q, posToCopy,
                                         joins, posVarsToReturn));

    //execute the plan and copy the results in the table
    NestedMergeJoinItr join(q, plan, firstItr, outputTable, LONG_MAX);
    if (multithreaded) {
        mutex.unlock();
    }
    if (join.hasNext()) {
        //Calling hasNext() of NWayJoin should populate the entire outputTable
        if (outputTable->getNRows() == 0) {
            throw 10; //This should not happen
        }
    } else {
        assert(outputTable->getNRows() == 0);
    }
}

std::shared_ptr<Column> TridentTable::checkIn(
    std::vector<Term_t> &values,
    const Literal &l,
    uint8_t posInL,
    size_t &sizeOutput) {

//Do some checks
    if (l.getNVars() == 0 || l.getNVars() == 3) {
        LOG(ERRORL) << "These cases are not supported.";
        throw 10;
    }

    //Output
    std::unique_ptr<ColumnWriter> col(new ColumnWriter());

    //Prepare the iterators
    VTuple t = l.getTuple();
    std::vector<uint8_t> fieldToSort;
    fieldToSort.push_back(l.getPosVars()[posInL]);
    TridentTupleItr itr1;
    itr1.init(q, &t, &fieldToSort, true, multithreaded ? &mutex : NULL);
    PairItr *pitrO = itr1.getPhysicalIterator();

    NewColumnTable *pitr = (NewColumnTable*)pitrO;
    size_t idx1 = 0;
    const bool firstCol = l.getNVars() == 2;
    sizeOutput = 0;
    if (pitr->hasNext()) {
        pitr->next();
        while (true) {
            const Term_t v1 = values[idx1];
            const Term_t v2 =  firstCol ? pitr->getValue1() : pitr->getValue2();
            if (v1 < v2) {
                idx1++;
                if (idx1 == values.size()) {
                    break;
                }
            } else if (v1 > v2) {
                if (pitr->hasNext()) {
                    pitr->next();
                } else {
                    break;
                }
            } else {
                col->add(v1);
                sizeOutput++;
                idx1++;
                if (idx1 == values.size()) {
                    break;
                }
                if (pitr->hasNext()) {
                    pitr->next();
                } else {
                    break;
                }
            }
        }
    }

    return col->getColumn();
}

std::vector<std::shared_ptr<Column>> TridentTable::checkNewIn(const Literal &l1,
                                  std::vector<uint8_t> &posInL1,
                                  const Literal &l2,
std::vector<uint8_t> &posInL2) {
    return performAntiJoin(l1, posInL1, l2, posInL2);
}

std::vector<std::shared_ptr<Column>> TridentTable::checkNewIn(
                                      std::vector <
                                      std::shared_ptr<Column >> &valuesToCheck,
                                      const Literal &l,
std::vector<uint8_t> &posInL) {
    return performAntiJoin(valuesToCheck, l, posInL);
}

size_t TridentTable::getCardinality(const Literal &query) {
    const Literal *literal = &query;
    long s, p, o;
    VTerm t = literal->getTermAtPos(0);
    if (t.isVariable()) {
        s = -1;
    } else {
        s = t.getValue();
    }
    t = literal->getTermAtPos(1);
    if (t.isVariable()) {
        p = -1;
    } else {
        p = t.getValue();
    }
    t = literal->getTermAtPos(2);
    if (t.isVariable()) {
        o = -1;
    } else {
        o = t.getValue();
    }
    if (multithreaded) {
        mutex.lock();
    }
    size_t result = q->getCard(s, p, o);
    if (multithreaded) {
        mutex.unlock();
    }
    if (query.getNUniqueVars() < query.getNVars()) {
        result = result / 10;   // ???
    }
    // LOG(DEBUGL) << "getCardinality, query = " << query.tostring(NULL, NULL) << ", result = " << result;
    return result;
}

//same as above
size_t TridentTable::estimateCardinality(const Literal &query) {
    const Literal *literal = &query;
    long s, p, o;
    VTerm t = literal->getTermAtPos(0);
    if (t.isVariable()) {
        s = -1;
    } else {
        s = t.getValue();
    }
    t = literal->getTermAtPos(1);
    if (t.isVariable()) {
        p = -1;
    } else {
        p = t.getValue();
    }
    t = literal->getTermAtPos(2);
    if (t.isVariable()) {
        o = -1;
    } else {
        o = t.getValue();
    }
    if (multithreaded) {
        mutex.lock();
    }
    size_t result = q->getCard(s, p, o);
    if (multithreaded) {
        mutex.unlock();
    }
    if (query.getNUniqueVars() < query.getNVars()) {
        result = result / 10;   // ???
    }
    // LOG(DEBUGL) << "EstimateCardinality, query = " << query.tostring(NULL, NULL) << ", result = " << result;
    return result;
}

bool TridentTable::isEmpty(const Literal &query,
                           std::vector<uint8_t> *posToFilter,
                           std::vector<Term_t> *valuesToFilter) {
    const Literal *literal = &query;
    long s, p, o;
    VTerm t = literal->getTermAtPos(0);
    if (t.isVariable()) {
        s = -1;
    } else {
        s = t.getValue();
    }
    t = literal->getTermAtPos(1);
    if (t.isVariable()) {
        p = -1;
    } else {
        p = t.getValue();
    }
    t = literal->getTermAtPos(2);
    if (t.isVariable()) {
        o = -1;
    } else {
        o = t.getValue();
    }

    if (posToFilter != NULL) {
        //Replace variables with constants
        for (int i = 0; i < posToFilter->size(); ++i) {
            uint8_t pos = posToFilter->at(i);
            Term_t value = valuesToFilter->at(i);
            if (pos == 0) {
                s = value;
            } else if (pos == 1) {
                p = value;
            } else {
                o = value;
            }
        }
    }

    if (multithreaded) {
        mutex.lock();
    }
    bool retval = q->isEmpty(s, p, o);
    if (multithreaded) {
        mutex.unlock();
    }
    /*
    LOG(DEBUGL) << "isEmpty, query = " << query.tostring(NULL, NULL)
                             << "posToFilter, s, p, o = " << (posToFilter != NULL) << ", " << s << ", " << p << ", " << o
                             << ", result = " << retval;
    */
    return retval;
}

void TridentTable::releaseIterator(EDBIterator * itr) {
    ((TridentIterator*)itr)->clear();
    if (multithreaded) {
        mutex.lock();
    }
    kbItrFactory.release((TridentIterator*)itr);
    if (multithreaded) {
        mutex.unlock();
    }
}

size_t TridentTable::getCardinalityColumn(const Literal &query,
        uint8_t posColumn) {
    const Literal *literal = &query;
    long s, p, o;
    VTerm t = literal->getTermAtPos(0);
    if (t.isVariable()) {
        s = -1;
    } else {
        s = t.getValue();
    }
    t = literal->getTermAtPos(1);
    if (t.isVariable()) {
        p = -1;
    } else {
        p = t.getValue();
    }
    t = literal->getTermAtPos(2);
    if (t.isVariable()) {
        o = -1;
    } else {
        o = t.getValue();
    }
    if (multithreaded) {
        mutex.lock();
    }
    size_t result = q->getCard(s, p, o, posColumn);
    if (multithreaded) {
        mutex.unlock();
    }
    // LOG(DEBUGL) << "getCardinalityColumn, query = " << query.tostring(NULL, NULL)
    //                          << ", posColumn = " << (int) posColumn << ", result = " << result;
    return result;
}

void TridentTable::query(QSQQuery *query, TupleTable *outputTable,
                         std::vector<uint8_t> *posToFilter,
                         std::vector<Term_t> *valuesToFilter) {
    if (query->getNRepeatedVars() > 0 && posToFilter != 0 &&
            posToFilter->size() > 0) {
        LOG(ERRORL) << "Not (yet) supported";
        throw 10;
    }
    if (posToFilter == NULL || posToFilter->size() == 0) {
        getQueryFromEDBRelation0(query, outputTable);
    } else if (posToFilter->size() < 3) {
        getQueryFromEDBRelation12(query, outputTable, posToFilter,
                                  valuesToFilter);
    } else {
        getQueryFromEDBRelation3(query, outputTable,
                                 valuesToFilter);
    }
    // LOG(DEBUGL) << "query, query = " << query->tostring()
    //                          << ", positionsToFilter = " << (posToFilter != NULL && posToFilter->size() > 0)
    //                          << ", result size = " << outputTable->getNRows();
}

TridentIterator *TridentTable::getTridentIter() {
    if (multithreaded) {
        mutex.lock();
    }
    TridentIterator *retval = kbItrFactory.get();
    if (multithreaded) {
        mutex.unlock();
    }
    return retval;
}

EDBIterator *TridentTable::getIterator(const Literal &query) {
    const Literal *literal = &query;
    // LOG(DEBUGL) << "Get iterator for query " << literal->tostring(NULL, NULL);
    TridentIterator *itr = getTridentIter();
    itr->init(query.getPredicate().getId(), q, *literal, multithreaded ? &mutex : NULL);
    return itr;
}

EDBIterator *TridentTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    const Literal *literal = &query;
    // LOG(DEBUGL) << "Get sorted iterator for query " << literal->tostring(NULL, NULL);
    TridentIterator *itr = getTridentIter();
    itr->init(query.getPredicate().getId(), q, *literal, fields, multithreaded ? &mutex : NULL);
    return itr;
}

bool TridentTable::getDictNumber(const char *text, const size_t sizeText,
                                 uint64_t &id) {
    return dict->getNumber(text, sizeText, (nTerm*)&id);
}

bool TridentTable::getDictText(const uint64_t id, char *text) {
    return dict->getText(id, text);
}

uint64_t TridentTable::getNTerms() {
    return kb->getNTerms();
}

uint64_t TridentTable::getSize() {
    return kb->getSize();
}
