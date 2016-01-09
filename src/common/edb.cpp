/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Vlog.

   Vlog is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Vlog is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Vlog.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <vlog/edb.h>
#include <vlog/concepts.h>
#include <vlog/idxtupletable.h>
#include <vlog/column.h>

#include <trident/iterators/arrayitr.h>
#include <trident/sparql/sparqloperators.h>

#include <unordered_map>
#include <climits>

// Local in edb.cpp.
void antiJoinOneColumn(int posJoin1, int posJoin2,
                       PairItr *pitr1, PairItr *pitr2,
                       std::shared_ptr<ColumnWriter> col) {
    if (posJoin1 == 1) {
        if (posJoin2 == 2) {
            /*//I must read the first field of the pair iterator and skip the second
            if (pitr1->has_next() && pitr2->has_next()) {
                pitr1->next_pair();
                pitr2->next_pair();
                while (true) {
                    if (pitr1->getValue1() == pitr2->getValue2()) {
                        if (pitr1->has_next()) {
                            pitr1->next_pair();
                            if (pitr2->has_next()) {
                                pitr2->next_pair();
                            } else {
                                more = true;
                                break;
                            }
                        } else {
                            break;
                        }
                    } else if (pitr1->getValue1() <  pitr2->getValue2()) {
                        col->add(pitr1->getValue1());
                        if (pitr1->has_next())
                            pitr1->next_pair();
                        else
                            break;
                    } else {
                        if (pitr2->has_next())
                            pitr2->next_pair();
                        else {
                            more = true;
                            break;
                        }
                    }
                }

                if (more) {
                    col->add(pitr1->getValue1());
                    while (pitr1->has_next()) {
                        pitr1->next_pair();
                        col->add(pitr1->getValue1());
                    }
                }
            }*/
            PairHandler *pair1 = pitr1->getPairHandler();
            if (pair1 == NULL) {
                pitr1->getPairHandler();
            }
            PairHandler *pair2 = pitr2->getPairHandler();
            pair1->columnNotIn(1, pair2, 2, col.get());
        } else { //posJoin2 == 1
            /*if (pitr1->has_next() && pitr2->has_next()) {
                ((StorageItr*)pitr1)->ignoreSecondColumn();
                ((StorageItr*)pitr2)->ignoreSecondColumn();
                pitr1->next_pair();
                pitr2->next_pair();
                while (true) {
                    if (pitr1->getValue1() == pitr2->getValue1()) {
                        if (pitr1->has_next()) {
                            pitr1->next_pair();
                            if (pitr2->has_next()) {
                                pitr2->next_pair();
                            } else {
                                more = true;
                                break;
                            }
                        } else {
                            break;
                        }
                    } else if (pitr1->getValue1() <  pitr2->getValue1()) {
                        col->add(pitr1->getValue1());
                        cout << pitr1->getValue1() << endl;
                        if (pitr1->has_next())
                            pitr1->next_pair();
                        else
                            break;
                    } else {
                        if (pitr2->has_next())
                            pitr2->next_pair();
                        else {
                            more = true;
                            break;
                        }
                    }
                }

                if (more) {
                    col->add(pitr1->getValue1());
                    while (pitr1->has_next()) {
                        pitr1->next_pair();
                        col->add(pitr1->getValue1());
                    }
                }
            }*/
            PairHandler *pair1 = pitr1->getPairHandler();
            PairHandler *pair2 = pitr2->getPairHandler();
            pair1->columnNotIn(1, pair2, 1, col.get());

        }
    } else {
        if (posJoin2 == 2) {
            /*//In both cases I must read the second field
            if (pitr1->has_next() && pitr2->has_next()) {
                pitr1->next_pair();
                pitr2->next_pair();
                while (true) {
                    if (pitr1->getValue2() == pitr2->getValue2()) {
                        if (pitr1->has_next()) {
                            pitr1->next_pair();
                            if (pitr2->has_next()) {
                                pitr2->next_pair();
                            } else {
                                more = true;
                                break;
                            }
                        } else {
                            break;
                        }
                    } else if (pitr1->getValue2() <  pitr2->getValue2()) {
                        col->add(pitr1->getValue2());
                        if (pitr1->has_next())
                            pitr1->next_pair();
                        else
                            break;
                    } else {
                        if (pitr2->has_next())
                            pitr2->next_pair();
                        else {
                            more = true;
                            break;
                        }
                    }
                }

                if (more) {
                    col->add(pitr1->getValue2());
                    while (pitr1->has_next()) {
                        pitr1->next_pair();
                        col->add(pitr1->getValue2());
                    }
                }
            }*/
            PairHandler *pair1 = pitr1->getPairHandler();
            PairHandler *pair2 = pitr2->getPairHandler();
            pair1->columnNotIn(2, pair2, 2, col.get());

        } else {
            /*if (pitr1->has_next() && pitr2->has_next()) {
                ((StorageItr*)pitr2)->ignoreSecondColumn();
                pitr1->next_pair();
                pitr2->next_pair();
                while (true) {
                    if (pitr1->getValue2() == pitr2->getValue1()) {
                        if (pitr1->has_next()) {
                            pitr1->next_pair();
                            if (pitr2->has_next()) {
                                pitr2->next_pair();
                            } else {
                                more = true;
                                break;
                            }
                        } else {
                            break;
                        }
                    } else if (pitr1->getValue2() <  pitr2->getValue1()) {
                        col->add(pitr1->getValue2());
                        if (pitr1->has_next())
                            pitr1->next_pair();
                        else
                            break;
                    } else {
                        if (pitr2->has_next())
                            pitr2->next_pair();
                        else {
                            more = true;
                            break;
                        }
                    }
                }

                if (more) {
                    col->add(pitr1->getValue2());
                    while (pitr1->has_next()) {
                        pitr1->next_pair();
                        col->add(pitr1->getValue2());
                    }
                }
            }*/

            PairHandler *pair1 = pitr1->getPairHandler();
            PairHandler *pair2 = pitr2->getPairHandler();
            pair1->columnNotIn(2, pair2, 1, col.get());
        }
    }
}

// Local in edb.cpp
void antiJoinTwoColumns(PairItr *pitr1, PairItr *pitr2,
                        std::shared_ptr<ColumnWriter> col1,
                        std::shared_ptr<ColumnWriter> col2) {

    bool more = false;
    if (pitr1->has_next() && pitr2->has_next()) {
        pitr1->next_pair();
        pitr2->next_pair();
        while (true) {
            if (pitr1->getValue1() == pitr2->getValue1() &&
                    pitr1->getValue2() == pitr2->getValue2()) {
                if (pitr1->has_next()) {
                    pitr1->next_pair();
                    if (pitr2->has_next()) {
                        pitr2->next_pair();
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

                if (pitr1->has_next())
                    pitr1->next_pair();
                else
                    break;
            } else {
                if (pitr2->has_next())
                    pitr2->next_pair();
                else {
                    more = true;
                    break;
                }
            }
        }

        if (more) {
            col1->add(pitr1->getValue1());
            col2->add(pitr1->getValue2());
            while (pitr1->has_next()) {
                pitr1->next_pair();
                col1->add(pitr1->getValue1());
                col2->add(pitr1->getValue2());
            }
        }
    }
}

// Local
std::vector<std::shared_ptr<Column>> EDBLayer::performAntiJoin(
                                      const Literal &l1,
                                      std::vector<uint8_t> &pos1,
                                      const Literal &l2,
std::vector<uint8_t> &pos2) {

    TupleKBItr itr1, itr2;

    //Prepare the iterators
    Tuple t1 = l1.getTuple();
    std::vector<uint8_t> fieldToSort;
    fieldToSort.push_back(l1.getPosVars()[pos1[0]]);
    if (pos1.size() == 2) {
        fieldToSort.push_back(l1.getPosVars()[pos1[1]]);
    }
    itr1.init(q, &t1, &fieldToSort, true);

    Tuple t2 = l2.getTuple();
    fieldToSort.clear();
    fieldToSort.push_back(l2.getPosVars()[pos2[0]]);
    if (pos2.size() == 2)
        fieldToSort.push_back(l2.getPosVars()[pos2[1]]);
    itr2.init(q, &t2, &fieldToSort, true);

    //Output
    std::vector<std::shared_ptr<ColumnWriter>> cols;
    cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));
    if (pos1.size() == 2)
        cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));


    //Do some checks
    if (l1.getNVars() == 0 || l1.getNVars() == 3) {
        BOOST_LOG_TRIVIAL(error) << "These cases are not supported.";
        throw 10;
    }

    //Work with the physical iterators... a dirty hack but much faster
    PairItr *pitr1 = itr1.getPhysicalIterator();
    PairItr *pitr2 = itr2.getPhysicalIterator();
    if (pos1.size() == 1) {
        //Join on one column
        if (l1.getNVars() == 1) {
            if (l2.getNVars() == 1) {
                antiJoinOneColumn(2, 2, pitr1, pitr2, cols[0]);
            } else {
                antiJoinOneColumn(2, 1, pitr1, pitr2, cols[0]);
            }
        } else {
            if (l2.getNVars() == 1) {
                antiJoinOneColumn(1, 2, pitr1, pitr2, cols[0]);
            } else {
                antiJoinOneColumn(1, 1, pitr1, pitr2, cols[0]);
            }
        }
    } else {
        //Join on two columns
        antiJoinTwoColumns(pitr1, pitr2, cols[0], cols[1]);
    }

    std::vector<std::shared_ptr<Column>> output;
    for (auto &writer : cols) {
        output.push_back(writer->getColumn());
    }
    return output;
}

std::vector<std::shared_ptr<Column>> EDBLayer::performAntiJoin(
                                      std::vector <
                                      std::shared_ptr<Column >> &valuesToCheck,
                                      const Literal &l,
std::vector<uint8_t> &pos) {

    Tuple t = l.getTuple();
    assert(l.getNVars() < l.getTupleSize());
    std::vector<uint8_t> fieldToSort;
    fieldToSort.push_back(l.getPosVars()[pos[0]]);
    if (pos.size() == 2)
        fieldToSort.push_back(l.getPosVars()[pos[1]]);
    TupleKBItr itr;
    itr.init(q, &t, &fieldToSort, true);

    //Output
    std::vector<std::shared_ptr<ColumnWriter>> cols;
    cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));
    if (pos.size() == 2)
        cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));

    //Do the antijoin
    if (valuesToCheck.size() == 1) {
        PairItr *pitr = itr.getPhysicalIterator();
        //size_t idx = 0;
        const bool firstCol = pos[0] == 1 || (pos[0] == 0 && l.getNVars() == 2);
        std::shared_ptr<Column> valuesToCh = valuesToCheck[0];
        std::unique_ptr<ColumnReader> valuesToChReader = valuesToCh->getReader();


        if (!valuesToChReader->hasNext()) {
            throw 10; //Can it happen?
        }
        Term_t prevV2 = (Term_t) -1;
        Term_t v2 = valuesToChReader->next();
        if (pitr->has_next()) {
            if (firstCol)
                ((StorageItr*)pitr)->ignoreSecondColumn();

            pitr->next_pair();

            while (true) {
                const Term_t v1 = firstCol ? pitr->getValue1() : pitr->getValue2();
                if (v1 == v2) {
                    //Move one side
                    if (!valuesToChReader->hasNext()) {
                        v2 = (Term_t) -1;
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
                                    v2 = (Term_t) -1;
                                    break;
                                }
                            } while (v2 == prevV2);

                            if (v2 == (Term_t) -1)
                                break;
                        }
                    }

                    //Move also the other one
                    if (pitr->has_next()) {
                        pitr->next_pair();
                    } else {
                        break;
                    }
                } else if (v1 < v2) {
                    if (pitr->has_next())
                        pitr->next_pair();
                    else
                        break;
                } else {
                    cols[0]->add(v2);
                    if (!valuesToChReader->hasNext()) {
                        v2 = (Term_t) -1;
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
                                    v2 = (Term_t) -1;
                                    break;
                                }
                            } while (v2 == prevV2);

                            if (v2 == (Term_t) -1)
                                break;
                        }
                    }
                }
            }
        }

        if (v2 != (Term_t) -1) {
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
        PairItr *pitr = itr.getPhysicalIterator();
        //size_t idx = 0;
        //const size_t sizeToCheck = valuesToCheck[0]->size();

        std::unique_ptr<ColumnReader> valuesToCheck1 = valuesToCheck[0]->getReader();
        std::unique_ptr<ColumnReader> valuesToCheck2 = valuesToCheck[1]->getReader();

        Term_t prevcv1 = (Term_t) -1;
        Term_t prevcv2 = (Term_t) -1;

        Term_t cv1 = (Term_t) -1;
        Term_t cv2 = (Term_t) -1;
        if (pitr->has_next()) {
            pitr->next_pair();
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
                    if (pitr->has_next()) {
                        pitr->next_pair();
                        if (!valuesToCheck1->hasNext()) {
                            cv1 = (Term_t) -1;
                            break;
                        } else if (!valuesToCheck2->hasNext()) {
                            cv1 = (Term_t) -1;
                            break;
                        } else {
                            cv1 = valuesToCheck1->next();
                            cv2 = valuesToCheck2->next();
                        }
                    } else {
                        break;
                    }
                } else if (v1 < cv1 || (v1 == cv1 && v2 < cv2)) {
                    if (pitr->has_next())
                        pitr->next_pair();
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
                        cv1 = (Term_t) -1;
                        break;
                    } else if (!valuesToCheck2->hasNext()) {
                        cv1 = (Term_t) -1;
                        break;
                    } else {
                        cv1 = valuesToCheck1->next();
                        cv2 = valuesToCheck2->next();
                    }
                }
            }
        }


        if (cv1 != (Term_t) -1) {
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

std::shared_ptr<Column> EDBLayer::checkIn(
    std::vector<Term_t> &values,
    const Literal &l,
    uint8_t posInL,
    size_t &sizeOutput) {

//Do some checks
    if (l.getNVars() == 0 || l.getNVars() == 3) {
        BOOST_LOG_TRIVIAL(error) << "These cases are not supported.";
        throw 10;
    }

    //Prepare the iterators
    Tuple t = l.getTuple();
    std::vector<uint8_t> fieldToSort;
    fieldToSort.push_back(l.getPosVars()[posInL]);
    TupleKBItr itr1;
    itr1.init(q, &t, &fieldToSort, true);

    //Output
    std::unique_ptr<ColumnWriter> col(new ColumnWriter());
    PairItr *pitr = itr1.getPhysicalIterator();
    size_t idx1 = 0;
    const bool firstCol = l.getNVars() == 2;
    sizeOutput = 0;
    while (true) {
        const Term_t v1 = values[idx1];
        const Term_t v2 =  firstCol ? pitr->getValue1() : pitr->getValue2();
        if (v1 < v2) {
            idx1++;
            if (idx1 == values.size()) {
                break;
            }
        } else if (v1 > v2) {
            if (pitr->has_next()) {
                pitr->next_pair();
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
            if (pitr->has_next()) {
                pitr->next_pair();
            } else {
                break;
            }
        }
    }

    return col->getColumn();
}

// Local
void EDBLayer::getQueryFromEDBRelation0(QSQQuery *query,
                                        TupleTable *outputTable/*,
                                        Timeout *timeout*/) {
    //No join to perform. Simply execute the query using TupleKBIterator
    TupleKBItr itr;
    Tuple tuple = query->getLiteral()->getTuple();
    itr.init(q, &tuple, NULL);
    uint64_t row[3];
    uint8_t *pos = query->getPosToCopy();
    const uint8_t npos = query->getNPosToCopy();
    while (itr.hasNext()) {
        //RAISE_IF_EXPIRED(timeout);
        itr.next();
        for (uint8_t i = 0; i < npos; ++i) {
            row[i] = itr.getElementAt(pos[i]);
        }
        outputTable->addRow(row);
    }
}

// Local
void EDBLayer::getQueryFromEDBRelation3(QSQQuery *query,
                                        TupleTable *outputTable,
                                        std::vector<Term_t> *valuesToFilter/*,
                                        Timeout *timeout*/) {
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
        //RAISE_IF_EXPIRED(timeout);
        std::vector<std::pair<uint64_t, uint64_t>> *pairs = itr->second;
        std::sort(pairs->begin(), pairs->end());
        ArrayItr *firstItr = q->getArrayIterator();
        firstItr->init(pairs, -1, -1);
        firstItr->setKey(itr->first);
        p2.predicate(itr->first);

        std::shared_ptr<NestedJoinPlan> plan(new NestedJoinPlan(p2, q,
                                             posToCopy, joins, posVarsToReturn));
        NestedMergeJoinItr join(q, plan, firstItr, outputTable, LONG_MAX);
        if (join.hasNext()) {
            //Calling hasNext() of NWayJoin should populate the entire outputTable
            if (outputTable->getNRows() == 0) {
                throw 10; //This should not happen
            }
        } else {
            assert(outputTable->getNRows() == 0);
        }
        delete pairs;
    }
}

// Local
void EDBLayer::getQueryFromEDBRelation12(QSQQuery *query,
        TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter/*,
        Timeout *timeout*/) {
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
            if (sorted && *itr < prevEl) {
                sorted = false;
                pairs.push_back(std::make_pair(0, *itr));
            } else if (*itr > prevEl) {
                prevEl = *itr;
                pairs.push_back(std::make_pair(0, *itr));
            }
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
        //RAISE_IF_EXPIRED(timeout);
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

// Local
void EDBLayer::getQueryFromEDBRelation12(Term s, Term p, Term o,
        TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<std::pair<uint64_t, uint64_t>> *pairs,
        std::vector<int> &posVarsToReturn,
        std::vector<std::pair<int, int>> &joins,
        std::vector<std::vector<int>> &posToCopy/*,
        Timeout *timeout*/) {
    //Sort pairs
    //RAISE_IF_EXPIRED(timeout);
    std::sort(pairs->begin(), pairs->end());
    ArrayItr *firstItr = q->getArrayIterator();
    firstItr->init(pairs, -1, -1);

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
    //RAISE_IF_EXPIRED(timeout);
    std::shared_ptr<NestedJoinPlan> plan(new NestedJoinPlan(p2, q, posToCopy,
                                         joins, posVarsToReturn));

    //execute the plan and copy the results in the table
    NestedMergeJoinItr join(q, plan, firstItr, outputTable, LONG_MAX);
    if (join.hasNext()) {
        //Calling hasNext() of NWayJoin should populate the entire outputTable
        if (outputTable->getNRows() == 0) {
            throw 10; //This should not happen
        }
    } else {
        assert(outputTable->getNRows() == 0);
    }
}

/*
long EDBLayer::getSizeOutput(QSQQuery *query,
                             std::vector<uint8_t> *posToFilter,
                             std::vector<Term_t> *valuesToFilter) {
    PredId_t predid = query->getLiteral()->getPredicate().getId();
    if (predid == kbId) {
        if (query->getNRepeatedVars() > 0) {
            BOOST_LOG_TRIVIAL(error) << "Not (yet) supported";
            throw 10;
        }
        std::unique_ptr<TupleTable> outputTable(new TupleTable(3));
        if (posToFilter == NULL || posToFilter->size() == 0) {
            //not supported
            throw 10;
        } else if (posToFilter->size() < 3) {
            //Inefficient, but should work for now
            getQueryFromEDBRelation12(query, outputTable.get(), posToFilter,
                                      valuesToFilter);
        } else {
            getQueryFromEDBRelation3(query, outputTable.get(),
                                     valuesToFilter);
        }
        return outputTable->getNRows();
    } else {
        //not supported
        throw 10;
    }
}
*/

void EDBLayer::query(QSQQuery *query, TupleTable *outputTable,
                     std::vector<uint8_t> *posToFilter,
                     std::vector<Term_t> *valuesToFilter/*,
                     Timeout *timeout*/) {
    PredId_t predid = query->getLiteral()->getPredicate().getId();

    if (predid == kbId) {
        if (query->getNRepeatedVars() > 0 && posToFilter != 0 &&
                posToFilter->size() > 0) {
            BOOST_LOG_TRIVIAL(error) << "Not (yet) supported";
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
                        row[0] = itr->first;
                        row[1] = itr->second;
                        outputTable->addRow(row);
                    }
                }
            } else if (posToFilter->size() == 1) {
                std::vector<std::pair<Term_t, Term_t>> *pairs;
                bool inverted = posToFilter->at(0) != 0;
                if (!inverted) {
                    pairs = rel->getTwoColumn1();
                    std::vector<std::pair<Term_t, Term_t>>::iterator itr1 = pairs->begin();
                    std::vector<Term_t>::iterator itr2 = valuesToFilter->begin();
                    while (itr1 != pairs->end() && itr2 != valuesToFilter->end()) {
                        while (itr1 != pairs->end() && itr1->first < *itr2) {
                            itr1++;
                        }
                        if (itr1 == pairs->end())
                            continue;

                        while (itr2 != valuesToFilter->end() && itr1->first > *itr2) {
                            itr2++;
                        }
                        if (itr1 != pairs->end() && itr2 != valuesToFilter->end()) {
                            bool valid = true;
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
                                row[0] = itr1->first;
                                row[1] = itr1->second;
                                outputTable->addRow(row);
                            }
                            itr1++;
                        }
                    }
                } else {
                    pairs = rel->getTwoColumn2();
                    std::vector<std::pair<Term_t, Term_t>>::iterator itr1 = pairs->begin();
                    std::vector<Term_t>::iterator itr2 = valuesToFilter->begin();
                    while (itr1 != pairs->end() && itr2 != valuesToFilter->end()) {
                        while (itr1 != pairs->end() && itr1->second < *itr2) {
                            itr1++;
                        }
                        if (itr1 == pairs->end())
                            continue;

                        while (itr2 != valuesToFilter->end() && itr1->second > *itr2) {
                            itr2++;
                        }
                        if (itr1 != pairs->end() && itr2 != valuesToFilter->end()) {
                            bool valid = true;
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
                                row[0] = itr1->first;
                                row[1] = itr1->second;
                                outputTable->addRow(row);
                            }
                            itr1++;
                        }
                    }
                }
            } else {
                //posToFilter==2
                std::vector<std::pair<Term_t, Term_t>> *pairs;
                bool inverted = posToFilter->at(0) != 0;
                if (!inverted) {
                    pairs = rel->getTwoColumn1();
                } else {
                    pairs = rel->getTwoColumn2();
                }

                for (std::vector<Term_t>::iterator itr = valuesToFilter->begin();
                        itr != valuesToFilter->end(); ) {
                    //Binary search
                    Term_t first = *itr;
                    itr++;
                    Term_t second = *itr;
                    itr++;
                    if (std::binary_search(pairs->begin(), pairs->end(), std::make_pair(first, second))) {
                        bool valid = true;
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
                            row[0] = first;
                            row[1] = second;
                            outputTable->addRow(row);
                        }
                    }
                }
            }
            break;
        }
        default:
            BOOST_LOG_TRIVIAL(error) << "This should not happen";
            throw 10;
        }
    }
}

EDBIterator *EDBLayer::getIterator(const QSQQuery * query) {
    const Literal *literal = query->getLiteral();
    PredId_t predid = literal->getPredicate().getId();


    if (predid == kbId) {
#if DEBUG
        BOOST_LOG_TRIVIAL(debug) << "Get query " << literal->tostring(NULL, NULL);
#endif
        EDBKBIterator *itr = kbItrFactory.get();
        itr->init(q, *literal);
        return itr;
    } else {
        bool equalFields = false;
        if (query->getNRepeatedVars() > 0) {
            equalFields = true;
        }
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
            itr->init1(rel->getSingleColumn(), c1, vc1);
            return itr;
        case 2:
            itr = memItrFactory.get();
            itr->init2(true, rel->getTwoColumn1(), c1, vc1, c2, vc2, equalFields);
            return itr;
        }
    }
    throw 10;
}

EDBIterator *EDBLayer::getSortedIterator(const QSQQuery * query, const std::vector<uint8_t> &fields) {
    const Literal *literal = query->getLiteral();
    PredId_t predid = literal->getPredicate().getId();


    if (predid == kbId) {
#if DEBUG
        BOOST_LOG_TRIVIAL(debug) << "Get query " << literal->tostring(NULL, NULL);
#endif
        EDBKBIterator *itr = kbItrFactory.get();
        itr->init(q, *literal, fields);
        return itr;
    } else {
        assert(literal->getTupleSize() <= 2);
        bool equalFields = false;
        if (query->getNRepeatedVars() > 0) {
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
        EDBMemIterator *itr;
        switch (size) {
        case 1:
            itr = memItrFactory.get();
            itr->init1(rel->getSingleColumn(), c1, vc1);
            return itr;
        case 2:
            itr = memItrFactory.get();
            if (c1) {
                itr->init2(true, rel->getTwoColumn1(), c1, vc1, c2, vc2, equalFields);
            } else {
                if (c2) {
                    itr->init2(false, rel->getTwoColumn2(), c1, vc1, c2, vc2, equalFields);
                } else {
                    //No constraints
                    if (fields.size() != 0 && fields[0] == 0) {
                        itr->init2(true, rel->getTwoColumn1(), c1, vc1, c2, vc2, equalFields);
                    } else {
                        itr->init2(false, rel->getTwoColumn2(), c1, vc1, c2, vc2, equalFields);
                    }
                }
            }
            return itr;
        }
    }
    throw 10;
}

std::vector<std::shared_ptr<Column>> EDBLayer::checkNewIn(const Literal &l1,
                                  std::vector<uint8_t> &posInL1,
                                  const Literal &l2,
std::vector<uint8_t> posInL2) {
    if (l1.getPredicate().getId() != l2.getPredicate().getId() ||
            l1.getPredicate().getId() != kbId) {
        BOOST_LOG_TRIVIAL(error) << "Not supported";
        throw 10;
    }
    return performAntiJoin(l1, posInL1, l2, posInL2);
}

std::vector<std::shared_ptr<Column>> EDBLayer::checkNewIn(
                                      std::vector <
                                      std::shared_ptr<Column >> &valuesToCheck,
                                      const Literal &l,
std::vector<uint8_t> posInL) {
    if (l.getPredicate().getId() != kbId) {
        BOOST_LOG_TRIVIAL(error) << "Not supported";
        throw 10;
    }
    return performAntiJoin(valuesToCheck, l, posInL);
}

size_t EDBLayer::getCardinalityColumn(const QSQQuery *query,
                                        uint8_t posColumn) {

    // if (query->getNRepeatedVars() > 0) {
    // BOOST_LOG_TRIVIAL(error) << "This should not happen";
    // throw 10;
    // Don't know what to do here.
    // Ignore?
    // }

    const Literal *literal = query->getLiteral();
    PredId_t predid = literal->getPredicate().getId();
    if (predid == kbId) {
        long s, p, o;
        Term t = literal->getTermAtPos(0);
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
        size_t result = q->getCard(s, p, o, posColumn);
        if (query->getNRepeatedVars() > 0) {
            result = result / 10;   // ???
        }
        return result;
    } else {
        throw 10;
    }

}

size_t EDBLayer::getCardinality(const QSQQuery * query) {
    // if (query->getNRepeatedVars() > 0) {
    //     BOOST_LOG_TRIVIAL(error) << "This should not happen";
    //     throw 10;
    // }

    const Literal *literal = query->getLiteral();
    PredId_t predid = literal->getPredicate().getId();
    if (predid == kbId) {
        long s, p, o;
        Term t = literal->getTermAtPos(0);
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
        size_t result = q->getCard(s, p, o);
        if (query->getNRepeatedVars() > 0) {
            result = result / 10;   // ???
        }
        return result;
    } else {
        assert(literal->getNVars() == literal->getTupleSize());
        IndexedTupleTable *rel = tmpRelations[predid];
        return rel->getNTuples();
    }
}

//same as above
size_t EDBLayer::estimateCardinality(const QSQQuery * query) {
    // if (query->getNRepeatedVars() > 0) {
    //     BOOST_LOG_TRIVIAL(error) << "This should not happen";
    //     throw 10;
    // }

    const Literal *literal = query->getLiteral();
    PredId_t predid = literal->getPredicate().getId();
    if (predid == kbId) {
        long s, p, o;
        Term t = literal->getTermAtPos(0);
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
        size_t result = q->getCard(s, p, o);
        if (query->getNRepeatedVars() > 0) {
            result = result / 10;   // ???
        }
        return result;
    } else {
        if (literal->getNVars() == literal->getTupleSize()) {
            BOOST_LOG_TRIVIAL(debug) << "Estimate is not very precise";
        }
        IndexedTupleTable *rel = tmpRelations[predid];
        return rel->getNTuples();
    }
}

bool EDBLayer::isEmpty(const QSQQuery * query, std::vector<uint8_t> *posToFilter,
                       std::vector<Term_t> *valuesToFilter) {
    const Literal *literal = query->getLiteral();
    PredId_t predid = literal->getPredicate().getId();
    if (predid == kbId) {
        long s, p, o;
        Term t = literal->getTermAtPos(0);
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

        return q->isEmpty(s, p, o);
    } else {
        IndexedTupleTable *rel = tmpRelations[predid];
        assert(literal->getTupleSize() <= 2);

        std::unique_ptr<Literal> rewrittenLiteral;
        if (posToFilter != NULL) {
            //Create a new literal where the var are replaced by the constants
            Tuple t = literal->getTuple();
            for (int i = 0; i < posToFilter->size(); ++i) {
                uint8_t pos = posToFilter->at(i);
                Term_t value = valuesToFilter->at(i);
                t.set(Term(0, value), pos);
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
                BOOST_LOG_TRIVIAL(error) << "Not supported";
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
    if (itr->getType() == EDB_KB) {
        ((EDBKBIterator*)itr)->clear();
        kbItrFactory.release((EDBKBIterator*)itr);
    } else {
        memItrFactory.release((EDBMemIterator*)itr);
    }
}


void EDBKBIterator::init(Querier * q, const Literal & literal) {
    Tuple tuple = literal.getTuple();
    kbItr.init(q, &tuple, NULL);
}

void EDBKBIterator::init(Querier * q, const Literal & literal, const std::vector<uint8_t> &fields) {
    this->nfields = literal.getNVars();
    Tuple tuple = literal.getTuple();
    if (fields.size() > 0) {
        std::vector<uint8_t> sortedFields;
        for (uint8_t i = 0; i < fields.size(); ++i) {
            uint8_t pos = fields[i];
            uint8_t nvars = 0;
            for (uint8_t j = 0; j < literal.getTupleSize(); ++j) {
                if (literal.getTermAtPos(j).isVariable()) {
                    if (pos == nvars)
                        sortedFields.push_back(j);
                    nvars++;
                }
            }
        }
        kbItr.init(q, &tuple, &sortedFields);
    } else {
        kbItr.init(q, &tuple, NULL);
    }
}

void EDBKBIterator::skipDuplicatedFirstColumn() {
    kbItr.ignoreSecondColumn();
}

bool EDBKBIterator::hasNext() {
    return kbItr.hasNext();
}

void EDBKBIterator::next() {
    kbItr.next();
}

void EDBKBIterator::clear() {
    kbItr.clear();
}

Term_t EDBKBIterator::getElementAt(const uint8_t p) {
    return kbItr.getElementAt(p);
}

void EDBMemIterator::init1(std::vector<Term_t>* v, const bool c1, const Term_t vc1) {
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

void EDBMemIterator::init2(const bool defaultSorting, std::vector<std::pair<Term_t, Term_t>>* v, const bool c1,
                           const Term_t vc1, const bool c2, const Term_t vc2,
                           const bool equalFields) {
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
            lowerOk = twoColumns->first == vc1 && twoColumns->second == vc2;
        } else {
            std::pair<Term_t, Term_t> pair = std::make_pair(vc1, 0);
            twoColumns = std::lower_bound(v->begin(), v->end(), pair);
            lowerOk = twoColumns->first == vc1;
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
            bool lowerOk = twoColumns->second == vc2;
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
            do {
                Term_t prevel = twoColumns->first;
                twoColumns++;
                if (twoColumns != endTwoColumns) {
                    if (twoColumns->first != prevel)
                        return true;
                } else {
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
