#include <trident/tests/common.h>
#include <trident/kb/kb.h>
#include <trident/kb/querier.h>
#include <tridentcompr/utils/lz4io.h>

bool _less_sop(const _Triple &p1, const _Triple &p2) {
    if (p1.s < p2.s) {
        return true;
    } else if (p1.s == p2.s) {
        if (p1.o < p2.o) {
            return true;
        } else if (p1.o == p2.o) {
            return p1.p < p2.p;
        }
    }
    return false;
}

bool _less_pos(const _Triple &p1, const _Triple &p2) {
    if (p1.p < p2.p) {
        return true;
    } else if (p1.p == p2.p) {
        if (p1.o < p2.o) {
            return true;
        } else if (p1.o == p2.o) {
            return p1.s < p2.s;
        }
    }
    return false;
}

bool _less_pso(const _Triple &p1, const _Triple &p2) {
    if (p1.p < p2.p) {
        return true;
    } else if (p1.p == p2.p) {
        if (p1.s < p2.s) {
            return true;
        } else if (p1.s == p2.s) {
            return p1.o < p2.o;
        }
    }
    return false;
}

bool _less_osp(const _Triple &p1, const _Triple &p2) {
    if (p1.o < p2.o) {
        return true;
    } else if (p1.o == p2.o) {
        if (p1.s < p2.s) {
            return true;
        } else if (p1.s == p2.s) {
            return p1.p < p2.p;
        }
    }
    return false;
}

bool _less_ops(const _Triple &p1, const _Triple &p2) {
    if (p1.o < p2.o) {
        return true;
    } else if (p1.o == p2.o) {
        if (p1.p < p2.p) {
            return true;
        } else if (p1.p == p2.p) {
            return p1.s < p2.s;
        }
    }
    return false;
}

void _copyCurrentFirst(int perm, long triple[3], long v) {
    switch (perm) {
    case IDX_SPO:
    case IDX_SOP:
        triple[0] = v;
        break;
    case IDX_POS:
    case IDX_PSO:
        triple[1] = v;
        break;
    case IDX_OSP:
    case IDX_OPS:
        triple[2] = v;
        break;
    }
}

void _copyCurrentFirstSecond(int perm, long triple[3], long v1, long v2) {
    switch (perm) {
    case IDX_SPO:
        triple[0] = v1;
        triple[1] = v2;
        break;
    case IDX_SOP:
        triple[0] = v1;
        triple[2] = v2;
        break;
    case IDX_POS:
        triple[1] = v1;
        triple[2] = v2;
        break;
    case IDX_PSO:
        triple[1] = v1;
        triple[0] = v2;
        break;
    case IDX_OSP:
        triple[2] = v1;
        triple[0] = v2;
        break;
    case IDX_OPS:
        triple[2] = v1;
        triple[1] = v2;
        break;
    }
}

void _reorderTriple(int perm, PairItr *itr, long triple[3]) {
    switch (perm) {
    case IDX_SPO:
        triple[0] = itr->getKey();
        triple[1] = itr->getValue1();
        triple[2] = itr->getValue2();
        break;
    case IDX_SOP:
        triple[0] = itr->getKey();
        triple[2] = itr->getValue1();
        triple[1] = itr->getValue2();
        break;
    case IDX_POS:
        triple[1] = itr->getKey();
        triple[2] = itr->getValue1();
        triple[0] = itr->getValue2();
        break;
    case IDX_PSO:
        triple[1] = itr->getKey();
        triple[0] = itr->getValue1();
        triple[2] = itr->getValue2();
        break;
    case IDX_OSP:
        triple[2] = itr->getKey();
        triple[0] = itr->getValue1();
        triple[1] = itr->getValue2();
        break;
    case IDX_OPS:
        triple[2] = itr->getKey();
        triple[1] = itr->getValue1();
        triple[0] = itr->getValue2();
        break;
    }
}

void _testKB(string inputfile, KB * kb) {
    // Load the entire KB in main memory
    std::vector<_Triple> triples;
    LZ4Reader reader(inputfile);
    while (!reader.isEof()) {
        triples.push_back(_Triple(reader.parseVLong(), reader.parseVLong(),
                                  reader.parseVLong()));
    }

    Querier *q = kb->query();
    //Sort the triples and launch the queries
    for (int perm = 0; perm < 6; ++perm) {
        BOOST_LOG_TRIVIAL(info) << "Testing permutation " << perm;
        if (perm != IDX_SPO) {
            //Sort the vector
            if (perm == IDX_SOP) {
                std::sort(triples.begin(), triples.end(), _less_sop);
            } else if (perm == IDX_POS) {
                std::sort(triples.begin(), triples.end(), _less_pos);
            } else if (perm == IDX_PSO) {
                std::sort(triples.begin(), triples.end(), _less_pso);
            } else if (perm == IDX_OSP) {
                std::sort(triples.begin(), triples.end(), _less_osp);
            } else if (perm == IDX_OPS) {
                std::sort(triples.begin(), triples.end(), _less_ops);
            } else {
                throw 10;
            }
        }

        //Test the scan queries
        BOOST_LOG_TRIVIAL(info) << "Check a complete scan...";
        uint64_t card = q->getCardOnIndex(perm, -1, -1, -1);
        if (card != triples.size()) {
            BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card << " " << triples.size();
            throw 10;
        }
        PairItr *currentItr = q->get(perm, -1, -1, -1);
        PairItr *scanWithoutLast = q->get(perm, -1, -1, -1);
        scanWithoutLast->ignoreSecondColumn();

        long countTriple = 0;
        long prevFirstEl = -1;
        long prevKey = -1;
        for (auto const el : triples) {
            //cout << el.s << " " << el.p << " " << el.o << endl;

            if (!currentItr->hasNext()) {
                throw 10; //should not happen
            }
            currentItr->next();

            long t[3];
            _reorderTriple(perm, currentItr, t);
            if (el.s != t[0] || el.p != t[1] || el.o != t[2]) {
                cout << "Mismatch! Comparing " << el.s << " " << el.p << " " << el.o << " with " <<
                     t[0] << " " << t[1] << " " << t[2] << " " << countTriple << endl;
                throw 10;
            }

            if (currentItr->getKey() != prevKey) {
                prevFirstEl = -1;
                prevKey = currentItr->getKey();
            }
            if (currentItr->getValue1() != prevFirstEl) {
                if (!scanWithoutLast->hasNext()) {
                    throw 10;
                }
                scanWithoutLast->next();
                if (currentItr->getKey() != scanWithoutLast->getKey() ||
                        currentItr->getValue1() != scanWithoutLast->getValue1()) {
                    throw 10;
                }
                prevFirstEl = currentItr->getValue1();
            }

            countTriple++;
        }
        if (currentItr->hasNext()) {
            throw 10; //The iterator should be finished
        }
        if (scanWithoutLast->hasNext()) {
            throw 10;
        }
        q->releaseItr(currentItr);
        q->releaseItr(scanWithoutLast);
        currentItr = NULL;
        BOOST_LOG_TRIVIAL(info) << "All OK";

        //Test a scan without the second and third columns
        currentItr = q->getTermList(perm);
        BOOST_LOG_TRIVIAL(info) << "Check a filtered scan...";
        long prevEl = -1;
        for (auto const el : triples) {
            //cout << el.s << " " << el.p << " " << el.o << endl;
            long first = 0;
            switch (perm) {
            case IDX_SPO:
            case IDX_SOP:
                first = el.s;
                break;
            case IDX_POS:
            case IDX_PSO:
                first = el.p;
                break;
            case IDX_OPS:
            case IDX_OSP:
                first = el.o;
                break;
            default:
                throw 10;
            }

            if (first != prevEl) {
                if (!currentItr->hasNext()) {
                    throw 10; //should not happen
                }
                currentItr->next();
                if (first != currentItr->getKey()) {
                    cout << "Mismatch! Comparing " << first << " " <<
                         currentItr->getValue1() << endl;
                    throw 10;
                }
                prevEl = first;
            }
        }
        if (currentItr->hasNext()) {
            throw 10; //The iterator should be finished
        }
        q->releaseItr(currentItr);
        currentItr = NULL;
        BOOST_LOG_TRIVIAL(info) << "All OK";

        //Filter on the first element
        long currentFirst = -1;
        long currentSecond = -1;
        long count = 0;
        long countFirst = 0;
        long countSecond = 0;
        long pattern[3];
        pattern[0] = -1;
        pattern[1] = -1;
        pattern[2] = -1;
        PairItr *currentItrFirst = NULL;
        BOOST_LOG_TRIVIAL(info) << "Check cardinalities on the first level ...";
        for (auto const el : triples) {
            long first, second;
            switch (perm) {
            case IDX_SPO:
                first = el.s;
                second = el.p;
                break;
            case IDX_SOP:
                first = el.s;
                second = el.o;
                break;
            case IDX_POS:
                first = el.p;
                second = el.o;
                break;
            case IDX_PSO:
                first = el.p;
                second = el.s;
                break;
            case IDX_OSP:
                first = el.o;
                second = el.s;
                break;
            case IDX_OPS:
                first = el.o;
                second = el.p;
                break;
            }

            if (first != currentFirst) {
                if (currentItr != NULL) {
                    if (currentItr->hasNext()) {
                        throw 10; //The iterator should be finished
                    }
                    q->releaseItr(currentItr);
                    currentItr = NULL;
                }
                if (currentItrFirst != NULL) {
                    q->releaseItr(currentItrFirst);
                    currentItrFirst = NULL;
                }

                if (currentFirst != -1) {
                    _copyCurrentFirst(perm, pattern, currentFirst);
                    uint64_t card = q->getCardOnIndex(perm,
                                                      pattern[0],
                                                      pattern[1],
                                                      pattern[2]);
                    if (card != count) {
                        BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card << " " << count;
                        throw 10;
                    }

                    uint64_t card2 = q->getCardOnIndex(perm, pattern[0],
                                                       pattern[1],
                                                       pattern[2],
                                                       true);
                    if (card2 != countFirst) {
                        BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card2 << " " << countFirst;
                        throw 10;
                    }
                }
                currentFirst = first;
                currentSecond = second;
                count = 0;
                countFirst = 1;
                countSecond = 0;

                //To check whether all triples are the same
                _copyCurrentFirst(perm, pattern, currentFirst);

                currentItr = q->get(perm, pattern[0], pattern[1], pattern[2]);
                currentItrFirst = q->get(perm, pattern[0], pattern[1],
                                         pattern[2]);
                currentItrFirst->ignoreSecondColumn();
            }

            if (second != currentSecond) {
                //Check the value with the one of currentItrFirst
                if (!currentItrFirst->hasNext()) {
                    throw 10; //should not happen
                }
                currentItrFirst->next();
                long c = currentItrFirst->getCount();
                if (currentItrFirst->getKey() != currentFirst ||
                        currentItrFirst->getValue1()
                        != currentSecond || c != countSecond) {
                    cout << "Mismatch: " << currentItrFirst->getKey() <<
                         " " << currentFirst <<
                         " " << currentItrFirst->getValue1() <<
                         " " << currentSecond <<
                         " " << c <<
                         " " << countSecond << endl;
                    throw 10;
                }
                countFirst++;
                countSecond = 0;
                currentSecond = second;
            }
            count++;
            countSecond++;

            //Check whether this triple corresponds to the currentItr
            if (!currentItr->hasNext()) {
                throw 10; //should not happen
            }
            currentItr->next();

            long t[3];
            _reorderTriple(perm, currentItr, t);
            if (el.s != t[0] || el.p != t[1] || el.o != t[2]) {
                cout << "Mismatch! Comparing " << el.s << " " << el.p << " " << el.o << " with " <<
                     t[0] << " " << t[1] << " " << t[2] << endl;
                throw 10;
            }
        }
        if (currentItr->hasNext()) {
            throw 10; //The iterator should be finished
        }
        if (currentItr != NULL)
            q->releaseItr(currentItr);
        if (currentItrFirst != NULL)
            q->releaseItr(currentItrFirst);

        if (currentFirst != -1) {
            _copyCurrentFirst(perm, pattern, currentFirst);
            uint64_t card = q->getCardOnIndex(perm, pattern[0], pattern[1], pattern[2]);
            if (card != count) {
                BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card << " " << count;
                throw 10;
            }

            uint64_t card2 = q->getCardOnIndex(perm, pattern[0],
                                               pattern[1],
                                               pattern[2],
                                               true);
            if (card2 != countFirst) {
                BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card2 << " " << countFirst;
                throw 10;
            }

        }
        BOOST_LOG_TRIVIAL(info) << "All OK";

        //Filter on the second element
        currentItr = NULL;
        currentFirst = -1;
        currentSecond = -1;
        count = 0;
        pattern[0] = -1;
        pattern[1] = -1;
        pattern[2] = -1;
        BOOST_LOG_TRIVIAL(info) << "Check cardinalities on the second level ...";
        for (auto const el : triples) {
            long first, second;
            switch (perm) {
            case IDX_SPO:
                first = el.s;
                second = el.p;
                break;
            case IDX_SOP:
                first = el.s;
                second = el.o;
                break;
            case IDX_POS:
                first = el.p;
                second = el.o;
                break;
            case IDX_PSO:
                first = el.p;
                second = el.s;
                break;
            case IDX_OSP:
                first = el.o;
                second = el.s;
                break;
            case IDX_OPS:
                first = el.o;
                second = el.p;
                break;
            }
            if (first != currentFirst || second != currentSecond) {
                if (currentFirst != -1) {
                    _copyCurrentFirstSecond(perm, pattern, currentFirst,
                                            currentSecond);
                    uint64_t card = q->getCardOnIndex(perm, pattern[0],
                                                      pattern[1], pattern[2]);
                    if (card != count) {
                        BOOST_LOG_TRIVIAL(error) << "Cardinalities " <<
                                                 "do not match: " <<
                                                 card << " " << count;
                        throw 10;
                    }
                }
                currentFirst = first;
                currentSecond = second;
                count = 0;

                //new iterator
                if (currentItr != NULL) {
                    if (currentItr->hasNext()) {
                        throw 10; //The iterator should be finished
                    }
                    q->releaseItr(currentItr);
                }
                _copyCurrentFirstSecond(perm, pattern, currentFirst, currentSecond);
                currentItr = q->get(perm, pattern[0], pattern[1], pattern[2]);
            }
            count++;

            //Check whether this triple corresponds to the currentItr
            if (!currentItr->hasNext()) {
                throw 10; //should not happen
            }
            currentItr->next();
            long t[3];
            _reorderTriple(perm, currentItr, t);
            if (el.s != t[0] || el.p != t[1] || el.o != t[2]) {
                cout << "Mismatch! Comparing " << el.s << " " << el.p << " " << el.o << " with " <<
                     t[0] << " " << t[1] << " " << t[2] << endl;
                throw 10;
            }
        }
        if (currentFirst != -1) {
            _copyCurrentFirstSecond(perm, pattern, currentFirst, currentSecond);
            uint64_t card = q->getCardOnIndex(perm, pattern[0], pattern[1], pattern[2]);
            if (card != count) {
                BOOST_LOG_TRIVIAL(error) << "Cardinalities do not match: " << card << " " << count;
                throw 10;
            }
        }
        BOOST_LOG_TRIVIAL(info) << "All OK";
    }
    delete q;
}
