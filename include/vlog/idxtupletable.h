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

#ifndef IDXTUPLETABLE_H
#define IDXTUPLETABLE_H

#include <vlog/qsqquery.h>
#include <trident/model/table.h>

#include <vector>
#include <algorithm>
#include <sparsehash/dense_hash_set>

typedef google::dense_hash_set<Term_t, std::hash<Term_t>, std::equal_to<Term_t>> GoogleSet;

class IndexedTupleTable {

private:
    const uint8_t sizeTuple;
    std::vector<Term_t> *singleColumn;

    std::vector<std::pair<Term_t, Term_t>> *twoColumn1;
    std::unique_ptr<GoogleSet> setColumn1;
    std::vector<std::pair<Term_t, Term_t>> *twoColumn2;
    std::unique_ptr<GoogleSet> setColumn2;

    TupleTable *spo;
    TupleTable *pos;
    TupleTable *osp;

    std::unique_ptr<GoogleSet> fillSet(std::vector<Term_t> &v) {
        std::unique_ptr<GoogleSet> ptr1(new GoogleSet());
        ptr1->set_empty_key((Term_t) -1);
        for (std::vector<Term_t>::iterator itr = v.begin(); itr != v.end(); ++itr) {
            ptr1->insert(*itr);
        }
        return ptr1;
    }

    std::unique_ptr<GoogleSet> fillSet(std::vector<std::pair<Term_t, Term_t>> &v, const uint8_t pos) {
        std::unique_ptr<GoogleSet> ptr1(new GoogleSet());
        ptr1->set_empty_key((Term_t) -1);
        for (std::vector<std::pair<Term_t, Term_t>>::iterator itr = v.begin(); itr != v.end(); ++itr) {
            if (pos == 0) {
                ptr1->insert(itr->first);
            } else {
                ptr1->insert(itr->second);
            }
        }
        return ptr1;
    }

public:
    IndexedTupleTable(TupleTable *table);

    ~IndexedTupleTable();

    uint8_t getSizeTuple() const {
        return sizeTuple;
    }

    std::vector<Term_t> *getSingleColumn() {
        return singleColumn;
    }

    size_t getNTuples() {
        if (sizeTuple == 1) {
            return singleColumn->size();
        } else if (sizeTuple == 2) {
            return twoColumn1->size();
        } else {
            return spo->getNRows();
        }
    }

    bool exists(const Term_t value) {
        if (setColumn1 == NULL) {
            setColumn1 = fillSet(*singleColumn);
        }
        return setColumn1->find(value) != setColumn1->end();
    }

    bool exists(const uint8_t colid, const Term_t value) {
        if (sizeTuple == 1) {
            return exists(value);
        } else if (sizeTuple == 2) {
            if (colid == 0) {
                if (setColumn1 == NULL) {
                    setColumn1 = fillSet(*twoColumn1, 0);
                }
                return setColumn1->find(value) != setColumn1->end();

                /*for (std::vector<std::pair<Term_t, Term_t>>::iterator itr = twoColumn1->begin();
                        itr != twoColumn1->end(); ++itr) {
                    if (itr->first > value) {
                        return false;
                    } else if (itr->first == value) {
                        return true;
                    }
                }*/
            } else {
                if (setColumn2 == NULL) {
                    setColumn2 = fillSet(*twoColumn2, 1);
                }
                return setColumn2->find(value) != setColumn2->end();

                /*for (std::vector<std::pair<Term_t, Term_t>>::iterator itr = twoColumn2->begin();
                        itr != twoColumn2->end(); ++itr) {
                    if (itr->second > value) {
                        return false;
                    } else if (itr->second == value) {
                        return true;
                    }
                }*/
            }
            return false;
        } else {
            //not supported
            throw 10;
        }
    }

    std::vector<std::pair<Term_t, Term_t>> *getTwoColumn1() {
        return twoColumn1;
    }

    std::vector<std::pair<Term_t, Term_t>> *getTwoColumn2() {
        return twoColumn2;
    }
};

#endif
