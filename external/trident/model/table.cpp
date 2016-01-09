/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Trident.

   Trident is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Trident is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Trident.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <trident/model/table.h>

#include <algorithm>
#include <string>
#include <cstring>
#include <cmath>

bool TupleTableItr::hasNext() {
    return counter + 1 < table->getNRows();
}

void TupleTableItr::next() {
    counter++;
}

size_t TupleTableItr::getTupleSize() {
    return table->getSizeRow();
}

uint64_t TupleTableItr::getElementAt(const int pos) {
    return table->getPosAtRow(counter, pos);
}

TupleTable *TupleTable::sortBy(std::vector<uint8_t> fields) {
    TupleTable *out = new TupleTable(sizeRow, signature);

    std::vector<uint64_t *> toBeSorted;
    for (size_t i = 0; i < getNRows(); ++i) {
        toBeSorted.push_back((uint64_t*)getRow(i));
    }

    //Sort the vector
    Sorter sorter(fields);
    std::sort(toBeSorted.begin(), toBeSorted.end(), std::ref(sorter));

    for (std::vector<uint64_t*>::iterator itr = toBeSorted.begin(); itr != toBeSorted.end();
            ++itr) {
        out->addRow(*itr);
    }

    return out;
}

TupleTable *TupleTable::sortByAll() {
    TupleTable *out = new TupleTable(sizeRow, signature);

    std::vector<uint64_t *> toBeSorted;
    for (size_t i = 0; i < getNRows(); ++i) {
        toBeSorted.push_back((uint64_t*)getRow(i));
    }

    //Sort the vector
    Sorter sorter((uint8_t) sizeRow);
    std::sort(toBeSorted.begin(), toBeSorted.end(), std::ref(sorter));

    for (std::vector<uint64_t*>::iterator itr = toBeSorted.begin(); itr != toBeSorted.end();
            ++itr) {
        out->addRow(*itr);
    }

    return out;
}

TupleTable *TupleTable::retain(TupleTable *t) {
    //Assume that both 'this' and 't' are sorted
    std::vector<uint64_t>::iterator itr1 = values.begin();
    std::vector<uint64_t>::iterator itr2 = t->values.begin();

    TupleTable *outputTable = new TupleTable(sizeRow);
    std::vector<uint64_t> *retainedVector = &(outputTable->values);
    uint64_t *row1 = new uint64_t[sizeRow];
    uint64_t *prevrow1 = new uint64_t[sizeRow];
    bool prevrow1valid = false;
    bool toRead1 = true;

    uint64_t *row2 = new uint64_t[sizeRow];
    bool toRead2 = true;

    while (itr1 != values.end() && itr2 != t->values.end()) {
        if (toRead1) {
            readArray(row1, itr1);
            if (prevrow1valid && cmp(prevrow1, row1, sizeRow) == 0)
                continue;
            memcpy(prevrow1, row1, sizeof(uint64_t) * sizeRow);
            prevrow1valid = true;
            toRead1 = false;
        }
        if (toRead2) {
            readArray(row2, itr2);
            toRead2 = false;
        }

        int res = cmp(row1, row2, sizeRow);
        if (res == 0) {
            toRead1 = true;
            toRead2 = true;
        } else if (res < 0) {
            copyArray(*retainedVector, row1);
            toRead1 = true;
        } else {
            toRead2 = true;
        }
    }

    //Copy the remaining
    if (!toRead1)
        copyArray(*retainedVector, row1);
    while (itr1 != values.end()) {
        readArray(row1, itr1);
        copyArray(*retainedVector, row1);
    }

    delete[] row1;
    delete[] prevrow1;
    delete[] row2;

    return outputTable;
}

TupleTable *TupleTable::merge(TupleTable *t) {
    //Assume that both 'this' and 't' are sorted
    std::vector<uint64_t>::iterator itr1 = values.begin();
    std::vector<uint64_t>::iterator itr2 = t->values.begin();

    TupleTable *outputTable = new TupleTable(sizeRow);
    std::vector<uint64_t> *retainedVector = &(outputTable->values);
    uint64_t *row1 = new uint64_t[sizeRow];
    bool toRead1 = true;
    uint64_t *row2 = new uint64_t[sizeRow];
    bool toRead2 = true;

    while ((! toRead1 || itr1 != values.end()) && (! toRead2 || itr2 != t->values.end())) {
        if (toRead1) {
            readArray(row1, itr1);
            toRead1 = false;
        }
        if (toRead2) {
            readArray(row2, itr2);
            toRead2 = false;
        }
        int compare = cmp(row1, row2, sizeRow);
        if (compare < 0) {
            copyArray(*retainedVector, row1);
            toRead1 = true;
        } else if (compare > 0) {
            copyArray(*retainedVector, row2);
            toRead2 = true;
        } else {
            copyArray(*retainedVector, row1);
            toRead2 = true;
            toRead1 = true;
        }
    }

    //Copy the remaining
    if (!toRead1)
        copyArray(*retainedVector, row1);
    while (itr1 != values.end()) {
        readArray(row1, itr1);
        copyArray(*retainedVector, row1);
    }
    if (!toRead2)
        copyArray(*retainedVector, row2);
    while (itr2 != t->values.end()) {
        readArray(row2, itr2);
        copyArray(*retainedVector, row2);
    }

    delete[] row1;
    delete[] row2;

    return outputTable;
}

void TupleTable::readArray(uint64_t *dest, std::vector<uint64_t>::iterator &itr) {
    for (size_t i = 0; i < sizeRow; ++i) {
        dest[i] = *itr;
        itr++;
    }
}

std::vector<std::pair<uint8_t, uint8_t>> TupleTable::getPosJoins(TupleTable *o) {
    //Calculate the number of fields on which we should perfom the join
    std::vector<std::pair<uint8_t, uint8_t>> joinFields;
    for (uint8_t i = 0; i < signature.size(); ++i) {
        for (uint8_t j = 0; j < o->signature.size(); ++j) {
            if (signature[i] == o->signature[j])
                joinFields.push_back(std::make_pair(i, j));
        }
    }
    return joinFields;
}

TupleTable::JoinHitStats TupleTable::joinHitRates(TupleTable *o) {
    if (signature.size() == 0 || o->signature.size() == 0) {
        BOOST_LOG_TRIVIAL(debug) << "This should not happen";
        throw 10;
    }

    //Calculate the number of fields on which we should perfom the join
    std::vector<std::pair<uint8_t, uint8_t>> joinFields;
    for (uint8_t i = 0; i < signature.size(); ++i) {
        for (uint8_t j = 0; j < o->signature.size(); ++j) {
            if (signature[i] == o->signature[j])
                joinFields.push_back(std::make_pair(i, j));
        }
    }

    if (joinFields.size() == 0) {
        JoinHitStats stats;
        //stats.selectivity1 = stats.selectivity2 = 0;
        stats.ratio = 0;
        return stats;
    } else { //Do a merge join

        //Sort the two tables according to the join keys
        std::vector<uint8_t> psort1;
        std::vector<uint8_t> psort2;
        for (std::vector<std::pair<uint8_t, uint8_t>>::iterator itr = joinFields.begin();
                itr != joinFields.end(); ++itr) {
            psort1.push_back(itr->first);
            psort2.push_back(itr->second);
        }
        TupleTable *sortedTable1 = sortBy(psort1);
        TupleTable *sortedTable2 = o->sortBy(psort2);

        //Linear scan of the table
        size_t idx1 = 0;
        size_t idx2 = 0;
        const uint8_t *p1 = &(psort1[0]);
        const uint8_t *p2 = &(psort2[0]);
        const uint8_t npos = (uint8_t) psort1.size();

        long count1 = 0;
        long count2 = 0;
        long output = 0;

        while (idx1 < getNRows() && idx2 < o->getNRows()) {
            const uint64_t *row1 = getRow(idx1);
            const uint64_t *row2 = o->getRow(idx2);
            int res = cmp(row1, row2, p1, p2, npos);
            if (res < 0) {
                ++idx1;
            } else if (res > 0) {
                ++idx2;
            } else {
                //Determine the range to perform the join
                size_t startrange1 = idx1;
                size_t startrange2 = idx2;
                size_t endrange1 = startrange1 + 1;
                size_t endrange2 = startrange2 + 1;
                while (endrange1 < getNRows()) {
                    if (cmp(row1, getRow(endrange1), p1, p1, npos) != 0)
                        break;
                    endrange1++;
                }
                while (endrange2 < o->getNRows()) {
                    if (cmp(row2, o->getRow(endrange2), p2, p2, npos) != 0)
                        break;
                    endrange2++;
                }

                //Increment the counters
                count1 += endrange1 - startrange1;
                count2 += endrange2 - startrange2;
                output += (endrange1 - startrange1) * (endrange2 - startrange2);

                idx1 = endrange1;
                idx2 = endrange2;
            }
        }

        JoinHitStats stats;
        //stats.selectivity1 = (double)count1 / sortedTable1->getNRows();
        //stats.selectivity2 = (double) count2 / sortedTable2->getNRows();
        stats.ratio = (double)output / (sortedTable1->getNRows() * sortedTable2->getNRows());
        stats.size = output;

        delete sortedTable1;
        delete sortedTable2;
        return stats;
    }
}

std::pair<std::shared_ptr<TupleTable>, std::shared_ptr<TupleTable>> TupleTable::getDenseSparseForBifocalSampling(TupleTable *o) {
    //Calculate the number of fields on which we should perfom the join
    std::vector<uint8_t> joinFields;
    for (uint8_t i = 0; i < signature.size(); ++i) {
        for (uint8_t j = 0; j < o->signature.size(); ++j) {
            if (signature[i] == o->signature[j])
                joinFields.push_back(i);
        }
    }

    //Sort the table by the joinFields
    TupleTable *sortedTable = sortBy(joinFields);

    //Threshold is...
    const double threshold = sqrt(this->getNRows());
    std::shared_ptr<TupleTable> denseTable(new TupleTable(this->getSizeRow()));
    std::shared_ptr<TupleTable> sparseTable(new TupleTable(this->getSizeRow()));

    int startRow = 0;
    const uint64_t *prevRow = this->getRow(0);
    for (size_t i = 1; i < this->getNRows(); ++i) {
        //cmp with previous row
        const uint64_t *row = this->getRow(i);
        bool same = true;
        for (std::vector<uint8_t>::iterator itr = joinFields.begin(); itr != joinFields.end();
                ++itr) {
            if (row[*itr] != prevRow[*itr]) {
                same = false;
                break;
            }
        }

        if (!same) {
            //Copy all the rows
            if ((i - startRow) > threshold) {
                //Copy in dense
                for (int j = startRow; j < i; ++j) {
                    denseTable->addRow(getRow(j));
                }
            } else {
                //Copy in sparse
                for (int j = startRow; j < i; ++j) {
                    sparseTable->addRow(getRow(j));
                }
            }
            startRow = i;
        }

        prevRow = row;
    }

    //Copy the last rows
    if ((getNRows() - startRow) > threshold) {
        //Copy in dense
        for (int j = startRow; j < getNRows(); ++j) {
            denseTable->addRow(getRow(j));
        }
    } else {
        //Copy in sparse
        for (int j = startRow; j < getNRows(); ++j) {
            sparseTable->addRow(getRow(j));
        }
    }

    delete sortedTable;

    return make_pair(denseTable, sparseTable);
}

TupleTable *TupleTable::join(TupleTable *o) {
    if (signature.size() == 0 || o->signature.size() == 0) {
        BOOST_LOG_TRIVIAL(debug) << "This should not happen";
        throw 10;
    }

    //Calculate the number of fields on which we should perfom the join
    std::vector<std::pair<uint8_t, uint8_t>> joinFields;
    for (uint8_t i = 0; i < signature.size(); ++i) {
        for (uint8_t j = 0; j < o->signature.size(); ++j) {
            if (signature[i] == o->signature[j])
                joinFields.push_back(std::make_pair(i, j));
        }
    }

    if (joinFields.size() == 0) {
        //Cartesian product
        std::vector<int> newsignature = signature;
        std::copy(o->signature.begin(), o->signature.end(), std::back_inserter(newsignature));
        TupleTable *output = new TupleTable(newsignature);

        //Create a table with the cartesian product
        uint64_t *newrow = new uint64_t[newsignature.size()];

        for (size_t i = 0; i < getNRows(); ++i) {
            const uint64_t *row = getRow(i);
            memcpy(newrow, row, sizeof(uint64_t)*sizeRow);
            for (size_t j = 0; j < o->getNRows(); ++j) {
                const uint64_t *row2 = o->getRow(j);
                memcpy(newrow + sizeRow, row2, sizeof(uint64_t) * o->getSizeRow());
                output->addRow(newrow);
            }
        }
        delete[] newrow;
        return output;
    } else { //Do a merge join

        //Calculate the number of positions to copy of the second table
        uint8_t npostocopy = 0;
        std::vector<uint8_t> posToCopy;
        if (joinFields.size() < o->getSizeRow()) {
            for (uint8_t i = 0; i < o->getSizeRow(); ++i) {
                bool toCopy = true;
                for (uint8_t j = 0; j < joinFields.size(); ++j) {
                    if (i == joinFields[j].second) {
                        toCopy = false;
                        break;
                    }
                }
                if (toCopy) {
                    posToCopy.push_back(i);
                    npostocopy++;
                }
            }
        }

        //Calculate the new signature
        std::vector<int> newsignature = signature;
        for (std::vector<uint8_t>::iterator itr = posToCopy.begin(); itr != posToCopy.end();
                ++itr) {
            newsignature.push_back(o->signature[*itr]);
        }
        TupleTable *output = new TupleTable(newsignature);

        //Sort the two tables according to the join keys
        std::vector<uint8_t> psort1;
        std::vector<uint8_t> psort2;
        for (std::vector<std::pair<uint8_t, uint8_t>>::iterator itr = joinFields.begin();
                itr != joinFields.end(); ++itr) {
            psort1.push_back(itr->first);
            psort2.push_back(itr->second);
        }
        TupleTable *sortedTable1 = sortBy(psort1);
        TupleTable *sortedTable2 = o->sortBy(psort2);

        //Linear scan of the table
        uint64_t *row = new uint64_t[newsignature.size()];
        size_t idx1 = 0;
        size_t idx2 = 0;
        const uint8_t *p1 = &(psort1[0]);
        const uint8_t *p2 = &(psort2[0]);
        const uint8_t npos = (uint8_t) psort1.size();

        while (idx1 < getNRows() && idx2 < o->getNRows()) {
            const uint64_t *row1 = getRow(idx1);
            const uint64_t *row2 = o->getRow(idx2);
            int res = cmp(row1, row2, p1, p2, npos);
            if (res < 0) {
                ++idx1;
            } else if (res > 0) {
                ++idx2;
            } else {
                //Determine the range to perform the join
                size_t startrange1 = idx1;
                size_t startrange2 = idx2;
                size_t endrange1 = startrange1 + 1;
                size_t endrange2 = startrange2 + 1;
                while (endrange1 < getNRows()) {
                    if (cmp(row1, getRow(endrange1), p1, p1, npos) != 0)
                        break;
                    endrange1++;
                }
                while (endrange2 < o->getNRows()) {
                    if (cmp(row2, o->getRow(endrange2), p2, p2, npos) != 0)
                        break;
                    endrange2++;
                }

                //Do the join
                for (size_t i = startrange1; i < endrange1; ++i) {
                    const uint64_t *r1 = getRow(i);
                    memcpy(row, r1, sizeof(uint64_t) * sizeRow);
                    for (size_t j = startrange2; j < endrange2; ++j) {
                        const uint64_t *r2 = o->getRow(j);
                        for (uint8_t m = 0; m < npostocopy; ++m) {
                            row[sizeRow + m] = r2[posToCopy[m]];
                        }
                        output->addRow(row);
                    }
                }

                idx1 = endrange1;
                idx2 = endrange2;
            }
        }

        delete[] row;
        delete sortedTable1;
        delete sortedTable2;
        return output;
    }
}

void TupleTable::copyArray(std::vector<uint64_t> &dest, uint64_t *row) {
    for (size_t i = 0; i < sizeRow; ++i) {
        dest.push_back(row[i]);
    }
}

int TupleTable::cmp(const uint64_t *r1, const uint64_t *r2, const size_t s) {
    for (size_t i = 0; i < s; ++i) {
        if (r1[i] < r2[i])
            return -1;
        else if (r1[i] > r2[i])
            return 1;
    }
    return 0;
}

int TupleTable::cmp(const uint64_t *r1, const uint64_t *r2, const uint8_t *p1,
                    const uint8_t *p2, const uint8_t npos) {
    for (size_t i = 0; i < npos; ++i) {
        if (r1[p1[i]] < r2[p2[i]])
            return -1;
        else if (r1[p1[i]] > r2[p2[i]])
            return 1;
    }
    return 0;
}
