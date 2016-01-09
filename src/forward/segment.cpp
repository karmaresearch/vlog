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

#include <vlog/segment.h>
#include <vlog/support.h>
#include <vlog/fcinttable.h>

#include <boost/log/trivial.hpp>

#include <random>
#include <memory>

Segment::Segment(const uint8_t nfields) : nfields(nfields) {
    columns = new std::shared_ptr<Column>[nfields];
    memset(columns, 0, sizeof(Column*)*nfields);
}

Segment::Segment(const uint8_t nfields, std::shared_ptr<Column> *c) : Segment(nfields) {
    for (uint8_t i = 0; i < nfields; ++i)
        this->columns[i] = c[i];
}

Segment::Segment(const uint8_t nfields, std::vector<std::shared_ptr<Column>> &c) : Segment(nfields) {
    assert(c.size() == nfields);
    for (uint8_t i = 0; i < nfields; ++i)
        this->columns[i] = c[i];
}

Segment& Segment::operator =(const std::shared_ptr<Column> *v) {
    for (uint8_t i = 0; i < nfields; ++i) {
        columns[i] = v[i];
    }
    return *this;
}

size_t Segment::estimate(const uint8_t nconstantsToFilter,
                           const uint8_t *posConstantsToFilter,
                           const Term_t *valuesConstantsToFilter,
                           const uint8_t nfields) const {

    if (nconstantsToFilter == 0) {
        return columns[0]->estimateSize();
    }

    size_t estimate = 0;
    std::vector<std::shared_ptr<ColumnReader>> readers;
    for (int i = 0; i < nconstantsToFilter; ++i) {
        readers.push_back(columns[posConstantsToFilter[i]]->getReader());
    }

    //if (nrows < 1000) {
    //linear scan

    uint32_t processedRows = 0;
    while (processedRows < 100) {
        bool cond = true;
        for (int i = 0; i < nconstantsToFilter && cond; ++i) {
            // cond = cond && readers[i]->hasNext();
            cond = readers[i]->hasNext();
        }
        if (!cond)
            break;

        bool ok = true;
        for (uint8_t j = 0; j < nconstantsToFilter; ++j) {
            if (readers[j]->next() != valuesConstantsToFilter[j]) {
                ok = false;
                break;
            }
        }
        if (ok)
            estimate++;
        processedRows++;
    }
    for (int i = 0; i < nconstantsToFilter; i++) {
	readers[i]->clear();
    }

    if (processedRows < 100) {
        return estimate;
    } else {
        size_t minSize = 1;
        size_t completeSize = columns[0]->estimateSize();
        size_t ratio = std::max(minSize, estimate /
                                  processedRows * completeSize);
        return std::min(completeSize, ratio);
    }

    /*} else {
        //sample 10% up to 1000 elements
        std::default_random_engine generator;
        std::uniform_int_distribution<size_t> distribution(0, nrows - 1);
        for (int i = 0; i < 1000; ++i) {
            size_t idx = distribution(generator) * nfields;
            bool ok = true;
            for (uint8_t j = 0; j < nconstantsToFilter; ++j) {
                if (readers[j]->get(idx) != valuesConstantsToFilter[j]) {
                    ok = false;
                    break;
                }
            }
            if (ok)
                estimate++;
        }
        estimate = ((double) estimate * nrows) / 1000;
    }*/
    return estimate;
    //return std::min(nrows, estimate);
}

bool Segment::areAllColumnsPartOftheSameQuery(EDBLayer **edb, const Literal **l,
        std::vector<uint8_t> *outputpos) const {
    bool sameLiteral = true;
    const Literal *lit;
    EDBLayer *layer;
    std::vector<uint8_t> posInLiteral;
    for (int i = 0; i < nfields; ++i) {
        if (columns[i]->isEDB()) {
            EDBColumn *edbC = (EDBColumn*) columns[i].get();
            if (i == 0) {
                lit = &(edbC->getLiteral());
                posInLiteral.push_back(edbC->posColumnInLiteral());
                layer = &(edbC->getEDBLayer());
            } else {
                //Check the literal is the same
                const Literal *newlit = &(edbC->getLiteral());
                Substitution subs[10];
                if (Literal::subsumes(subs, *newlit, *lit) != -1
                        && Literal::subsumes(subs, *lit, *newlit) != -1) {
                    posInLiteral.push_back(edbC->posColumnInLiteral());
                } else {
                    sameLiteral = false;
                    break;
                }
            }
        } else {
            sameLiteral = false;
            break;
        }
    }

    if (sameLiteral) {
        assert(lit->getNVars() <= posInLiteral.size());
        //I check also that every posInLiteral is unique because the
        //procedure below does not work for repeated fields
        std::vector<uint8_t> test = posInLiteral;
        std::sort(test.begin(), test.end());
        auto newlim = std::unique(test.begin(), test.end());
        // assert(newlim == test.end());
        if (newlim != test.end()) {
            return false;
        }

        if (edb != NULL) {
            *edb = layer;
            *l = lit;
            *outputpos = posInLiteral;
        }
    }

    return sameLiteral;
}

std::shared_ptr<Segment> Segment::sortBy(const std::vector<uint8_t> *fields) const {

    //Special case: the fields are all EDBs and part of the same literal
    if (fields == NULL && nfields > 1) {
        //Check they are all EDB and part of the same literal
        EDBLayer *layer;
        const Literal *lit;
        std::vector<uint8_t> posInLiteral;
        bool sameLiteral = areAllColumnsPartOftheSameQuery(&layer, &lit, &
                           posInLiteral);

        if (sameLiteral) {
            //I can sort the segment by re-creating the columns according
            //the same ordering.

            std::vector<std::shared_ptr<Column>> columns;
            columns.resize(nfields);
            std::vector<uint8_t> posPresorting;
            for (int i = 0; i < nfields; ++i) {
                //The flag unique is set to false because we have multiple fields
                //and therefore the field itself cannot be unique
                columns[i] = std::shared_ptr<Column>(
                                 new EDBColumn(*layer, *lit, posInLiteral[i],
                                               posPresorting,
                                               false));
                posPresorting.push_back(posInLiteral[i]);
            }

            std::shared_ptr<Segment> newSeg(new Segment(nfields,
                                            columns));

            /*//DEBUG: Check the output
            std::unique_ptr<SegmentIterator> itr = newSeg->iterator();
            std::shared_ptr<Segment> otherSeg = intsort(fields);
            std::unique_ptr<SegmentIterator> itr2 = otherSeg->iterator();
            int i = 0;
            while (itr->hasNext()) {
                if (!itr2->hasNext())
                    throw 10;
                itr->next();
                itr2->next();
                if (itr->get(0) != itr2->get(0) || itr->get(1) != itr2->get(1))
                    cout << "W" << itr->get(0) << " " << itr2->get(0) << " " <<
                         itr->get(1) << " " << itr2->get(1) << endl;
                else
                    cout << "C" << itr->get(0) << " " << itr2->get(0) << " " <<
                         itr->get(1) << " " << itr2->get(1) << endl;
                i++;
            }
            cout << "card is " << i << endl;*/

            return newSeg;
        }
    } //End special case


    return intsort(fields);
}

std::shared_ptr<Segment> Segment::intsort(
    const std::vector<uint8_t> *fields) const {
    if (!isEmpty()) {
        //Get all fields that are not constant
        std::vector<std::shared_ptr<Column>> varColumns;
        std::vector<std::unique_ptr<ColumnReader>> varColumnsR;
        std::vector<uint8_t> idxVarColumns;
        for (uint8_t i = 0; i < nfields; ++i) {
            if (!columns[i]->isConstant()) {
                varColumns.push_back(columns[i]);
                varColumnsR.push_back(columns[i]->getReader());
                idxVarColumns.push_back(i);
            }
        }
        std::vector<std::shared_ptr<Column>> sortedColumns;

        if (varColumns.size() == 0) {
            return std::shared_ptr<Segment>(new Segment(nfields, columns));
        } else if (varColumns.size() == 1) {
            sortedColumns.push_back(varColumns[0]->sort());
        } else {
            if (fields != NULL) {
                //Rearrange the fields
                std::vector<std::shared_ptr<Column>> newVarColumns;
                std::vector<uint8_t> newIdxVarColumns;
                for (uint8_t j = 0; j < fields->size(); ++j) {
                    for (uint8_t m = 0; m < idxVarColumns.size(); ++m) {
                        if (idxVarColumns[m] == fields->at(j)) {
                            newVarColumns.push_back(varColumns[m]);
                            newIdxVarColumns.push_back(idxVarColumns[m]);
                        }
                    }
                }

                for (uint8_t m = 0; m < idxVarColumns.size(); ++m) {
                    bool found = false;
                    for (uint8_t n = 0; n < newIdxVarColumns.size() && !found; ++n) {
                        if (newIdxVarColumns[n] == idxVarColumns[m]) {
                            found = true;
                        }
                    }
                    if (!found) {
                        newVarColumns.push_back(varColumns[m]);
                        newIdxVarColumns.push_back(idxVarColumns[m]);
                    }
                }
                assert(varColumns.size() == newVarColumns.size());
                varColumns = newVarColumns;
                idxVarColumns = newIdxVarColumns;
            }

            //Sort function
            SegmentSorter sorter(varColumns);

            std::vector<uint32_t> rows;
            const size_t allRows = sorter.getColumns()[0].size();
            for (uint32_t i = 0; i < allRows; ++i) {
                rows.push_back(i);
            }

            std::sort(rows.begin(), rows.end(), std::ref(sorter));
            // BOOST_LOG_TRIVIAL(debug) << "Sort done.";

            //Reconstruct the fields
            const uint8_t nvarColumns = (uint8_t) varColumns.size();
            std::vector<ColumnWriter> sortedColumnsInserters;
            sortedColumnsInserters.resize(nvarColumns);

            std::vector<std::vector<Term_t>> &uncomprColumns = sorter.getColumns();

            for (std::vector<uint32_t>::iterator itr = rows.begin();
                    itr != rows.end(); ++itr) {
                for (uint8_t i = 0; i < nvarColumns; ++i) {
                    sortedColumnsInserters[i].add(uncomprColumns[i][*itr]);
                }
            }
            //Populate sortedColumns
            assert(sortedColumns.size() == 0);
            for (auto &writer : sortedColumnsInserters)
                sortedColumns.push_back(writer.getColumn());
        }

        //Reconstruct all the fields
        std::vector<std::shared_ptr<Column>> allSortedColumns;
        for (uint8_t i = 0; i < nfields; ++i) {
            bool isVar = false;
            for (uint8_t j = 0; j < varColumns.size() && !isVar; ++j) {
                if (idxVarColumns[j] == i) {
                    allSortedColumns.push_back(sortedColumns[j]);
                    isVar = true;
                }
            }
            if (!isVar) {
                allSortedColumns.push_back(columns[i]);
            }
        }
        return std::shared_ptr<Segment>(new Segment(nfields, allSortedColumns));
    } else {
        return std::shared_ptr<Segment>(new Segment(nfields));
    }
}

void SegmentInserter::addRow(const Term_t *row, const uint8_t *posToCopy) {
    if (segmentSorted) {
        assert(nfields > 0);
        if (!columns[0].isEmpty()) {
            for (uint8_t i = 0; i < nfields; ++i) {
                if (row[posToCopy[i]] < columns[posToCopy[i]].lastValue()) {
                    segmentSorted = false;
                    break;
                } else if (row[posToCopy[i]] > columns[posToCopy[i]].lastValue()) {
                    break;
                }
            }
        }
    }

    for (int i = 0; i < nfields; ++i) {
        columns[i].add(row[posToCopy[i]]);
    }
}

/*void SegmentInserter::addRow(const Segment *seg, const uint32_t rowid) {
    if (segmentSorted) {
        assert(nfields > 0);
        if (!columns[0].isEmpty()) {
            for (uint8_t i = 0; i < nfields; ++i) {
                if (seg->at(rowid, i) < columns[i].lastValue()) {
                    segmentSorted = false;
                    break;
                } else if (seg->at(rowid, i) > columns[i].lastValue()) {
                    break;
                }
            }
        }
    }

    for (int i = 0; i < nfields; ++i) {
        columns[i].add(seg->at(rowid, i));
    }
}*/

void SegmentInserter::addRow(SegmentIterator &itr) {
    if (segmentSorted) {
        assert(nfields > 0);
        if (!columns[0].isEmpty()) {
            for (uint8_t i = 0; i < nfields; ++i) {
                if (itr.get(i) < columns[i].lastValue()) {
                    segmentSorted = false;
                    break;
                } else if (itr.get(i) > columns[i].lastValue()) {
                    break;
                }
            }
        }
    }

    for (uint8_t i = 0; i < nfields; ++i) {
        columns[i].add(itr.get(i));
    }
}

void SegmentInserter::addRow(FCInternalTableItr *itr, const uint8_t *posToCopy) {
    if (segmentSorted) {
        assert(nfields > 0);
        if (!columns[0].isEmpty()) {
            for (uint8_t i = 0; i < nfields; ++i) {
                if (itr->getCurrentValue(posToCopy[i]) < columns[posToCopy[i]].lastValue()) {
                    segmentSorted = false;
                    break;
                } else if (itr->getCurrentValue(posToCopy[i]) > columns[posToCopy[i]].lastValue()) {
                    break;
                }
            }
        }
    }

    for (int i = 0; i < nfields; ++i) {
        columns[i].add(itr->getCurrentValue(posToCopy[i]));
    }
}

void SegmentInserter::addRow(const Term_t *row) {
    if (segmentSorted) {
        assert(nfields > 0);
        if (!columns[0].isEmpty()) {
            for (uint8_t i = 0; i < nfields; ++i) {
                if (row[i] < columns[i].lastValue()) {
                    segmentSorted = false;
                    break;
                } else if (row[i] > columns[i].lastValue()) {
                    break;
                }
            }
        }
    }

    for (size_t i = 0; i < nfields; ++i) {
        columns[i].add(row[i]);
    }
}

bool SegmentInserter::isEmpty() const {
    assert(nfields > 0);
    if (copyColumns[0] != std::shared_ptr<Column>()) {
        if (!copyColumns[0]->isEmpty())
            return false;
    }
    return columns[0].isEmpty();
}

void SegmentInserter::addColumns(std::vector<std::shared_ptr<Column>> &c,
                                 const bool sorted, const bool lastInsert) {
    assert(c.size() == nfields);
    segmentSorted = isEmpty() && sorted;
    if (nfields == 1 && isEmpty() &&
            !c[0]->containsDuplicates()) {
        duplicates = false;
    } else {
        duplicates = true;
    }

    if (/*lastInsert && */isEmpty()) {
        for (uint8_t i = 0; i < c.size(); ++i) {
            copyColumns[i] = c[i];
        }
    } else {
        for (uint8_t i = 0; i < c.size(); ++i) {
            /*if (copyColumns[i] != NULL) {
                //Concatenate first the column
                //columns[i].concatenate(copyColumns[i].get());
                //copyColumns[i] = std::shared_ptr<Column>();
            }*/

            columns[i].concatenate(c[i].get());
        }
    }
}

void SegmentInserter::addColumn(const uint8_t pos,
                                std::shared_ptr<Column> column,
                                const bool sorted) {
    segmentSorted = isEmpty() && sorted;
    duplicates = !(segmentSorted && nfields == 1);

    if (isEmpty() && nfields == 1) {
        copyColumns[pos] = column;
    } else {
        /*if (copyColumns[pos] != std::shared_ptr<Column>()) {
            columns[pos].concatenate(copyColumns[pos].get());
            copyColumns[pos] = std::shared_ptr<Column>();
        }*/
        columns[pos].concatenate(column.get());
    }
}

std::shared_ptr<const Segment> SegmentInserter::getSegment() {
    std::vector<std::shared_ptr<Column>> c;
    for (int i = 0; i < nfields; ++i) {
        if (copyColumns[i] != std::shared_ptr<Column>()) {
            if (!columns[i].isEmpty()) {
                columns[i].concatenate(copyColumns[i].get());
                c.push_back(columns[i].getColumn());
            } else {
                c.push_back(copyColumns[i]);
            }
        } else {
            c.push_back(columns[i].getColumn());
        }
    }
    return std::shared_ptr<const Segment>(new Segment(nfields, c));
}

std::shared_ptr<const Segment> SegmentInserter::getSortedAndUniqueSegment() {
#if DEBUG
    BOOST_LOG_TRIVIAL(debug) << "getSortedAndUniqueSegment: segmentSorted = " << segmentSorted << ", duplicates = " << duplicates;
#endif
    if (segmentSorted && !duplicates) {
        return getSegment();
    }  else if (copyColumns[0] != NULL && !columns[0].isEmpty()) {
        //First sort the first segment
        std::vector<std::shared_ptr<Column>> c;
        for (int i = 0; i < nfields; ++i)
            c.push_back(copyColumns[i]);
        std::shared_ptr<const Segment> seg1(new Segment(nfields, c));
	if (seg1->getNRows() > 1) {
	    seg1 = seg1->sortBy(NULL);
	    if (duplicates) {
		seg1 = SegmentInserter::unique(seg1);
	    }
	}

        //Now sort the second segment
        std::vector<std::shared_ptr<Column>> c2;
        for (int i = 0; i < nfields; ++i)
            c2.push_back(columns[i].getColumn());
        std::shared_ptr<const Segment> seg2(new Segment(nfields, c2));
	if (seg2->getNRows() > 1) {
	    seg2 = seg2->sortBy(NULL);
	    if (duplicates) {
		seg2 = SegmentInserter::unique(seg2);
	    }
	}

        //Now merge them
        std::vector<std::shared_ptr<const Segment>> segments;
        segments.push_back(seg1);
        segments.push_back(seg2);
        std::shared_ptr<const Segment> finalSeg = SegmentInserter::merge(
                    segments);
        return finalSeg;

    } else {
        std::shared_ptr<const Segment> seg = getSegment();
	if (seg->getNRows() > 1) {
	    if (!segmentSorted) {
		seg = seg->sortBy(NULL);
	    }
	    if (duplicates) {
		seg = SegmentInserter::unique(seg);
	    }
        }
        return seg;
    }
}

size_t SegmentInserter::getNRows() const {
    assert(nfields > 0);
    size_t s = 0;
    if (copyColumns[0] != std::shared_ptr<Column>()) {
        if (!copyColumns[0]->isEmpty())
            s = copyColumns[0]->size();
    }
    s += columns[0].size();
    return s;
}

void SegmentInserter::addRow(FCInternalTableItr *itr) {
    if (segmentSorted) {
        if (!isEmpty()) {
            for (uint8_t i = 0; i < nfields; ++i) {
                if (itr->getCurrentValue(i) < columns[i].lastValue()) {
                    segmentSorted = false;
                    break;
                } else if (itr->getCurrentValue(i) > columns[i].lastValue()) {
                    break;
                }
            }
        }
    }

    for (uint8_t i = 0; i < nfields; ++i) {
        columns[i].add(itr->getCurrentValue(i));
    }
}

void SegmentInserter::addAt(const uint8_t p, const Term_t v) {
    //assert(copyColumns[p] == NULL);
    segmentSorted = false;
    columns[p].add(v);
}

/*void SegmentInserter::moveFrom(std::shared_ptr<Segment> seg, const bool sorted) {
    segment = seg;
    this->segmentSorted = sorted;
}*/

void SegmentInserter::copyArray(SegmentIterator &source) {
    for (uint8_t i = 0; i < nfields; ++i) {
        addAt(i, source.get(i));
    }
}

/*std::shared_ptr<const Segment> SegmentInserter::unique(
    std::shared_ptr<const Segment> &segment) {
    retain(segment, NULL);
}*/

/*void SegmentInserter::sortSegment() {
    if (!isSorted()) {
        segment = segment->sortBy(NULL);
        segmentSorted = true;
    }
}*/

std::shared_ptr<const Segment> SegmentInserter::retainMemMem(Column *c1,
        Column *c2) {
    ColumnWriter co;

    //size_t idx1 = 0;
    //size_t idx2 = 0;
    std::unique_ptr<ColumnReader> c1R = c1->getReader();
    std::unique_ptr<ColumnReader> c2R = c2->getReader();

    Term_t prevv1 = (Term_t) -1;
    Term_t v1 = (Term_t) -1;
    Term_t v2 = (Term_t) -1;
    if (!c1R->hasNext())
        throw 10;
    else
        v1 = c1R->next();
    if (!c2R->hasNext())
        throw 10;
    else
        v2 = c2R->next();

    while (true) {
        if (v1 < v2) {
            if (v1 != prevv1) {
                co.add(v1);
                prevv1 = v1;
            }
            if (c1R->hasNext()) {
                v1 = c1R->next();
            } else {
                v1 = (Term_t) -1;
                break;
            }
        } else if (v1 > v2) {
            if (c2R->hasNext()) {
                v2 = c2R->next();
            } else {
                v2 = (Term_t) -1;
                break;
            }
        } else {
            prevv1 = v1;
            if (c1R->hasNext()) {
                v1 = c1R->next();
            } else {
                v1 = (Term_t) -1;
                break;
            }
            if (c2R->hasNext()) {
                v2 = c2R->next();
            } else {
                v2 = (Term_t) -1;
                break;
            }
        }
    }

    if (v1 != (Term_t) -1) {
        if (v1 != prevv1) {
            co.add(v1);
            prevv1 = v1;
        }
        while (c1R->hasNext()) {
            v1 = c1R->next();
            if (v1 != prevv1) {
                co.add(v1);
                prevv1 = v1;
            }
        }
    }

    std::vector<std::shared_ptr<Column>> columns;
    columns.push_back(co.getColumn());
    return std::shared_ptr<Segment>(new Segment(1, columns));
}

std::shared_ptr<const Segment> SegmentInserter::retainMemEDB(
    std::shared_ptr<const Segment> seg,
    std::shared_ptr<const FCInternalTable > existingValues,
    uint8_t nfields) {

    std::shared_ptr<const Column> edbCol = existingValues->getColumn(0);
    const Literal l = ((EDBColumn*) edbCol.get())->getLiteral();
    EDBLayer &layer = ((EDBColumn*)edbCol.get())->getEDBLayer();

    uint8_t pos1 = ((EDBColumn*)edbCol.get())->posColumnInLiteral();
    std::vector<uint8_t> posColumns;
    posColumns.push_back(pos1);

    std::vector<std::shared_ptr<Column>> valuesToCheck;
    valuesToCheck.push_back(seg->getColumn(0));

    if (nfields == 2) {
        //Get also the second column
        std::shared_ptr<const Column> edbCol2 = existingValues->getColumn(1);
        const Literal l2 = ((EDBColumn*) edbCol2.get())->getLiteral();
        uint8_t pos2 = ((EDBColumn*)edbCol2.get())->posColumnInLiteral();
        posColumns.push_back(pos2);
        valuesToCheck.push_back(seg->getColumn(1));
    }

    //Do an anti join on the EDB layer
    std::vector<std::shared_ptr<Column>> output = layer.checkNewIn(
                                          valuesToCheck,
                                          l, posColumns);
    return std::shared_ptr<Segment>(new Segment(nfields, output));
}

std::shared_ptr<const Segment> SegmentInserter::retainEDB(
    std::shared_ptr<const Segment> seg,
    std::shared_ptr<const FCInternalTable> existingValues,
    uint8_t nfields) {
    //Column is an EDB column
    std::shared_ptr<Column> column = seg->getColumn(0);
    EDBLayer &layer = ((EDBColumn*)column.get())->getEDBLayer();
    const Literal &l1 = ((EDBColumn*)column.get())->getLiteral();
    uint8_t pos1 = ((EDBColumn*)column.get())
                   ->posColumnInLiteral();
    std::vector<uint8_t> posColumns1;
    posColumns1.push_back(pos1);

    //Get literal and pos join
    std::shared_ptr<const Column> firstCol = existingValues->getColumn(0);
    const Literal l2 = ((EDBColumn*)firstCol.get())->getLiteral();
    uint8_t pos2 = ((EDBColumn*)firstCol.get())
                   ->posColumnInLiteral();
    std::vector<uint8_t> posColumns2;
    posColumns2.push_back(pos2);

    assert(nfields > 0 && nfields < 3);
    if (nfields == 2) {
        std::shared_ptr<Column> column12 = seg->getColumn(1);
        const Literal &l12 = ((EDBColumn*)column12.get())->getLiteral();
        uint8_t pos12 = ((EDBColumn*)column12.get())
                        ->posColumnInLiteral();
        //Chech the two literals are equivalent and that the rel. are
        //different positions
        Substitution subs[MAX_ROWSIZE];
        if (!l1.sameVarSequenceAs(l12) || l1.subsumes(subs, l1, l12) == -1
                || pos1 == pos12) {
            //The columns come from different literals. This is not yet
            //supported
            throw 10;
        }
        posColumns1.push_back(pos12);

        //Repeat the same process for the second field
        std::shared_ptr<const Column> column22 = existingValues->getColumn(1);
        const Literal &l22 = ((EDBColumn*)column22.get())->getLiteral();
        uint8_t pos22 = ((EDBColumn*)column22.get())
                        ->posColumnInLiteral();
        if (!l2.sameVarSequenceAs(l22) || l1.subsumes(subs, l2, l22) == -1
                || pos2 == pos22) {
            //The columns come from different literals. This is not yet
            //supported
            throw 10;
        }
        posColumns2.push_back(pos22);
    }

    std::vector<std::shared_ptr<Column>> columns = layer.
                                      checkNewIn(l1, posColumns1, l2,
                                              posColumns2);
    return std::shared_ptr<Segment>(new Segment(nfields, columns));
}

std::shared_ptr<const Segment> SegmentInserter::retain(
    std::shared_ptr<const Segment> &segment,
    std::shared_ptr<const FCInternalTable> existingValues,
    const bool duplicates) {

    if (segment->isEmpty())
        return segment;

    //Special cases: one of the two sides have one column each and are EDB views
    if ((segment->getNColumns() == 1 || segment->getNColumns() == 2)
            && existingValues != NULL && existingValues->isEDB()) {
        if (segment->isEDB()) {
            segment = retainEDB(segment, existingValues,
                                segment->getNColumns());
        } else  {
            segment = retainMemEDB(segment, existingValues,
                                   segment->getNColumns());
        }
        return segment;
    }

    std::vector<uint8_t> posConstants;
    std::vector<uint8_t> posToCopy;
    std::vector<Term_t> valueConstants;
    if (!segment->isEmpty()) {
        for (uint8_t i = 0; i < segment->getNColumns(); ++i) {
            if (segment->isConstantField(i)) {
                posConstants.push_back(i);
                valueConstants.push_back(segment->firstInColumn(i));
            } else {
                posToCopy.push_back(i);
            }
        }
    }

    bool match = true;
    bool superset = false;
    if (existingValues != NULL && !existingValues->isEmpty()
            && posConstants.size() > 0) {
        //Is existingValues a superset of the current values?
        for (uint8_t i = 0; i < posConstants.size() && match; ++i) {
            if (existingValues->isColumnConstant(posConstants[i])) {
                if (valueConstants[i] !=
                        existingValues->
                        getValueConstantColumn(posConstants[i])) {
                    match = false;
                }
            } else {
                superset = true;
            }
        }
    }

    if (superset) {
        existingValues = existingValues->filter((uint8_t) posToCopy.size(),
                                                posToCopy.size() > 0 ?
                                                & (posToCopy[0]) : NULL,
                                                (uint8_t) posConstants.size(),
                                                &(posConstants[0]),
                                                &(valueConstants[0]),
                                                0, NULL);
    } else if (existingValues != NULL &&
               posToCopy.size() < existingValues->getRowSize()) {
        existingValues = existingValues->filter((uint8_t) posToCopy.size(),
                                                &(posToCopy[0]), 0, NULL,
                                                NULL, 0, NULL);
    }

    const size_t nPosToCompare = posToCopy.size();
    const uint8_t *posToCompare = posToCopy.size() > 0 ? &(posToCopy[0]) : NULL;

    /*if (duplicates && nPosToCompare == 1) {
        //Clean the duplicated lines
        std::shared_ptr<Column> col = segment->getColumn(posToCompare[0])->sort();
        uint32_t newsize = col->size();
        //Update all other columns with the new cardinality
        for (uint8_t i = 0; i < posConstants.size(); ++i) {
            Column *c = segment->getColumn(posConstants[i]).get();
            assert(c->isConstant());
            segment->replaceColumn(posConstants[i],
                                   std::shared_ptr<Column>(new
                                           ColumnImpl(c->getReader()->first(),
                                                   newsize)));
        }
        //duplicates = false;
    }*/

    FCInternalTableItr *itr2 = existingValues != NULL &&
                               match ? existingValues->getSortedIterator() : NULL;

    //uint32_t idx1 = 0;
    bool toRead1 = false;
    bool toRead2 = true;
    bool active1 = true;
    bool active2 = match && itr2 != NULL && itr2->hasNext();

    if (!active2 && !duplicates) {
        //Don't need to do anything else. The values are already unique
        if (itr2 != NULL) {
            existingValues->releaseIterator(itr2);
        }
        return segment;
    }

    if (nPosToCompare == 1 && segment->getNColumns() == 1 && itr2 != NULL) {
        //It is column vs. column. Launch a faster algo than the one below
        std::shared_ptr<Column> c1 = segment->getColumn(posToCompare[0]);
        std::vector<uint8_t> fields;
        fields.push_back((uint8_t)0);
        std::vector<std::shared_ptr<Column>> c2 = itr2->getColumn(1, &(fields[0]));
        segment = retainMemMem(c1.get(), c2[0].get());
        existingValues->releaseIterator(itr2);
        return segment;
    }

    const uint8_t nfields = segment->getNColumns();
    std::unique_ptr<SegmentIterator> segmentIterator = segment->iterator();
    active1 = segmentIterator->hasNext();
    assert(active1);
    segmentIterator->next();

    Term_t *prevrow1 = new Term_t[nfields];
    bool prevrow1valid = false;
    SegmentInserter retainedValues(nfields);

    while (active1 && active2) {
        if (toRead1) {
            segmentIterator->next();

            if (duplicates) {
                if (prevrow1valid) {
                    bool same = true;
                    for (uint8_t i = 0; i < nfields && same; ++i)
                        if (prevrow1[i] != segmentIterator->get(i))
                            same = false;
                    if (same) {
                        active1 = segmentIterator->hasNext();
                        continue;
                    }
                }

                for (uint8_t i = 0; i < nfields; ++i) {
                    prevrow1[i] = segmentIterator->get(i);
                }
                prevrow1valid = true;
            }
            toRead1 = false;
        }

        if (toRead2) {
            itr2->next(); //I assume all values are unique
            toRead2 = false;
        }

        int res = 0;
        for (uint8_t i = 0; i < nPosToCompare; ++i) {
            if (segmentIterator->get(posToCompare[i]) != itr2->getCurrentValue(i)) {
                res = segmentIterator->get(posToCompare[i]) - itr2->getCurrentValue(i);
                break;
            }
        }
        if (res == 0) {
            toRead1 = true;
            active1 = segmentIterator->hasNext();
        } else if (res < 0) {
            retainedValues.copyArray(*segmentIterator.get());
            toRead1 = true;
            active1 = segmentIterator->hasNext();
        } else {
            toRead2 = true;
            active2 = itr2->hasNext();
        }
    }

//Copy the remaining
    if (!toRead1) {
        retainedValues.copyArray(*segmentIterator.get());
        toRead1 = true;
        if (!prevrow1valid) {
            for (uint8_t i = 0; i < nfields; ++i) {
                prevrow1[i] = segmentIterator->get(i);
            }
            prevrow1valid = true;
        }
    }

    if (duplicates) {
        //for (; idx1 < segment->getNRows(); ++idx1) {
        while (segmentIterator->hasNext()) {
            segmentIterator->next();
            if (prevrow1valid) {
                bool same = true;
                for (uint8_t i = 0; i < nfields && same; ++i)
                    if (prevrow1[i] != segmentIterator->get(i))
                        same = false;
                if (same) {
                    continue;
                }
            }

            for (uint8_t i = 0; i < nfields; ++i) {
                prevrow1[i] = segmentIterator->get(i);
            }
            prevrow1valid = true;
            retainedValues.copyArray(*segmentIterator.get());
        }
    } else {
        //boost::chrono::system_clock::time_point startFiltering = boost::chrono::system_clock::now();

        //for (; idx1 < segment->getNRows(); ++idx1) {
        //    retainedValues.copyArray(segmentIterator);
        //}
        //
        while (segmentIterator->hasNext()) {
            segmentIterator->next();
            retainedValues.copyArray(*segmentIterator.get());
        }

        //boost::chrono::duration<double> secFiltering = boost::chrono::system_clock::now() - startFiltering;
        //cout << "Time filtering " << secFiltering.count() * 1000 << endl;
    }

    segmentIterator->clear();

    delete[] prevrow1;

    if (itr2 != NULL) {
        existingValues->releaseIterator(itr2);
    }

    return retainedValues.getSegment();
}

bool SegmentInserter::isSorted() const {
    return segmentSorted;
}

bool SegmentInserter::containsDuplicates() const {
    return this->duplicates;
}

std::shared_ptr<const Segment> SegmentInserter::concatenate(
    std::vector<std::shared_ptr<const Segment>> &segments) {
    //Check all segments have the same size
    for (int i = 1; i < segments.size(); ++i)
        if (segments[i]->getNColumns() != segments[i - 1]->getNColumns())
            throw 10; //not possible

    const uint8_t nfields = segments[0]->getNColumns();
    SegmentInserter inserter(nfields);

    for (auto &segment : segments) {
        for (uint8_t i = 0; i < nfields; ++i) {
            inserter.addColumn(i, segment->getColumn(i), false);
        }
    }
    return inserter.getSegment();
}

std::shared_ptr<const Segment> SegmentInserter::unique(
    std::shared_ptr<const Segment> seg) {
    if (seg->getNColumns() == 1) {
        std::shared_ptr<Column> c = seg->getColumn(0);
        //I assume c is sorted
        auto c2 = c->unique();
        std::vector<std::shared_ptr<Column>> fields;
        fields.push_back(c2);
        return std::shared_ptr<const Segment>(new Segment(1, fields));
    } else {

        bool sameLiteral = seg->areAllColumnsPartOftheSameQuery(NULL, NULL,
                           NULL);
        if (sameLiteral)
            return seg;

        std::unique_ptr<SegmentIterator> itr = seg->iterator();
        const uint8_t ncolumns = seg->getNColumns();
        std::unique_ptr<Term_t[]> fields(new Term_t[ncolumns]);
        for (int i = 0; i < ncolumns; ++i)
            fields[i] = (Term_t) -1;
        std::vector<ColumnWriter> writers;
        writers.resize(ncolumns);
        while (itr->hasNext()) {
            itr->next();
            bool unq = false;
            for (uint8_t i = 0; i < ncolumns; ++i)
                if (itr->get(i) != fields[i]) {
                    unq = true;
                    break;
                }

            if (unq) {
                for (uint8_t i = 0; i < ncolumns; ++i) {
                    writers[i].add(itr->get(i));
                    fields[i] = itr->get(i);
                }
            }
        }
	itr->clear();

        std::vector<std::shared_ptr<Column>> outputColumns;
        for (int i = 0; i < ncolumns; ++i)
            outputColumns.push_back(writers[i].getColumn());
        return std::shared_ptr<const Segment>(new Segment(ncolumns,
                                              outputColumns));
    }
}

bool varsHasNext(std::unique_ptr<ColumnReader> *p, const uint8_t s) {
    for (int i = 0; i < s; ++i) {
	if (! p[i]->hasNext()) {
	    return false;
	}
    }
    return true;
}

void varsNext(Term_t *values, std::unique_ptr<ColumnReader> *p,
              const uint8_t s) {
    for (int i = 0; i < s; ++i)
        values[i] = p[i]->next();
}

std::shared_ptr<const Segment> SegmentInserter::merge(
    std::vector<std::shared_ptr<const Segment>> &segments) {

    BOOST_LOG_TRIVIAL(debug) << "SegmentInserter::merge";
    //Check all segments have the same size
    const uint8_t nfields = segments[0]->getNColumns();
    for (int i = 1; i < segments.size(); ++i)
        if (segments[i]->getNColumns() != segments[i - 1]->getNColumns())
            throw 10; //not possible

    //Should I compare all fields or only a few of them?
    std::vector<uint8_t> fieldsToCompare;
    for (uint8_t i = 0; i < nfields; ++i) {
        bool identical = true;
        for (int j = 1; j < segments.size(); ++j) {
            if (!segments[j - 1]->getColumn(i)->isConstant() ||
                    !segments[j]->getColumn(i)->isConstant()) {
                identical = false;
            } else {
                std::unique_ptr<ColumnReader> r1 = segments[j - 1]->getColumn(i)->getReader();
                std::unique_ptr<ColumnReader> r2 = segments[j]->getColumn(i)->getReader();

                if (r1->first() != r2->first()) {
                    identical = false;
                }
                r1->clear();
                r2->clear();
            }
        }
        if (!identical) {
            fieldsToCompare.push_back(i);
        }
    }

    if (fieldsToCompare.size() == 0) {
        //Nothing to merge. The columns are equal to the existing one
        return segments[0];
    }

    std::shared_ptr<const Segment> lastSegment = segments[0];
    for (int idxCurSegment = 1;
            idxCurSegment < segments.size();
            ++idxCurSegment) {
        std::shared_ptr<const Segment> curSegment = segments[idxCurSegment];

        std::vector<std::unique_ptr<ColumnReader>> vars1;
        std::vector<std::unique_ptr<ColumnReader>> vars2;
        for (auto pos : fieldsToCompare) {
            vars1.push_back(lastSegment->getColumn(pos)->getReader());
            vars2.push_back(curSegment->getColumn(pos)->getReader());
        }

        SegmentInserter out((uint8_t) fieldsToCompare.size());
        /*uint32_t s1 = 0;
        uint32_t s2 = 0;
        const uint32_t e1 = lastSegment->getNRows();
        const uint32_t e2 = curSegment->getNRows();*/
        const uint8_t nvars = (uint8_t) fieldsToCompare.size();
        //BOOST_LOG_TRIVIAL(trace) << "Segment::merge, nvars = " << (int) nvars
        //                         << ", e1 = " << e1 << ", e2 = " << e2;

        bool lastValueEOF = false;
        Term_t lastValues[MAX_ROWSIZE];
        bool curValueEOF = false;
        Term_t curValues[MAX_ROWSIZE];

        long count1 = 1;
        long count2 = 1;

        if (varsHasNext(&vars1[0], nvars)) {
            varsNext(lastValues, &vars1[0], nvars);
            //lastValue = lastSegment->next();

            if (varsHasNext(&vars2[0], nvars)) {
                varsNext(curValues, &vars2[0], nvars);
                //curValue = curSegment->next();

                while (!lastValueEOF && !curValueEOF) {
                    //Compare the columns
                    int res = 0;
                    for (uint8_t i = 0; i < nvars && res == 0; ++i) {
                        Term_t v1 = lastValues[i];
                        Term_t v2 = curValues[i];

                        if (v2 > v1) {
                            res = -1;
                            for (uint8_t j = 0; j < nvars; ++j)
                                out.addAt(j, lastValues[j]);
                            if (varsHasNext(&vars1[0], nvars)) {
                                varsNext(lastValues, &vars1[0], nvars);
                                count1++;
                            } else {
                                lastValueEOF = true;
                                break;
                            }
                        } else if (v2 < v1) {
                            res = 1;
                            for (uint8_t j = 0; j < nvars; ++j)
                                out.addAt(j, curValues[j]);
                            if (varsHasNext(&vars2[0], nvars)) {
                                varsNext(curValues, &vars2[0], nvars);
                                count2++;
                            } else {
                                curValueEOF = true;
                                break;
                            }
                        }
                    }

                    if (res == 0) {
                        //Add only one and increment both
                        for (uint8_t j = 0; j < nvars; ++j)
                            out.addAt(j, lastValues[j]);
                        if (varsHasNext(&vars1[0], nvars)) {
                            varsNext(lastValues, &vars1[0], nvars);
                            count1++;
                        } else {
                            lastValueEOF = true;
                            // No break here, we still need to increment vars2.
                        }
                        if (varsHasNext(&vars2[0], nvars)) {
                            varsNext(curValues, &vars2[0], nvars);
                            count2++;
                        } else {
                            curValueEOF = true;
                            break;
                        }
                    }
                }
            } else {
                curValueEOF = true;
            }
        } else {
            lastValueEOF = true;
        }

        //Add remaining
        while (!lastValueEOF) {
            for (uint8_t j = 0; j < nvars; ++j)
                out.addAt(j, lastValues[j]);
            lastValueEOF = !varsHasNext(&vars1[0], nvars);
            if (!lastValueEOF) {
                varsNext(lastValues, &vars1[0], nvars);
                count1++;
            }
        }

        while (!curValueEOF) {
            for (uint8_t j = 0; j < nvars; ++j)
                out.addAt(j, curValues[j]);
            curValueEOF = !varsHasNext(&vars2[0], nvars);
            if (!curValueEOF) {
                varsNext(curValues, &vars2[0], nvars);
                count2++;
            }
        }
        BOOST_LOG_TRIVIAL(debug) << "Merged segments of " << count1 << " and "
                                 << count2 << " elements";

        //copy remaining fields
        uint8_t nv = 0;
        uint32_t nsize = out.getNRows();
        std::vector<std::shared_ptr<Column>> newcolumns;
        std::shared_ptr<const Segment> outSegment = out.getSegment();

        newcolumns.resize(nfields);
        for (uint8_t i = 0; i < nfields; ++i) {
            bool found = false;
            for (uint8_t j = 0; j < nvars && !found; ++j) {
                if (fieldsToCompare[j] == i) {
                    found = true;
                }
            }

            if (found) {
                //Replace it with the merged one.
                newcolumns[i] = outSegment->getColumn(nv++);
            } else {
                newcolumns[i] = std::shared_ptr<Column>(
                                    new CompressedColumn(
                                        lastSegment->firstInColumn(i), nsize));
            }
        }

        lastSegment = std::shared_ptr<const Segment>(
                          new Segment(nfields, newcolumns));

        BOOST_LOG_TRIVIAL(trace) << "Segment::merge done";
        for (int i = 0; i < vars1.size(); i++) {
            if (vars1[i] != NULL) {
                vars1[i]->clear();
            }
        }
        for (int i = 0; i < vars2.size(); i++) {
            if (vars2[i] != NULL) {
                vars2[i]->clear();
            }
        }
    }

    return lastSegment;
}

std::unique_ptr<SegmentIterator> Segment::iterator() const {
    return std::unique_ptr<SegmentIterator>(
               new SegmentIterator(nfields, columns));
}

SegmentIterator::SegmentIterator(const uint8_t nfields,
                                 std::shared_ptr<Column> *columns) {
    for (int i = 0; i < nfields; ++i) {
        readers.push_back(columns[i]->getReader());
    }
}

void SegmentIterator::clear() {
    for (const auto  &reader : readers) {
	reader->clear();
    }
}

bool SegmentIterator::hasNext() {
    bool resp = true;
    for (const auto  &reader : readers) {
        resp = resp && reader->hasNext();
        if (!resp)
            break;
    }
    return resp;
}

void SegmentIterator::next() {
    uint8_t idx = 0;
    for (const auto  &reader : readers) {
        values[idx++] = reader->next();
    }
}

Term_t SegmentIterator::get(const uint8_t pos) {
    return values[pos];
}
