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

#ifndef _SEGMENT_H
#define _SEGMENT_H

#include <vlog/column.h>

#include <cstdio>
#include <algorithm>
#include <memory>
#include <vector>
#include <assert.h>

struct SegmentSorter {
    const uint8_t nfields;
    std::vector<std::vector<Term_t>> columns;
    size_t maxSize;

    SegmentSorter(std::vector<std::shared_ptr<Column>> columnsToSort)
        : nfields((uint8_t) columnsToSort.size())  {
        assert(columnsToSort.size() > 0);
        for (uint8_t i = 0; i < nfields; ++i) {
            columns.push_back(columnsToSort[i]->getReader()->asVector());
        }
        maxSize = columns[0].size();
    }

    std::vector<std::vector<Term_t>> &getColumns() {
        return columns;
    }

    bool operator ()(const uint32_t f1, const uint32_t f2) const {
        assert(f1 < maxSize);
        assert(f2 < maxSize);
        for (uint8_t i = 0; i < nfields; ++i) {
            if (columns[i][f1] != columns[i][f2])
                return columns[i][f1] < columns[i][f2];
        }
        return false;
    }

    Term_t get(const uint8_t i, const uint32_t j) {
        assert(j < maxSize);
        return columns[i][j];
    }

};

class SegmentIterator {
private:
    std::vector<std::unique_ptr<ColumnReader>> readers;
    Term_t values[32];

public:
    SegmentIterator(const uint8_t nfields, std::shared_ptr<Column> *columns);

    bool hasNext();

    void next();

    void clear();

    Term_t get(const uint8_t pos);
};

class Segment {
private:
    const uint8_t nfields;
    std::shared_ptr<Column> *columns;

    std::shared_ptr<Segment> intsort(
        const std::vector<uint8_t> *fields) const;

public:
    Segment(const uint8_t nfields);

    Segment(const uint8_t nfields, std::shared_ptr<Column> *columns);

    Segment(const uint8_t nfields, std::vector<std::shared_ptr<Column>> &columns);

    Segment& operator =(const std::shared_ptr<Column> *v);

    bool areAllColumnsPartOftheSameQuery(EDBLayer **layer, const Literal **lit,
                                         std::vector<uint8_t> *posInLiteral)
    const;

    size_t getNRows() const {
        for (uint8_t i = 0; i < nfields; ++i) {
            if (columns[i] != NULL) {
                if (!columns[i]->isEDB()) {
                    return columns[i]->size();
                }
            }
        }

        //What to do now?
        for (uint8_t i = 0; i < nfields; ++i) {
            if (columns[i] != NULL) {
                return columns[i]->size();
            }
        }

        return 0;
    }

    bool isEmpty() const {
        for (uint8_t i = 0; i < nfields; ++i) {
            if (columns[i] != NULL) {
                if (!columns[i]->isEmpty())
                    return false;
            }
        }
        return true;
    }

    bool supportDirectAccess() const {
        bool resp = true;
        for (int i = 0; i < nfields && resp; ++i) {
            std::shared_ptr<Column> col = columns[i];
            resp = resp && col->supportsDirectAccess();
        }
        return resp;
    }

    size_t estimate(const uint8_t nconstantsToFilter,
                      const uint8_t *posConstantsToFilter,
                      const Term_t *valuesConstantsToFilter,
                      const uint8_t nfields) const;

    std::unique_ptr<SegmentIterator> iterator() const;

    ~Segment() {
        delete[] columns;
    }

    Term_t get(const uint32_t rowid, const uint8_t columnid) const {
        assert(columns[columnid]->supportsDirectAccess());
        return columns[columnid]->getValue(rowid);
    }

    Term_t firstInColumn(const uint8_t columnid) const {
        return columns[columnid]->getReader()->first();
    }

    Term_t lastInColumn(const uint8_t columnid) const {
        return columns[columnid]->getReader()->last();
    }

    std::shared_ptr<Column> getColumn(const uint8_t idx) const {
        return columns[idx];
    }

    std::shared_ptr<Segment> sortBy(const std::vector<uint8_t> *fields) const;

    uint8_t getNConstantFields() const {
        uint8_t n = 0;
        for (uint8_t i = 0; i < nfields; ++i) {
            if (columns[i] == NULL || columns[i]->isConstant()) {
                n++;
            }
        }
        return n;
    }

    uint8_t getNColumns() const {
        return nfields;
    }

    bool isConstantField(const uint8_t i) const {
        return columns[i] == NULL || columns[i]->isConstant();
    }

    bool isEDB() const {
        for (uint8_t i = 0; i < nfields; ++i) {
            if (columns[i] != NULL) {
                if (!(columns[i]->isEDB())) {
                    return false;
                }
            }
        }
        return nfields > 0;
    }
};

class FCInternalTableItr;
class FCInternalTable;

class SegmentInserter {
private:
    const uint8_t nfields;
    std::vector<ColumnWriter> columns;

    //This contains a copy of the first columns
    std::vector<std::shared_ptr<Column>> copyColumns;

    bool segmentSorted;
    bool duplicates;

    void copyArray(SegmentIterator &source);

    static std::shared_ptr<const Segment> retainEDB(
        std::shared_ptr<const Segment> seg,
        std::shared_ptr<const FCInternalTable> existingValues,
        uint8_t nfields);

    static std::shared_ptr<const Segment> retainMemEDB(
        std::shared_ptr<const Segment> seg,
        std::shared_ptr<const FCInternalTable> existingValues,
        uint8_t nfields);

    static std::shared_ptr<const Segment> retainMemMem(Column *c1, Column *c2);

public:
    SegmentInserter(const uint8_t nfields) : nfields(nfields),
        segmentSorted(true), duplicates(true) {
        columns.resize(nfields);
        copyColumns.resize(nfields);
    }

    void addRow(const Term_t *row, const uint8_t *posToCopy);

    void addRow(FCInternalTableItr *itr);

    void addRow(const Term_t *row);

    void addRow(SegmentIterator &itr);

    void addRow(FCInternalTableItr *itr, const uint8_t *posToCopy);

    void addAt(const uint8_t p, const Term_t v);

    void addColumns(std::vector<std::shared_ptr<Column>> &columns,
                    const bool sorted, const bool lastInsert);

    void addColumn(const uint8_t pos, std::shared_ptr<Column> column,
                   const bool sorted);

    size_t getNRows() const;

    bool isEmpty() const;

    bool isSorted() const;

    bool containsDuplicates() const;

    std::shared_ptr<const Segment> getSegment();

    std::shared_ptr<const Segment> getSortedAndUniqueSegment();

    static std::shared_ptr<const Segment> unique(
        std::shared_ptr<const Segment> seg);

    static std::shared_ptr<const Segment> retain(
        std::shared_ptr<const Segment> &segment,
        std::shared_ptr<const FCInternalTable> existingValues,
        const bool duplicates);

    static std::shared_ptr<const Segment> merge(
        std::vector<std::shared_ptr<const Segment>> &segments);

    static std::shared_ptr<const Segment> concatenate(
        std::vector<std::shared_ptr<const Segment>> &segments);
};

#endif
