#ifndef _SEGMENT_H
#define _SEGMENT_H

#include <vlog/column.h>

//#include <tbb/parallel_for.h>

#include <cstdio>
#include <algorithm>
#include <memory>
#include <vector>
#include <assert.h>

struct GetVectors {
    const std::vector<std::shared_ptr<Column>> &cols;
    std::vector<const std::vector<Term_t> *> &vectors;

    GetVectors(const std::vector<std::shared_ptr<Column>> &cols, std::vector<const std::vector<Term_t> *> &vectors) : cols(cols), vectors(vectors) {
    }

    void operator()(const ParallelRange& r) const {
        for (int i = r.begin(); i != r.end(); ++i) {
            if (! cols[i]->isBackedByVector()) {
                std::vector<Term_t> *v = new std::vector<Term_t>();
                *v = cols[i]->getReader()->asVector();
                vectors[i] = v;
            } else {
                vectors[i] = &cols[i]->getVectorRef();
            }
        }
    }
};

struct SegmentSorter {
    const std::vector<const std::vector<Term_t> *> &vectors;
    size_t maxSize;

    SegmentSorter(const std::vector<const std::vector<Term_t> *> &vectors)
        : vectors(vectors) {
            maxSize = vectors[0]->size();
        }

    bool operator ()(const size_t f1, const size_t f2) const {
        assert(f1 < maxSize);
        assert(f2 < maxSize);
        for (uint8_t i = 0; i < vectors.size(); ++i) {
            if ((*vectors[i])[f1] != (*vectors[i])[f2])
                return (*vectors[i])[f1] < (*vectors[i])[f2];
        }
        return false;
    }
};

class SegmentIterator {
    private:
        std::vector<std::unique_ptr<ColumnReader>> readers;

    protected:
        Term_t values[32];
        SegmentIterator() {
        }

    public:
        SegmentIterator(const uint8_t nfields, std::shared_ptr<Column> *columns) {
            for (int i = 0; i < nfields; i++) {
                readers.push_back(columns[i]->getReader());
            }
        }

        virtual bool hasNext() {
            for (const auto  &reader : readers) {
                if (! reader->hasNext()) {
                    return false;
                }
            }
            return true;
        }

        virtual void next() {
            int idx = 0;
            for (const auto  &reader : readers) {
                values[idx++] = reader->next();
            }
        }

        virtual void clear() {
            for (const auto  &reader : readers) {
                reader->clear();
            }
        }

        virtual Term_t get(const uint8_t pos) {
            return values[pos];
        }

        virtual ~SegmentIterator() {
        }
};

class VectorSegmentIterator : public SegmentIterator {
    private:
        const std::vector<const std::vector<Term_t> *> vectors;
        int currentIndex;
        bool first;
        int endIndex;
        int ncols;
        std::vector<bool> *allocatedVectors;
    public:
        VectorSegmentIterator(const std::vector<const std::vector<Term_t> *> &vectors, int firstIndex, int endIndex, std::vector<bool> *allocatedVectors)
            : vectors(vectors), currentIndex(firstIndex), first(true), endIndex(endIndex), ncols(vectors.size()), allocatedVectors(allocatedVectors) {
                if (endIndex > vectors[0]->size()) {
                    this->endIndex = vectors[0]->size();
                }
            }

        bool hasNext() {
            return currentIndex < endIndex - 1 || (first && currentIndex < endIndex);
        }

        void next() {
            if (! first) {
                currentIndex++;
            }
            first = false;
            for (int i = 0; i < ncols; i++) {
                values[i] = (*vectors[i])[currentIndex];
            }
        }

        void clear() {
            if (allocatedVectors != NULL) {
                for (int i = 0; i < allocatedVectors->size(); i++) {
                    if ((*allocatedVectors)[i]) {
                        delete vectors[i];
                    }
                }
                delete allocatedVectors;
                allocatedVectors = NULL;
            }
        }

        int getNColumns() {
            return ncols;
        }

        ~VectorSegmentIterator() {
            clear();
        }
};

class Segment {
    private:
        const uint8_t nfields;
        std::shared_ptr<Column> *columns;

        std::shared_ptr<Segment> intsort(
                const std::vector<uint8_t> *fields) const;

        std::shared_ptr<Segment> intsort(
                const std::vector<uint8_t> *fields, const int nthreads,
                const bool filterDupl) const;

    public:
        Segment(const uint8_t nfields);

        Segment(const uint8_t nfields, std::shared_ptr<Column> *columns);

        Segment(const uint8_t nfields, std::vector<std::shared_ptr<Column>> &columns);

        Segment& operator =(const std::shared_ptr<Column> *v);

        bool areAllColumnsPartOftheSameQuery(EDBLayer **layer, const Literal **lit,
                std::vector<uint8_t> *posInLiteral)
            const;

#if DEBUG
        void checkSizes() const;
#endif

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

        std::unique_ptr<VectorSegmentIterator> vectorIterator() const;

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

        std::shared_ptr<Segment> sortBy(const std::vector<uint8_t> *fields,
                const int nthreads,
                const bool filterDupl) const;

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

        static std::vector<const std::vector<Term_t> *> getAllVectors(const std::vector<std::shared_ptr<Column>> &cols) {
            std::vector<const std::vector<Term_t> *> vectors(cols.size());
            for (int i = 0; i < cols.size(); i++) {
                if (cols[i]->isBackedByVector()) {
                    vectors[i] = &cols[i]->getVectorRef();
                } else {
                    std::vector<Term_t> *v = new std::vector<Term_t>();
                    *v = cols[i]->getReader()->asVector();
                    vectors[i] = v;
                }
            }
            return vectors;
        }

        static std::vector<const std::vector<Term_t> *> getAllVectors(const std::vector<std::shared_ptr<Column>> &cols, int nthreads) {
            std::vector<const std::vector<Term_t> *> vectors(cols.size());
            int count = cols.size();
            for (int i = 0; i < cols.size(); i++) {
                if (cols[i]->isBackedByVector()) {
                    count--;
                }
            }

            if (count > 1 && nthreads > 1) {
                //tbb::parallel_for(tbb::blocked_range<int>(0, cols.size(), 1),
                //        GetVectors(cols, vectors));
                ParallelTasks::parallel_for(0, cols.size(), 1,
                        GetVectors(cols, vectors));
            } else {
                for (int i = 0; i < cols.size(); i++) {
                    if (! cols[i]->isBackedByVector()) {
                        std::vector<Term_t> *v = new std::vector<Term_t>();
                        *v = cols[i]->getReader()->asVector();
                        vectors[i] = v;
                    } else {
                        vectors[i] = &cols[i]->getVectorRef();
                    }
                }
            }

            return vectors;
        }

        std::vector<const std::vector<Term_t> *> getAllVectors() const {
            std::vector<std::shared_ptr<Column>> cols;
            for (int i = 0; i < nfields; i++) {
                cols.push_back(columns[i]);
            }
            return getAllVectors(cols);
        }

        std::vector<const std::vector<Term_t> *> getAllVectors(int nthreads) const {
            std::vector<std::shared_ptr<Column>> cols;
            for (int i = 0; i < nfields; i++) {
                cols.push_back(columns[i]);
            }
            return Segment::getAllVectors(cols, nthreads);
        }

        static void deleteAllVectors(const std::vector<std::shared_ptr<Column>> &cols, std::vector<const std::vector<Term_t> *> vectors) {
            for (int i = 0; i < cols.size(); i++) {
                if (! cols[i]->isBackedByVector()) {
                    delete vectors[i];
                }
            }
        }

        void deleteAllVectors(std::vector<const std::vector<Term_t> *> vectors) const {
            std::vector<std::shared_ptr<Column>> cols;
            for (int i = 0; i < nfields; i++) {
                cols.push_back(columns[i]);
            }
            Segment::deleteAllVectors(cols, vectors);
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

        struct Concat {
            private:
                std::vector<std::shared_ptr<Column>> &c;
                SegmentInserter *s;

            public:
                Concat(std::vector<std::shared_ptr<Column>> &c, SegmentInserter *s) : c(c), s(s) {
                }

                void operator()(const ParallelRange& r) const {
                    for (int i = r.begin(); i != r.end(); ++i) {
                        if (s->copyColumns[i] != std::shared_ptr<Column>()) {
                            if (!s->columns[i].isEmpty()) {
                                s->columns[i].concatenate(s->copyColumns[i].get());
                                c[i] = s->columns[i].getColumn();
                            } else {
                                c[i] = s->copyColumns[i];
                            }
                        } else {
                            c[i] = s->columns[i].getColumn();
                        }
                    }
                }
        };

    public:
        SegmentInserter(const uint8_t nfields) : nfields(nfields),
        segmentSorted(true), duplicates(true) {
            columns.resize(nfields);
            copyColumns.resize(nfields);
        }
#if DEBUG
        void checkSizes() const;
#endif

        void addRow(const Term_t *row, const uint8_t *posToCopy);

        void addRow(FCInternalTableItr *itr);

        void addRow(const Term_t *row);

        void addRow(const Term_t *row, int rowsize) {
            segmentSorted = false;
            for (int i = 0; i < rowsize; ++i) {
                columns[i].add(row[i]);
            }
        }

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

        std::shared_ptr<const Segment> getSegment(const int nthreads);

        std::shared_ptr<const Segment> getSortedAndUniqueSegment();

        std::shared_ptr<const Segment> getSortedAndUniqueSegment(const int nthreads);

        static std::shared_ptr<const Segment> unique(
                std::shared_ptr<const Segment> seg);

        static std::shared_ptr<const Segment> retain(
                std::shared_ptr<const Segment> &segment,
                std::shared_ptr<const FCInternalTable> existingValues,
                const bool duplicates,
                const int nthreads);

        static std::shared_ptr<const Segment> merge(
                std::vector<std::shared_ptr<const Segment>> &segments);

        static std::shared_ptr<const Segment> concatenate(
                std::vector<std::shared_ptr<const Segment>> &segments);

        static std::shared_ptr<const Segment> concatenate(
                std::vector<std::shared_ptr<const Segment>> &segments, const int nthreads);
};

#endif
