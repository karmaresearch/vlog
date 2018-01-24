#ifndef _FCINTTAB
#define _FCINTTAB

#include <vlog/edb.h>
#include <vlog/segment.h>
#include <kognac/factory.h>

#include <vector>
#include <inttypes.h>
#include <string>
#include <unordered_map>

class FCInternalTableItr {
    public:
        virtual size_t getCurrentIteration() const = 0;

        virtual Term_t getCurrentValue(const uint8_t pos) = 0;

        virtual bool hasNext() = 0;

        virtual uint8_t getNColumns() const = 0;

        virtual std::vector<std::shared_ptr<Column>> getColumn(const uint8_t ncolumns,
                const uint8_t *columns) = 0;

        virtual std::vector<std::shared_ptr<Column>> getAllColumns() = 0;

        virtual void next() = 0;

        virtual void clear() {
        }

        virtual FCInternalTableItr *copy() const {
            throw 10;
        }

        virtual void reset() {
            throw 10;
        }

        virtual bool sameAs(
                const std::vector<Term_t> &row,
                const std::vector<uint8_t> &fields) {

            for (std::vector<uint8_t>::const_iterator itr = fields.cbegin(); itr != fields.cend();
                    ++itr) {
                if (getCurrentValue(*itr) != row[*itr]) {
                    return false;
                }
            }
            return true;
        }

        std::vector<const std::vector<Term_t> *> getAllVectors() {
            std::vector<std::shared_ptr<Column>> cols = getAllColumns();
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

        std::vector<const std::vector<Term_t> *> getAllVectors(int nthreads) {
            std::vector<std::shared_ptr<Column>> cols = getAllColumns();
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

        void deleteAllVectors(std::vector<const std::vector<Term_t> *> vectors) {
            std::vector<std::shared_ptr<Column>> cols = getAllColumns();
            for (int i = 0; i < cols.size(); i++) {
                if (! cols[i]->isBackedByVector()) {
                    delete vectors[i];
                }
            }
        }

        virtual ~FCInternalTableItr() {}
};

class FCInternalTable {
    private:
        //    int references;

    public:

        virtual bool isEmpty() const = 0;

        virtual uint8_t getRowSize() const = 0;

        virtual bool supportsDirectAccess() const = 0;

        virtual std::shared_ptr<const FCInternalTable> cloneWithIteration(
                const size_t iteration) const = 0;

        virtual FCInternalTableItr *getIterator() const = 0;

        virtual FCInternalTableItr *getSortedIterator() const = 0;

        virtual FCInternalTableItr *getSortedIterator(int nthreads) const = 0;

        virtual size_t estimateNRows(const uint8_t nconstantsToFilter,
                const uint8_t *posConstantsToFilter,
                const Term_t *valuesConstantsToFilter) const = 0;

        size_t estimateNRows() const {
            return estimateNRows(0, NULL, NULL);
        }

        virtual std::shared_ptr<const FCInternalTable> filter(
                const uint8_t nPosToCopy, const uint8_t *posVarsToCopy,
                const uint8_t nPosToFilter, const uint8_t *posConstantsToFilter,
                const Term_t *valuesConstantsToFilter, const uint8_t nRepeatedVars,
                const std::pair<uint8_t, uint8_t> *repeatedVars, int nthreads) const = 0;

        virtual bool isSorted() const = 0;

        virtual std::shared_ptr<Column> getColumn(const uint8_t columnIdx) const = 0;

        virtual bool isColumnConstant(const uint8_t columnid) const = 0;

        virtual Term_t getValueConstantColumn(const uint8_t columnid) const = 0;

        virtual Term_t get(const size_t rowId, const uint8_t columnId) const {
            throw 10;
        }

        virtual std::shared_ptr<const FCInternalTable> merge(
                std::shared_ptr<const FCInternalTable> t, int nthreads) const = 0;

        virtual FCInternalTableItr *sortBy(
                const std::vector<uint8_t> &fields) const = 0;

        virtual FCInternalTableItr *sortBy(
                const std::vector<uint8_t> &fields, const int nthreads) const = 0;

        virtual void releaseIterator(FCInternalTableItr *itr) const = 0;

        virtual bool isEDB() const {
            return false;
        }

        virtual size_t getNRows() const = 0;

        virtual ~FCInternalTable();
};

class VectorFCInternalTableItr : public FCInternalTableItr {
    private:
        const std::vector<const std::vector<Term_t> *> vectors;
        int beginIndex;
        int endIndex;
        int currentIndex;
        bool first;

    public:
        VectorFCInternalTableItr(const std::vector<const std::vector<Term_t> *> &vectors,
                int beginIndex, int endIndex) :
            vectors(vectors), beginIndex(beginIndex), endIndex(endIndex),
            currentIndex(beginIndex), first(true) {
                if (endIndex > vectors[0]->size()) {
                    endIndex = vectors[0]->size();
                }
            }

        FCInternalTableItr *copy() const {
            return new VectorFCInternalTableItr(vectors, beginIndex, endIndex);
        }

        Term_t getCurrentValue(const uint8_t pos) {
            assert(pos < vectors.size());
            return (*vectors[pos])[currentIndex];
        }

        size_t getCurrentIteration() const {
            throw 10;
        }

        uint8_t getNColumns() const {
            return vectors.size();
        }

        std::vector<std::shared_ptr<Column>> getColumn(const uint8_t ncolumns,
                const uint8_t *columns) {
            throw 10;
        }

        std::vector<std::shared_ptr<Column>> getAllColumns() {
            throw 10;
        }

        bool hasNext() {
            return currentIndex < endIndex - 1 || (first && currentIndex < endIndex);
        }

        void next() {
            if (! first) {
                currentIndex++;
            }
            first = false;
        }

        void clear() {
        }

        void reset() {
            currentIndex = beginIndex;
            first = true;
        }

        ~VectorFCInternalTableItr() {
            clear();
        }
};

class InmemoryFCInternalTableItr : public FCInternalTableItr {
    private:
        uint8_t nfields;
        size_t iteration;

        std::shared_ptr<const Segment> values;
        //long idx;
        std::unique_ptr<SegmentIterator> segmentIterator;

    public:
        void init(const uint8_t nfields, const size_t iteration,
                std::shared_ptr<const Segment> values);

        Term_t getCurrentValue(const uint8_t pos) {
            assert(pos < values->getNColumns());
            //return values->at(idx, pos);
            return segmentIterator->get(pos);
        }

        size_t getCurrentIteration() const {
            return iteration;
        }

        uint8_t getNColumns() const;

        std::vector<std::shared_ptr<Column>> getColumn(const uint8_t ncolumns,
                const uint8_t *columns);

        std::vector<std::shared_ptr<Column>> getAllColumns();

        bool hasNext() {
            //return (idx + 1) < values->getNRows();
            return segmentIterator->hasNext();
        }

        void next() {
            segmentIterator->next();
        }

        void clear() {
            if (segmentIterator != NULL) {
                segmentIterator->clear();
            }
            segmentIterator = NULL;
        }

        void reset() {
            clear();
            segmentIterator = values->iterator();
        }

        FCInternalTableItr *copy() const {
            InmemoryFCInternalTableItr *itr = new InmemoryFCInternalTableItr();
            itr->init(nfields, iteration, values);
            return itr;
        }

        ~InmemoryFCInternalTableItr() {
            clear();
        }
};

class EDBFCInternalTableItr : public FCInternalTableItr {
    private:
        std::vector<uint8_t> fields;
        EDBIterator *edbItr;
        uint8_t nfields;
        uint8_t posFields[SIZETUPLE];
        bool compiled;
        size_t iteration;

        EDBLayer *layer;
        const Literal *query;

    public:
        void init(const size_t iteration,
                const std::vector<uint8_t> &fields,
                const uint8_t nfields, uint8_t const *posFields,
                EDBIterator *edbItr, EDBLayer *layer, const Literal *query);

        EDBIterator *getEDBIterator();

        inline Term_t getCurrentValue(const uint8_t pos);

        size_t getCurrentIteration() const {
            return iteration;
        }

        uint8_t getNColumns() const;

        std::vector<std::shared_ptr<Column>> getColumn(const uint8_t ncolumns,
                const uint8_t *columns);

        std::vector<std::shared_ptr<Column>> getAllColumns();

        inline bool hasNext();

        inline void next();

        FCInternalTableItr *copy() const ;

        ~EDBFCInternalTableItr() {}
};

struct MITISorter {
    const std::vector<std::pair<FCInternalTableItr*, size_t>> iterators;
    const uint8_t tuplesize;
    uint8_t *sortPos;

    MITISorter(const std::vector<std::pair<FCInternalTableItr*, size_t>> iterators,
            const uint8_t tuplesize, uint8_t *sortPos) : iterators(iterators),
    tuplesize(tuplesize), sortPos(sortPos) {}

    bool operator ()(const uint8_t i1, const uint8_t i2) const;
};

class MergerInternalTableItr : public FCInternalTableItr {
    private:
        std::vector<uint8_t> indices;
        const std::vector<std::pair<FCInternalTableItr*, size_t>> iterators;
        uint8_t sortPos[10];
        bool firstCall;
        FCInternalTableItr *first;
        const MITISorter sorter;
        const uint8_t nfields;

    public:
        MergerInternalTableItr(const std::vector<std::pair<FCInternalTableItr*, size_t>> &iterators,
                const std::vector<uint8_t> &positionsToSort,
                const uint8_t nfields);

        Term_t getCurrentValue(const uint8_t pos) {
            const Term_t v = first->getCurrentValue(pos);
            return v;
        }

        size_t getCurrentIteration() const {
            return first->getCurrentIteration();
        }

        uint8_t getNColumns() const;

        std::vector<std::shared_ptr<Column>> getColumn(const uint8_t ncolumns,
                const uint8_t *columns);

        std::vector<std::shared_ptr<Column>> getAllColumns();

        bool hasNext();

        void next();

};

typedef std::unordered_map<std::string, std::shared_ptr<Segment>, std::hash<std::string>, std::equal_to<std::string>> SortingCache;

#define _INMEMORYUNMSEGM_MAXCONSTS 20
struct InmemoryFCInternalTableUnmergedSegment {
    private:

    public:
        const uint8_t nconstants;
        std::shared_ptr<const Segment> values;
        std::pair<uint8_t, Term_t> constants[_INMEMORYUNMSEGM_MAXCONSTS];

        InmemoryFCInternalTableUnmergedSegment(const uint8_t nfields, std::shared_ptr<const Segment> values) :
            nconstants(values->getNConstantFields()) {
                this->values = values;
                uint8_t j = 0;
                for (uint8_t i = 0; i < nfields; ++i) {
                    if (values->isConstantField(i)) {
                        constants[j++] = std::make_pair(i, values->firstInColumn(i));
                    }
                }
                assert(j <= _INMEMORYUNMSEGM_MAXCONSTS);
            }

        InmemoryFCInternalTableUnmergedSegment(const uint8_t nconstants, const std::pair<uint8_t, Term_t> *constants,
                const uint8_t nfields, std::shared_ptr<Column> *columns)
            : nconstants(nconstants), values(new Segment(nfields, columns)) {
                assert(columns[0] != NULL);
                for (uint8_t i = 0; i < nconstants; ++i) {
                    this->constants[i] = constants[i];
                }
                assert(nconstants <= _INMEMORYUNMSEGM_MAXCONSTS);
            }
};

class InmemoryFCInternalTable : public FCInternalTable {
    private:
        const uint8_t nfields;
        const size_t iteration;

        std::shared_ptr<const Segment> values;
        std::vector<InmemoryFCInternalTableUnmergedSegment> unmergedSegments;

        //size_t nrows;
        bool sorted;

        InmemoryFCInternalTable(const size_t iteration, const uint8_t nfields,
                const bool sorted,
                std::shared_ptr<const Segment> values) :
            nfields(nfields), iteration(iteration), values(values), sorted(sorted) {
            }

        void readArray(Term_t *dest, std::vector<Term_t>::iterator &itr);

        void readArray(Term_t *dest, std::vector<Term_t>::const_iterator &itr);

        //void copyArray(Segment *dest, FCInternalTableItr *itr);

        bool isPrimarySorting(const std::vector<uint8_t> &fields) const;

        static std::string vector2string(const std::vector<uint8_t> &v);

        std::shared_ptr<Segment> doSort(
                std::shared_ptr<Segment> input,
                const std::vector<uint8_t> *fields);

        void copyRawValues(const std::vector<const Term_t*> *rowIdx,
                const uint8_t nfields, std::vector<Term_t> &output);

        static std::shared_ptr<const Segment> mergeUnmergedSegments(
                std::shared_ptr<const Segment> values, bool isSorted,
                const std::vector <
                InmemoryFCInternalTableUnmergedSegment > &unmergedSegments,
                const bool outputSorted, const int nthreads);

        int cmp(FCInternalTableItr *itr1, FCInternalTableItr *itr2) const;

        bool colAIsContainedInColB(const Segment *A, const Segment *B) const;

        void getDistinctValues(std::shared_ptr<Column> c,
                std::vector<Term_t> &existingValues, const uint32_t threshold) const;

        std::shared_ptr<const FCInternalTable> cloneWithIteration(
                const size_t it) const {
            // if (!unmergedSegments.empty())
            //     throw 10;

            std::shared_ptr<const FCInternalTable> newtab(
                    unmergedSegments.empty() ?
                    new InmemoryFCInternalTable(it, nfields, sorted, this->values) :
                    new InmemoryFCInternalTable(nfields, it, sorted, values, unmergedSegments));
            return newtab;
        }

        static std::shared_ptr<const Segment> filter_row(std::shared_ptr<const Segment> seg,
                const uint8_t nConstantsToFilter, const uint8_t *posConstantsToFilter,
                const Term_t *valuesConstantsToFilter, const uint8_t nRepeatedVars,
                const std::pair<uint8_t, uint8_t> *repeatedVars, int nthreads);

    public:
        static std::shared_ptr<const Segment> filter_row(SegmentIterator *itr,
                const uint8_t nConstantsToFilter, const uint8_t *posConstantsToFilter,
                const Term_t *valuesConstantsToFilter, const uint8_t nRepeatedVars,
                const std::pair<uint8_t, uint8_t> *repeatedVars, SegmentInserter &inserter);

        InmemoryFCInternalTable(const uint8_t nfields, const size_t iteration);

        InmemoryFCInternalTable(const uint8_t nfields, const size_t iteration, const bool sorted, std::shared_ptr<const Segment> values);

        InmemoryFCInternalTable(const uint8_t nfields, const size_t iteration, const bool sorted,
                std::shared_ptr<const Segment> values, std::vector<InmemoryFCInternalTableUnmergedSegment> unmergedSegments);

        bool isEDB() const {
            if (values != NULL) {
                for (uint8_t i = 0; i < nfields; ++i) {
                    if (!values->getColumn(i)->isEDB()) {
                        return false;
                    }
                }
            }
            return nfields > 0;
        }

        size_t getNRows() const;

        bool isEmpty() const;

        bool supportsDirectAccess() const {
            return unmergedSegments.empty() && values->supportDirectAccess();
        }

        uint8_t getRowSize() const;

        std::shared_ptr<Column> getColumn(const uint8_t columnIdx) const;

        bool isColumnConstant(const uint8_t columnid) const;

        Term_t getValueConstantColumn(const uint8_t columnid) const;

        Term_t get(const size_t rowId, const uint8_t columnId) const;

        FCInternalTableItr *getIterator() const;

        FCInternalTableItr *getSortedIterator() const;

        FCInternalTableItr *getSortedIterator(int nthreads) const;

        size_t estimateNRows(const uint8_t nconstantsToFilter,
                const uint8_t *posConstantsToFilter,
                const Term_t *valuesConstantsToFilter) const;

        std::shared_ptr<const FCInternalTable> merge(std::shared_ptr<const FCInternalTable> t, int nthreads) const;

        std::shared_ptr<const FCInternalTable> merge(std::shared_ptr<const Segment> seg, int nthreads) const;

        std::vector<Term_t> getDistinctValues(const uint8_t columnid, const uint32_t threshold) const;

        bool isSorted() const;

        std::shared_ptr<const FCInternalTable> filter(const uint8_t nPosToCopy, const uint8_t *posVarsToCopy,
                const uint8_t nPosToFilter, const uint8_t *posConstantsToFilter,
                const Term_t *valuesConstantsToFilter, const uint8_t nRepeatedVars,
                const std::pair<uint8_t, uint8_t> *repeatedVars, int nthreads) const;

        FCInternalTableItr *sortBy(const std::vector<uint8_t> &fields) const;

        FCInternalTableItr *sortBy(const std::vector<uint8_t> &fields,
                const int nthreads) const;

        void releaseIterator(FCInternalTableItr * itr) const;

        std::shared_ptr<const Segment> getUnderlyingSegment() const {
            return values;
        }

        ~InmemoryFCInternalTable();
};

class EDBFCInternalTable : public FCInternalTable {
    private:
        const size_t iteration;
        const uint8_t nfields;
        uint8_t posFields[SIZETUPLE];
        const QSQQuery query;
        EDBLayer *layer;
        Factory<EDBFCInternalTableItr> factory;
        std::vector<uint8_t> defaultSorting;

        EDBFCInternalTable(const size_t iteration,
                const uint8_t nfields, uint8_t const posFields[SIZETUPLE],
                const QSQQuery &query,
                EDBLayer *layer,
                const std::vector<uint8_t> &defaultSorting) :
            iteration(iteration),
            nfields(nfields),
            query(query),
            layer(layer),
            defaultSorting(defaultSorting) {
                for (int j = 0; j < nfields; ++j)
                    this->posFields[j] = posFields[j];
            }

    public:
        EDBFCInternalTable(const size_t iteration,
                const Literal &literal, EDBLayer *layer);

        bool isEDB() const {
            return true;
        }

        Literal getQuery() {
            return *query.getLiteral();
        }

        size_t getNRows() const;

        bool isEmpty() const;

        bool supportsDirectAccess() const {
            return false;
        }

        std::shared_ptr<const FCInternalTable> cloneWithIteration(
                const size_t it) const {
            std::shared_ptr<const FCInternalTable> newtab(
                    new EDBFCInternalTable(it, nfields, posFields,
                        query, layer, defaultSorting));
            return newtab;

        }

        uint8_t getRowSize() const;

        FCInternalTableItr *getIterator() const;

        FCInternalTableItr *getSortedIterator() const;

        FCInternalTableItr *getSortedIterator(int nthreads) const {
            return getSortedIterator();
        }

        std::shared_ptr<const FCInternalTable> merge(std::shared_ptr<const FCInternalTable> t, int nthreads) const;

        bool isSorted() const;

        std::shared_ptr<Column> getColumn(const uint8_t columnIdx) const;

        bool isColumnConstant(const uint8_t columnid) const;

        Term_t getValueConstantColumn(const uint8_t columnid) const;

        size_t estimateNRows(const uint8_t nconstantsToFilter,
                const uint8_t *posConstantsToFilter,
                const Term_t *valuesConstantsToFilter) const;

        std::shared_ptr<const FCInternalTable> filter(const uint8_t nPosToCopy, const uint8_t *posVarsToCopy,
                const uint8_t nPosToFilter, const uint8_t *posConstantsToFilter,
                const Term_t *valuesConstantsToFilter, const uint8_t nRepeatedVars,
                const std::pair<uint8_t, uint8_t> *repeatedVars, int nthreads) const;

        FCInternalTableItr *sortBy(const std::vector<uint8_t> &fields) const;

        FCInternalTableItr *sortBy(const std::vector<uint8_t> &fields,
                const int nthreads) const;

        void releaseIterator(FCInternalTableItr *itr) const;

        ~EDBFCInternalTable();
};

class SingletonItr : public FCInternalTableItr {
    private:
        const size_t iteration;
        bool first;

    public:

        SingletonItr(const size_t iteration) : iteration(iteration) {
            first = true;
        }

        FCInternalTableItr *copy() const {
            return new SingletonItr(iteration);
        }

        Term_t getCurrentValue(const uint8_t pos) {
            throw 10;
        }

        size_t getCurrentIteration() const {
            return iteration;
        }

        uint8_t getNColumns() const {
            return 0;
        }

        std::vector<std::shared_ptr<Column>> getColumn(const uint8_t ncolumns,
                const uint8_t *columns) {
            std::vector<std::shared_ptr<Column>> retval;
            if (ncolumns > 0) {
                throw 10;
            }
            return retval;
        }

        std::vector<std::shared_ptr<Column>> getAllColumns() {
            std::vector<std::shared_ptr<Column>> retval;
            return retval;
        }

        bool hasNext() {
            return first;
        }

        void next() {
            first = false;
        }

};

class SingletonTable : public FCInternalTable {
    private:
        const size_t iteration;

    public:
        SingletonTable(const size_t iteration) : iteration(iteration) {
        }

        size_t getNRows() const {
            return 1;
        }

        bool isEmpty() const {
            return false;
        }

        uint8_t getRowSize() const {
            return 0;
        }

        bool supportsDirectAccess() const {
            return true;
        }

        FCInternalTableItr *getIterator() const {
            return new SingletonItr(iteration);
        };

        std::shared_ptr<const FCInternalTable> cloneWithIteration(
                const size_t it) const {
            return std::shared_ptr<const FCInternalTable>(new SingletonTable(it));
        }

        /*Term_t getValueAt(const size_t rowId, const uint8_t columnId) const {
          return 0;
          }*/

        FCInternalTableItr *getSortedIterator() const {
            return new SingletonItr(iteration);
        }

        FCInternalTableItr *getSortedIterator(int nthreads) const {
            return new SingletonItr(iteration);
        }

        std::shared_ptr<const FCInternalTable> merge(std::shared_ptr<const FCInternalTable> t, int nthreads) const {
            throw 10;
        }

        bool isSorted() const {
            return true;
        }

        std::shared_ptr<Column> getColumn(const uint8_t columnIdx) const {
            return std::shared_ptr<Column>();
        }

        bool isColumnConstant(const uint8_t columnid) const {
            return true;
        }

        Term_t getValueConstantColumn(const uint8_t columnid) const {
            throw 10; //currently not supported
        }

        size_t estimateNRows(const uint8_t nconstantsToFilter,
                const uint8_t *posConstantsToFilter,
                const Term_t *valuesConstantsToFilter) const {
            return 1;
        }

        std::shared_ptr<const FCInternalTable> filter(const uint8_t nPosToCopy, const uint8_t *posVarsToCopy,
                const uint8_t nPosToFilter, const uint8_t *posConstantsToFilter,
                const Term_t *valuesConstantsToFilter, const uint8_t nRepeatedVars,
                const std::pair<uint8_t, uint8_t> *repeatedVars, int nthreads) const {
            throw 10;
        }

        FCInternalTableItr *sortBy(const std::vector<uint8_t> &fields) const {
            return new SingletonItr(iteration);
        }

        FCInternalTableItr *sortBy(const std::vector<uint8_t> &fields,
                const int nthreads) const {
            return new SingletonItr(iteration);
        }

        void releaseIterator(FCInternalTableItr *itr) const {
            delete itr;
        }
};



#endif
