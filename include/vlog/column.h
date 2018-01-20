#ifndef _COLUMN_H
#define _COLUMN_H

#include <vlog/concepts.h>
#include <vlog/edb.h>

#include <trident/utils/parallel.h>

#include <inttypes.h>
#include <assert.h>
#include <algorithm>

#include <memory>
#include <cstring>
#include <vector>

//----- GENERIC INTERFACES -------
class ColumnReader {
    public:
        virtual Term_t last() = 0;

        virtual Term_t first() = 0;

        virtual bool hasNext() = 0;

        virtual Term_t next() = 0;

        virtual void clear() = 0;

        virtual std::vector<Term_t> asVector() = 0;
};

class ColumnWriter;

class Column {
    public:
        virtual bool isEmpty() const = 0;

        virtual bool isEDB() const = 0;

        virtual bool containsDuplicates() const {
            return true;
        }

        virtual size_t size() const = 0;

        virtual size_t estimateSize() const = 0;

        virtual Term_t getValue(const size_t pos) const = 0;

        virtual bool supportsDirectAccess() const = 0;

        virtual bool isBackedByVector() {
            return false;
        }

        virtual const std::vector<Term_t> &getVectorRef() {
            throw 10; //Should be used only on subclasses that supports this
        }

        virtual bool isIn(const Term_t t) const = 0;

        virtual std::unique_ptr<ColumnReader> getReader() const = 0;

        virtual std::shared_ptr<Column> sort() const = 0;

        virtual std::shared_ptr<Column> sort(const int nthreads) const = 0;

        virtual std::shared_ptr<Column> unique() const = 0;

        virtual std::shared_ptr<Column> sort_and_unique() const {
            return this->sort()->unique();
        }

        virtual std::shared_ptr<Column> sort_and_unique(const int nthreads) const {
            return this->sort(nthreads)->unique();
        }

        virtual bool isConstant() const = 0;

        static void intersection(
                std::shared_ptr<Column> c1,
                std::shared_ptr<Column> c2,
                ColumnWriter &writer);

        static void intersection(
                std::shared_ptr<Column> c1,
                std::shared_ptr<Column> c2,
                ColumnWriter &writer,
                int nthreads);

        static uint64_t countMatches(
                std::shared_ptr<Column> c1,
                std::shared_ptr<Column> c2);

        static bool subsumes(
                std::shared_ptr<Column> subsumer,
                std::shared_ptr<Column> subsumed);

        virtual ~Column() {
        }
};

//----- COMPRESSED COLUMN ----------

#define COLCOMPRB 1000000
struct CompressedColumnBlock {
    const Term_t value;
    int64_t delta;
    size_t size;

    CompressedColumnBlock(const Term_t value,
            const int64_t delta,
            size_t size) : value(value),
    delta(delta), size(size) {}
};

class CompressedColumn: public Column {
    private:
        std::vector<CompressedColumnBlock> blocks;
        size_t _size;

        //CompressedColumn(const CompressedColumn &o);

    public:
        CompressedColumn(const Term_t v, const uint32_t size) : Column() {
            _size = size;
            blocks.push_back(CompressedColumnBlock(v, 0, size - 1));
        }

        CompressedColumn(std::vector<CompressedColumnBlock> &blocks,
                const size_t size) : _size(size) {
            this->blocks.swap(blocks);
        }

        size_t size() const {
            return _size;
        }

        size_t estimateSize() const {
            return _size;
        }

        Term_t getValue(const size_t pos) const;

        bool supportsDirectAccess() const {
            return true;
        }

        bool isEmpty() const {
            return _size == 0;
        }

        std::unique_ptr<ColumnReader> getReader() const;

        bool isEDB() const {
            return false;
        }

        std::shared_ptr<Column> sort() const;

        std::shared_ptr<Column> sort_and_unique() const;

        std::shared_ptr<Column> sort(const int nthreads) const;

        std::shared_ptr<Column> sort_and_unique(const int nthreads) const;

        std::shared_ptr<Column> unique() const;

        bool isIn(const Term_t t) const;

        bool isConstant() const {
            assert(_size > 0);
            return blocks.size() == 1 && blocks.back().delta == 0;
        }
};
//----- END COMPRESSED COLUMN ----------

class ColumnWriter {
    private:
        bool cached;
        std::shared_ptr<Column> cachedColumn;

        std::vector<CompressedColumnBlock> blocks;
        std::vector<Term_t> values;
        size_t _size;
        Term_t lastv;
        bool compressed;

    public:
        ColumnWriter() : cached(false), _size(0), lastv((Term_t) - 1), compressed(true) {}

        ColumnWriter(std::vector<Term_t> &values) : cached(false), _size(values.size()), compressed(false) {
            this->values.swap(values);
            lastv = _size > 0 ? this->values[this->values.size()-1] : (Term_t) -1;
        }

        void add(const uint64_t v) {
#ifdef DEBUG
            if (cached)
                throw 10;
#endif

#ifdef USE_COMPRESSED_COLUMNS
            if (! compressed) {
                values.push_back((Term_t) v);
            } else {
                if (isEmpty()) {
                    blocks.push_back(CompressedColumnBlock((Term_t) v, 0, 0));
                } else {
                    CompressedColumnBlock *b = &blocks.back();
                    if (v == lastv + b->delta) {
                        b->size++;
                    } else if (b->size == 0) {
                        b->delta = v - b->value;
                        b->size++;
                    } else {
                        blocks.push_back(CompressedColumnBlock((Term_t) v, 0, 0));
                        if (_size > 16384 && blocks.size() > _size / 4) {
                            // Compression not very effective; convert to uncompressed
                            compressed = false;
                            CompressedColumn col(blocks, /*offsetsize, deltas,*/ _size);
                            values = col.getReader()->asVector();
                            blocks.clear();
                        }
                    }
                }
            }
#else
            values.push_back((Term_t) v);
#endif
            lastv = v;
            _size++;
        }

        bool isEmpty() const { return _size == 0; }

        size_t size() const { return _size; }

        Term_t lastValue() const {
            return lastv;
        }

        void concatenate(Column *c);

        std::shared_ptr<Column> getColumn();

        static std::shared_ptr<Column> getColumn(std::vector<Term_t> &values, bool isSorted);
};

//----- END GENERIC INTERFACES -------

class ColumnReaderImpl : public ColumnReader {
    private:
        /*uint32_t beginRange, endRange;
          uint64_t lastBasePos;
          const int32_t *lastDelta;*/

        const std::vector<CompressedColumnBlock> &blocks;
        const size_t _size;

        size_t currentBlock;
        size_t posInBlock;

        //Term_t get(const size_t pos);

    public:
        ColumnReaderImpl(const std::vector<CompressedColumnBlock> &blocks,
                const size_t size) : /*beginRange(0), endRange(0),*/
            blocks(blocks), /*offsetsize(offsetsize), deltas(deltas),*/
            _size(size), currentBlock(0), posInBlock(0) {
            }

        Term_t first();

        Term_t last();

        std::vector<Term_t> asVector();

        bool hasNext();

        Term_t next();

        void clear() {
        }
};

//----- INMEMORY COLUMN ----------
class InmemColumnReader : public ColumnReader {
    private:
        const std::vector<Term_t> &col;
        size_t currentPos;
        size_t end;

    public:
        InmemColumnReader(const std::vector<Term_t> &vals) : col(vals),
        currentPos(0), end(vals.size()) {
        }

        InmemColumnReader(const std::vector<Term_t> &vals,
                uint64_t start, uint64_t len) : col(vals),
        currentPos(start), end(start + len) {
        }

        Term_t first() {
            return col.front();
        }

        Term_t last() {
            return col.back();
        }

        std::vector<Term_t> asVector() {
            return col;
        }

        bool hasNext() {
            return currentPos < end;
        }

        Term_t next() {
            return col[currentPos++];
        }

        void clear() {
        }
};

class InmemoryColumn : public Column {
    private:
        std::vector<Term_t> values;

    public:
        InmemoryColumn(std::vector<Term_t> &v) : InmemoryColumn(v, false) {
        }

        InmemoryColumn(std::vector<Term_t> &v, bool swap) {
            if (swap)
                values.swap(v);
            else
                values = v;
        }

        size_t size() const {
            return values.size();
        }

        size_t estimateSize() const {
            return values.size();
        }

        bool isEmpty() const {
            return values.empty();
        }

        Term_t getValue(const size_t pos) const {
            return values[pos];
        }

        bool supportsDirectAccess() const {
            return true;
        }

        bool isBackedByVector() {
            return true;
        }

        const std::vector<Term_t> &getVectorRef() {
            return values;
        }


        bool isEDB() const {
            return false;
        }

        bool containsDuplicates() const {
            return values.size() > 1;
        }

        std::unique_ptr<ColumnReader> getReader() const {
            return std::unique_ptr<ColumnReader>(new InmemColumnReader(this->values));
        }

        std::shared_ptr<Column> sort() const {
            std::vector<Term_t> newvals = values;
            std::sort(newvals.begin(), newvals.end());
            return std::shared_ptr<Column>(new InmemoryColumn(newvals, true));
        }

        std::shared_ptr<Column> sort_and_unique() const {
            std::vector<Term_t> newvals = values;
            std::sort(newvals.begin(), newvals.end());
            auto last = std::unique(newvals.begin(), newvals.end());
            newvals.erase(last, newvals.end());
            newvals.shrink_to_fit();
            return std::shared_ptr<Column>(new InmemoryColumn(newvals, true));
        }

        std::shared_ptr<Column> sort(const int nthreads) const {
            if (nthreads <= 1) {
                return sort();
            }
            std::vector<Term_t> newvals = values;
            //TBB is configured at the beginning to use nthreads to parallelize the computation
            ParallelTasks::sort_int(newvals.begin(), newvals.end());
            return std::shared_ptr<Column>(new InmemoryColumn(newvals, true));
        }

        std::shared_ptr<Column> sort_and_unique(const int nthreads) const {
            if (nthreads <= 1) {
                return sort_and_unique();
            }
            std::vector<Term_t> newvals = values;
            ParallelTasks::sort_int(newvals.begin(), newvals.end());
            auto last = std::unique(newvals.begin(), newvals.end());
            newvals.erase(last, newvals.end());
            newvals.shrink_to_fit();
            return std::shared_ptr<Column>(new InmemoryColumn(newvals, true));
        }

        std::shared_ptr<Column> unique() const {
            //I assume the column is already sorted
            std::vector<Term_t> newvals;
            newvals.reserve(values.size());
            Term_t prev = (Term_t) - 1;
            for (size_t i = 0; i < values.size(); i++) {
                Term_t v = values[i];
                if (v != prev) {
                    newvals.push_back(v);
                    prev = v;
                }
            }
            newvals.shrink_to_fit();

            return std::shared_ptr<Column>(new InmemoryColumn(newvals, true));
        }

        bool isConstant() const {
            return values.size() < 2;
        }

        bool isIn(const Term_t t) const {
            /*
               if (values.size() > 100) {
               LOG(WARNL) << "InmemoryColumn::isIn(): Performance problem. Linear search on array of " << values.size() << " elements";
               }
               for(size_t i = 0; i < values.size(); ++i) {
               if (values[i] == t)
               return true;
               }
               return false;
               */
            return std::binary_search(values.begin(), values.end(), t);
        }
};
//----- END INMEMORY COLUMN ----------

// START SUBCOLUMN (contains a subrange of an inmemory column)
class SubColumn : public Column {
    private:
        std::shared_ptr<Column> parentColumn;
        const std::vector<Term_t> &values;
        const uint64_t start, len;
    public:
        SubColumn(std::shared_ptr<Column> parentColumn,
                uint64_t start, uint64_t len) : parentColumn(parentColumn),
        values(parentColumn->getVectorRef()),
        start(start), len(len) {
        }

        size_t size() const {
            return len;
        }

        size_t estimateSize() const {
            return len;
        }

        bool isEmpty() const {
            return len == 0;
        }

        Term_t getValue(const size_t pos) const {
            return values[start + pos];
        }

        bool supportsDirectAccess() const {
            return true;
        }

        bool isBackedByVector() {
            return false;
        }

        bool isEDB() const {
            return false;
        }

        bool containsDuplicates() const {
            return len > 1;
        }

        std::unique_ptr<ColumnReader> getReader() const {
            return std::unique_ptr<ColumnReader>(new InmemColumnReader(
                        this->values, start, len));
        }

        std::shared_ptr<Column> sort() const {
            std::vector<Term_t> newvals;
            for(uint64_t i = start; i < start + len; ++i) {
                newvals.push_back(values[i]);
            }
            std::sort(newvals.begin(), newvals.end());
            return std::shared_ptr<Column>(new InmemoryColumn(newvals, true));
        }

        std::shared_ptr<Column> sort_and_unique() const {
            std::vector<Term_t> newvals;
            for(uint64_t i = start; i < start + len; ++i) {
                newvals.push_back(values[i]);
            }
            std::sort(newvals.begin(), newvals.end());
            auto last = std::unique(newvals.begin(), newvals.end());
            newvals.erase(last, newvals.end());
            newvals.shrink_to_fit();
            return std::shared_ptr<Column>(new InmemoryColumn(newvals, true));
        }

        std::shared_ptr<Column> sort(const int nthreads) const {
            if (nthreads <= 1) {
                return sort();
            }
            std::vector<Term_t> newvals;
            for(uint64_t i = start; i < start + len; ++i) {
                newvals.push_back(values[i]);
            }
            //Uses all cores!
            ParallelTasks::sort_int(newvals.begin(), newvals.end());
            return std::shared_ptr<Column>(new InmemoryColumn(newvals, true));
        }

        std::shared_ptr<Column> sort_and_unique(const int nthreads) const {
            if (nthreads <= 1) {
                return sort_and_unique();
            }
            std::vector<Term_t> newvals;
            for(uint64_t i = start; i < start + len; ++i) {
                newvals.push_back(values[i]);
            }
            //Uses all cores!
            ParallelTasks::sort_int(newvals.begin(), newvals.end());
            auto last = std::unique(newvals.begin(), newvals.end());
            newvals.erase(last, newvals.end());
            newvals.shrink_to_fit();
            return std::shared_ptr<Column>(new InmemoryColumn(newvals, true));
        }

        std::shared_ptr<Column> unique() const {
            //I assume the column is already sorted
            std::vector<Term_t> newvals;
            newvals.reserve(len);
            Term_t prev = (Term_t) - 1;
            for (size_t i = start; i < start + len; i++) {
                Term_t v = values[i];
                if (v != prev) {
                    newvals.push_back(v);
                    prev = v;
                }
            }
            newvals.shrink_to_fit();
            return std::shared_ptr<Column>(new InmemoryColumn(newvals, true));
        }

        bool isConstant() const {
            return len < 2;
        }

        bool isIn(const Term_t t) const {
            return std::binary_search(values.begin() + start,
                    values.begin() + start + len, t);
        }
};
//----- END SUBCOLUMN ----------


//----- EDB COLUMN ----------
class EDBColumnReader : public ColumnReader {
    private:
        const Literal &l;
        EDBLayer &layer;
        const uint8_t posColumn;
        const std::vector<uint8_t> presortPos;
        const bool unq;
        const uint8_t posInItr;

        EDBIterator *itr;

        //Cached values
        Term_t firstCached;
        Term_t lastCached;

        static std::vector<Term_t> load(const Literal &l,
                const uint8_t posColumn,
                const std::vector<uint8_t> presortPos,
                EDBLayer &layer, const bool unq);


        void setupItr();

    public:
        EDBColumnReader(const Literal &l, const uint8_t posColumn,
                const std::vector<uint8_t> presortPos,
                EDBLayer &layer, const bool unq);

        Term_t first();

        Term_t last();

        std::vector<Term_t> asVector();

        bool hasNext();

        Term_t next();

        const char *getUnderlyingArray();
        std::pair<uint8_t, std::pair<uint8_t, uint8_t>> getSizeElemUnderlyingArray();

        size_t size();

        void clear() {
            //Release the iterator
            if (itr != NULL) {
                layer.releaseIterator(itr);
            }
            itr = NULL;
        }

        ~EDBColumnReader() {
            clear();
        }
};


class EDBColumn : public Column {
    private:
        EDBLayer &layer;
        const Literal l;
        const uint8_t posColumn;
        const std::vector<uint8_t> presortPos;
        bool unq;

        EDBColumn(const EDBColumn &el) : layer(el.layer),
        l(el.l), posColumn(el.posColumn), presortPos(el.presortPos),
        unq(el.unq) {
        }

        std::shared_ptr<Column> clone() const;

    public:
        EDBColumn(EDBLayer &layer, const Literal &l, uint8_t posColumn,
                const std::vector<uint8_t> presortPos, const bool unq);

        size_t size() const;

        size_t estimateSize() const;

        bool isEmpty() const {
            return false;
        }

        bool isEDB() const {
            return true;
        }

        bool supportsDirectAccess() const {
            return false;
        }

        Term_t getValue(const size_t pos) const {
            throw 10;
        }

        EDBLayer &getEDBLayer() {
            return layer;
        }

        const Literal &getLiteral() const {
            return l;
        }

        uint8_t posColumnInLiteral() const {
            return posColumn;
        }

        std::vector<uint8_t> getPresortPos() const {
            return presortPos;
        }

        bool containsDuplicates() const {
            return !unq;
        }

        bool isIn(const Term_t t) const;

        std::unique_ptr<ColumnReader> getReader() const;

        std::shared_ptr<Column> sort() const;

        std::shared_ptr<Column> sort(const int nthreads) const;

        std::shared_ptr<Column> unique() const;

        bool isConstant() const;

        //Returns a Trident column -- hack to get data to export
        const char *getUnderlyingArray() const;
        std::pair<uint8_t, std::pair<uint8_t, uint8_t>> getSizeElemUnderlyingArray() const;

};
//----- END EDB COLUMN ----------
//
//----- FUNCTIONAL COLUMN ----------
class ChaseMgmt;
class FunctionalColumn : public Column {
    private:
        uint64_t nvalues;
        uint64_t startvalue;
        std::shared_ptr<ChaseMgmt> chase;

        FunctionalColumn(const FunctionalColumn &c) {
            nvalues = c.nvalues;
            startvalue = c.startvalue;
            chase = c.chase;
        }

    public:
        FunctionalColumn(std::shared_ptr<ChaseMgmt> chase,
                std::vector<std::shared_ptr<Column>> &columns);

        bool isEmpty() const;

        bool isEDB() const;

        size_t size() const;

        size_t estimateSize() const;

        Term_t getValue(const size_t pos) const;

        bool supportsDirectAccess() const;

        bool isIn(const Term_t t) const;

        std::unique_ptr<ColumnReader> getReader() const;

        std::shared_ptr<Column> sort() const;

        std::shared_ptr<Column> sort(const int nthreads) const;

        std::shared_ptr<Column> unique() const;

        bool isConstant() const;
};

#endif
