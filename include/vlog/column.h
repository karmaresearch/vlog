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

#ifndef _COLUMN_H
#define _COLUMN_H

#include <vlog/concepts.h>
#include <vlog/edb.h>

#include <inttypes.h>
#include <assert.h>
#include <algorithm>
#include <trident/storage/pairhandler.h>

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

struct CompressedColumnBlock;
class ColumnWriter : public SequenceWriter {
private:
    bool cached;
    std::shared_ptr<Column> cachedColumn;

    std::vector<CompressedColumnBlock> blocks;
    size_t _size;

public:
    ColumnWriter() : cached(false), _size(0) {}

    ColumnWriter(std::vector<Term_t> &values);

    void add(const uint64_t v);		// No Term_t: overrides method in trident

    bool isEmpty() const;

    size_t size() const;

    Term_t lastValue() const;

    void concatenate(Column *c);

    std::shared_ptr<Column> getColumn();

    static std::shared_ptr<Column> compress(const std::vector<Term_t> &values);
};

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

    virtual std::unique_ptr<ColumnReader> getReader() const = 0;

    virtual std::shared_ptr<Column> sort() const = 0;

    virtual std::shared_ptr<Column> unique() const = 0;

    virtual bool isConstant() const = 0;

    static void intersection(
        std::shared_ptr<Column> c1,
        std::shared_ptr<Column> c2,
        ColumnWriter &writer);
};
//----- END GENERIC INTERFACES -------

//----- COMPRESSED COLUMN ----------
#define COLCOMPRB 1000000
struct CompressedColumnBlock {
    const Term_t value;
    int32_t delta;
    size_t size;

    CompressedColumnBlock(const Term_t value,
                          const int32_t delta,
                          size_t size) : value(value),
        delta(delta), size(size) {}
};

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

    std::shared_ptr<Column> unique() const;

    bool isConstant() const {
        assert(_size > 0);
        return blocks.size() == 1 && blocks.back().delta == 0;
    }
};
//----- END COMPRESSED COLUMN ----------

//----- INMEMORY COLUMN ----------
class InmemColumnReader : public ColumnReader {
private:
    const std::vector<Term_t> &col;
    size_t currentPos;

public:
    InmemColumnReader(const std::vector<Term_t> &vals) : col(vals),
        currentPos(0) {
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
        return currentPos < col.size();
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
    InmemoryColumn(std::vector<Term_t> v) {
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
        return std::shared_ptr<Column>(new InmemoryColumn(newvals));
    }

    std::shared_ptr<Column> unique() const {
        //I assume the column is already sorted
        std::vector<Term_t> newvals = values;
        auto last = std::unique(newvals.begin(), newvals.end());
        newvals.erase(last, newvals.end());
        return std::shared_ptr<Column>(new InmemoryColumn(newvals));
    }

    bool isConstant() const {
        return values.size() < 2;
    }
};
//----- END INMEMORY COLUMN ----------

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
                    EDBLayer &layer, const bool unq)
        : l(l), layer(layer), posColumn(posColumn), presortPos(presortPos),
          unq(unq),
          posInItr(l.getPosVars()[posColumn]), itr(NULL), firstCached((Term_t) -1),
          lastCached((Term_t) -1) {
    }

    Term_t first();

    Term_t last();

    std::vector<Term_t> asVector();

    bool hasNext();

    Term_t next();

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

    std::unique_ptr<ColumnReader> getReader() const;

    std::shared_ptr<Column> sort() const;

    std::shared_ptr<Column> unique() const;

    bool isConstant() const;
};
//----- END EDB COLUMN ----------

#endif
