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

#include <vlog/column.h>
#include <vlog/qsqquery.h>

#include <boost/log/trivial.hpp>
#include <boost/chrono.hpp>

#include <iostream>
#include <inttypes.h>

/*CompressedColumn::CompressedColumn(const CompressedColumn &o) : blocks(o.blocks), offsetsize(o.offsetsize),
    deltas(o.deltas), _size(o._size) {
}*/

std::shared_ptr<Column> CompressedColumn::sort() const {
    //boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();

    std::vector<Term_t> newValues;
    std::unique_ptr<ColumnReader> reader = getReader();
    /*for (uint32_t i = 0; i < _size; ++i) {
        newValues.push_back(reader->get(i));
    }*/
    while (reader->hasNext()) {
        newValues.push_back(reader->next());
    }
    std::sort(newValues.begin(), newValues.end());
    //auto last = std::unique(newValues.begin(), newValues.end());
    //newValues.erase(last, newValues.end());

    ColumnWriter writer(newValues);

    //boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    //BOOST_LOG_TRIVIAL(debug) << "Time sorting = " << sec.count() * 1000 << " size()=" << _size;

    return writer.getColumn();
}

std::shared_ptr<Column> CompressedColumn::unique() const {
    //This method assumes the vector is already sorted
    //Therefore, I only need to reset all 0 delta blocks
    auto newblocks = blocks;
    size_t newsize = _size;
    for (auto &el : newblocks) {
        if (el.delta == 0) {
            newsize -= el.size;
            el.size = 0;
        }
    }
    return std::shared_ptr<Column>(new CompressedColumn(newblocks, newsize));
}

std::unique_ptr<ColumnReader> CompressedColumn::getReader() const {
    return std::unique_ptr<ColumnReader>(new ColumnReaderImpl(
            blocks, _size));
}

Term_t CompressedColumn::getValue(const size_t pos) const {
    //Linear search -- inefficient. I can improve it with binary search
    size_t p = 0;
    for (const auto &block : blocks) {
        if (pos < p + block.size + 1) {
            return block.value + (pos - p) * block.delta;
        }
        p += block.size + 1;
    }

    throw 10;
}

bool ColumnReaderImpl::hasNext() {
    return currentBlock < blocks.size() - 1 ||
           posInBlock < blocks.back().size + 1;
}

Term_t ColumnReaderImpl::next() {
    if (posInBlock == 0) {
        posInBlock++;
        return blocks[currentBlock].value;
    } else {
        if (posInBlock == blocks[currentBlock].size + 1) {
            //Move to the next block
            assert(currentBlock < blocks.size() - 1);
            currentBlock++;
            posInBlock = 1;
            return blocks[currentBlock].value;
        } else {
            return blocks[currentBlock].value + blocks[currentBlock].delta *
                   posInBlock++;
        }
    }
}

/*Term_t ColumnReaderImpl::get(const size_t pos) {

    if (pos >= beginRange && pos < endRange) {
        if (lastDelta == NULL || pos == beginRange) {
            return lastBasePos;
        } else {
            return lastBasePos + lastDelta[pos - beginRange - 1];
        }
    }

    const uint32_t blockIdx = pos / offsetsize;
    const uint32_t offset = pos % offsetsize;
    Term_t returnedValue;
    if (offset == 0 || blocks[blockIdx].idxDelta == -1) {
        returnedValue = blocks[blockIdx].value;
    } else {
        //Delta - 32bit
        returnedValue = (Term_t)((int64_t)blocks[blockIdx].value +
                                   (int32_t)deltas[blocks[blockIdx].idxDelta + offset - 1]);
    }

    beginRange = blockIdx * offsetsize;
    endRange = beginRange + offsetsize;
    lastBasePos = blocks[blockIdx].value;
    if (blocks[blockIdx].idxDelta == -1) {
        lastDelta = NULL;
    } else {
        lastDelta = &(deltas[blocks[blockIdx].idxDelta]);
    }

    return returnedValue;
}*/

std::vector<Term_t> ColumnReaderImpl::asVector() {
    std::vector<Term_t> output;
    output.reserve(_size);

    for (std::vector<CompressedColumnBlock>::const_iterator itr = blocks.begin();
            itr != blocks.end(); ++itr) {
        output.push_back(itr->value);
        for(int32_t i = 1; i < itr->size + 1; ++i) {
            output.push_back(itr->value + itr->delta * i);
        }
    }
    return output;
}

Term_t ColumnReaderImpl::last() {
    return blocks.back().value + blocks.back().delta * blocks.back().size;
}

Term_t ColumnReaderImpl::first() {
    return blocks.front().value;
}

EDBColumn::EDBColumn(EDBLayer &edb, const Literal &lit, uint8_t posColumn,
                     const std::vector<uint8_t> presortPos, const bool unq) :
    layer(edb),
    l(lit),
    posColumn(posColumn),
    presortPos(presortPos),
    unq(unq) {
    assert(!unq || presortPos.empty());
}

size_t EDBColumn::estimateSize() const {
    QSQQuery query(l);
    if (!unq) {
        return layer.getCardinality(&query);
    } else {
        return layer.getCardinalityColumn(&query, posColumn);
    }
}

size_t EDBColumn::size() const {
    QSQQuery query(l);
    if (!unq) {
        if (l.getNVars() == 2) {
            return layer.getCardinality(&query);
        } else {
            /*BOOST_LOG_TRIVIAL(warning) << "Must go through all the column"
                                       " to count the size";*/
            return getReader()->asVector().size();
        }
    } else {
        if (l.getNVars() == 2) {
            return layer.getCardinalityColumn(&query, posColumn);
        } else {
            BOOST_LOG_TRIVIAL(warning) << "Must go through all the column"
                                       " to count the size";
            return getReader()->asVector().size();
        }
    }
}

std::shared_ptr<Column> EDBColumn::sort() const {
    if (presortPos.empty()) {
        return clone(); //Should be always sorted
    } else {
        return std::shared_ptr<Column>(new EDBColumn(layer, l, posColumn,
                                       std::vector<uint8_t>(), unq));
    }
}

std::shared_ptr<Column> EDBColumn::unique() const {
    if (unq || l.getNVars() == 1) { //The second condition is used to avoid
        //to set the unq flag to unique when the data is already unique
        return clone();
    } else {
        std::vector<uint8_t> presortPos;
        return std::shared_ptr<Column>(new EDBColumn(layer, l, posColumn,
                                       presortPos, true));
    }
}

std::shared_ptr<Column> EDBColumn::clone() const {
    return std::shared_ptr<Column>(new EDBColumn(*this));
}

bool EDBColumn::isConstant() const {
    return false;
}

std::unique_ptr<ColumnReader> EDBColumn::getReader() const {
    return std::unique_ptr<ColumnReader>(new EDBColumnReader(l, posColumn,
                                         presortPos, layer, unq));
}

void EDBColumnReader::setupItr() {
    if (itr == NULL) {
        QSQQuery query(l);
        std::vector<uint8_t> fields;
        for (int i = 0; i < presortPos.size(); ++i) {
            fields.push_back(presortPos[i]);
        }
        fields.push_back(posColumn);
        itr = layer.getSortedIterator(&query, fields);
        if (unq) {
            itr->skipDuplicatedFirstColumn();
        }
    }
}

std::vector<Term_t> EDBColumnReader::load(const Literal &l,
        const uint8_t posColumn,
        const std::vector<uint8_t> presortPos,
        EDBLayer & layer, const bool unq) {

    boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
    QSQQuery query(l);
    std::vector<uint8_t> fields;
    for (int i = 0; i < presortPos.size(); ++i) {
        fields.push_back(presortPos[i]);
    }
    fields.push_back(posColumn);
    std::vector<Term_t> values;

    EDBIterator *itr = layer.getSortedIterator(&query, fields);
    const uint8_t posInItr = l.getPosVars()[posColumn];
    Term_t prev = (Term_t) -1;
    while (itr->hasNext()) {
        itr->next();
	Term_t el = itr->getElementAt(posInItr);
        if (!unq || el != prev) {
            values.push_back(el);
            prev = el;
        }
    }
    layer.releaseIterator(itr);
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(debug) << "Time loading a vector of " << values.size() << " is " << sec.count() * 1000;
    return values;
}

Term_t EDBColumnReader::last() {
    if (lastCached == (Term_t) -1) {
        lastCached = load(l, posColumn, presortPos, layer, unq).back();
    }
    return lastCached;
}

Term_t EDBColumnReader::first() {
    if (firstCached == (Term_t) -1) {
        QSQQuery query(l);
        std::vector<uint8_t> fields;
        fields.push_back(posColumn);
        EDBIterator *itr = layer.getSortedIterator(&query, fields);
        if (itr->hasNext()) {
            itr->next();
            firstCached = itr->getElementAt(posInItr);
        } else {
            throw 10; //I'm asking first to an empty iterator
        }
	layer.releaseIterator(itr);
    }
    return firstCached;
}

bool EDBColumnReader::hasNext() {
    setupItr();
    bool resp = itr->hasNext();
    if (!resp) {
        //release the itr
        layer.releaseIterator(itr);
        itr = NULL;
    }
    return resp;
}

Term_t EDBColumnReader::next() {
    itr->next();
    return itr->getElementAt(posInItr);
}

std::vector<Term_t> EDBColumnReader::asVector() {
    return load(l, posColumn, presortPos, layer, unq);
}

ColumnWriter::ColumnWriter(std::vector<Term_t> &values) :  ColumnWriter() {
    for (auto &value : values) {
        add(value);
    }
}

void ColumnWriter::add(const uint64_t v) {
    if (cached)
        throw 10;

    if (isEmpty()) {
        blocks.push_back(CompressedColumnBlock((Term_t) v, 0, 0));
    } else {
        if (v == lastValue() + blocks.back().delta) {
            blocks.back().size++;
        } else {
            if (blocks.back().size == 0) {
                blocks.back().delta = v - blocks.back().value;
                blocks.back().size++;
            } else {
                blocks.push_back(CompressedColumnBlock((Term_t) v, 0, 0));
            }
        }
    }
    _size++;
}

bool ColumnWriter::isEmpty() const {
    return _size == 0;
}

size_t ColumnWriter::size() const {
    return _size;
}

Term_t ColumnWriter::lastValue() const {
    return blocks.back().value + blocks.back().size * blocks.back().delta;
}

void ColumnWriter::concatenate(Column * c) {
    std::unique_ptr<ColumnReader> reader = c->getReader();
    while (reader->hasNext()) {
        add(reader->next());
    }
}

std::shared_ptr<Column> ColumnWriter::getColumn() {
    if (cached) {
        //The column was already being requested
        return cachedColumn;
    }
    cached = true;

    if (blocks.size() < _size / 5) {
        cachedColumn = std::shared_ptr<Column>(new CompressedColumn(
                blocks, _size));
    } else {
        CompressedColumn col(blocks, /*offsetsize, deltas,*/ _size);
        std::vector<Term_t> values = col.getReader()->asVector();
        cachedColumn = std::shared_ptr<Column>(new InmemoryColumn(values));
    }
    return cachedColumn;
}

/*std::shared_ptr<Column> ColumnWriter::compress(
    const std::vector<Term_t> &values) {

    bool shouldCompress = false;
    //I should compress the table only if the number of unique terms is much
    //smaller than the number of total terms. I use simple heuristics to
    //determine that
    if (values.empty() || values.front() == values.back()) {
        shouldCompress = true;
    }


    if (shouldCompress) {
        std::vector<CompressedColumnBlock> blocks;
        uint32_t offsetsize = 1;
        std::vector<int32_t> deltas;

        for (size_t xxx = 0; xxx < values.size(); ++xxx) {
            Term_t v = values[xxx];
            int32_t currentSize = xxx % offsetsize;
            if (currentSize == 0) {
                assert(offsetsize >= COLCOMPRB || blocks.size() == 0);
                //Create new block and add element as first
                blocks.push_back(CompressedColumnBlock(v, -1));
            } else {
                //Add the value in the current block
                //Ceriel: What if v - beginvalue does not fit in an uint32_t?
                const Term_t beginValue = blocks.back().value;
                if (blocks.back().idxDelta == -1) {
                    if (v != beginValue) {
                        blocks.back().idxDelta = deltas.size();
                        //Convert the block to deltas
                        //Add all existing values
                        while (currentSize > 1) { //Insert all minus one
                            deltas.push_back(0);
                            currentSize--;
                        }
                        deltas.push_back(v - beginValue);
                    }
                } else {
                    deltas.push_back(v - beginValue);
                }
            }

            if (offsetsize < COLCOMPRB || (blocks.size() == 1
                                           && blocks[0].idxDelta == -1)) {
                offsetsize++;
            }
        }

        return std::shared_ptr<Column>(new CompressedColumn(blocks, offsetsize,
                                       deltas, values.size()));
    } else {
        return std::shared_ptr<Column>(new InmemoryColumn(values));
    }
}*/

void Column::intersection(std::shared_ptr<Column> c1,
                          std::shared_ptr<Column> c2, ColumnWriter &writer) {
    std::unique_ptr<ColumnReader> r1 = c1->getReader();
    std::unique_ptr<ColumnReader> r2 = c2->getReader();
    Term_t v1, v2;
    bool ok1 = r1->hasNext();
    if (ok1)
        v1 = r1->next();
    bool ok2 = r2->hasNext();
    if (ok2)
        v2 = r2->next();

    long count1 = ok1 ? 1 : 0;
    long count2 = ok2 ? 1 : 0;
    long countout = 0;

    //boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();


    while (ok1 && ok2) {
        if (v1 < v2) {
            ok1 = r1->hasNext();
            if (ok1) {
                v1 = r1->next();
                count1++;
            }
        } else if (v1 > v2) {
            ok2 = r2->hasNext();
            if (ok2) {
                v2 = r2->next();
                count2++;
            }
        } else {
            writer.add(v1);
            countout++;
            ok2 = r2->hasNext();
            if (ok2) {
                v2 = r2->next();
                count2++;
            }
        }
    }
    //boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    //BOOST_LOG_TRIVIAL(info) << "Count1=" << count1 << " Count2=" << count2 <<
    //                        " countout=" << countout << " " << sec.count() * 1000;
}
