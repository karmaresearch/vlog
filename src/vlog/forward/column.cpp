#include <vlog/column.h>
#include <vlog/segment.h>
#include <vlog/qsqquery.h>
#include <vlog/trident/tridentiterator.h>
#include <kognac/utils.h>

#include <iostream>
#include <inttypes.h>

/*CompressedColumn::CompressedColumn(const CompressedColumn &o) : blocks(o.blocks), offsetsize(o.offsetsize),
  deltas(o.deltas), _size(o._size) {
  }*/

bool CompressedColumn::isIn(const Term_t t) const {
    //Not implemented yet. Assume the element is there.
    return true;
}

std::shared_ptr<Column> CompressedColumn::sort() const {
    //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    std::unique_ptr<ColumnReader> reader = getReader();
    std::vector<Term_t> newValues = reader->asVector();
    std::sort(newValues.begin(), newValues.end());

    ColumnWriter writer(newValues);

    //std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    //LOG(TRACEL) << "Time sorting = " << sec.count() * 1000 << " size()=" << _size;

    return writer.getColumn();
}

std::shared_ptr<Column> CompressedColumn::sort_and_unique() const {
    //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    std::unique_ptr<ColumnReader> reader = getReader();
    std::vector<Term_t> newValues = reader->asVector();
    std::sort(newValues.begin(), newValues.end());
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    auto last = std::unique(newValues.begin(), newValues.end());
    newValues.erase(last, newValues.end());
    newValues.shrink_to_fit();
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(TRACEL) << "Time std::unique = " << sec.count() * 1000 << " size()=" << _size;

    //std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    //LOG(TRACEL) << "Time sorting = " << sec.count() * 1000 << " size()=" << _size;
    return std::shared_ptr<Column>(new InmemoryColumn(newValues, true));
}

std::shared_ptr<Column> CompressedColumn::sort(const int nthreads) const {
    if (nthreads <= 1) {
        return sort();
    }

    //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    std::unique_ptr<ColumnReader> reader = getReader();
    std::vector<Term_t> newValues = reader->asVector();

    if (newValues.size() > 4096) {
        ParallelTasks::sort_int(newValues.begin(), newValues.end());
    } else {
        std::sort(newValues.begin(), newValues.end());
    }

    ColumnWriter writer(newValues);
    //std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    //LOG(TRACEL) << "Time sorting = " << sec.count() * 1000 << " size()=" << _size;
    return writer.getColumn();

}

std::shared_ptr<Column> CompressedColumn::sort_and_unique(const int nthreads) const {
    if (nthreads <= 1) {
        return sort_and_unique();
    }

    // First, remove the easy duplicates
    auto newblocks = blocks;
    size_t newsize = _size;
    for (auto &el : newblocks) {
        if (el.delta == 0) {
            newsize -= el.size;
            el.size = 0;
        }
    }

    CompressedColumn *col = new CompressedColumn(newblocks, newsize);
    //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    std::unique_ptr<ColumnReader> reader = col->getReader();
    std::vector<Term_t> newValues = reader->asVector();
    delete col;

    if (newValues.size() > 4096) {
        ParallelTasks::sort_int(newValues.begin(), newValues.end());
    } else {
        std::sort(newValues.begin(), newValues.end());
    }

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    auto last = std::unique(newValues.begin(), newValues.end());
    newValues.erase(last, newValues.end());
    newValues.shrink_to_fit();
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(TRACEL) << "Time std::unique = " << sec.count() * 1000 << " size()=" << _size;
    //std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    //LOG(TRACEL) << "Time sorting = " << sec.count() * 1000 << " size()=" << _size;
    return std::shared_ptr<Column>(new InmemoryColumn(newValues, true));
}

std::shared_ptr<Column> CompressedColumn::unique() const {
    //This method assumes the vector is already sorted
    //Therefore, I only need to reset all 0 delta blocks
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    auto newblocks = blocks;
    size_t newsize = _size;
    for (auto &el : newblocks) {
        if (el.delta == 0) {
            newsize -= el.size;
            el.size = 0;
        }
    }
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(TRACEL) << "Time CompressedColumn::unique = " << sec.count() * 1000 << " size()=" << _size;
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
        if (itr->delta == 0) {
            for (int32_t i = 1; i <= itr->size; ++i) {
                output.push_back(itr->value);
            }
        } else {
            for (int32_t i = 1; i <= itr->size; ++i) {
                output.push_back(itr->value + itr->delta * i);
            }
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
        return layer.getCardinality(*query.getLiteral());
    } else {
        return layer.getCardinalityColumn(*query.getLiteral(), posColumn);
    }
}

size_t EDBColumn::size() const {
    QSQQuery query(l);
    size_t retval;
    if (!unq) {
        if (l.getNVars() == 2) {
            retval = layer.getCardinality(*query.getLiteral());
        } else if (l.getNVars() == 1) {
            retval = layer.getCardinality(*query.getLiteral());
        } else {
            retval = EDBColumnReader(l, posColumn, presortPos, layer, unq).size();
        }
    } else {
        if (l.getNVars() == 2) {
            retval = layer.getCardinalityColumn(*query.getLiteral(), posColumn);
        } else {
            LOG(WARNL) << "Must go through all the column"
                " to count the size";
            retval = EDBColumnReader(l, posColumn, presortPos, layer, unq).size();
        }
    }
#if DEBUG
    size_t sz = getReader()->asVector().size();
    if (sz != retval) {
        LOG(TRACEL) << "query = " << l.tostring();
        LOG(TRACEL) << "sz = " << sz << ", should be " << retval;
        LOG(TRACEL) << "unq = " << unq << ", l.getNVars = " << (int) l.getNVars();
        throw 10;
    }
#endif
    return retval;
}

std::shared_ptr<Column> EDBColumn::sort(const int nthreads) const {
    //nthreads is ignored, because this computation does not require any parallelism
    if (presortPos.empty()) {
        return clone(); //Should be always sorted
    } else {
        return std::shared_ptr<Column>(new EDBColumn(layer, l, posColumn,
                    std::vector<uint8_t>(), unq));
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

bool EDBColumn::isIn(const Term_t t) const {
    VTuple tuple = l.getTuple();
    tuple.set(VTerm(0, t), posColumn);
    return layer.getCardinalityColumn(Literal(l.getPredicate(), tuple),
            posColumn) > 0;
}

std::unique_ptr<ColumnReader> EDBColumn::getReader() const {
    return std::unique_ptr<ColumnReader>(new EDBColumnReader(l, posColumn,
                presortPos, layer, unq));
}

const char *EDBColumn::getUnderlyingArray() const {
    EDBColumnReader reader(l, posColumn, presortPos, layer, unq);
    return reader.getUnderlyingArray();
}

std::pair<uint8_t, std::pair<uint8_t, uint8_t>> EDBColumn::getSizeElemUnderlyingArray() const {
    EDBColumnReader reader(l, posColumn, presortPos, layer, unq);
    return reader.getSizeElemUnderlyingArray();
}

EDBColumnReader::EDBColumnReader(const Literal &l, const uint8_t posColumn,
        const std::vector<uint8_t> presortPos,
        EDBLayer &layer, const bool unq)
    : l(l), layer(layer), posColumn(posColumn), presortPos(presortPos),
    unq(unq), //posInItr((l.getTupleSize() - l.getNVars()) + presortPos.size()),
    posInItr(l.getPosVars()[posColumn]),
    itr(NULL), firstCached((Term_t) - 1),
    lastCached((Term_t) - 1) {
    }

const char *EDBColumnReader::getUnderlyingArray() {
    if (hasNext()) {
        return itr->getUnderlyingArray(posInItr);
    }
    return NULL;
}

std::pair<uint8_t, std::pair<uint8_t, uint8_t>> EDBColumnReader::getSizeElemUnderlyingArray() {
    if (hasNext()) {
        return itr->getSizeElemUnderlyingArray(posInItr);
    } else {
        return std::make_pair(0, std::make_pair(0, 0));
    }
}

void EDBColumnReader::setupItr() {
    if (itr == NULL) {
        std::vector<uint8_t> fields;
        for (int i = 0; i < presortPos.size(); ++i) {
            fields.push_back(presortPos[i]);
        }
        fields.push_back(posColumn);
        itr = layer.getSortedIterator(l, fields);
        if (unq) {
            itr->skipDuplicatedFirstColumn();
        }
    }
}

std::vector<Term_t> EDBColumnReader::load(const Literal &l,
        const uint8_t posColumn,
        const std::vector<uint8_t> presortPos,
        EDBLayer & layer, const bool unq) {

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    std::vector<uint8_t> fields;
    for (int i = 0; i < presortPos.size(); ++i) {
        fields.push_back(presortPos[i]);
    }
    fields.push_back(posColumn);
    std::vector<Term_t> values;

    EDBIterator *itr = layer.getSortedIterator(l, fields);
    //const uint8_t posInItr = (l.getTupleSize() - l.getNVars()) + presortPos.size(); //n constants + presortPos
    const uint8_t posInItr = l.getPosVars()[posColumn];
    Term_t prev = (Term_t) - 1;
    const char *rawarray = itr->getUnderlyingArray(posInItr);
    if (rawarray != NULL) {
        std::pair<uint8_t, std::pair<uint8_t, uint8_t>> sizeelements
            = itr->getSizeElemUnderlyingArray(posInItr);
        const int totalsize = sizeelements.first + sizeelements.second.first + sizeelements.second.second;

        size_t nrows = ((TridentIterator *) itr)->getCardinality();

        values.reserve(nrows);

        if (sizeelements.second.first != 0) {
            //I must read the first column of a table.
            //I must read also the counter and add a
            //corresponding number of duplicates.

            const uint8_t sizecount = sizeelements.second.first;
            for (uint64_t j = 0; j < nrows; j++) {
                const uint64_t el = Utils::decode_longFixedBytes(rawarray, sizeelements.first);
                if (! unq || el != prev) {
                    values.push_back(el);
                    prev = el;
                }
                uint64_t count = Utils::decode_longFixedBytes(rawarray + sizeelements.first, sizecount);
                j += count - 1;
                if (! unq) {
                    while (--count > 0) {
                        values.push_back(el);
                    }
                }
                rawarray += totalsize;
            }

        } else {
            for (uint64_t j = 0; j < nrows; ++j) {
                const uint64_t el = Utils::decode_longFixedBytes(rawarray, sizeelements.first);
                if (! unq || el != prev) {
                    values.push_back(el);
                    prev = el;
                }
                rawarray += totalsize;
            }
        }
    } else {
        while (itr->hasNext()) {
            itr->next();
            Term_t el = itr->getElementAt(posInItr);
            if (!unq || el != prev) {
                values.push_back(el);
                prev = el;
            }
        }
    }

    layer.releaseIterator(itr);
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(TRACEL) << "Time loading a vector of " << values.size() << " is " << sec.count() * 1000;
    return values;
}

size_t EDBColumnReader::size() {
    std::vector<uint8_t> fields;
    for (int i = 0; i < presortPos.size(); ++i) {
        fields.push_back(presortPos[i]);
    }
    fields.push_back(posColumn);

    size_t sz = 0;

    EDBIterator *itr = layer.getSortedIterator(l, fields);
    Term_t prev = (Term_t) - 1;
    while (itr->hasNext()) {
        itr->next();
        Term_t el = itr->getElementAt(posInItr);
        if (!unq || el != prev) {
            sz++;
            prev = el;
        }
    }
    layer.releaseIterator(itr);
    return sz;
}

Term_t EDBColumnReader::last() {
    if (lastCached == (Term_t) - 1) {
        lastCached = load(l, posColumn, presortPos, layer, unq).back();
    }
    return lastCached;
}

Term_t EDBColumnReader::first() {
    if (firstCached == (Term_t) - 1) {
        std::vector<uint8_t> fields;
        fields.push_back(posColumn);
        EDBIterator *itr = layer.getSortedIterator(l, fields);
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
    if (itr == NULL) {
        setupItr();
    }
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

void ColumnWriter::concatenate(Column * c) {
    std::vector<Term_t> values = c->getReader()->asVector();
    for (auto &value : values) {
        add(value);
    }
}

std::shared_ptr<Column> ColumnWriter::getColumn() {
    if (cached) {
        //The column was already being requested
        return cachedColumn;
    }
    cached = true;

#ifdef USE_COMPRESSED_COLUMNS
    if (compressed) {
        LOG(TRACEL) << "ColumnWriter::getColumn: blocks.size() = " << blocks.size() << ", _size = " << _size;

        if (blocks.size() < _size / 5) {
            cachedColumn = std::shared_ptr<Column>(new CompressedColumn(
                        blocks, _size));
        } else {
            CompressedColumn col(blocks, /*offsetsize, deltas,*/ _size);
            std::vector<Term_t> values = col.getReader()->asVector();
            cachedColumn = std::shared_ptr<Column>(new InmemoryColumn(values, true));
        }
    } else {
        cachedColumn = std::shared_ptr<Column>(new InmemoryColumn(values, true));
    }
#else
    cachedColumn = std::shared_ptr<Column>(new InmemoryColumn(values, true));
#endif
    return cachedColumn;
}

std::shared_ptr<Column> ColumnWriter::getColumn(std::vector<Term_t> &values, bool isSorted) {

#ifdef USE_COMPRESSED_COLUMNS
    bool shouldCompress = false;
    //I should compress the table only if the number of unique terms is much
    //smaller than the number of total terms. I use simple heuristics to
    //determine that
    if (values.empty() || (isSorted && values.front() == values.back())) {
        shouldCompress = true;
    }


    if (shouldCompress) {
        return std::shared_ptr<Column>(new CompressedColumn(values.front(), values.size()));
        /*std::vector<CompressedColumnBlock> blocks;
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
        deltas, values.size()));*/
    } else {
        //swap the values. After, "values" is empty
        return std::shared_ptr<Column>(new InmemoryColumn(values, true));
    }
#else
    return std::shared_ptr<Column>(new InmemoryColumn(values, true));
#endif
}

void Column::intersection(std::shared_ptr<Column> c1,
        std::shared_ptr<Column> c2, ColumnWriter &writer) {
    std::unique_ptr<ColumnReader> r1 = c1->getReader();
    std::unique_ptr<ColumnReader> r2 = c2->getReader();
    Term_t v1, v2;
    if (! r1->hasNext()) {
        return;
    }
    v1 = r1->next();
    if (! r2->hasNext()) {
        return;
    }
    v2 = r2->next();

    // long count1 = ok1 ? 1 : 0;
    // long count2 = ok2 ? 1 : 0;
    // long countout = 0;

    //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();


    for (;;) {
        if (v1 < v2) {
            if (! r1->hasNext()) {
                return;
            }
            v1 = r1->next();
            // count1++;
        } else if (v1 > v2) {
            if (! r2->hasNext()) {
                return;
            }
            v2 = r2->next();
            // count2++;
        } else {
            writer.add(v1);
            // countout++;
            if (! r1->hasNext()) {
                return;
            }
            v1 = r1->next();
            // count1++;
            if (! r2->hasNext()) {
                return;
            }
            v2 = r2->next();
            // count2++;
        }
    }
    //std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    //LOG(INFOL) << "Count1=" << count1 << " Count2=" << count2 <<
    //                        " countout=" << countout << " " << sec.count() * 1000;
}

// The parallel version may very well be slower than the sequential one, because the parallel
// version has to actually obtain the complete columns.
void Column::intersection(std::shared_ptr<Column> c1,
        std::shared_ptr<Column> c2, ColumnWriter &writer, int nthreads) {
    if (nthreads <= 1 || c1->size() < 1024 || c2->size() < 1024) {
        return intersection(c1, c2, writer);
    }
    std::vector<std::shared_ptr<Column>> cols;
    cols.push_back(c1);
    cols.push_back(c2);
    const std::vector<const std::vector<Term_t> *> vectors = Segment::getAllVectors(cols);
    size_t c1Size = vectors[0]->size();
    size_t c2Size = vectors[1]->size();

    // TODO: parallelize this!
    Term_t v1, v2;
    size_t i1 = 1, i2 = 1;
    v1 = (*vectors[0])[0];
    v2 = (*vectors[1])[0];
    for (;;) {
        if (v1 < v2) {
            if (i1 >= c1Size) {
                return;
            }
            v1 = (*vectors[0])[i1++];
        } else if (v1 > v2) {
            if (i2 >= c2Size) {
                return;
            }
            v2 = (*vectors[1])[i2++];
        } else {
            writer.add(v1);
            if (i1 >= c1Size) {
                return;
            }
            v1 = (*vectors[0])[i1++];
            if (i2 >= c2Size) {
                return;
            }
            v2 = (*vectors[1])[i2++];
        }
    }
}

uint64_t Column::countMatches(
        std::shared_ptr<Column> c1,
        std::shared_ptr<Column> c2) {

    std::unique_ptr<ColumnReader> r1 = c1->getReader();
    std::unique_ptr<ColumnReader> r2 = c2->getReader();
    Term_t v1, v2;
    bool ok1 = r1->hasNext();
    if (ok1)
        v1 = r1->next();
    bool ok2 = r2->hasNext();
    if (ok2)
        v2 = r2->next();

    long countout = 0;

    //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    while (ok1 && ok2) {
        if (v1 < v2) {
            ok1 = r1->hasNext();
            if (ok1) {
                v1 = r1->next();
            }
        } else {
            if (v1 == v2) {
                countout++;
            }
            ok2 = r2->hasNext();
            if (ok2) {
                v2 = r2->next();
            }
        }
    }
    //std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    return countout;
}

// Assumes both columns are sorted
bool Column::subsumes(
        std::shared_ptr<Column> subsumer,
        std::shared_ptr<Column> subsumed) {

    std::unique_ptr<ColumnReader> r1 = subsumer->getReader();
    std::unique_ptr<ColumnReader> r2 = subsumed->getReader();
    Term_t v1, v2;
    bool ok1 = r1->hasNext();
    if (ok1)
        v1 = r1->next();
    bool ok2 = r2->hasNext();
    if (ok2)
        v2 = r2->next();

    while (ok1 && ok2) {
        // LOG(TRACEL) << "v1 = " << v1 << ", v2 = " << v2;
        if (v1 > v2) {
            // LOG(TRACEL) << "subsumes returns false";
            return false;
        }
        if (v1 == v2) {
            ok1 = r1->hasNext();
            if (ok1) {
                v1 = r1->next();
            }
            ok2 = r2->hasNext();
            if (ok2) {
                v2 = r2->next();
            }
        } else {
            ok1 = r1->hasNext();
            if (ok1) {
                v1 = r1->next();
            }
        }
    }
    // LOG(TRACEL) << "subsumes returns: ok2 = " << ok2 << ", return " << (! ok2);
    return ! ok2;
}
