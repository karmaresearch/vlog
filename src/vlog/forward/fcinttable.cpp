#include <vlog/fcinttable.h>
#include <vlog/column.h>

#include <string>
#include <random>

FCInternalTable::~FCInternalTable() {
}

InmemoryFCInternalTable::InmemoryFCInternalTable(const uint8_t nfields, const size_t iteration) :
    nfields(nfields), iteration(iteration), values(new Segment(nfields)), /*nrows(0),*/ sorted(true) {
}

InmemoryFCInternalTable::InmemoryFCInternalTable(const uint8_t nfields, const size_t iteration, const bool sorted, std::shared_ptr<const Segment> values) :
    nfields(nfields), iteration(iteration), values(values), /*nrows(values->getNRows()),*/ sorted(sorted) {
    assert(values->getNColumns() == nfields);
}

InmemoryFCInternalTable::InmemoryFCInternalTable(const uint8_t nfields, const size_t iteration, const bool sorted,
        std::shared_ptr<const Segment> values,
        std::vector<InmemoryFCInternalTableUnmergedSegment> unmergedSegments) :
    nfields(nfields), iteration(iteration), values(values), unmergedSegments(unmergedSegments),
    /*nrows(values->getNRows()),*/ sorted(sorted) {
    assert(values->getNColumns() == nfields);
}

size_t InmemoryFCInternalTable::getNRows() const {
    size_t output = values->getNRows();
    for (const auto &el : unmergedSegments) {
        output += el.values->getNRows();
    }
    return output;
}

bool InmemoryFCInternalTable::isEmpty() const {
    return values->isEmpty() && unmergedSegments.size() == 0;
}

uint8_t InmemoryFCInternalTable::getRowSize() const {
    return nfields;
}

FCInternalTableItr *InmemoryFCInternalTable::getIterator() const {

    assert(values == NULL || values->getNColumns() == nfields);

    std::shared_ptr<const Segment> allValues = InmemoryFCInternalTable::mergeUnmergedSegments(values, sorted, unmergedSegments, false);
    InmemoryFCInternalTableItr *itr = new InmemoryFCInternalTableItr();
    itr->init(nfields, iteration, allValues);
    return itr;
}

bool InmemoryFCInternalTable::colAIsContainedInColB(const Segment *a,
        const Segment *b) const {
    assert(a->getNColumns() == b->getNColumns());
    for (uint8_t i = 0; i < nfields; ++i) {
        assert(!b->isEmpty());
        assert(!a->isEmpty());
        if (b->isConstantField(i)) {
            if (a->isConstantField(i)) {
                if (a->firstInColumn(i) != b->firstInColumn(i)) {
                    return false;
                }
            } else {
                return false;
            }
        }
    }
    return true;
}

Term_t InmemoryFCInternalTable::get(const size_t rowId,
                                    const uint8_t columnId) const {
    assert(this->unmergedSegments.size() == 0);
    return values->get(rowId, columnId);
}

std::shared_ptr<const FCInternalTable> InmemoryFCInternalTable::merge(std::shared_ptr<const FCInternalTable> t) const {
    BOOST_LOG_TRIVIAL(debug) << "InmemoryFCInternalTable::merge";
    assert(!t->isEmpty() && t->getRowSize() == nfields);

    if (isEmpty()) {
        return t;
    }

    FCInternalTableItr *itr = t->getSortedIterator();
    std::vector<std::shared_ptr<Column>> allColumns = itr->getAllColumns();
    std::shared_ptr<const Segment> seg(new Segment(nfields, allColumns));
    std::shared_ptr<const FCInternalTable> newTable = merge(seg);
    t->releaseIterator(itr);
    BOOST_LOG_TRIVIAL(debug) << "InmemoryFCInternalTable::merge done";
    return newTable;
}

void InmemoryFCInternalTable::getDistinctValues(std::shared_ptr<Column> c,
        std::vector<Term_t> &existingValues, const uint32_t threshold) const {
    Term_t lastValue = std::numeric_limits<Term_t>::max();
    std::unique_ptr<ColumnReader> cr = c->getReader();

    for (uint32_t i = 0; cr->hasNext() &&
            existingValues.size() < threshold &&
            (i == 0 || !c->isConstant()); ++i) {
        //const Term_t currentValue = cr->get(i);
        const Term_t currentValue = cr->next();
        if (currentValue != lastValue) {
            //Could still be equal to the previous ones
            bool ok = true;
            for (int j = 0; j < ((int)existingValues.size() - 1) && ok; ++j) {
                if (existingValues[j] == currentValue)
                    ok = false;
            }

            if (ok) {
                lastValue = currentValue;
                existingValues.push_back(lastValue);
            }
        }
    }
}

std::vector<Term_t> InmemoryFCInternalTable::getDistinctValues(const uint8_t columnid,
        const uint32_t threshold) const {
    std::vector<Term_t> distinctValues;

    if (!values->isEmpty()) {
        getDistinctValues(values->getColumn(columnid), distinctValues, threshold);
    }

    for (std::vector<InmemoryFCInternalTableUnmergedSegment>::const_iterator itr = unmergedSegments.begin();
            itr != unmergedSegments.end() && distinctValues.size() < threshold; ++itr) {
        getDistinctValues(itr->values->getColumn(columnid), distinctValues, threshold);
    }

    return distinctValues;
}

std::shared_ptr<const FCInternalTable> InmemoryFCInternalTable::merge(
    std::shared_ptr<const Segment> seg) const {

    assert(seg->getNColumns() == nfields);

    //Does it match one of the unmergedSegments?
    bool found = false;

    std::vector<InmemoryFCInternalTableUnmergedSegment> newUnmergedSegments;
    for (const auto &unmergedSegment : unmergedSegments) {
        bool f = colAIsContainedInColB(seg.get(), unmergedSegment.values.get());
        if (f) {

            std::vector<std::shared_ptr<const Segment>> segmentsToMerge;
            segmentsToMerge.push_back(unmergedSegment.values);
            segmentsToMerge.push_back(seg);

            std::shared_ptr<const Segment> cloneSegment =
                SegmentInserter::merge(segmentsToMerge);
            found = true;
            newUnmergedSegments.push_back(
                InmemoryFCInternalTableUnmergedSegment(
                    nfields, cloneSegment));
        } else {
            newUnmergedSegments.push_back(unmergedSegment);
        }
    }

    //Is it contained in the main collection?
    std::shared_ptr<const Segment> newValues = values;
    if (!found) {
        if (!values->isEmpty()) {
            found = colAIsContainedInColB(seg.get(), values.get());
            if (found) {

                std::vector<std::shared_ptr<const Segment>> segmentsToMerge;
                if (!sorted) {
                    segmentsToMerge.push_back(values->sortBy(NULL));
                } else {
                    segmentsToMerge.push_back(values);
                }
                segmentsToMerge.push_back(seg);
                newValues = SegmentInserter::merge(segmentsToMerge);
            }
        } else {
            assert(unmergedSegments.size() == 0);
            newValues = seg;
            found = true;
        }
    }

    if (!found) {
        //Two options: we merge all of them or we add a new segment
        bool conflictsWithAllSets = !values->isEmpty() &&
                                    !colAIsContainedInColB(
                                        values.get(), seg.get());
        for (std::vector <
                InmemoryFCInternalTableUnmergedSegment >::const_iterator itr
                = unmergedSegments.cbegin();
                itr != unmergedSegments.cend() && conflictsWithAllSets;
                ++itr) {
            conflictsWithAllSets = !colAIsContainedInColB(
                                       itr->values.get(), seg.get());
        }

        if (conflictsWithAllSets) {
            newUnmergedSegments.push_back(InmemoryFCInternalTableUnmergedSegment(nfields, seg));
        } else {
            if (!isEmpty()) {
                std::vector<std::shared_ptr<const Segment>> segmentsToMerge;
                if (unmergedSegments.size() > 0) {
                    std::shared_ptr<const Segment> allValues =
                        InmemoryFCInternalTable::mergeUnmergedSegments(
                            values, sorted, unmergedSegments, true);

                    segmentsToMerge.push_back(allValues);
                    newUnmergedSegments.clear();
                } else {
                    segmentsToMerge.push_back(values);
                }
                segmentsToMerge.push_back(seg);
                newValues = SegmentInserter::merge(segmentsToMerge);
            } else {
                newValues = seg;
            }
        }
    }

    assert(newValues->getNColumns() == nfields);
    std::shared_ptr<const FCInternalTable> retval =
        std::shared_ptr<const FCInternalTable>(
            new InmemoryFCInternalTable(nfields, iteration, true,
                                        newValues, newUnmergedSegments));
    return retval;
}

std::shared_ptr<const Segment> InmemoryFCInternalTable::mergeUnmergedSegments(
    std::shared_ptr<const Segment> values,
    bool isSorted,
    const std::vector<InmemoryFCInternalTableUnmergedSegment> &unmergedSegments,
    const bool outputSorted) {
    if (unmergedSegments.size() > 0) {
        //Copy the original values in mergedValues
        std::vector<std::shared_ptr<const Segment>> allSegments;

        if (!values->isEmpty()) {
            allSegments.push_back(values);
        }

        for (std::vector<InmemoryFCInternalTableUnmergedSegment>::
                const_iterator itr = unmergedSegments.begin();
                itr != unmergedSegments.end(); ++itr) {
            allSegments.push_back(itr->values);
        }

        SegmentInserter inserter(allSegments[0]->getNColumns());
        if (outputSorted) {
            //merge
            return inserter.merge(allSegments);
        } else {
            //concatenate
            return inserter.concatenate(allSegments);
        }
    } else {
        return values;
    }
}

void InmemoryFCInternalTable::readArray(Term_t *dest, std::vector<Term_t>::iterator & itr) {
    for (size_t i = 0; i < nfields; ++i) {
        dest[i] = *itr;
        itr++;
    }
}

void InmemoryFCInternalTable::readArray(Term_t *dest, std::vector<Term_t>::const_iterator & itr) {
    for (size_t i = 0; i < nfields; ++i) {
        dest[i] = *itr;
        itr++;
    }
}

/*void InmemoryFCInternalTable::copyArray(Segment *dest,
                                        FCInternalTableItr *itr) {

    for (size_t i = 0; i < nfields; ++i) {
        dest->push_back(i, itr->getCurrentValue(i));
    }

}*/

int InmemoryFCInternalTable::cmp(FCInternalTableItr * itr1,
                                 FCInternalTableItr * itr2) const {
    for (uint8_t i = 0; i < nfields; ++i) {
        if (itr1->getCurrentValue(i) != itr2->getCurrentValue(i))
            return itr1->getCurrentValue(i) - itr2->getCurrentValue(i);
    }
    return 0;
}

bool InmemoryFCInternalTable::isSorted() const {
    return sorted;
}

std::shared_ptr<Segment> InmemoryFCInternalTable::doSort(std::shared_ptr<Segment> input,
        const std::vector<uint8_t> *fields) {
    return input->sortBy(fields);
}

void InmemoryFCInternalTable::copyRawValues(const std::vector<const Term_t*> *rowIdx,
        const uint8_t nfields, std::vector<Term_t> &output) {
    for (std::vector<const Term_t*>::const_iterator itr =  rowIdx->begin(); itr != rowIdx->end(); ++itr) {
        const Term_t *ri = *itr;
        for (uint8_t i = 0; i < nfields; ++i) {
            output.push_back(ri[i]);
        }
    }
}

size_t InmemoryFCInternalTable::estimateNRows(
    const uint8_t nconstantsToFilter, const uint8_t *posConstantsToFilter,
    const Term_t *valuesConstantsToFilter) const {
    size_t estimate = 0;

    for (std::vector<InmemoryFCInternalTableUnmergedSegment>::
            const_iterator itr = unmergedSegments.cbegin();
            itr != unmergedSegments.cend(); ++itr) {
        //If the segment is compatible with the constraints, then we count all triples
        bool subsumes = true;
        uint8_t ncOutsidePattern = 0;
        for (uint8_t j = 0; j < nconstantsToFilter; ++j) {
            uint8_t posConstant = posConstantsToFilter[j];
            bool found = false;
            for (uint8_t i = 0; i < itr->nconstants; ++i) {
                if (posConstant == itr->constants[i].first) {
                    found = true;
                    if (valuesConstantsToFilter[j] != itr->constants[i].second) {
                        subsumes = false;
                    }
                }
            }
            if (!found) {
                ncOutsidePattern++;
            }
        }
        if (subsumes) {
            if (ncOutsidePattern > 0) {
                estimate += itr->values->estimate(nconstantsToFilter,
                                                  posConstantsToFilter,
                                                  valuesConstantsToFilter,
                                                  nfields);
            } else {
                estimate += itr->values->estimate(0, NULL, NULL, nfields);
            }
        }
    }

    estimate += values->estimate(nconstantsToFilter, posConstantsToFilter,
                                 valuesConstantsToFilter, nfields);

    return estimate;
}

bool InmemoryFCInternalTable::isPrimarySorting(const std::vector<uint8_t> &fields) const {
    int prev = -1;

    if (fields.size() > 0) {
        for (uint8_t i = 0; i < fields[0]; ++i) {
            if (!values->getColumn(i)->isConstant()) {
                return false;
            }
        }

        for (std::vector<uint8_t>::const_iterator itr = fields.begin(); itr != fields.end(); ++itr) {
            if (*itr != prev + 1)
                return false;
            prev = *itr;
        }
    }
    return true;
}

std::string InmemoryFCInternalTable::vector2string(const std::vector<uint8_t> &v) {
    std::string output;
    for (std::vector<uint8_t>::const_iterator itr = v.begin(); itr != v.end(); ++itr)
        output += std::to_string(*itr) + std::string("-");
    return output;
}

std::shared_ptr<const Segment> InmemoryFCInternalTable::filter_row(std::shared_ptr<const Segment> seg,
        const uint8_t nConstantsToFilter, const uint8_t *posConstantsToFilter,
        const Term_t *valuesConstantsToFilter, const uint8_t nRepeatedVars,
        const std::pair<uint8_t, uint8_t> *repeatedVars) {
    const Segment *pseg = seg.get();
    SegmentInserter inserter(pseg->getNColumns());

    std::unique_ptr<SegmentIterator> itr = pseg->iterator();
    while (itr->hasNext()) {
        itr->next();
        bool ok = true;
        for (uint8_t m = 0; m < nConstantsToFilter; ++m) {
            if (itr->get(posConstantsToFilter[m]) != valuesConstantsToFilter[m]) {
                ok = false;
                break;
            }
        }
        for (uint8_t m = 0; m < nRepeatedVars; ++m) {
            if (itr->get(repeatedVars[m].first) !=
                    itr->get(repeatedVars[m].second)) {
                ok = false;
                break;
            }
        }
        if (ok) {
            //Add the variables
            //inserter.addRow(pseg, i);
            inserter.addRow(*itr.get());
        }
    }
    itr->clear();
    return std::shared_ptr<const Segment>(inserter.getSegment());
}

std::shared_ptr<Column> InmemoryFCInternalTable::getColumn(
    const uint8_t columnIdx) const {
    if (unmergedSegments.size() > 0) {
        //I must concatenate all columns
        ColumnWriter writer;
        auto c = values->getColumn(columnIdx);
        writer.concatenate(c.get());
        for (const auto &segment : unmergedSegments) {
            auto c1 = segment.values->getColumn(columnIdx);
            writer.concatenate(c1.get());
        }
        return writer.getColumn();
    }
    return values->getColumn(columnIdx);
}

bool InmemoryFCInternalTable::isColumnConstant(const uint8_t columnid) const {
    if (!values->isEmpty() && !values->getColumn(columnid)->isConstant()) {
        return false;
    }

    //Check all unmerged segments
    Term_t valToCheck = values->firstInColumn(columnid);
    for (std::vector<InmemoryFCInternalTableUnmergedSegment>::const_iterator itr = unmergedSegments.begin();
            itr != unmergedSegments.end(); ++itr) {
        if (!itr->values->getColumn(columnid)->isConstant() || itr->values->firstInColumn(columnid) != valToCheck) {
            return false;
        }
    }

    return true;
}

Term_t InmemoryFCInternalTable::getValueConstantColumn(const uint8_t columnid) const {
    if (!values->isEmpty()) {
        return values->firstInColumn(columnid);
    }

    //Check all unmerged segments
    for (std::vector<InmemoryFCInternalTableUnmergedSegment>::const_iterator itr = unmergedSegments.begin();
            itr != unmergedSegments.end(); ++itr) {
        return itr->values->firstInColumn(columnid);
    }

    throw 10;
}

std::shared_ptr<const FCInternalTable> InmemoryFCInternalTable::filter(const uint8_t nVarsToCopy, const uint8_t *posVarsToCopy,
        const uint8_t nConstantsToFilter, const uint8_t *posConstantsToFilter,
        const Term_t *valuesConstantsToFilter, const uint8_t nRepeatedVars,
        const std::pair<uint8_t, uint8_t> *repeatedVars) const {

    std::vector<std::shared_ptr<const Segment>> possibleSegments;

    //First check values
    bool isSetBigger = false;
    bool match = true;
    /*
    BOOST_LOG_TRIVIAL(debug) << "nConstantsToFilter = " << (int) nConstantsToFilter;
    for (uint8_t i = 0; i < nConstantsToFilter; ++i) {
    BOOST_LOG_TRIVIAL(debug) << "posConstantsToFilter[" << (int) i << "] = " << (int) posConstantsToFilter[i];
    BOOST_LOG_TRIVIAL(debug) << "valuesConstantsToFilter[" << (int) i << "] = " << valuesConstantsToFilter[i];
    }
    */
    for (uint8_t i = 0; i < nConstantsToFilter && match; ++i) {
        if (values->getColumn(posConstantsToFilter[i])->isConstant()) {
            const Term_t v = values->getColumn(posConstantsToFilter[i])->getReader()->first();
            if (v != valuesConstantsToFilter[i]) {
                match = false;
            }
        } else {
            isSetBigger = true; //If there is a match, then I must filter it
            //If the column is the first one, then it is sorted. I can apply binary search to see if the constant exists
            if (posConstantsToFilter[i] == 0) {
                if (!values->getColumn(posConstantsToFilter[i])->isIn(valuesConstantsToFilter[i])) {
                    match = false;
                }
            }
        }
    }

    if (match && !isSetBigger && nRepeatedVars > 0) {
        for (uint8_t i = 0; i < nRepeatedVars; ++i) {
            //Check the values in the two positions. If they are both constants
            //then I check the value
            Column *c1 = values->getColumn(repeatedVars[i].first).get();
            Column *c2 = values->getColumn(repeatedVars[i].second).get();
            if (c1->isConstant() && c2->isConstant()) {
                if (c1->getReader()->first() != c2->getReader()->first()) {
                    match = false;
                    break;
                }
            } else {
                isSetBigger = true;
                break;
            }
        }
    }

    // BOOST_LOG_TRIVIAL(debug) << "P1: match = " << match << ", isSetBigger = " << isSetBigger;

    if (match) {
        if (isSetBigger) {
            std::shared_ptr<const Segment> fs = filter_row(values, nConstantsToFilter,
                                                posConstantsToFilter, valuesConstantsToFilter,
                                                nRepeatedVars, repeatedVars);
            if (!fs->isEmpty())
                possibleSegments.push_back(fs);
        } else {
            possibleSegments.push_back(values);
        }
    }
    // else {
    //Look at the other segments
    for (std::vector<InmemoryFCInternalTableUnmergedSegment>::const_iterator itr = unmergedSegments.begin();
            itr != unmergedSegments.end(); ++itr) {
        isSetBigger = false;
        match = true;

        for (uint8_t i = 0; i < nConstantsToFilter && match; ++i) {
            bool found = false;
            uint8_t j = 0;
            for (; j < itr->nconstants; ++j) {
                if (itr->constants[j].first == posConstantsToFilter[i]) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                isSetBigger = true;
            } else {
                if (valuesConstantsToFilter[i] != itr->constants[j].second) {
                    match = false;
                }
            }
        }

        //Check repeated variables
        if (match && !isSetBigger && nRepeatedVars > 0) {
            for (uint8_t i = 0; i < nRepeatedVars; ++i) {
                //Check the values in the two positions. If they are both constants
                //then I check the value
                Column *c1 = values->getColumn(repeatedVars[i].first).get();
                Column *c2 = values->getColumn(repeatedVars[i].second).get();
                if (c1->isConstant() && c2->isConstant()) {
                    if (c1->getReader()->first() != c2->getReader()->first()) {
                        match = false;
                        break;
                    }
                } else {
                    isSetBigger = true;
                    break;
                }
            }
        }

        // BOOST_LOG_TRIVIAL(debug) << "P2: match = " << match << ", isSetBigger = " << isSetBigger;

        if (match) {
            if (isSetBigger) {
                std::shared_ptr<const Segment> fs = filter_row(itr->values,
                                                    nConstantsToFilter, posConstantsToFilter,
                                                    valuesConstantsToFilter,
                                                    nRepeatedVars, repeatedVars);

                if (!fs->isEmpty())
                    possibleSegments.push_back(fs);
            } else {
                possibleSegments.push_back(itr->values);
            }
        }
        //}
    }

    //Merge them if multiple than one
    std::shared_ptr<const Segment> filteredSegment;
    if (possibleSegments.size() > 1) {
        /*while (possibleSegments.size() > 1) {
            std::vector<std::shared_ptr<const Segment>> newPossibleSegments;
            for (int i = 0; i < possibleSegments.size(); i += 2) {
                if (i + 1 < possibleSegments.size()) {
                    std::shared_ptr<Segment> mergedSegment =
                        possibleSegments[i]->cloneSegment();
                    mergedSegment->merge(*possibleSegments[i + 1]);
                    newPossibleSegments.push_back(mergedSegment);
                } else {
                    newPossibleSegments.push_back(possibleSegments[i]);
                }
            }
            possibleSegments = newPossibleSegments;
        }
        filteredSegment = possibleSegments[0];*/

        //Collect all segments
        //assert(nVarsToCopy == possibleSegments[0]->getNColumns());
        SegmentInserter inserter(nfields);
        filteredSegment = inserter.merge(possibleSegments);
    } else if (possibleSegments.size() == 1) {
        filteredSegment = possibleSegments[0];
    }

    if (filteredSegment != NULL && !filteredSegment->isEmpty()) {

        // BOOST_LOG_TRIVIAL(debug) << "We have a filteredSegment";

        //Do the projection
        if (nVarsToCopy > 0 && nVarsToCopy < nfields && filteredSegment != NULL) {
            SegmentInserter newSegment(nVarsToCopy);
            for (uint8_t i = 0; i < nVarsToCopy; ++i) {
                newSegment.addColumn(i,
                                     filteredSegment->
                                     getColumn(posVarsToCopy[i]), true);
            }
            filteredSegment = newSegment.getSegment();
        }

        if (nVarsToCopy > 0) {
            return std::shared_ptr<const FCInternalTable>(new InmemoryFCInternalTable(nVarsToCopy, iteration, true, filteredSegment));
        } else {
            return std::shared_ptr<const FCInternalTable>(new SingletonTable(iteration));
        }
    } else {
        return std::shared_ptr<const FCInternalTable>();
    }
}

FCInternalTableItr *InmemoryFCInternalTable::getSortedIterator() const {
    std::shared_ptr<const Segment> sortedValues;

    if (unmergedSegments.size() > 0) {
        sortedValues = InmemoryFCInternalTable::mergeUnmergedSegments(values, sorted, unmergedSegments, true);
        assert(sortedValues->getNColumns() == nfields);
    } else {
        if (!isSorted()) {
            sortedValues = values->sortBy(NULL);
        } else {
            sortedValues = values;
        }
    }

    InmemoryFCInternalTableItr *itr = new InmemoryFCInternalTableItr();
    itr->init(nfields, iteration, sortedValues);
    return itr;
}

FCInternalTableItr *InmemoryFCInternalTable::sortBy(const std::vector<uint8_t> &fields) const {
    bool primarySort = isPrimarySorting(fields);
    std::shared_ptr<const Segment> sortedValues;

    BOOST_LOG_TRIVIAL(debug) << "InmemoryFCInternalTable::sortBy";

    if (unmergedSegments.size() > 0) {
        BOOST_LOG_TRIVIAL(debug) << "InmemoryFCInternalTable::mergeUnmergedSegments";
        sortedValues = InmemoryFCInternalTable::mergeUnmergedSegments(values, sorted, unmergedSegments, primarySort);
    } else {
        if (primarySort && !isSorted()) {
            BOOST_LOG_TRIVIAL(debug) << "InmemoryFCInternalTable::sorting";
            sortedValues = values->sortBy(NULL);
        } else {
            sortedValues = values;
        }
    }

    if (!primarySort) {
        BOOST_LOG_TRIVIAL(debug) << "InmemoryFCInternalTable::sorting2";
        sortedValues = sortedValues->sortBy(&fields);
    }

    InmemoryFCInternalTableItr *tableItr = new InmemoryFCInternalTableItr();
    tableItr->init(nfields, iteration, sortedValues);
    return tableItr;
}

void InmemoryFCInternalTable::releaseIterator(FCInternalTableItr * itr) const {
    itr->clear();
    delete itr;
}

InmemoryFCInternalTable::~InmemoryFCInternalTable() {
}

void InmemoryFCInternalTableItr::init(const uint8_t nfields, const size_t iteration,
                                      std::shared_ptr<const Segment> values) {
    this->nfields = nfields;
    this->values = values;
    this->iteration = iteration;
    segmentIterator = values->iterator();
    //idx = -1;
}

bool InmemoryFCInternalTableItr::sameAs(
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

std::vector<std::shared_ptr<Column>> InmemoryFCInternalTableItr::getColumn(
const uint8_t ncolumns, const uint8_t *columns) {
    std::vector<std::shared_ptr<Column>> output;
    for (uint8_t i = 0; i < ncolumns; ++i) {
        output.push_back(values->getColumn(columns[i]));
    }
    return output;
}

uint8_t InmemoryFCInternalTableItr::getNColumns() const {
    return values->getNColumns();
}

std::vector<std::shared_ptr<Column>> InmemoryFCInternalTableItr::getAllColumns() {
    std::vector<uint8_t> idxs;
    for (uint8_t i = 0; i < nfields; ++i) {
        idxs.push_back(i);
    }
    return getColumn(nfields, &(idxs[0]));
}

std::vector<std::shared_ptr<Column>> MergerInternalTableItr::getColumn(
const uint8_t ncolumns, const uint8_t *columns) {
    SegmentInserter seg(ncolumns);

    while (hasNext()) {
        next();
        for (uint8_t i = 0; i < ncolumns; ++i) {
            seg.addAt(i, getCurrentValue(columns[i]));
        }
    }

    auto segment = seg.getSegment();
    std::vector<std::shared_ptr<Column>> output;
    for (uint8_t i = 0; i < ncolumns; ++i) {
        output.push_back(segment->getColumn(i));
    }
    return output;
}

uint8_t MergerInternalTableItr::getNColumns() const {
    return nfields;
}

std::vector<std::shared_ptr<Column>> MergerInternalTableItr::getAllColumns() {
    std::vector<uint8_t> idxs;
    for (uint8_t i = 0; i < nfields; ++i) {
        idxs.push_back(i);
    }
    return getColumn(nfields, &(idxs[0]));
}

bool MergerInternalTableItr::hasNext() {
    return indices.size() > 1 || first->hasNext();
}

MergerInternalTableItr::MergerInternalTableItr(const std::vector<std::pair<FCInternalTableItr*, size_t>> &iterators,
        const std::vector<uint8_t> &positionsToSort, const uint8_t nfields)
    : iterators(iterators), firstCall(true),
      sorter(iterators, (uint8_t) positionsToSort.size(), sortPos), nfields(nfields) {
    for (uint8_t i = 0; i < iterators.size(); ++i) {
        indices.push_back(i);
    }
    for (uint8_t i = 0; i < positionsToSort.size(); ++i)
        sortPos[i] = positionsToSort[i];
    std::sort(indices.begin(), indices.end(), std::ref(sorter));
    first = iterators[indices.back()].first;
}

void MergerInternalTableItr::next() {
    if (firstCall == true) {
        firstCall = false;
    } else {
        //Remove the smallest
        if (first->hasNext()) {
            first->next();
        } else {
            indices.pop_back();
        }
        //TODO: Shouldn't indices always be greater than 0?
        if (indices.size() > 0) {
            std::sort(indices.begin(), indices.end(), std::ref(sorter));
            first = iterators[indices.back()].first;
        }
    }
}

bool MITISorter::operator ()(const uint8_t i1, const uint8_t i2) const {
    for (uint8_t i = 0; i < tuplesize; ++i) {
        Term_t v1 = iterators[i1].first->getCurrentValue(sortPos[i]);
        Term_t v2 = iterators[i2].first->getCurrentValue(sortPos[i]);
        if (v1 > v2)
            return true;
        else if (v1 < v2)
            return false;
    }
    return true;
}
