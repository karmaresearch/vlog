#include <vlog/idxtupletable.h>

#include <trident/model/table.h>

IndexedTupleTable::IndexedTupleTable(TupleTable *table) : sizeTuple((uint8_t) table->getSizeRow()) {

    singleColumn = NULL;
    twoColumn1 = NULL;
    twoColumn2 = NULL;

    //idx1 = idx2 = NULL;
    //values1 = values2 = NULL;
    spo = pos = osp = NULL;

    if (sizeTuple == 0 || sizeTuple > 3) {
        LOG(ERRORL) << "Not supported";
        throw 10;
    }

    if (sizeTuple == 1) {
        singleColumn = new std::vector<Term_t>();
        //Populate it
        for (size_t i = 0; i < table->getNRows(); ++i) {
            singleColumn->push_back(table->getPosAtRow(i, 0));
        }
        std::sort(singleColumn->begin(), singleColumn->end());
    } else if (sizeTuple == 2) {
        twoColumn1 = new std::vector<std::pair<Term_t, Term_t>>();
        twoColumn2 = new std::vector<std::pair<Term_t, Term_t>>();

        for (size_t i = 0; i < table->getNRows(); ++i) {
            const uint64_t *row = table->getRow(i);
            twoColumn1->push_back(std::make_pair(row[0], row[1]));
            twoColumn2->push_back(std::make_pair(row[0], row[1]));
        }
        std::sort(twoColumn1->begin(), twoColumn1->end());
        std::sort(twoColumn2->begin(), twoColumn2->end(), [](const std::pair<uint64_t, uint64_t>& lhs, const std::pair<uint64_t, uint64_t>& rhs) {
            return lhs.second < rhs.second || (lhs.second == rhs.second && lhs.first < rhs.first);
        });
    } else if (sizeTuple == 3) {
        std::vector<uint8_t> fields;
        fields.push_back(0);
        fields.push_back(1);
        fields.push_back(2);
        spo = table->sortBy(fields);

        fields.clear();
        fields.push_back(1);
        fields.push_back(2);
        fields.push_back(0);
        pos = table->sortBy(fields);

        fields.clear();
        fields.push_back(2);
        fields.push_back(0);
        fields.push_back(1);
        osp = table->sortBy(fields);
    }
}

/*void IndexedTupleTable::query(QSQQuery *query, std::vector<uint8_t> *posToFilter,
                              std::vector<uint64_t> *valuesToFilter, TupleTable *outputTable) {
}*/

/*IndexedTupleTableItr2 *IndexedTupleTable::getIterator2(std::vector<uint8_t> *posToFilter,
        std::vector<uint64_t> *valuesToFilter) {
    if (posToFilter == NULL || posToFilter->size() == 0) {
        return new IndexedTupleTableItr2(false, idx1, values1, false, false, NULL);
    } else if (posToFilter->size() == 1) {
        if (posToFilter->at(0) == 0) {
            return new IndexedTupleTableItr2(false, idx1, values1, true, false, valuesToFilter);
        } else {
            return new IndexedTupleTableItr2(true, idx2, values2, true, false, valuesToFilter);
        }
    } else {
        if (posToFilter->at(0) == 0) {
            return new IndexedTupleTableItr2(false, idx1, values1, true, true, valuesToFilter);
        } else {
            return new IndexedTupleTableItr2(true, idx2, values2, true, true, valuesToFilter);
        }
    }
}*/

IndexedTupleTable::~IndexedTupleTable() {
    if (singleColumn != NULL) {
        delete singleColumn;
    }
    if (twoColumn1 != NULL) {
        delete twoColumn1;
    }
    if (twoColumn2 != NULL) {
        delete twoColumn2;
    }
    if (spo != NULL) {
        delete spo;
    }
    if (pos != NULL) {
        delete pos;
    }
    if (osp != NULL) {
        delete osp;
    }
}

/*IndexedTupleTableItr2::IndexedTupleTableItr2(bool invert, std::vector<std::pair<uint64_t, size_t>> *idx,
        std::vector<uint64_t> *values, bool filterKey, bool filterValues,
        std::vector<uint64_t> *filterElements) : invert(invert), filterKey(filterKey),
    filterValues(filterValues), idx(idx), values(values), filterElements(filterElements) {
    if (filterKey) {
        setAndMoveFilter();
    }
    currentIdx1 = currentIdx2 = 0;
}

void IndexedTupleTableItr2::setAndMoveFilter() {
    filterItr = filterElements->begin();
    currentFilterIdx = *filterItr;
    filterItr++;
    if (filterValues) {
        currentFilterValue =  *filterItr;
        filterItr++;
    }
}

bool IndexedTupleTableItr2::hasNext() {
    if (currentIdx2 >= values->size()) {
        return false;
    }

    if (filterKey) {
        if (idx->at(currentIdx1).first != currentFilterIdx) {
            uint64_t first = idx->at(currentIdx1).first;
            while (first < currentFilterIdx && currentIdx1 < idx->size() - 1) {
                currentIdx1++;
                first = idx->at(currentIdx1).first;
                currentIdx2 = idx->at(currentIdx1).second;
            }
            while (filterItr != filterElements->end() && first > currentFilterIdx) {
                setAndMoveFilter();
            }
            if (first != currentFilterIdx) {
                return false;
            }
        }

        if (filterValues && values->at(currentIdx2) != currentFilterValue) {
            size_t boundIdx = currentIdx1 < idx->size() - 1 ?
                              idx->at(currentIdx1 + 1).second : values->size();

        }
    }

    return true;
}

std::pair<uint64_t, uint64_t> IndexedTupleTableItr2::next() {
    std::pair<uint64_t, uint64_t> pair;
    if (invert) {
        pair = std::make_pair(values->at(currentIdx1), idx->at(currentIdx2).first);
    } else {
        pair = std::make_pair(idx->at(currentIdx1).first, values->at(currentIdx2));
    }
    currentIdx2++;
    //TODO: should I also move currentIdx1?
    return pair;
}*/
