#include <vlog/bindingstable.h>
#include <trident/model/table.h>

Term_t const * const EMPTY_TUPLE = {0};
BindingsRow EMPTY_ROW(0, EMPTY_TUPLE);

bool BindingsRow::operator==(const BindingsRow &other) const {
    if (size == other.size) {
        if (row == other.row)
            return true;
        for (int i = 0; i < size; ++i) {
            if (row[i] != other.row[i])
                return false;
        }
        return true;
    }
    LOG(ERRORL) << "Should never happen";
    return false;
}

BindingsTable::BindingsTable(uint8_t sizeAdornment, uint8_t adornment) {
    uniqueElements.max_load_factor(0.75);
    //Mark positions to copy
    std::vector<int> pc;
    for (int i = 0; i < sizeAdornment; ++i) {
        if (adornment & 1) {
            pc.push_back(i);
        }
        adornment >>= 1;
    }
    nPosToCopy = pc.size();

    if (nPosToCopy > 0) {
        posToCopy = new size_t[nPosToCopy];
        int i = 0;
        for (std::vector<int>::iterator itr = pc.begin(); itr != pc.end(); ++itr) {
            posToCopy[i++] = *itr;
        }
        rawBindings = new RawBindings((uint8_t) nPosToCopy);
        currentRow = rawBindings->newRow();
    } else {
        posToCopy = NULL;
        rawBindings = NULL;
        currentRow = NULL;
    }
}

BindingsTable::BindingsTable(size_t sizeTuple) {
    uniqueElements.max_load_factor(0.75);
    nPosToCopy = sizeTuple;
    if (nPosToCopy > 0) {
        rawBindings = new RawBindings((uint8_t) sizeTuple);
        currentRow = rawBindings->newRow();
    } else {
        rawBindings = NULL;
        currentRow = NULL;
    }
    posToCopy = NULL;
}

BindingsTable::BindingsTable(uint8_t npc, std::vector<int> pc) {
    uniqueElements.max_load_factor(0.75);
    this->nPosToCopy = npc;
    if (nPosToCopy > 0) {
        this->posToCopy = new size_t[nPosToCopy];
        for (int i = 0; i < nPosToCopy; ++i) {
            posToCopy[i] = pc.at(i);
        }
        rawBindings = new RawBindings((uint8_t) nPosToCopy);
        currentRow = rawBindings->newRow();
    } else {
        this->posToCopy = NULL;
        rawBindings = NULL;
        currentRow = NULL;
    }
}

void BindingsTable::insertIfNotExists(Term_t const * const cr) {
    if (cr == EMPTY_TUPLE) {
        uniqueElements.insert(EMPTY_ROW);
        return;
    }
    BindingsRow row((uint8_t) nPosToCopy, cr);
    /*
    std::unordered_set<BindingsRow, hash_BindingsRow>::iterator itr = uniqueElements.find(row);
    if (itr == uniqueElements.end()) {
        uniqueElements.insert(row);
    */
    std::pair<std::unordered_set<BindingsRow, hash_BindingsRow>::iterator, bool> result = uniqueElements.insert(row);
    if (result.second) {
        currentRow = rawBindings->newRow();
    }
}

void BindingsTable::addTuple(const Literal *t) {
    if (nPosToCopy == 0) {
        insertIfNotExists(EMPTY_TUPLE);
    } else {
        for (int i = 0; i < nPosToCopy; ++i) {
            currentRow[i] = (Term_t) t->getTermAtPos(posToCopy[i]).getValue();
        }
        insertIfNotExists(currentRow);
    }
}

#if ! TERM_IS_UINT64
void BindingsTable::addTuple(const uint64_t *t) {
    if (nPosToCopy == 0) {
        insertIfNotExists(EMPTY_TUPLE);
    } else {
        for (int i = 0; i < nPosToCopy; ++i) {
            currentRow[i] = t[posToCopy[i]];
        }
        insertIfNotExists(currentRow);
    }
}
#endif

void BindingsTable::addTuple(const Term_t *t) {
    if (nPosToCopy == 0) {
        insertIfNotExists(EMPTY_TUPLE);
    } else {
        for (int i = 0; i < nPosToCopy; ++i) {
            currentRow[i] = t[posToCopy[i]];
        }
        insertIfNotExists(currentRow);
    }
}

void BindingsTable::addTuple(const uint64_t *t1, const uint8_t sizeT1,
                             const uint64_t *t2, const uint8_t sizeT2) {
    if (nPosToCopy == 0) {
        insertIfNotExists(EMPTY_TUPLE);
    } else {
        for (int i = 0; i < nPosToCopy; ++i) {
            if (posToCopy[i] >= sizeT1) {
                currentRow[i] = (Term_t) t2[posToCopy[i] - sizeT1];
            } else {
                currentRow[i] = (Term_t) t1[posToCopy[i]];
            }
        }
        insertIfNotExists(currentRow);
    }
}

void BindingsTable::addTuple(const uint64_t *t, const uint8_t *positions) {
    if (nPosToCopy == 0) {
        insertIfNotExists(EMPTY_TUPLE);
    } else {
        for (int i = 0; i < nPosToCopy; ++i) {
            currentRow[i] = (Term_t) t[positions[i]];
        }
        insertIfNotExists(currentRow);
    }
}

void BindingsTable::addRawTuple(Term_t *r) {
    if (nPosToCopy == 0) {
        insertIfNotExists(EMPTY_TUPLE);
    } else {
        for (int i = 0; i < nPosToCopy; ++i) {
            currentRow[i] = r[i];
        }
        insertIfNotExists(currentRow);
    }
}

void BindingsTable::clear() {
    uniqueElements.clear();
    if (nPosToCopy > 0) {
        rawBindings->clear();
        currentRow = rawBindings->newRow();
    }
}

TupleTable *BindingsTable::sortBy(std::vector<uint8_t> &fields) {
    std::vector<BindingsRow> rowsToSort;
    for (size_t i = 0; i < uniqueElements.size(); ++i) {
        BindingsRow row((uint8_t) nPosToCopy, rawBindings->getOffset(i * nPosToCopy));
        rowsToSort.push_back(row);
    }
    FieldsSorter sorter(fields);
    std::sort(rowsToSort.begin(), rowsToSort.end(), std::ref(sorter));

    //Create a TupleTable and return it
    TupleTable *outputTable = new TupleTable(nPosToCopy);
    for (std::vector<BindingsRow>::iterator itr = rowsToSort.begin(); itr != rowsToSort.end();
            ++itr) {
        outputTable->addRow((uint64_t*)itr->row);
    }
    return outputTable;
}

TupleTable *BindingsTable::projectAndFilter(const Literal &l, const std::vector<uint8_t> *posToFilter,
        const std::vector<Term_t> *valuesToFilter) {
    uint8_t vars[SIZETUPLE];
    uint8_t consts[SIZETUPLE];
    uint8_t nconsts = 0;
    uint8_t nvars = 0;
    uint64_t currentRow[SIZETUPLE];	// Not Term_t; used in trident api.

    for (uint8_t i = 0; i < l.getTupleSize(); ++i) {
        if (l.getTermAtPos(i).isVariable()) {
            vars[nvars++] = i;
        } else {
            consts[nconsts++] = i;
        }
    }
    TupleTable *output = new TupleTable(nvars);
#if DEBUG
    bool warn_done = false;
#endif
    for (size_t i = 0; i < uniqueElements.size(); ++i) {
        Term_t *row = rawBindings->getOffset(i * nPosToCopy);
        bool ok = true;
        for (uint8_t j = 0; j < nconsts; ++j) {
            if (row[consts[j]] != l.getTermAtPos(consts[j]).getValue()) {
                ok = false;
                break;
            }
        }

        //check the variables
        if (ok && posToFilter != NULL && posToFilter->size() != 0) {
            ok = false;
            const size_t sizePosToFilter = posToFilter->size();
            const size_t sizeValuesToFilter = valuesToFilter->size();
            uint8_t copyPosToFilter[sizePosToFilter];

#if DEBUG
	    if (! warn_done) {
		LOG(DEBUGL) << "Performing linear search (bindingsTable::projectAndFilter). Perhaps this should be optimized";
		LOG(DEBUGL) << "size = " << sizeValuesToFilter / sizePosToFilter;
		warn_done = true;
	    }
#endif
            for (uint8_t m = 0; m < sizePosToFilter; ++m) {
                copyPosToFilter[m] = posToFilter->at(m);
            }

            for (size_t j = 0; j < sizeValuesToFilter && !ok; j += sizePosToFilter) {
                bool okRow = true;
                for (uint8_t m = 0; m < sizePosToFilter; ++m) {
                    if (row[copyPosToFilter[m]] != valuesToFilter->at(j + m)) {
                        okRow = false;
                        break;
                    }
                }
                if (okRow) {
                    ok = true;
                }
            }
        }

        if (ok) {
            for (uint8_t j = 0; j < nvars; ++j)
                currentRow[j] = row[vars[j]];
            output->addRow(currentRow);
        }
    }
    return output;
}

TupleTable *BindingsTable::filter(const Literal &l, const std::vector<uint8_t> *posToFilter,
                                  const std::vector<Term_t> *valuesToFilter) {

    Term_t consts[SIZETUPLE];
    uint8_t posConsts[SIZETUPLE];
    uint8_t nconsts = 0;
    for (uint8_t i = 0; i < l.getTupleSize(); ++i) {
        if (!l.getTermAtPos(i).isVariable()) {
            posConsts[nconsts] = i;
            consts[nconsts++] = l.getTermAtPos(i).getValue();
        }
    }

    std::unique_ptr<std::unordered_set<Term_t>> filterSet;
    if (valuesToFilter != NULL && posToFilter->size() == 1) {
        filterSet = std::unique_ptr<std::unordered_set<Term_t>>(new std::unordered_set<Term_t>());
        for (std::vector<Term_t>::const_iterator itr = valuesToFilter->begin();
                itr != valuesToFilter->end(); ++itr) {
            filterSet->insert(*itr);
        }
    }

    TupleTable *output = new TupleTable(nPosToCopy);
#if DEBUG
    bool warn_done = false;
#endif
    for (size_t i = 0; i < uniqueElements.size(); ++i) {
        Term_t *row = rawBindings->getOffset(i * nPosToCopy);

        bool ok = true;
        for (uint8_t j = 0; j < nconsts; ++j) {
            if (row[posConsts[j]] != consts[j]) {
                ok = false;
                break;
            }
        }

        if (ok && posToFilter != NULL && posToFilter->size() != 0) {
            if (filterSet != NULL) {
                ok = filterSet->find(row[posToFilter->at(0)]) != filterSet->end();
            } else {
                //linear search
                ok = false;
#if DEBUG
		if (! warn_done) {
		    LOG(DEBUGL) << "Performing linear search (bindingsTable::asTupleTable). Perhaps this should be optimized";
		    LOG(DEBUGL) << "size = " << valuesToFilter->size() / posToFilter->size();
		    warn_done = true;
		}
#endif
                const uint8_t sizePosToFilter = (uint8_t) posToFilter->size();
                for (size_t j = 0; j < valuesToFilter->size() && !ok; j += sizePosToFilter) {
                    bool okRow = true;
                    for (uint8_t m = 0; m < posToFilter->size(); ++m) {
                        if (row[posToFilter->at(m)] != valuesToFilter->at(j + m)) {
                            okRow = false;
                            break;
                        }
                    }
                    if (okRow) {
                        ok = true;
                    }
                }
            }
        }

        if (ok) {
#if TERM_IS_UINT64
	    output->addRow(row);
#else
	    uint64_t *rc = new uint64_t[nPosToCopy];
	    for (int i = 0; i < nPosToCopy; i++) {
		rc[i] = row[i];
	    }
            output->addRow(rc);
	    delete rc;
#endif
	}
    }
    return output;
}

std::vector<Term_t> BindingsTable::getProjection(std::vector<uint8_t> pos) {
    size_t size = uniqueElements.size();
    std::vector<Term_t> outputVector;
    for (int i = 0; i < size; ++i) {
        Term_t *startTuple = rawBindings->getOffset(i * nPosToCopy);
        for (std::vector<uint8_t>::iterator itr = pos.begin(); itr != pos.end();
                ++itr) {
            outputVector.push_back(*(startTuple + *itr));
        }
        startTuple += nPosToCopy;
    }
    return outputVector;
}

std::vector<Term_t> BindingsTable::getUniqueSortedProjection(std::vector<uint8_t> pos) {
    size_t size = uniqueElements.size();
    std::vector<Term_t> outputVector;

    if (pos.size() == 1) {
        const uint8_t p = pos[0];
        for (int i = 0; i < size; ++i) {
            Term_t *startTuple = rawBindings->getOffset(i * nPosToCopy);
            outputVector.push_back(startTuple[p]);
        }
        sort(outputVector.begin(), outputVector.end());
        unique(outputVector.begin(), outputVector.end());
    } else if (pos.size() == 2) {
        std::vector<std::pair<Term_t, Term_t>> pairs;
        const uint8_t p1 = pos[0];
        const uint8_t p2 = pos[1];
        for (int i = 0; i < size; ++i) {
            Term_t *startTuple = rawBindings->getOffset(i * nPosToCopy);
            pairs.push_back(make_pair(startTuple[p1], startTuple[p2]));
        }
        sort(pairs.begin(), pairs.end());
        outputVector.push_back(pairs[0].first);
        outputVector.push_back(pairs[0].second);
        for (int i = 1; i < size; ++i) {
            if (pairs[i].first > pairs[i - 1].first || (pairs[i].first == pairs[i - 1].first && pairs[i].second > pairs[i - 1].second)) {
                outputVector.push_back(pairs[i].first);
                outputVector.push_back(pairs[i].second);
            }
        }
    } else {
        //not yet supported. TODO
        for (int i = 0; i < size; ++i) {
            Term_t *startTuple = rawBindings->getOffset(i * nPosToCopy);
            for (std::vector<uint8_t>::iterator itr = pos.begin(); itr != pos.end();
                    ++itr) {
                outputVector.push_back(*(startTuple + *itr));
            }
            startTuple += nPosToCopy;
        }
    }

    return outputVector;

}

const Term_t *BindingsTable::getTuple(size_t idx) {
    if (rawBindings == NULL)
        return EMPTY_TUPLE;
    else
        return rawBindings->getOffset(idx * nPosToCopy);
}

size_t BindingsTable::getNTuples() {
    return uniqueElements.size();
}

void BindingsTable::print() {
    size_t size = uniqueElements.size();
    for (int i = 0; i < size; ++i) {
        Term_t *startTuple = rawBindings->getOffset(i * nPosToCopy);
        for (int j = 0; j < nPosToCopy; ++j)
            cout << startTuple[j] << " ";
        cout << endl;
    }
}

#ifdef DEBUG
void BindingsTable::statistics() {
    /*
    size_t nbuckets = uniqueElements.bucket_count();
    for (size_t i = 0; i < nbuckets; i++) {
	LOG(DEBUGL) << "Size bucket " << i << ": " << uniqueElements.bucket_size(i);
    }
    */
}
#endif

BindingsTable::~BindingsTable() {
    if (posToCopy != NULL)
        delete[] posToCopy;
    if (rawBindings != NULL)
        delete rawBindings;
}
