#include <trident/sparql/sparqloperators.h>

#include <sparsehash/dense_hash_map>

#include <stdint.h>

struct JoinSorter {
    const std::vector<uint64_t> *vector;
    const size_t idxColumn;

    JoinSorter(const std::vector<uint64_t> *vector, const size_t idxColumn) : vector(vector),
        idxColumn(idxColumn) {}

    bool operator() (const size_t i, const size_t j) {
        return vector->at(i + idxColumn) < vector->at(j + idxColumn);
    }
};

struct DoubleJoinSorter {
    const std::vector<uint64_t> *vector;
    const size_t idxColumn1;
    const size_t idxColumn2;

    DoubleJoinSorter(const std::vector<uint64_t> *vector, const size_t idxColumn1, const size_t idxColumn2) : vector(vector),
        idxColumn1(idxColumn1), idxColumn2(idxColumn2) {}

    bool operator() (const size_t i, const size_t j) {
        if (vector->at(i + idxColumn1) == vector->at(j + idxColumn1)) {
            return vector->at(i + idxColumn2) < vector->at(j + idxColumn2);
        } else {
            return vector->at(i + idxColumn1) < vector->at(j + idxColumn1);
        }
    }
};

void HashJoinItr::fillNextMap(const int i, std::vector<uint64_t> &currentMapValues,
                              int &currentMapRowSize,
                              std::unique_ptr<HashJoinMap> &currentMap1,
                              std::unique_ptr<DoubleHashJoinMap> &currentMap2,
                              std::vector<uint64_t> &tmpContainer,
                              const int tmpContainerRowSize,
                              std::vector<size_t> &idxRows) {

    const JoinPoint *joins = &(plan->joins[i + 1][0]);
    const int njoins = plan->joins[i + 1].size();
    const int joinField1 = joins[0].posRow;
    const int joinField2 = njoins == 2 ? joins[1].posRow : 0;
    assert(njoins > 0 && njoins < 3);

    currentMapValues.clear();
    currentMapRowSize = tmpContainerRowSize;
    if (njoins == 1) {
        JoinSorter sorter(&tmpContainer, joinField1);
        std::sort(idxRows.begin(), idxRows.end(), std::ref(sorter));
        currentMap1 = std::unique_ptr<HashJoinMap>(new HashJoinMap());
        currentMap1->set_empty_key(UINT64_MAX);
        currentMap2 = NULL;
    } else if (njoins == 2) {
        DoubleJoinSorter sorter(&tmpContainer, joinField1, joinField2);
        std::sort(idxRows.begin(), idxRows.end(), std::ref(sorter));
        currentMap2 = std::unique_ptr<DoubleHashJoinMap>(new DoubleHashJoinMap());
        std::pair<uint64_t, uint64_t> emptyKey;
        emptyKey.first = UINT64_MAX;
        emptyKey.second = UINT64_MAX;
        currentMap2->set_empty_key(emptyKey);
        currentMap1 = NULL;
    }

    uint64_t prevValue1 = 0;
    uint64_t prevValue2 = 0;
    size_t beginIdx = 0;

    // BOOST_LOG_TRIVIAL(debug) << "fillNextMap: idxRows.size() = " << idxRows.size();
    for (size_t i = 0; i < idxRows.size(); ++i) {
        size_t beginRow = idxRows[i];
        // BOOST_LOG_TRIVIAL(debug) << "fillNextMap: idxRows[" << i << "] = " << beginRow;
        if (i == 0) {
            prevValue1 = tmpContainer[beginRow + joinField1];
            if (njoins == 2)
                prevValue2 = tmpContainer[beginRow + joinField2];
            currentMapValues.push_back(1);
        } else if (tmpContainer[beginRow + joinField1] != prevValue1
                   || (njoins == 2 && tmpContainer[beginRow + joinField2] != prevValue2)) {

            if (njoins == 1) {
                currentMap1->insert(std::make_pair(prevValue1, beginIdx));
            } else {
                std::pair<uint64_t, uint64_t> key;
                key.first = prevValue1;
                key.second = prevValue2;
                currentMap2->insert(std::make_pair(key, beginIdx));
            }

            long nValuesPerKey = (currentMapValues.size() - beginIdx - 1) / tmpContainerRowSize;
            if (nValuesPerKey > 1) {
                currentMapValues[beginIdx] = nValuesPerKey;
            }
            prevValue1 = tmpContainer[beginRow + joinField1];
            if (njoins == 2)
                prevValue2 = tmpContainer[beginRow + joinField2];
            beginIdx = currentMapValues.size();
            currentMapValues.push_back(1);
        }
        for (int i = 0; i < tmpContainerRowSize; ++i) {
            currentMapValues.push_back(tmpContainer[beginRow + i]);
        }
    }

    if (njoins == 1) {
        // BOOST_LOG_TRIVIAL(debug) << "CurrentMap1.insert(" << prevValue1 << ", "
        //     << beginIdx << ")";
        currentMap1->insert(std::make_pair(prevValue1, beginIdx));
    } else {
        std::pair<uint64_t, uint64_t> key;
        key.first = prevValue1;
        key.second = prevValue2;
        currentMap2->insert(std::make_pair(key, beginIdx));
    }

    long nValuesPerKey = (currentMapValues.size() - beginIdx - 1) / tmpContainerRowSize;
    if (nValuesPerKey > 1) {
        currentMapValues[beginIdx] = nValuesPerKey;
    }
}

void sortPairElements(std::vector<uint64_t> &vector) {
    std::vector<std::pair<uint64_t, uint64_t>> pairs;
    for (int i = 0; i < vector.size(); ) {
        std::pair<uint64_t, uint64_t> pair;
        pair.first = vector[i++];
        pair.second = vector[i++];
        pairs.push_back(pair);
    }
    sort(pairs.begin(), pairs.end());
    vector.clear();
    for (std::vector<std::pair<uint64_t, uint64_t>>::iterator itr = pairs.begin();
            itr != pairs.end(); ++itr) {
        vector.push_back(itr->first);
        vector.push_back(itr->second);
    }
}

void HashJoinItr::execJoin() {
    //Current map
    std::vector<uint64_t> currentMapValues;
    std::unique_ptr<HashJoinMap> currentMap1;
    std::unique_ptr<DoubleHashJoinMap> currentMap2;

    int currentMapRowSize = 0;

    for (int i = 0; i < children.size(); ++i) {
        const uint8_t *varsToCopy = plan->posVarsToCopy[i].size() > 0 ?
                                    &(plan->posVarsToCopy[i][0]) : NULL;
        const size_t nvarstocopy = plan->posVarsToCopy[i].size();

        if (i == 0) {
            BOOST_LOG_TRIVIAL(debug) << "Process pattern " << i;
        } else {
            BOOST_LOG_TRIVIAL(debug) << "Process pattern " << i << " Results so far: " << currentMapValues.size() / currentMapRowSize;
        }

        SPARQLOperator *scan = children[i].get();
        //Sideways information passing?
        TupleIterator *itr;
        if (scan->doesSupportsSideways() && i != 0) {
            const int njoins = plan->joins[i].size();
            // BOOST_LOG_TRIVIAL(debug) << "njoins = " << njoins;
            assert(njoins > 0 && njoins < 3);
            const JoinPoint *joins = &(plan->joins[i][0]);

            std::vector<uint8_t> posJoins;
            std::vector<uint64_t> allvalues;
            if (njoins == 1) {
                posJoins.push_back((uint8_t) joins[0].posPattern);
                // BOOST_LOG_TRIVIAL(debug) << "posJoin[0] = " << (int) joins[0].posPattern;
                for (HashJoinMap::iterator itr = currentMap1->begin();
                        itr != currentMap1->end();
                        ++itr) {
                    allvalues.push_back(itr->first);
                }
                sort(allvalues.begin(), allvalues.end());
            } else { //joins = 2
                // BOOST_LOG_TRIVIAL(debug) << "posJoin[0] = " << (int) joins[0].posPattern;
                // BOOST_LOG_TRIVIAL(debug) << "posJoin[1] = " << (int) joins[1].posPattern;
                posJoins.push_back((uint8_t) joins[0].posPattern);
                posJoins.push_back((uint8_t) joins[1].posPattern);
                for (DoubleHashJoinMap::iterator itr = currentMap2->begin();
                        itr != currentMap2->end();
                        ++itr) {
                    allvalues.push_back(itr->first.first);
                    allvalues.push_back(itr->first.second);
                }
                sortPairElements(allvalues);
            }
            BOOST_LOG_TRIVIAL(debug) << "Possible bindings passed to the reasoner " << allvalues.size() / posJoins.size();
            scan->optimize(&posJoins, &allvalues);
            itr = scan->getIterator(posJoins, allvalues);
        } else {
            itr = scan->getIterator();
        }

        if (i < children.size() - 1) {
            std::vector<uint64_t> tmpContainer;
            std::vector<size_t> idxRows;
            int tmpContainerRowSize;

            if (i == 0) {
                while (itr->hasNext()) {
                    // BOOST_LOG_TRIVIAL(debug) << "Pushing entry to " << tmpContainer.size();
                    idxRows.push_back(tmpContainer.size());
                    itr->next();
                    for (int i = 0; i < nvarstocopy; ++i) {
                        tmpContainer.push_back(itr->getElementAt(varsToCopy[i]));
                    }
                }
                tmpContainerRowSize = nvarstocopy;
            } else {
                const int njoins = plan->joins[i].size();
                assert(njoins > 0 && njoins < 3);
                const JoinPoint *joins = &(plan->joins[i][0]);

#if DEBUG
                if (currentMap1 != NULL)
                    BOOST_LOG_TRIVIAL(debug) << "Size bindings " << currentMap1->size();
#endif

                long nTuples = 0;
                while (itr->hasNext()) {
                    nTuples++;
                    itr->next();
                    //Join against the current map
                    bool found = false;
                    size_t beginRow;

                    if (njoins == 1) {
                        HashJoinMap::iterator mapItr =
                            currentMap1->find(itr->getElementAt(joins[0].posPattern));
                        if (mapItr != currentMap1->end()) {
                            beginRow = mapItr->second;
                            found = true;
                        }
                    } else if (njoins == 2) {
                        std::pair<uint64_t, uint64_t> key;
                        key.first = itr->getElementAt(joins[0].posPattern);
                        key.second = itr->getElementAt(joins[1].posPattern);
                        DoubleHashJoinMap::iterator mapItr = currentMap2->find(key);
                        if (mapItr != currentMap2->end()) {
                            beginRow = mapItr->second;
                            found = true;
                        }
                    }

                    if (found) {
                        //Add values in the next map
                        const long nRows = currentMapValues[beginRow++];
                        assert(nRows > 0);
                        for (long i = 0; i < nRows; ++i) {
                            idxRows.push_back(tmpContainer.size());
                            //Copy1
                            for (int i = 0; i < currentMapRowSize; ++i) {
                                tmpContainer.push_back(currentMapValues[beginRow + i]);
                            }
                            //Copy2
                            for (int i = 0; i < nvarstocopy; ++i) {
                                tmpContainer.push_back(itr->getElementAt(varsToCopy[i]));
                            }
                            beginRow += currentMapRowSize;
                        }
                    }
                }
                BOOST_LOG_TRIVIAL(debug) << "The iterator returned " << nTuples << " tuples";

                tmpContainerRowSize = currentMapRowSize + nvarstocopy;
            }

            if (idxRows.size() > 0) {
                fillNextMap(i, currentMapValues, currentMapRowSize, currentMap1,
                            currentMap2, tmpContainer, tmpContainerRowSize, idxRows);
                BOOST_LOG_TRIVIAL(debug) << "Finished loading the map";
            } else {
                scan->releaseIterator(itr);
                return; //No more joins
            }
        } else { //last join
            const int njoins = plan->joins[i].size();
            assert(njoins > 0 && njoins < 3);
            const JoinPoint *joins = &(plan->joins[i][0]);

            //This is used only for the vars in the existing container
            const uint8_t nValuesToCopy = (uint8_t) plan->posVarsToReturn.size();
            const uint8_t *valuesToCopy = &(plan->posVarsToReturn[0]);

            //These are used for the vars in the last pattern
            const uint8_t nValuesToCopy2 = (uint8_t) plan->posVarsToCopy[i].size();
            const uint8_t *valuesToCopy2 = nValuesToCopy2 == 0 ? NULL : &(plan->posVarsToCopy[i][0]);

            output = new TupleTable(nValuesToCopy);

            while (itr->hasNext()) {
                itr->next();
                bool found = false;
                size_t beginRow;

                if (njoins == 1) {
                    long key = itr->getElementAt(joins[0].posPattern);
                    // BOOST_LOG_TRIVIAL(debug) << "Join: key = " << key;
                    HashJoinMap::iterator mapItr = currentMap1->find(key);
                    if (mapItr != currentMap1->end()) {
                        found = true;
                        beginRow = mapItr->second;
                    }
                } else if (njoins == 2) {
                    std::pair<uint64_t, uint64_t> key;
                    key.first = itr->getElementAt(joins[0].posPattern);
                    key.second = itr->getElementAt(joins[1].posPattern);
                    DoubleHashJoinMap::iterator mapItr = currentMap2->find(key);
                    if (mapItr != currentMap2->end()) {
                        found = true;
                        beginRow = mapItr->second;
                    }
                }

                if (found) {
                    const long nRows = currentMapValues[beginRow++];
                    for (long i = 0; i < nRows; ++i) {
                        for (int j = 0; j < nValuesToCopy; ++j) {
                            int index = valuesToCopy[j];
                            if (index < currentMapRowSize) {
                                output->addValue(currentMapValues[beginRow + index]);
                            } else {
                                index -= currentMapRowSize;
                                output->addValue(itr->getElementAt(valuesToCopy2[index]));
                            }
                        }
                        beginRow += currentMapRowSize;
                    }
                }
            }
        }
        scan->releaseIterator(itr);
    }
}

bool HashJoinItr::hasNext() {
    if (!isComputed) {
        execJoin();
        isComputed = true;
    }

    if (output != NULL && rowIdx < (int)output->getNRows() - 1) {
        return true;
    } else {
        return false;
    }
}

void HashJoinItr::next() {
    rowIdx++;
}

size_t HashJoinItr::getTupleSize() {
    return plan->posVarsToReturn.size();
}

uint64_t HashJoinItr::getElementAt(const int pos) {
    return output->getPosAtRow(rowIdx, pos);
}

HashJoinItr::~HashJoinItr() {
    if (output != NULL) {
        delete output;
    }
}
