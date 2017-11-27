#include <vlog/filterhashjoin.h>
#include <vlog/joinprocessor.h>

FilterHashJoinSorter::FilterHashJoinSorter(const uint8_t s, const std::pair<uint8_t, uint8_t> *positions) : nfields(s) {
    for (uint8_t i = 0; i < s; ++i) {
        fields[i] = positions[i].second;
    }
}

FilterHashJoin::FilterHashJoin(ResultJoinProcessor *output,
        const JoinHashMap *map1, const DoubleJoinHashMap *map2,
        std::vector<Term_t> *mapValues,
        const uint8_t mapRowSize, const uint8_t njoinfields,
        const uint8_t joinField1, const uint8_t joinField2,
        const Literal *literal, const bool isDerivationUnique,
        const bool literalSubsumesHead,
        std::vector<DuplicateContainers> *existingTuples,
        const uint8_t nLastLiteralPosConstsInHead,
        const Term_t *lastLiteralValueConstsInHead,
        const uint8_t *lastLiteralPosConstsInHead) :
    output(output), map1(map1), map2(map2),
    mapValues(mapValues), mapRowSize(mapRowSize), njoinfields(njoinfields),
    joinField1(joinField1), joinField2(joinField2),
    nValuesHead(this->output->getNCopyFromSecond()), posValuesHead(this->output->getPosFromSecond()),
    nValuesHashHead(this->output->getNCopyFromFirst()), posValuesHashHead(this->output->getPosFromFirst()),
    literal(literal), isDerivationUnique(isDerivationUnique),
    literalSubsumesHead(literalSubsumesHead), sorter(nValuesHashHead, posValuesHashHead),
    existingTuples(existingTuples), nLastLiteralPosConstsInHead(nLastLiteralPosConstsInHead),
    lastLiteralValueConstsInHead(lastLiteralValueConstsInHead), lastLiteralPosConstsInHead(lastLiteralPosConstsInHead),
    processedElements(0) {
        for (int i = 0; i < nValuesHead; ++i) {
            posValuesHeadRowEdition.push_back(posValuesHead[i]);
        }
#if DEBUG
        for (int i = 0; i < nValuesHead; ++i) {
            LOG(TRACEL) << "posValuesHead[" << i << "] = [" << (int) posValuesHead[i].first << ", " << (int) posValuesHead[i].second << "]";
        }
        for (int i = 0; i < nValuesHashHead; ++i) {
            LOG(TRACEL) << "posValuesHashHead[" << i << "] = [" << (int) posValuesHashHead[i].first << ", " << (int) posValuesHashHead[i].second << "]";
        }
#endif
        std::sort(posValuesHeadRowEdition.begin(), posValuesHeadRowEdition.end());
    }

inline void FilterHashJoin::doJoin_join(const Term_t *constantValues,
        const std::vector<Term_t> &joins1,
        const std::vector<std::pair<Term_t, Term_t>> &joins2,
        std::vector<Term_t> &otherVariablesContainer) {

    matches.clear();
    if (njoinfields == 1) {
        for (std::vector<Term_t>::const_iterator itr = joins1.begin(); itr != joins1.end(); ++itr) {
            JoinHashMap::const_iterator mapItr = map1->find(*itr);
            if (mapItr != map1->end()) {
                //Check whether the derivation does not clash with the input
                size_t start = mapItr->second.first;
                const size_t end = mapItr->second.second;
                while (start < end) {
                    const Term_t *row = &(mapValues->at(start));
                    bool ok = true;
                    for (std::vector<Term_t>::iterator itr = otherVariablesContainer.begin();
                            ok && itr != otherVariablesContainer.end();) {
                        bool same = true;
                        for (uint8_t i = 0; i < nValuesHashHead; ++i) {
                            if (row[posValuesHashHead[i].second] != *itr) {
                                same = false;
                            }
                            itr++;
                        }
                        ok = !same;
                    }
                    if (ok) {
                        //output and add it to the list of used values
                        for (uint8_t i = 0; i < nValuesHashHead; ++i) {
                            Term_t r = row[posValuesHashHead[i].second];
                            otherVariablesContainer.push_back(r);
                            // potentialOutput[posValuesHashHead[i].first] = r;
                        }

                        //Output the unique derivation
                        //output->addRawRow(isDerivationUnique);
                        matches.push_back(row);
                        //matches.push_back(row);
                    }
                    start += mapRowSize;
                }
            }
        }
    } else {
        for (std::vector<std::pair<Term_t, Term_t>>::const_iterator itr = joins2.begin();
                itr != joins2.end(); ++itr) {
            DoubleJoinHashMap::const_iterator mapItr = map2->find(*itr);
            if (mapItr != map2->end()) {
                //Check whether the derivation does not clash with the input
                size_t start = mapItr->second.first;
                const size_t end = mapItr->second.second;
                while (start < end) {
                    const Term_t *row = &(mapValues->at(start));
                    bool ok = true;
                    for (std::vector<Term_t>::iterator itr = otherVariablesContainer.begin();
                            ok && itr != otherVariablesContainer.end();) {
                        bool same = true;
                        for (uint8_t i = 0; i < nValuesHashHead; ++i) {
                            if (row[posValuesHashHead[i].second] != *itr) {
                                same = false;
                            }
                            itr++;
                        }
                        ok = !same;
                    }
                    if (ok) {
                        //output and add it to the list of used values
                        for (uint8_t i = 0; i < nValuesHashHead; ++i) {
                            Term_t r = row[posValuesHashHead[i].second];
                            otherVariablesContainer.push_back(r);
                            // potentialOutput[posValuesHashHead[i].first] = r;
                        }

                        //Output the unique derivation
                        //output->addRawRow(isDerivationUnique);
                        matches.push_back(row);
                        //matches.push_back(row);
                    }
                    start += mapRowSize;
                }
            }
        }
    }

    if (matches.size() > 0) {
        Term_t *potentialOutput = output->getRawRow();
        //Add the group variables
        for (uint8_t i = 0; i < nValuesHead; ++i) {
            potentialOutput[posValuesHead[i].first] = constantValues[i];
        }

        //sort?
        if (matches.size() > 1) {
            //Sort the rows so that the output is sorted
            std::sort(matches.begin(), matches.end(), std::ref(sorter));
        }

        int i = 0;
        for (std::vector<const Term_t*>::iterator itr = matches.begin(); itr != matches.end(); ++itr) {
            for (uint8_t i = 0; i < nValuesHashHead; ++i) {
                potentialOutput[posValuesHashHead[i].first] = (*itr)[posValuesHashHead[i].second];
            }
            output->processResults(i, isDerivationUnique);
            i++;
        }
    }
#if DEBUG
    output->checkSizes();
#endif
}

inline void FilterHashJoin::doJoin_cartprod(const Term_t *constantValues,
        size_t start, const size_t end,
        std::vector<Term_t> &otherVariablesContainer) {

    Term_t *potentialOutput = NULL;

    size_t i = 0;
    while (start < end) {
        if (existingTuples == NULL || existingTuples->at(i).isEmpty() ||
                !existingTuples->at(i).exists(constantValues)) {

            if (potentialOutput == NULL) {
                potentialOutput = output->getRawRow();
                //Add the group variables
                for (uint8_t i = 0; i < nValuesHead; ++i) {
                    potentialOutput[posValuesHeadRowEdition[i].first] = constantValues[i];
                }
            }

            const Term_t *row = &(mapValues->at(start));
            bool ok = true;
            for (std::vector<Term_t>::iterator itr = otherVariablesContainer.begin();
                    ok && itr != otherVariablesContainer.end();) {
                bool same = true;
                for (uint8_t i = 0; i < nValuesHashHead; ++i) {
                    if (row[posValuesHashHead[i].second] != *itr) {
                        same = false;
                    }
                    itr++;
                }
                ok = !same;
            }

            if (ok) {
                //output and add it to the list of used values
                for (uint8_t i = 0; i < nValuesHashHead; ++i) {
                    Term_t r = row[posValuesHashHead[i].second];
                    otherVariablesContainer.push_back(r);
                    potentialOutput[posValuesHashHead[i].first] = r;
                }

                //Output the unique derivation
                output->processResults(i, isDerivationUnique);
            }
        }
        start += mapRowSize;
        i++;
    }
#if DEBUG
    output->checkSizes();
#endif
}

void FilterHashJoin::run(const std::vector<FilterHashJoinBlock> &inputTables, const bool cartprod,
        const size_t startCarprod, const size_t endCartprod,
        const std::vector<uint8_t> ps, int &processedTables,
        const std::vector<std::pair<uint8_t, Term_t>> *valueColumnsToFilter,
        const std::vector<std::pair<uint8_t, uint8_t>> *columnsToFilterOut) {

    std::vector<uint8_t> posToSort = ps;

    LOG(TRACEL) << "FilterHashJoin::run: start = " << startCarprod << ", end = " << endCartprod;
    //Calculate the correct offset of the variables to retrieve the existing bindings
    uint8_t posOtherVariables[SIZETUPLE];
    uint8_t nconstants = 0; //I need to remove constants when I calculate the positions on the last literal
    if (literalSubsumesHead) {
        int j = 0;
        for (uint8_t i = 0; i < nValuesHashHead; ++i) {
            while (j < posValuesHashHead[i].first) {
                if (!literal->getTermAtPos(j).isVariable())
                    nconstants++;
                j++;
            }
            posOtherVariables[i] = posValuesHashHead[i].first - nconstants;
        }
    }
    if (inputTables.size() == 1) {
        const FCInternalTable *table = inputTables[0].table;
        FCInternalTableItr *itr;
        if (posToSort.size() > 0) {
            itr = table->sortBy(posToSort);
        } else {
            itr = table->getIterator();
        }
        if (itr->hasNext()) {
            processedTables++;

            if (existingTuples == NULL && cartprod) {
                run_processitr_columnversion(itr, startCarprod, endCartprod,
                        posOtherVariables, valueColumnsToFilter, columnsToFilterOut);
            } else {
                run_processitr_rowversion(itr, cartprod, startCarprod,
                        endCartprod, posOtherVariables, valueColumnsToFilter,
                        columnsToFilterOut);
            }
        }
        table->releaseIterator(itr);
    } else {
        //Collect all the iterators in a single merged one. Give to each iterator the counter
        std::vector<std::pair<FCInternalTableItr*, size_t>> iterators;
        std::vector<const FCInternalTable*> tables;

        std::vector<uint8_t> sortingCriteria;
        if (posToSort.size() == 0 && tables.size() > 0) {
            //Sort by all columns
            for (uint8_t i = 0; i < tables.back()->getRowSize(); ++i) {
                sortingCriteria.push_back(i);
            }
        } else {
            sortingCriteria = posToSort;
        }

        for (std::vector<FilterHashJoinBlock>::const_iterator itrTable = inputTables.cbegin();
                itrTable != inputTables.cend(); ++itrTable) {
            const FCInternalTable *table = itrTable->table;
            FCInternalTableItr *itr;
            if (sortingCriteria.size() > 0) {
                itr = table->sortBy(sortingCriteria);
            } else {
                itr = table->getIterator();
            }
            if (itr->hasNext()) {
                processedTables++;
                itr->next();
                tables.push_back(table);
                iterators.push_back(std::make_pair(itr, itrTable->iteration));
            } else {
                table->releaseIterator(itr);
            }
        }

        for (int i = 0; i < tables.size(); ++i) {
            assert(tables[i]->getRowSize() == tables[0]->getRowSize());
            assert(tables[i]->getRowSize() == iterators[0].first->getNColumns());
        }

        MergerInternalTableItr mergeItr(iterators, sortingCriteria, inputTables[0].table->getRowSize());
        if (existingTuples == NULL && cartprod) {
            run_processitr_columnversion(&mergeItr, startCarprod, endCartprod, posOtherVariables,
                    valueColumnsToFilter, columnsToFilterOut);
        } else {
            run_processitr_rowversion(&mergeItr, cartprod, startCarprod, endCartprod, posOtherVariables,
                    valueColumnsToFilter, columnsToFilterOut);
        }
        //Release all counters
        for (int i = 0; i < iterators.size(); ++i) {
            tables[i]->releaseIterator(iterators[i].first);
        }
    }
}

void FilterHashJoin::run_processitr_columnversion(FCInternalTableItr *itr,
        const size_t startCarprod, const size_t endCartprod,
        const uint8_t *posOtherVariables,
        const std::vector<std::pair<uint8_t, Term_t>> *valueColumnsToFilter,
        const std::vector<std::pair<uint8_t, uint8_t>> *columnsToFilterOut) {

    std::vector<std::shared_ptr<Column>> columns(output->getRowSize());
    uint8_t pos = 0;
    uint8_t otherPos[SIZETUPLE];

    for (uint8_t i = 0; i < columns.size(); ++i) {
        bool found = false;
        for (uint8_t j = 0; j < nValuesHashHead && !found; ++j) {
            if (posValuesHashHead[j].first == i) {
                // LOG(TRACEL) << "Found HashHead " << (int) j << " at position " << (int) i << ", second = " << (int) posValuesHashHead[j].second;
                found = true;
            }
        }
        for (uint8_t j = 0; j < nValuesHead && !found; ++j) {
            if (posValuesHead[j].first == i) {
                // LOG(TRACEL) << "Found Head " << (int) j << " at position " << (int) i << ", second = " << (int) posValuesHashHead[j].second;
                found = true;
            }
        }
        if (!found) {
            // LOG(TRACEL) << "OtherPos " << (int) pos << " at position " << (int) i;
            otherPos[pos++] = i;
        }
    }


    Term_t valueToFilter;
    uint8_t posColumnToFilter = 0;
    if (valueColumnsToFilter != NULL) {
        //Check the column matches
        assert(valueColumnsToFilter->size() == 1);
        posColumnToFilter = valueColumnsToFilter->at(0).first;
        valueToFilter = valueColumnsToFilter->at(0).second;
    }

    if (columnsToFilterOut != NULL) {
        assert(columnsToFilterOut->size() == 1);
        assert(columnsToFilterOut->at(0).first < itr->getNColumns());
        assert(columnsToFilterOut->at(0).second < itr->getNColumns());
    }


    //Are all variables not involved in the join appearing also in the head? (otherwise I need filtering)
    size_t size = 0; //This function is called when I am sure that the literal has some elements. Therefore, I set 1 as default value
    uint8_t columnIdx[SIZETUPLE];
    for (uint8_t i = 0; i < nValuesHead; ++i) {
        columnIdx[i] = posValuesHead[i].second;
    }
    std::vector<std::shared_ptr<Column>> c = itr->getColumn(nValuesHead, columnIdx);

    LOG(TRACEL) << "literal->getNVars() = " << (int) literal->getNVars();
    if (nValuesHead == literal->getNVars() &&
            nValuesHead > 0 &&
            valueColumnsToFilter == NULL &&
            columnsToFilterOut == NULL) {

        for (uint8_t i = 0; i < nValuesHead; ++i) {
            columns[posValuesHead[i].first] = c[i];
            size_t sz = c[i]->size();
            if (i > 0) {
                assert(size == sz);
            } else {
                size = sz;
            }
        }

    } else {
        //There might be duplicates to single out or values to filter
        if (nValuesHead == 0) {
            //In this case, I do not need to collect any variable from the new pattern.
            //I only needed to know that the query was bound to some results. Since I
            //entered in this method only because the query found some matches, then I
            //can simply set the size to 1
            size = 1;
        } else if (nValuesHead == 1) {
            //Filter away the duplicates. I assume it is already sorted
            assert(posColumnToFilter == 0);
            Column *singleC = c[0].get();
            std::unique_ptr<ColumnReader> singleCReader = singleC->getReader();
            Term_t prevEl = singleCReader->first();
            ColumnWriter p;

            //We cannot have columnToFilterOut != NULL because there is only one column
            assert(columnsToFilterOut == NULL);
            if (valueColumnsToFilter == NULL || prevEl != valueToFilter) {
                p.add(prevEl);
                size++;
            }

            //Add the element
            //for (uint32_t i = 1; i < singleC->size(); ++i) {
            while (singleCReader->hasNext()) {
                Term_t v = singleCReader->next();
                if (v != prevEl) {
                    if (valueColumnsToFilter == NULL || v != valueToFilter) {
                        p.add(v);
                        size++;
                    }
                }
                prevEl = v;
                //}
        }

        columns[posValuesHead[0].first] = p.getColumn();
        //size = columns[posValuesHead[0].first]->size();
        } else if (nValuesHead == 2) {
            //Filter away the duplicates. I assume it is already sorted
            Column *c1 = c[0].get();
            Column *c2 = c[1].get();
            std::unique_ptr<ColumnReader> c1R = c1->getReader();
            std::unique_ptr<ColumnReader> c2R = c2->getReader();

            Term_t prevEl1 = c1R->first();
            Term_t prevEl2 = c2R->first();

            ColumnWriter p1;
            ColumnWriter p2;

            if (columnsToFilterOut == NULL || (prevEl1 != prevEl2)) {
                if (valueColumnsToFilter == NULL ||
                        (posColumnToFilter == 0 && prevEl1 != valueToFilter) ||
                        (posColumnToFilter == 1 && prevEl2 != valueToFilter)) {
                    p1.add(prevEl1);
                    p2.add(prevEl2);
                    size++;
                }
            }

            //Add the element
            //for (uint32_t i = 1; i < c1->size(); ++i) {
            while (c1R->hasNext() && c2R->hasNext()) {
                const Term_t valueC1R = c1R->next();
                const Term_t valueC2R = c2R->next();
                if (valueC1R != prevEl1 || valueC2R != prevEl2) {
                    if (columnsToFilterOut == NULL || (valueC1R != valueC2R)) {
                        if (valueColumnsToFilter == NULL ||
                                (posColumnToFilter == 0 && prevEl1 != valueToFilter) ||
                                (posColumnToFilter == 1 && prevEl2 != valueToFilter)) {
                            p1.add(valueC1R);
                            p2.add(valueC2R);
                            size++;
                        }
                    }
                }
                prevEl1 = valueC1R;
                prevEl2 = valueC2R;
            }

            columns[posValuesHead[0].first] = p1.getColumn();
            columns[posValuesHead[1].first] = p2.getColumn();
            //size = columns[posValuesHead[0].first]->size();
        } else {
            throw 10;
        }
        }

        LOG(TRACEL) << "startCarprod = " << startCarprod << ", mapRowSize = " << (int) mapRowSize
            << ", endCartprod = " << endCartprod << ", size = " << size << ", pos = " << (int) pos;
        LOG(TRACEL) << "nValuesHead = " << (int) nValuesHead << ", nValuesHashHead = " << (int) nValuesHashHead;
        if (size > 0) {
            //Add other constants
            const Term_t *row = output->getRawRow();
            for (uint8_t i = 0; i < pos; ++i) {
                columns[otherPos[i]] = std::shared_ptr<Column>(
                        new CompressedColumn(row[otherPos[i]], size));
            }

            uint32_t s = startCarprod;
            uint32_t i = 0;
            while (s < endCartprod) {
                //create constants with the values of the map
                const Term_t *row = &(mapValues->at(s));
                for (uint8_t i = 0; i < nValuesHashHead; ++i) {
                    columns[posValuesHashHead[i].first] = std::shared_ptr<Column>(
                            new CompressedColumn(row[posValuesHashHead[i].second], size));
                }

                /*
                   if (s != startCarprod) {
                   for (uint8_t i = 0; i < nValuesHead; ++i) {
                   columns[posValuesHead[i].first] = columns[posValuesHead[i].first]; // NOOP??? --Ceriel
                   }
                   for (uint8_t i = 0; i < pos; ++i) {
                   columns[otherPos[i]] = columns[otherPos[i]]; // NOOP??? --Ceriel
                   }
                   }
                   */

#if DEBUG
                for (int i = 0; i < columns.size(); i++) {
                    size_t sz = columns[i]->size();
                    assert(size == sz);
                }
#endif

                //Add the columns to the output container
                output->addColumns(i, columns, isDerivationUnique,
                        true);


                s += mapRowSize;
                i++;
            }
        }
#if DEBUG
        output->checkSizes();
#endif
    }

    void FilterHashJoin::run_processitr_rowversion(FCInternalTableItr *itr, const bool cartprod,
            const size_t startCarprod, const size_t endCartprod,
            const uint8_t *posOtherVariables,
            const std::vector<std::pair<uint8_t, Term_t>> *valueColumnsToFilter,
            const std::vector<std::pair<uint8_t, uint8_t>> *columnsToFilterOut) {

        uint8_t posToFilter;
        Term_t valueToFilter;
        if (valueColumnsToFilter != NULL) {
            assert(valueColumnsToFilter->size() == 1);
            posToFilter = valueColumnsToFilter->at(0).first;
            valueToFilter = valueColumnsToFilter->at(0).second;
        }
        uint8_t c1, c2;
        if (columnsToFilterOut != NULL) {
            assert(columnsToFilterOut->size() == 1);
            c1 = columnsToFilterOut->at(0).first;
            c2 = columnsToFilterOut->at(0).second;
        }

        std::vector<Term_t> joinsContainer1;
        std::vector<std::pair<Term_t, Term_t>> joinsContainer2;
        Term_t valuesHead[SIZETUPLE];
        bool firstGroup = true;
        std::vector<Term_t> otherVariablesContainer;
        while (itr->hasNext()) {
            processedElements++;
            itr->next();

            if (valueColumnsToFilter != NULL
                    && itr->getCurrentValue(posToFilter) == valueToFilter) {
                LOG(TRACEL) << "Avoid to consider the value "
                    << valueToFilter << " of column " << (int) posToFilter;
                continue;
            }
            if (columnsToFilterOut != NULL
                    && itr->getCurrentValue(c1) == itr->getCurrentValue(c2)) {
                LOG(TRACEL) << "The columns " << c1 << " and " << c2
                    << " are equivalent " << itr->getCurrentValue(c1);
                continue;
            }

            if (firstGroup) {
                for (uint8_t i = 0; i < nValuesHead; ++i) {
                    valuesHead[i] = itr->getCurrentValue(posValuesHeadRowEdition[i].second);
                }
                firstGroup = false;
            } else {
                //Check if the current value is equivalent to the previous one
                bool ok = true;
                for (uint8_t i = 0; i < nValuesHead; ++i) {
                    if (itr->getCurrentValue(posValuesHeadRowEdition[i].second) != valuesHead[i]) {
                        ok = false;
                    }
                }

                if (!ok) {
                    if (!cartprod) {
                        doJoin_join(valuesHead, joinsContainer1, joinsContainer2, otherVariablesContainer);
                        if (njoinfields == 1) {
                            joinsContainer1.clear();
                        } else {
                            joinsContainer2.clear();
                        }
                    } else {
                        doJoin_cartprod(valuesHead, startCarprod, endCartprod, otherVariablesContainer);
                    }
                    otherVariablesContainer.clear();
                    for (uint8_t i = 0; i < nValuesHead; ++i) {
                        valuesHead[i] = itr->getCurrentValue(posValuesHeadRowEdition[i].second);
                    }
                }
            }

            if (!cartprod) {
                if (njoinfields == 1) {
                    joinsContainer1.push_back(itr->getCurrentValue(joinField1));
                } else {
                    joinsContainer2.push_back(std::make_pair(itr->getCurrentValue(joinField1),
                                itr->getCurrentValue(joinField2)));
                }
            }

            if (literalSubsumesHead) {
                //Filter first
                bool ok = true;
                for (uint8_t i = 0; i < nLastLiteralPosConstsInHead && ok; ++i) {
                    if (itr->getCurrentValue(lastLiteralPosConstsInHead[i]) != lastLiteralValueConstsInHead[i]) {
                        ok = false;
                    }
                }
                if (ok) {
                    for (uint8_t i = 0; i < nValuesHashHead; ++i) {
                        otherVariablesContainer.push_back(itr->getCurrentValue(posOtherVariables[i]));
                    }

                }
            }
        }
        if (!firstGroup) {
            if (!cartprod) {
                doJoin_join(valuesHead, joinsContainer1, joinsContainer2, otherVariablesContainer);
                if (njoinfields == 1) {
                    joinsContainer1.clear();
                } else {
                    joinsContainer2.clear();
                }
            } else {
                doJoin_cartprod(valuesHead, startCarprod, endCartprod, otherVariablesContainer);
            }
            otherVariablesContainer.clear();
        }
    }
