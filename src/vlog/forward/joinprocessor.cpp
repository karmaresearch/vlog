#include <vlog/filterer.h>
#include <vlog/joinprocessor.h>
#include <vlog/seminaiver.h>
#include <vlog/filterhashjoin.h>
#include <vlog/finalresultjoinproc.h>
#include <trident/model/table.h>

#include <google/dense_hash_map>
#include <limits.h>
#include <vector>
#include <inttypes.h>

bool JoinExecutor::isJoinTwoToOneJoin(const RuleExecutionPlan &hv,
        const int currentLiteral) {
    return hv.joinCoordinates[currentLiteral].size() == 1 &&
        hv.posFromFirst[currentLiteral].size() == 0 &&
        hv.sizeOutputRelation[currentLiteral] == 1 &&
        hv.posFromSecond[currentLiteral].size() == 1;
}

void _joinTwoToOne_cur(std::shared_ptr<Column> firstColumn,
        std::shared_ptr<const FCInternalTable> table,
        const RuleExecutionPlan &hv,
        const int currentLiteral,
        ResultJoinProcessor *output,
        const int nthreads) {
    std::chrono::system_clock::time_point startC = std::chrono::system_clock::now();

    //I need a sorted table for the merge join
    std::vector<uint8_t> joinFields;
    joinFields.push_back(hv.joinCoordinates[currentLiteral][0].second);
    assert(hv.joinCoordinates[currentLiteral].size() == 1);
    FCInternalTableItr *itr;
    itr = table->sortBy(joinFields, nthreads);
    LOG(TRACEL) << "sorted joinfields";

    //Get the second columns. One is for joining and the other for the
    //output.
    uint8_t columns[2];
    columns[0] = hv.joinCoordinates[currentLiteral][0].second; //Join col.
    columns[1] = hv.posFromSecond[currentLiteral][0].second;
    assert(columns[0] != columns[1]);
    std::vector<std::shared_ptr<Column>> cols = itr->getColumn(2,
            columns);

    cols.push_back(firstColumn);
    LOG(TRACEL) << "Getting all vectors";

    std::vector<const std::vector<Term_t> *> vectors = Segment::getAllVectors(cols, nthreads);

    LOG(TRACEL) << "Got all vectors";

    //Merge join
    Term_t v1, v2, vout;

    size_t i1 = 0;
    size_t i2 = 0;
    size_t v2size = vectors[0]->size();
    size_t v1size = vectors[2]->size();

    Term_t prevout = (Term_t) - 1;

    if (i1 < v1size) {
        v1 = (*vectors[2])[i1++];
        if (i2 < v2size) {
            v2 = (*vectors[0])[i2++];
            for (;;) {
                if (v1 < v2) {
                    //Move on with v1
                    if (i1 >= v1size) {
                        break;
                    }
                    v1 = (*vectors[2])[i1++];
                } else {
                    if (v1 == v2) {
                        //Output all rout with the same v2
                        vout = (*vectors[1])[i2-1];
                        if (vout != prevout) {
                            output->processResultsAtPos(0, 0, vout, false);
                            prevout = vout;
                        }
                    }

                    //Move on
                    if (i2 >= v2size) {
                        break;
                    }
                    v2 = (*vectors[0])[i2++];
                }
            }
        }
    }

    Segment::deleteAllVectors(cols, vectors);

    table->releaseIterator(itr);
#if DEBUG
    output->checkSizes();
#endif
    std::chrono::duration<double> d =
        std::chrono::system_clock::now() - startC;
    LOG(TRACEL) << "Time joining the two columns " << d.count() * 1000;
}

void JoinExecutor::joinTwoToOne(
        SemiNaiver *naiver,
        const FCInternalTable *intermediateResults,
        const Literal &literal,
        const size_t min,
        const size_t max,
        ResultJoinProcessor *output,
        const RuleExecutionPlan &hv,
        const int currentLiteral,
        const int nthreads) {

    //Get the first column to join. I need it sorted and only the unique els.
    assert(hv.posFromFirst[currentLiteral].size() == 0);
    std::shared_ptr<Column> firstColumn = intermediateResults->getColumn(
            hv.joinCoordinates[currentLiteral][0].first);
    std::chrono::system_clock::time_point startC = std::chrono::system_clock::now();
    firstColumn = firstColumn->sort_and_unique(nthreads);
    std::chrono::duration<double> d =
        std::chrono::system_clock::now() - startC;
    LOG(TRACEL) << "Time sorting and unique " << d.count() * 1000;

    FCIterator tableItr = naiver->getTable(literal, min, max);
    while (!tableItr.isEmpty()) {
        std::shared_ptr<const FCInternalTable> table = tableItr.
            getCurrentTable();

        LOG(TRACEL) << "Calling _joinTwoToOne_cur";
        //Newer faster version. It is not completely tested. I leave the old
        //version commented in case we still need it.
        _joinTwoToOne_cur(firstColumn, table, hv, currentLiteral, output,
                nthreads);
        // _joinTwoToOne_prev(firstColumn, table, plan, currentLiteral, output);

        tableItr.moveNextCount();
    }
}

bool JoinExecutor::isJoinVerificative(
        const FCInternalTable *t1,
        const RuleExecutionPlan &hv,
        const int currentLiteral) {
    //All the fields in the result belong to the existing relation
    return hv.posFromSecond[currentLiteral].size() == 0 &&
        hv.joinCoordinates[currentLiteral].size() == 1 &&
        (t1->supportsDirectAccess() ||
         (hv.posFromFirst[currentLiteral].size() > 0 && hv.posFromFirst[currentLiteral][0].second ==
          hv.joinCoordinates[currentLiteral][0].second && currentLiteral == hv.posFromFirst.size() - 1));
    //The second condition is because we haven't implemented the optimized code
    //yet. The third condition is needed because we need to read the values to
    //return as output directly from memory. If the table does not support it,
    //then we must do the join in another way. This is until I fix it ofcourse.
}

void JoinExecutor::verificativeJoinOneColumnSameOutput(
        SemiNaiver *naiver,
        const FCInternalTable *intermediateResults,
        const Literal &literal,
        const size_t min,
        const size_t max,
        ResultJoinProcessor *output,
        const RuleExecutionPlan &hv,
        const int currentLiteral,
        int nthreads) {

    //Get the first column to join. I need it sorted and only the unique els.
    std::shared_ptr<Column> firstColumn = intermediateResults->getColumn(
            hv.joinCoordinates[currentLiteral][0].first);
    //std::chrono::system_clock::time_point startC = std::chrono::system_clock::now();
    firstColumn = firstColumn->sort_and_unique(nthreads);

    // LOG(TRACEL) << "firstColumn: size = " << firstColumn->size();

    //Get the second column to join
    FCIterator tableItr = naiver->getTable(literal, min, max);
    if (tableItr.getNTables() == 1) {
        // LOG(TRACEL) << "tableItr.getNTables() == 1";
        ColumnWriter writer;
        while (!tableItr.isEmpty()) {
            std::shared_ptr<const FCInternalTable> table = tableItr.
                getCurrentTable();
            //I need a sorted table for the merge join
            std::vector<uint8_t> joinFields;
            joinFields.push_back(hv.joinCoordinates[currentLiteral][0].second);
            FCInternalTableItr *itr = table->sortBy(joinFields, nthreads);

            uint8_t columns[1];
            columns[0] = hv.joinCoordinates[currentLiteral][0].second; //Join col.
            std::vector<std::shared_ptr<Column>> cols = itr->getColumn(1,
                    columns);
            std::shared_ptr<Column> secondColumn = cols[0]->sort_and_unique(nthreads);
            // LOG(TRACEL) << "secondColumn: size = " << cols[0]->size();

            //Now I have two columns
            Column::intersection(firstColumn, secondColumn, writer, nthreads);
            // Column::intersection(firstColumn, secondColumn, writer);

            table->releaseIterator(itr);
            tableItr.moveNextCount();
        }
        if (!writer.isEmpty()) {
            LOG(TRACEL) << "writer not empty, size = " << writer.getColumn()->size();
            // Intersection of sorted-and-unique columns; result does not need sorting. --Ceriel
            output->addColumn(0, 0, writer.getColumn()/*->sort_and_unique(nthreads)*/, false,  true);
        }
#if DEBUG
        output->checkSizes();
#endif
    } else {
        std::vector<std::shared_ptr<Column>> allColumns;
        while (!tableItr.isEmpty()) {
            std::shared_ptr<const FCInternalTable> table = tableItr.getCurrentTable();
            std::shared_ptr<Column> column =
                table->getColumn(hv.joinCoordinates[currentLiteral][0].second);
            //column = column->sort_and_unique(nthreads);
            allColumns.push_back(column);
            tableItr.moveNextCount();
        }

        ColumnWriter secondColumnCreator;
        for (auto t : allColumns) {
            secondColumnCreator.concatenate(t.get());
        }
        std::shared_ptr<Column> secondColumn =
            secondColumnCreator.getColumn()->sort_and_unique(nthreads);
        LOG(TRACEL) << "Finished sorting the columns";
        /*
           bool isSubsumed = secondColumn->size() >= firstColumn->size() && Column::subsumes(secondColumn, firstColumn);
           if (isSubsumed) {
           LOG(TRACEL) << "Subsumed!";
           output->addColumn(0, 0, firstColumn, false,  true);
           } else
           */
        {
            //Now I have two columns. We merge-join them
            ColumnWriter writer;
            // Commented out: multithreaded version seems to be slower.
            // Column::intersection(firstColumn, secondColumn, writer, nthreads);
            Column::intersection(firstColumn, secondColumn, writer);
            if (!writer.isEmpty()) {
                LOG(TRACEL) << "writer not empty, size = " << writer.getColumn()->size();
                // Intersection of sorted-and-unique columns; result does not need sorting. --Ceriel
                output->addColumn(0, 0, writer.getColumn()/*->sort_and_unique(nthreads)*/, false,  true);
            }
        }
#if DEBUG
        output->checkSizes();
#endif
    }
}

void JoinExecutor::verificativeJoinOneColumn(
        SemiNaiver * naiver,
        const FCInternalTable * intermediateResults,
        const Literal & literal,
        const size_t min,
        const size_t max,
        ResultJoinProcessor * output,
        const RuleExecutionPlan &hv,
        const int currentLiteral,
        int nthreads) {

    assert(output->getNCopyFromSecond() == 0);

    //1- Sort the existing results by the join field
    std::vector<uint8_t> joinField;
    joinField.push_back(hv.joinCoordinates[currentLiteral][0].first);

    FCInternalTableItr *itr = intermediateResults->sortBy(joinField, nthreads);
    const uint8_t intResSizeRow = intermediateResults->getRowSize();

    //2- For each distinct key, create an entry in a vector. We'll use
    //these entries to filter the table
    /*
       if (!intermediateResults->supportsDirectAccess()) {
       throw 10; //I need direct access to fetch the rows
    // No longer true, I think. --Ceriel
    }
    */
    std::vector<std::pair<Term_t, std::pair<size_t, size_t>>> keys;
    size_t beginning = 0;
    size_t currentIdx = 0;
    Term_t prevKey = (Term_t) - 1;
    while (itr->hasNext()) {
        itr->next();
        Term_t curr = itr->getCurrentValue(joinField[0]);
        if (curr != prevKey) {
            if (prevKey != (Term_t) - 1) {
                keys.push_back(make_pair(prevKey, std::make_pair(beginning,
                                currentIdx)));
                beginning = currentIdx;
            }
            prevKey = curr;
        }
        currentIdx++;
    }
    keys.push_back(make_pair(prevKey, std::make_pair(beginning,
                    currentIdx)));
    intermediateResults->releaseIterator(itr);

    //3- Query the KB
    FCIterator tableItr = naiver->getTable(literal, min, max);
    int count = 0;
    while (!tableItr.isEmpty() && keys.size() > 0) {
        count++;
        std::vector<std::pair<Term_t, std::pair<size_t, size_t>>> newKeys;

        std::shared_ptr<const FCInternalTable> table = tableItr.
            getCurrentTable();

        //Get the column from the table. Is it EDB? Then offload the join
        //to the EDB layer
        shared_ptr<const Column> column = table->
            getColumn(hv.joinCoordinates[currentLiteral][0].second);
        LOG(TRACEL) << "Count = " << count;
        FCInternalTableItr *itr = intermediateResults->sortBy(joinField /*, nthreads */);
        EDBLayer &layer = naiver->getEDBLayer();
        if (column->isEDB() && layer.supportsCheckIn(((EDBColumn*) column.get())->getLiteral())) {
            //Offload a merge join to the EDB layer
            EDBColumn *edbColumn = (EDBColumn*)column.get();
            LOG(TRACEL) << "EDBcolumn: literal = " << edbColumn->getLiteral().tostring(naiver->getProgram(), &layer);
            size_t sizeColumn = 0;
            std::vector<Term_t> possibleKeys;
            for (auto v : keys) {
                possibleKeys.push_back(v.first);
            }

            std::shared_ptr<Column> matchedKeys = layer.checkIn(possibleKeys,
                    edbColumn->getLiteral(),
                    edbColumn->posColumnInLiteral(),
                    sizeColumn);

            if (sizeColumn == keys.size()) {
                //All keys were matched.
                while (itr->hasNext()) {
                    itr->next();
                    output->processResults(0, itr, NULL, false);
                }
            } else {
                //Merge join and output only the matched rows
                bool next = true;
                if (next && itr->hasNext()) {
                    itr->next();
                } else {
                    next = false;
                }
                size_t idx1 = 0;
                size_t idx2 = 0;
                Term_t outputRow[SIZETUPLE];
                std::unique_ptr<ColumnReader> matchedKeysR = matchedKeys->getReader();

                if (matchedKeysR->hasNext()) {
                    Term_t matchedKeyValue = matchedKeysR->next();
                    // LOG(TRACEL) << "matchedKeyValue = " << matchedKeyValue;

                    while (idx1 < keys.size()) {
                        if (keys[idx1].first < matchedKeyValue) {
                            newKeys.push_back(keys[idx1]);
                            idx1++;
                        } else if (keys[idx1].first > matchedKeyValue) {
                            if (matchedKeysR->hasNext()) {
                                matchedKeyValue = matchedKeysR->next();
                            } else {
                                break;
                            }
                        } else {
                            //Copy the results to the output
                            size_t rowId = keys[idx1].second.first;
                            size_t limit = keys[idx1].second.second;
                            while (idx2 < rowId) {
                                if (next && itr->hasNext()) {
                                    itr->next();
                                } else {
                                    next = false;
                                }
                                idx2++;
                            }
                            for (; rowId < limit; rowId++, idx2++) {
                                assert(next);
                                for (uint8_t i = 0; i < intResSizeRow; ++i) {
                                    outputRow[i] = itr->getCurrentValue(i);
                                }
#if DEBUG
                                // Consistency check
                                if (outputRow[joinField[0]] != matchedKeyValue) {
                                    LOG(ERRORL) << "Oops, outputRow["
                                        << (int) joinField[0] << "] = " << outputRow[joinField[0]] << ", should be " << matchedKeyValue;
                                }
#endif
                                output->processResults(0, outputRow, NULL, false);
                                if (next && itr->hasNext()) {
                                    itr->next();
                                } else {
                                    next = false;
                                }
                            }

                            idx1++;

                            if (matchedKeysR->hasNext()) {
                                matchedKeyValue = matchedKeysR->next();
                            } else {
                                break;
                            }
                        }
                    }
                }

                while (idx1 < keys.size()) {
                    newKeys.push_back(keys[idx1++]);
                }
            }
#if DEBUG
            output->checkSizes();
#endif

        } else { //Column is not EDB
            if (!itr->hasNext())
                throw 10;

            itr->next();
            Term_t outputRow[SIZETUPLE];
            size_t idx1 = 0;
            size_t idx2 = 0;
            //FCInternalTableItr *titr = table->getSortedIterator();
            std::unique_ptr<ColumnReader> titr = column->getReader();
            if (titr->hasNext() && idx1 < keys.size()) {
                Term_t v2 = titr->next();
                //const uint8_t cId = plan.joinCoordinates[currentLiteral][0]
                //                    .second;

                //merge join
                while (true) {
                    const Term_t v1 = keys[idx1].first;
                    //const Term_t v2 = titr->getCurrentValue(cId);
                    if (v1 < v2) {
                        newKeys.push_back(keys[idx1]);
                        idx1++;
                        if (idx1 == keys.size()) {
                            break;
                        }
                    } else if (v1 > v2) {
                        if (titr->hasNext())
                            v2 = titr->next();
                        else
                            break;
                    } else {
                        //Copy the results to the output
                        size_t rowId = keys[idx1].second.first;
                        size_t limit = keys[idx1].second.second;
                        while (idx2 < rowId) {
                            if (!itr->hasNext())
                                throw 10;
                            itr->next();
                            idx2++;
                        }
                        for (; rowId < limit; rowId++, idx2++) {
                            for (uint8_t i = 0; i < intResSizeRow; ++i) {
                                outputRow[i] = itr->getCurrentValue(i);
                            }
#if DEBUG
                            if (outputRow[joinField[0]] != v1) {
                                LOG(ERRORL) << "Oops, outputRow["
                                    << (int) joinField[0] << "] = " << outputRow[joinField[0]] << ", should be " << v1;
                                throw 10;
                            }
#endif
                            output->processResults(0, outputRow, NULL, false);
                            if (itr->hasNext())
                                itr->next();
                        }
                        idx1++;
                        if (idx1 == keys.size()) {
                            break;
                        }
                        if (titr->hasNext())
                            v2 = titr->next();
                        else
                            break;
                    }
                }
            }
            //table->releaseIterator(titr);

            //Add all unmatched keys to the newKeys
            while (idx1 < keys.size()) {
                newKeys.push_back(keys[idx1++]);
            }
#if DEBUG
            output->checkSizes();
#endif
        }
        intermediateResults->releaseIterator(itr);
        keys.swap(newKeys);
        tableItr.moveNextCount();
    }
    LOG(TRACEL) << "Loop executed " << count << " times";
}

void JoinExecutor::verificativeJoin(
        SemiNaiver * naiver,
        const FCInternalTable * intermediateResults,
        const Literal & literal,
        const size_t min,
        const size_t max,
        ResultJoinProcessor * output,
        const RuleExecutionPlan &hv,
        const int currentLiteral,
        int nthreads) {

    //LOG(TRACEL) << "plan.posFromFirst[currentLiteral][0].second = "
    //  << plan.posFromFirst[currentLiteral][0].second
    //  << ", plan.joinCoordinates[currentLiteral][0].first = "
    //       << plan.joinCoordinates[currentLiteral][0].first;

    // If the index of the result in the intermediate is equal to the index of the join in the intermediate...
    if (hv.posFromFirst[currentLiteral][0].second ==
            hv.joinCoordinates[currentLiteral][0].first &&
            hv.sizeOutputRelation[currentLiteral] == 1 &&
            currentLiteral == hv.posFromFirst.size() - 1) { //The last literal checks that this is the last join we execute
        LOG(TRACEL) << "Verificative join one column same output";
        verificativeJoinOneColumnSameOutput(naiver, intermediateResults,
                literal, min,
                max, output, hv, currentLiteral, nthreads);
    } else if (hv.joinCoordinates[currentLiteral].size() == 1) {
        LOG(TRACEL) << "Verificative join one column";
        verificativeJoinOneColumn(naiver, intermediateResults, literal, min,
                max, output, hv, currentLiteral, nthreads);
    } else {
        //not yet supported. Should never occur.
        throw 10;
    }
}

void JoinExecutor::join(SemiNaiver * naiver, const FCInternalTable * t1,
        const std::vector<Literal> *outputLiterals, const Literal & literal,
        const size_t min, const size_t max,
        const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
        std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
        ResultJoinProcessor * output, const bool lastLiteral
        , const RuleExecutionDetails & ruleDetails,
        const RuleExecutionPlan &hv,
        int &processedTables,
        const int currentLiteral,
        const int nthreads) {

    //First I calculate whether the join is verificative or explorative.
    if (JoinExecutor::isJoinVerificative(t1, hv, currentLiteral)) {
        LOG(TRACEL) << "Executing verificativeJoin. t1->getNRows()=" << t1->getNRows();
        verificativeJoin(naiver, t1, literal, min, max, output, hv,
                currentLiteral, nthreads);
    } else if (JoinExecutor::isJoinTwoToOneJoin(hv, currentLiteral)) {
        //Is the join of the like (A),(A,B)=>(A|B). Then we can speed up the merge join
        LOG(TRACEL) << "Executing joinTwoToOne";
        joinTwoToOne(naiver, t1, literal, min, max, output, hv,
                currentLiteral, nthreads);
    } else {
        //This code is to execute more generic joins. We do hash join if
        //keys are few and there is no ordering. Otherwise, merge join.
        if (t1->estimateNRows() <= THRESHOLD_HASHJOIN
                && joinsCoordinates.size() < 3
                && (joinsCoordinates.size() > 1 ||
                    joinsCoordinates[0].first != joinsCoordinates[0].second ||
                    joinsCoordinates[0].first != 0)) {
            LOG(TRACEL) << "Executing hashjoin. t1->getNRows()=" << t1->getNRows();
            hashjoin(t1, naiver, outputLiterals, literal, min, max, filterValueVars,
                    joinsCoordinates, output,
                    lastLiteral, ruleDetails, hv, processedTables, nthreads);
#ifdef DEBUG
            output->checkSizes();
#endif
        } else {
            LOG(TRACEL) << "Executing mergejoin. t1->getNRows()=" << t1->getNRows();
            mergejoin(t1, naiver, outputLiterals, literal, min, max,
                    joinsCoordinates, output, nthreads);
#ifdef DEBUG
            output->checkSizes();
#endif
        }
    }
}

bool JoinExecutor::isJoinSelective(JoinHashMap & map, const Literal & literal,
        const size_t minIteration, const size_t maxIteration,
        SemiNaiver * naiver, const uint8_t joinPos) {
    size_t totalCardinality = naiver->estimateCardinality(literal, minIteration, maxIteration);
    size_t filteringCardinality = 0;
    for (JoinHashMap::iterator itr = map.begin(); itr != map.end(); ++itr) {
        VTuple tuple = literal.getTuple();
        tuple.set(VTerm(0, itr->first), joinPos);
        Literal literalToQuery(literal.getPredicate(), tuple);
        filteringCardinality += naiver->estimateCardinality(literalToQuery, minIteration, maxIteration);
    }

    double ratio = (double)filteringCardinality / totalCardinality;
    // LOG(TRACEL) << "Optimizer: Total cardinality " << totalCardinality
    //                          << " Filtering Cardinality " << filteringCardinality << " ratio " << ratio;
    return ratio < 0.5;
}

uint8_t removePosConstants(uint8_t c, Literal literal) {
    uint8_t result = c;
    for (int i = 0; i < c; ++i) {
        if (!literal.getTermAtPos(i).isVariable()) {
            result--;
        }
    }
    return result;
}

void JoinExecutor::execSelectiveHashJoin(const RuleExecutionDetails & currentRule,
        SemiNaiver * naiver, const JoinHashMap & map,
        const DoubleJoinHashMap & doublemap,
        ResultJoinProcessor *resultsContainer, const uint8_t njoinfields,
        const uint8_t idxJoinField1, const uint8_t idxJoinField2,
        const std::vector<Literal> *outputLiterals, const Literal & literal, const uint8_t rowSize,
        const std::vector<uint8_t> &posToSort, std::vector<Term_t> &values,
        const bool literalSharesVarsWithHead,
        const size_t min, const size_t max,
        const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
        int &processedTables) {

    //Change the position in the output container since we replace a number of variables with constants.
    std::pair<uint8_t, uint8_t> *posFromSecond = resultsContainer->getPosFromSecond();
    for (int i = 0; i < resultsContainer->getNCopyFromSecond(); ++i) {
        if (njoinfields == 2 && posFromSecond[i].second > idxJoinField2) {
            posFromSecond[i].second--;
        }
        if (posFromSecond[i].second > idxJoinField1) {
            posFromSecond[i].second--;
        }
    }
    {
        //Rewrite the positions to sort
        std::vector<uint8_t> newPosToSort;
        if (njoinfields < 2) {
            for (int i = 0; i < posToSort.size(); ++i) {
                if (posToSort[i] > idxJoinField1) {
                    newPosToSort.push_back(posToSort[i] - 1);
                } else if (posToSort[i] != idxJoinField1) {
                    newPosToSort.push_back(posToSort[i]);
                }
            }
        } else {
            for (uint8_t i = 0; i < (uint8_t) posToSort.size(); ++i) {
                uint8_t possort = posToSort[i];
                if (possort != idxJoinField1 && possort != idxJoinField2) {
                    if (possort > idxJoinField2) {
                        possort--;
                    }
                    if (possort > idxJoinField1) {
                        possort--;
                    }
                    newPosToSort.push_back(possort);
                }
            }
        }

        //Go through the literal. Every constant increase the index
        uint8_t idxJoinFieldInLiteral1 = idxJoinField1;
        uint8_t idxJoinFieldInLiteral2 = idxJoinField2;
        if (njoinfields < 2) {
            uint8_t nvars = 0;
            for (uint8_t i = 0; nvars <= idxJoinField1; ++i) {
                if (!literal.getTermAtPos(i).isVariable()) {
                    idxJoinFieldInLiteral1++;
                } else {
                    nvars++;
                }
            }
        } else {
            uint8_t nvars = 0;
            for (uint8_t i = 0; nvars <= idxJoinField1 || nvars <= idxJoinField2; ++i) {
                if (!literal.getTermAtPos(i).isVariable()) {
                    if (nvars <= idxJoinField1)
                        idxJoinFieldInLiteral1++;
                    if (nvars <= idxJoinField2)
                        idxJoinFieldInLiteral2++;
                } else {
                    nvars++;
                }
            }
            assert(idxJoinFieldInLiteral1 < idxJoinFieldInLiteral2);
        }

        //We need these to filter duplicates
        const std::pair<uint8_t, uint8_t> *posFromFirst = resultsContainer->getPosFromFirst();
        const uint8_t nPosFromFirst = resultsContainer->getNCopyFromFirst();
        VTuple tuple = literal.getTuple();
        std::vector<DuplicateContainers> existingTuples;

        //Iterating through the hashmap
        JoinHashMap::const_iterator itr1;
        DoubleJoinHashMap::const_iterator itr2;
        {
            bool ok;
            if (njoinfields < 2) {
                itr1 = map.begin();
                ok = itr1 != map.end();
            } else {
                itr2 = doublemap.begin();
                ok = itr2 != doublemap.end();
            }

            while (ok) {
                existingTuples.clear();

                size_t start, end;
                //set up the query and coordinates
                if (njoinfields < 2) {
                    if (njoinfields > 0)
                        tuple.set(VTerm(0, itr1->first), idxJoinFieldInLiteral1);
                    start = itr1->second.first;
                    end = itr1->second.second;
                } else {
                    tuple.set(VTerm(0, itr2->first.first), idxJoinFieldInLiteral1);
                    tuple.set(VTerm(0, itr2->first.second), idxJoinFieldInLiteral2);
                    start = itr2->second.first;
                    end = itr2->second.second;
                }
                Literal literalToQuery(literal.getPredicate(), tuple);

                //These are values that cannot appear in certain positions,
                //otherwise they will generate duplicated derivations
                std::vector<std::pair<uint8_t, Term_t>> valuesToFilterOut;
                std::vector<std::pair<uint8_t, uint8_t>> columnsToFilterOut;

                TableFilterer queryFilterer(naiver);

                //Query the rewritten literal
                std::chrono::system_clock::time_point startRetr = std::chrono::system_clock::now();
                FCIterator tableItr = naiver->getTable(literalToQuery, min, max, &queryFilterer);
#if DEBUG
                std::chrono::duration<double> secRetr = std::chrono::system_clock::now() - startRetr;
                LOG(TRACEL) << "Time retrieving table " << secRetr.count() * 1000;
                LOG(TRACEL) << "literal to query = " << literal.tostring();
#endif
                if (tableItr.isEmpty()) {
                    LOG(TRACEL) << "Empty table!";
                    //Move to the next entry in the hashmap
                    if (njoinfields < 2) {
                        itr1++;
                        ok = itr1 != map.end();
                    } else {
                        itr2++;
                        ok = itr2 != doublemap.end();
                    }
                    continue;
                }

                std::vector<std::pair<const FCBlock *, uint32_t>> tables;
                LOG(TRACEL) << "rowSize = " << (int) rowSize << ", start = " << start << ", end = " << end;
                const size_t nderivations = (end - start) / rowSize;
                while (!tableItr.isEmpty()) {
                    tables.push_back(std::make_pair(tableItr.getCurrentBlock(), nderivations));
                    tableItr.moveNextCount();
                }

                //Query the head of the rule to see whether there is previous data to check for duplicates
                //std::chrono::system_clock::time_point startD = std::chrono::system_clock::now();
                bool emptyIterals = true;
#if DEBUG
                LOG(TRACEL) << "Check " << (end - start) / rowSize << " duplicates";
#endif
                // This block was commented out. WHY???
                // Fixed now for when outputLiterals has length 1.
                // Note that this code is essential for correct functioning ... --Ceriel
                while (outputLiterals != NULL && outputLiterals->size() == 1 &&  start < end) {
                    VTuple t = (*outputLiterals)[0].getTuple();
                    for (uint8_t i = 0; i < nPosFromFirst; ++i) {
                        t.set(VTerm(0, values[start + posFromFirst[i].second]), posFromFirst[i].first);
                    }
                    Literal l((*outputLiterals)[0].getPredicate(), t);

                    //Filter the tables in input checking whether the input query produced
                    //by the rule can have produced output tuples in following derivations
                    uint32_t nEmptyDerivations = 0;
                    for (std::vector<std::pair<const FCBlock *, uint32_t>>::iterator itr = tables.begin();
                            itr != tables.end(); ++itr) {
                        if (queryFilterer.
                                producedDerivationInPreviousSteps(l,
                                    literalToQuery, itr->first)) {
                            itr->second--;
                            nEmptyDerivations++;
                        }
                    }

                    if (nEmptyDerivations < tables.size()) {
                        FCIterator outputItr = naiver->getTable(l, 0, (size_t) - 1);
                        if (!outputItr.isEmpty()) {
                            existingTuples.
                                push_back(DuplicateContainers(
                                            outputItr,
                                            resultsContainer->getNCopyFromSecond()));
                            emptyIterals = false;
                        } else {
                            existingTuples.push_back(DuplicateContainers());
                        }
                    } else {
                        existingTuples.push_back(DuplicateContainers());
                    }
                    start += rowSize;
                }


                {
                    std::vector<FilterHashJoinBlock> retainedTables;
                    for (std::vector<std::pair<const FCBlock *, uint32_t>>::iterator itr = tables.begin();
                            itr != tables.end(); ++itr) {
                        if (itr->second > 0) {
                            FilterHashJoinBlock b;
                            b.table = itr->first->table.get();
                            b.iteration = itr->first->iteration;
                            retainedTables.push_back(b);
                        }
                    }
                    //std::chrono::duration<double> secD = std::chrono::system_clock::now() - startD;

#if DEBUG
                    LOG(TRACEL) << "Start actual join...";
#endif
                    //std::chrono::system_clock::time_point startJ = std::chrono::system_clock::now();
                    {
                        std::vector<uint8_t> ps = newPosToSort;

                        FilterHashJoin exec(resultsContainer, &map, &doublemap, &values, rowSize, njoinfields,
                                idxJoinField1, idxJoinField2,
                                &literalToQuery, true, false, (emptyIterals) ? NULL : &existingTuples,
                                0, NULL, NULL); //The last three parameters are
                        //not set because the flag 'isDerivationUnique' is set to false
                        LOG(TRACEL) << "Retained table size = " << retainedTables.size();
                        if (retainedTables.size() > 0) {
                            if (njoinfields == 1) {
                                // LOG(TRACEL) << "first = " << (int) itr1->second.first << ", second = " << (int) itr1->second.second;
                                // LOG(TRACEL) << "idxJoinField = " << (int) idxJoinField1;
                                exec.run(retainedTables, true, itr1->second.first, itr1->second.second,
                                        newPosToSort, processedTables,
                                        valuesToFilterOut.size() > 0 ? &valuesToFilterOut : NULL,
                                        columnsToFilterOut.size() > 0 ? &columnsToFilterOut : NULL);
                            } else {
                                exec.run(retainedTables, true, itr2->second.first, itr2->second.second,
                                        newPosToSort, processedTables,
                                        valuesToFilterOut.size() > 0 ? &valuesToFilterOut : NULL,
                                        columnsToFilterOut.size() > 0 ? &columnsToFilterOut : NULL);

                            }
                        }
                    }
                    //std::chrono::duration<double> secJ = std::chrono::system_clock::now() - startJ;
                } //retained tables

                for (std::vector<DuplicateContainers>::iterator itr = existingTuples.begin();
                        itr != existingTuples.end();
                        ++itr) {
                    itr->clear();
                }

                //size_t uniqueDerivation = out->getUniqueDerivation();
                //size_t unfilteredDerivation = out->getUnfilteredDerivation();
                //std::chrono::system_clock::time_point startC = std::chrono::system_clock::now();
                resultsContainer->consolidate(false);
                //std::chrono::duration<double> secC = std::chrono::system_clock::now() - startC;

#if DEBUG
                /*** LOGGING ***/
                std::string sMapValues = "";
                /*
                   for (int i = itr->second.first; i < itr->second.second; ++i) {
                   sMapValues += to_string(values[i]) + " ";
                   }
                   */
                //LOG(TRACEL) << "HashJoin: ntables=" << tableItr.getNTables() << " exitingTuples=" << existingTuples.size() << " GetRetrTime=" << secRetr.count() * 1000 << " GetDuplTime=" << secD.count() * 1000 << " JoinTime=" << secJ.count() * 1000 << "ms ConsolidationTime=" << secC.count() * 1000 << "ms. Input=" << exec.getProcessedElements() << " Output(f)=" << uniqueDerivation << " Output(nf)=" << unfilteredDerivation << " JoinKey=" << itr->first << " MapValues=" << sMapValues;
                /*** END LOGGING ***/
#endif

                //Move to the next entry in the hashmap
                if (njoinfields == 1) {
                    itr1++;
                    ok = itr1 != map.end();
                } else {
                    itr2++;
                    ok = itr2 != doublemap.end();
                }
            }


        } //before map
    } // exiting containers

}

void JoinExecutor::hashjoin(const FCInternalTable * t1, SemiNaiver * naiver,
        const std::vector<Literal> *outputLiterals, const Literal & literal,
        const size_t min, const size_t max,
        const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
        std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
        ResultJoinProcessor * output,
        const int lastLiteral,
        const RuleExecutionDetails & ruleDetails,
        const RuleExecutionPlan &hv,
        int &processedTables,
        int nthreads) {

    bool literalSharesVarsWithHead;
    std::vector<uint8_t> lastPosToSort;
    if (lastLiteral != -1) {
        lastPosToSort = hv.lastSorting;
        literalSharesVarsWithHead = hv.lastLiteralSharesWithHead;
    }

    JoinHashMap map;
    DoubleJoinHashMap doublemap;

    if (joinsCoordinates.size() < 2) {
        map.set_empty_key(std::numeric_limits<Term_t>::max());
    } else {
        doublemap.set_empty_key(std::make_pair(std::numeric_limits<Term_t>::max(),
                    std::numeric_limits<Term_t>::max()));
    }

    std::vector<Term_t> values;

    //Fill the hashmap
    {
        std::vector<uint8_t> fields;
        for (uint32_t i = 0; i < joinsCoordinates.size(); ++i) {
            fields.push_back(joinsCoordinates[i].first);
        }
        // No parallel sort, t1 is not supposed to be large.
        FCInternalTableItr *t2 = t1->sortBy(fields);

        size_t startpos = 0;
        bool first = true;

        if (joinsCoordinates.size() < 2) {
            //we can have 0 or 1 joins
            Term_t currentKey;
            uint8_t keyField = 0;
            if (joinsCoordinates.size() > 0)
                keyField = joinsCoordinates[0].first;

            //Filter equal value in the join and head position
            bool filterRowsInhashMap = lastLiteral && hv.filterLastHashMap;
            uint8_t filterRowsPosJoin, filterRowsPosOther = 0;
            if (filterRowsInhashMap) {
                filterRowsPosJoin = joinsCoordinates[0].first;
                FinalRuleProcessor* o = (FinalRuleProcessor*)output;
                assert(o->getNCopyFromFirst() == 1);
                filterRowsPosOther = o->getPosFromFirst()[0].second;
                if (filterRowsPosJoin == filterRowsPosOther) {
                    filterRowsInhashMap = false;
                }
            }

            while (t2->hasNext()) {
                t2->next();

                if (filterRowsInhashMap &&
                        t2->getCurrentValue(filterRowsPosJoin) == t2->getCurrentValue(filterRowsPosOther)) {
                    continue;
                }
                Term_t newKey = t2->getCurrentValue(keyField);
                if (first) {
                    currentKey = newKey;
                    first = false;
                } else if (newKey != currentKey) {
                    size_t end = values.size();
                    map.insert(std::make_pair(currentKey, std::make_pair(startpos, end)));
                    currentKey = newKey;
                    startpos = values.size();
                }
                for (uint8_t j = 0; j < t1->getRowSize(); ++j)
                    values.push_back(t2->getCurrentValue(j));
            }

            if (!first) {
                size_t end = values.size();
                map.insert(std::make_pair(currentKey, std::make_pair(startpos, end)));
            }

            //If the map does not contain any entry, then I can safetly exit
            if (map.size() == 0) {
                t1->releaseIterator(t2);
                return;
            }

        } else {
            const uint8_t keyField1 = joinsCoordinates[0].first;
            const uint8_t keyField2 = joinsCoordinates[1].first;
            std::pair<Term_t, Term_t> currentKey;
            while (t2->hasNext()) {
                t2->next();
                Term_t newFirst = t2->getCurrentValue(keyField1);
                Term_t newSecond = t2->getCurrentValue(keyField2);
                if (first) {
                    currentKey.first = newFirst;
                    currentKey.second = newSecond;
                    first = false;
                } else {
                    if (newFirst != currentKey.first ||
                            newSecond != currentKey.second) {
                        size_t end = values.size();
                        doublemap.insert(std::make_pair(currentKey, std::make_pair(startpos, end)));
                        currentKey.first = newFirst;
                        currentKey.second = newSecond;
                        startpos = values.size();
                    }
                }
                for (uint8_t j = 0; j < t1->getRowSize(); ++j)
                    values.push_back(t2->getCurrentValue(j));
            }

            if (!first) {
                size_t end = values.size();
                doublemap.insert(std::make_pair(currentKey, std::make_pair(startpos, end)));
            }

            if (doublemap.size() == 0) {
                t1->releaseIterator(t2);
                return;
            }
        }

        t1->releaseIterator(t2);
    }

    if (joinsCoordinates.size() < 2) {
        LOG(DEBUGL) << "Hashmap size = " << map.size();
    } else {
        LOG(DEBUGL) << "Hashmap size = " << doublemap.size();
    }
    
    //Perform as many joins as the rows in the hashmap
    execSelectiveHashJoin(ruleDetails, naiver, map, doublemap, output, (uint8_t) joinsCoordinates.size(),
            (joinsCoordinates.size() > 0) ? joinsCoordinates[0].second : 0,
            (joinsCoordinates.size() > 1) ? joinsCoordinates[1].second : 0,
            outputLiterals, literal, t1->getRowSize(), lastPosToSort, values,
            literalSharesVarsWithHead, min, max, filterValueVars,
            processedTables);
}

int JoinExecutor::cmp(const Term_t *r1, const Term_t *r2, const uint8_t s) {
    for (uint8_t i = 0; i < s; ++i)
        if (r1[i] != r2[i])
            return r1[i] - r2[i];
    return 0;
}

void JoinExecutor::doPhysicalHashJoin(FCIterator & itr2, JoinHashMap & map,
        std::vector<Term_t> &mapValues, const uint8_t joinIdx2,
        const uint8_t rowSize, const uint8_t s2,
        ResultJoinProcessor * output) {
    while (!itr2.isEmpty()) {
        std::shared_ptr<const FCInternalTable> table = itr2.getCurrentTable();
        itr2.moveNextCount();
        FCInternalTableItr *itr = table->getIterator();
        while (itr->hasNext()) {
            itr->next();
            JoinHashMap::iterator mapItr = map.find(itr->getCurrentValue(joinIdx2));
            if (mapItr != map.end()) {
                //Perform the join
                size_t start = mapItr->second.first;
                const size_t end = mapItr->second.second;
                int i = 0;
                while (start < end) {
                    const Term_t *idxValues = &(mapValues[start]);
                    output->processResults(i, idxValues, itr, false);
                    start += rowSize;
                    i++;
                }
            }
        }
        table->releaseIterator(itr);
    }
}

void JoinExecutor::mergejoin(const FCInternalTable * t1, SemiNaiver * naiver,
        const std::vector<Literal> *outputLiterals,
        const Literal &literalToQuery,
        const uint32_t min, const uint32_t max,
        std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
        ResultJoinProcessor * output,
        int nthreads) {
    //Find whether some of the join fields have a very low cardinality. We can group them.
    std::vector<uint8_t> idxColumnsLowCardInMap;
    std::vector<uint8_t> idxColumnsLowCardInLiteral;
    std::vector<std::vector<Term_t>> bagValuesColumns;
    std::vector<uint8_t> fields1;
    std::vector<uint8_t> fields2;

    for (uint32_t i = 0; i < joinsCoordinates.size(); ++i) {
        std::vector<Term_t> distinctValues =
            ((InmemoryFCInternalTable*)t1)->getDistinctValues(joinsCoordinates[i].first, 20);
        LOG(TRACEL) << "distinctValues.size() = " << distinctValues.size();
        if (distinctValues.size() < 20) {
            //std::sort(distinctValues.begin(), distinctValues.end());
            idxColumnsLowCardInMap.push_back(joinsCoordinates[i].first);
            idxColumnsLowCardInLiteral.push_back(joinsCoordinates[i].second);
            bagValuesColumns.push_back(distinctValues);
        } else { /*if (isJoin)*/
            fields1.push_back(joinsCoordinates[i].first);
            fields2.push_back(joinsCoordinates[i].second);
        }
    }
    //LOG(TRACEL) << "MergeJoin: ExistingResults " << t1->getNRows()
    //                         << " SizeRowsT1=" << (int)t1->getRowSize() << " Number columns to filter: "
    //                         << idxColumnsLowCardInMap.size() << " literalToQuery="
    //                         << literalToQuery.tostring(NULL, NULL);

    //remove all constants from fields2 and update all the others
    std::vector<uint8_t> sortedIdx = idxColumnsLowCardInLiteral;
    std::sort(sortedIdx.begin(), sortedIdx.end());
    for (int i = sortedIdx.size() - 1; i >= 0; --i) {
        //Adapt outputCopyCoordinates
        const uint8_t nPosFromSecond = output->getNCopyFromSecond();
        std::pair<uint8_t, uint8_t> *posFromSecond = output->getPosFromSecond();
        for (uint8_t m = 0; m < nPosFromSecond; ++m) {
            if (posFromSecond[m].second > sortedIdx[i]) {
                posFromSecond[m].second--;
            }
        }

        //Remove constants from fields2
        for (uint32_t j = 0; j < fields2.size(); ++j) {
            if (fields2[j] > sortedIdx[i]) {
                fields2[j]--;
            }
        }
    }

    //Do the join
    if (idxColumnsLowCardInMap.size() == 0) {
        LOG(TRACEL) << "Calling do_mergejoin";

        TableFilterer filterer(naiver);
        std::vector<std::shared_ptr<const FCInternalTable>> tablesToMergeJoin;
        FCIterator it = naiver->getTable(literalToQuery, min, max,
                &filterer);

        while (!it.isEmpty()) {
            std::shared_ptr<const FCInternalTable> t = it.getCurrentTable();
            bool ok = true;

            //The first condition tests we are evaluating the last literal
            bool isEligibleForPruning = outputLiterals != NULL &&
                filterer.isEligibleForPartialSubs(
                        it.getCurrentBlock(),
                        *outputLiterals,
                        t1,
                        output->getNCopyFromFirst(),
                        joinsCoordinates.size());
            if (isEligibleForPruning) {
                if (filterer.producedDerivationInPreviousStepsWithSubs(
                            it.getCurrentBlock(),
                            *outputLiterals, literalToQuery, t1,
                            output->getNCopyFromFirst(),
                            output->getPosFromFirst(),
                            joinsCoordinates.size(),
                            &joinsCoordinates[0])) {
                    ok = false;
                }
            }
            if (ok)
                tablesToMergeJoin.push_back(t);
            it.moveNextCount();
        }

        if (tablesToMergeJoin.size() > 0)
            do_mergejoin(t1, fields1, tablesToMergeJoin, fields1, NULL, NULL,
                    fields2, output, nthreads);
    } else {
        //Positions to return when filtering the input query
        std::vector<uint8_t> posToCopy;
        for (uint8_t i = 0; i < (uint8_t) t1->getRowSize(); ++i) {
            posToCopy.push_back(i);
        }

        std::vector<uint32_t> idxs;
        idxs.push_back(0);
        while (idxs.size() > 0) {
            //std::string keys = "";
            if (idxs.size() < idxColumnsLowCardInLiteral.size()) {
                idxs.push_back(0);
                continue;
            }

            VTuple t = literalToQuery.getTuple();
            std::vector<Term_t> valuesToFilterInFirstSide;
            for (uint32_t j = 0; j < idxColumnsLowCardInLiteral.size(); ++j) {
                uint8_t idxInLiteral = 0;
                uint8_t nvars = 0;
                for (uint8_t r = 0; r < literalToQuery.getTupleSize(); ++r) {
                    if (literalToQuery.getTermAtPos(r).isVariable()) {
                        if (nvars == idxColumnsLowCardInLiteral[j]) {
                            idxInLiteral = r;
                        }
                        nvars++;
                    }
                }
                t.set(VTerm(0, bagValuesColumns[j][idxs[j]]), idxInLiteral);
                valuesToFilterInFirstSide.push_back(bagValuesColumns[j][idxs[j]]);
            }

            //Filter
            std::chrono::system_clock::time_point startFiltering = std::chrono::system_clock::now();
            std::shared_ptr<const FCInternalTable> filteredT1 = t1->filter((uint8_t) posToCopy.size(),
                    &(posToCopy[0]), (uint8_t) idxColumnsLowCardInMap.size(), &(idxColumnsLowCardInMap[0]),
                    &(valuesToFilterInFirstSide[0]), 0, NULL, nthreads);
            std::chrono::duration<double> secFiltering = std::chrono::system_clock::now() - startFiltering;
            LOG(TRACEL) << "Time filtering " << secFiltering.count() * 1000;

            if (filteredT1 != NULL  && !filteredT1->isEmpty()) {
                //Look for additional variables that have a low cardinality in this set
                //For now we limit to one additional variable. More will require additional code that does not seem necessary
                std::vector<uint8_t> idxOtherPos;
                std::vector<std::vector<Term_t>> valueOtherPos;
                for (uint8_t i = 0; i < filteredT1->getRowSize() && idxOtherPos.size() == 0; ++i) {
                    bool found = false;
                    for (uint8_t j = 0; j < joinsCoordinates.size(); ++j) {
                        if (i == joinsCoordinates[j].first) {
                            found = true;
                            break;
                        }
                    }

                    //TODO: is this variable used in the next container?

                    if (!found) {
                        std::vector<Term_t> distinctValues = ((InmemoryFCInternalTable*)filteredT1.get())->getDistinctValues(i, 20);
                        if (distinctValues.size() < 20) {
                            idxOtherPos.push_back(i);
                            std::sort(distinctValues.begin(), distinctValues.end());
                            valueOtherPos.push_back(distinctValues);
                        }
                    }
                }

                //Add also the additional variables in the sorting list
                std::vector<uint8_t> fieldsToSortInMap = fields1;
                for (std::vector<uint8_t>::iterator itr = idxOtherPos.begin(); itr != idxOtherPos.end();
                        ++itr) {
                    fieldsToSortInMap.push_back(*itr);
                }

                //Merge-join
                Literal newLiteralToQuery(literalToQuery.getPredicate(), t);

#if DEBUG
                std::chrono::system_clock::time_point startGI = std::chrono::system_clock::now();
#endif


                TableFilterer filterer(naiver);
                std::vector<std::shared_ptr<const FCInternalTable>> tablesToMergeJoin;
                FCIterator itr2 = naiver->getTable(newLiteralToQuery, min, max,
                        &filterer);
                while (!itr2.isEmpty()) {
                    std::shared_ptr<const FCInternalTable> t = itr2.getCurrentTable();
                    tablesToMergeJoin.push_back(t);
                    itr2.moveNextCount();
                }

                //FCIterator itr2 = naiver->getTable(, min, max);

#if DEBUG
                std::chrono::duration<double> secGI = std::chrono::system_clock::now() - startGI;
                LOG(TRACEL) << "Time getting iterator " << secGI.count() * 1000;
#endif

                //std::chrono::system_clock::time_point startJ = std::chrono::system_clock::now();
                if (idxOtherPos.size() > 0 && valueOtherPos[0].size() > 1) {
                    do_mergejoin(filteredT1.get(), fieldsToSortInMap, tablesToMergeJoin,
                            fields1, &(idxOtherPos[0]), &(valueOtherPos[0]), fields2, output, nthreads);
                } else {
                    do_mergejoin(filteredT1.get(), fieldsToSortInMap, tablesToMergeJoin,
                            fields1, NULL, NULL, fields2, output, nthreads);
                }
                //std::chrono::duration<double> secJ = std::chrono::system_clock::now() - startJ;

                //size_t uniqueDerivation = output->getUniqueDerivation();
                //size_t unfilteredDerivation = output->getUnfilteredDerivation();
                //std::chrono::system_clock::time_point startC = std::chrono::system_clock::now();
                output->consolidate(false);
                //std::chrono::duration<double> secC = std::chrono::system_clock::now() - startC;

                /*** LOGGING ***/
                //#if DEBUG
                //                LOG(TRACEL) << "MergeJoin: JoinTime=" << secJ.count() * 1000 << "ms. ConsTime="
                //                                         << secC.count() * 1000 << "ms. Output(f)=" << uniqueDerivation
                //                                         << " Output(nf)=" << unfilteredDerivation;// << " keys=" << keys;
                //#endif
                /*** END LOGGING ***/
            }
            if (idxs.size() > 0) {
                idxs[idxs.size() - 1]++;
                while (idxs.back() == bagValuesColumns[idxs.size() - 1].size()) {
                    idxs.pop_back();
                    if (idxs.size() > 0)
                        idxs[idxs.size() - 1]++;
                    else
                        break;
                }
            }
        }
    }

}

void JoinExecutor::do_merge_join_classicalgo(FCInternalTableItr * sortedItr1,
        FCInternalTableItr * sortedItr2,
        const std::vector<uint8_t> &fields1,
        const std::vector<uint8_t> &fields2,
        const uint8_t posBlocks,
        const Term_t *valBlocks,
        Output * output) {

    //Classic merge join
    bool active1 = true;
    if (sortedItr1->hasNext()) {
        sortedItr1->next();
    } else {
        active1 = false;
    }

    bool active2 = true;
    if (sortedItr2->hasNext()) {
        sortedItr2->next();
    } else {
        active2 = false;
    }
    int res = 0;

    //Special case. There is no merge join
    if (fields1.size() == 0 && active1 && active2) {
        if (sortedItr1->getNColumns() == 0) {
            assert(sortedItr2->getNColumns() != 0);
            do {
                output->processResults(0, sortedItr1, sortedItr2, false);
                active2 = sortedItr2->hasNext();
                if (active2) {
                    sortedItr2->next();
                }
            } while (active2);
        } else if (sortedItr2->getNColumns() == 0) {
            do {
                output->processResults(0, sortedItr1, sortedItr2, false);
                active1 = sortedItr1->hasNext();
                if (active1) {
                    sortedItr1->next();
                }
            } while (active1);
        }
    }

    std::chrono::system_clock::time_point startL = std::chrono::system_clock::now();

    size_t total = 0;
    size_t max = 65536;

    while (active1 && active2) {
        //Are they matching?
        res = JoinExecutor::cmp(sortedItr1, sortedItr2, fields1, fields2);
        while (res < 0 && sortedItr1->hasNext()) {
            sortedItr1->next();
            res = JoinExecutor::cmp(sortedItr1, sortedItr2, fields1, fields2);
        }

        if (res < 0) //The first iterator is finished
            break;

        while (res > 0 && sortedItr2->hasNext()) {
            sortedItr2->next();
            res = JoinExecutor::cmp(sortedItr1, sortedItr2, fields1, fields2);
        }

        if (res > 0) { //The second iterator is finished
            break;
        } else if (res < 0) {
            if (sortedItr1->hasNext()) {
                sortedItr1->next();
                continue;
            } else {
                break;
            }
        }

        assert(res == 0);

        //Go to the end of the first iterator
        uint32_t count = 1;
        std::vector<Term_t> rowsToJoin;
        for (uint8_t idxInItr = 0; idxInItr < sortedItr1->getNColumns();
                ++idxInItr) {
            Term_t v1 = sortedItr1->getCurrentValue(idxInItr);
            rowsToJoin.push_back(v1);
        }

        active1 = sortedItr1->hasNext();
        while (active1) {
            sortedItr1->next();
            if (!sortedItr1->sameAs(rowsToJoin, fields1))
                break;
            for (uint8_t idxInItr = 0; idxInItr < sortedItr1->getNColumns();
                    ++idxInItr) {
                Term_t v1 = sortedItr1->getCurrentValue(idxInItr);
                rowsToJoin.push_back(v1);
            }
            count++;
            active1 = sortedItr1->hasNext();
        }

        do {
            uint8_t idxBlock = 0;
            const Term_t *rowsSortedItr1 = &rowsToJoin[0];
            for (size_t i = 0; i < count; i++) {
                if (valBlocks != NULL) {
                    Term_t currentValue = rowsSortedItr1[posBlocks];
                    while (valBlocks[idxBlock] < currentValue) {
                        idxBlock++;
                    }
                    assert(currentValue == valBlocks[idxBlock]);
                }

                output->processResults(idxBlock, rowsSortedItr1, sortedItr2, false);
                rowsSortedItr1 += sortedItr1->getNColumns();
            }
            total += count;

            while (total >= max) {
                LOG(TRACEL) << "Count = " << count << ", total = " << total;
                max = max + max;
            }

            if (!sortedItr2->hasNext()) {
                break;
            } else {
                sortedItr2->next();
            }
        } while (JoinExecutor::cmp(rowsToJoin, sortedItr2, fields1, fields2) == 0);
    }
    LOG(TRACEL) << "Total = " << total;
#if DEBUG
    std::chrono::duration<double> secL = std::chrono::system_clock::now() - startL;
    LOG(TRACEL) << "do_merge_join: time loop: " << secL.count() * 1000 << ", fields.size()=" << fields1.size();
#endif
}

int JoinExecutor::cmp(const std::vector<const std::vector<Term_t> *> &vectors1, size_t i1,
        const std::vector<const std::vector<Term_t> *> &vectors2, size_t i2,
        const std::vector<uint8_t> &fields1,
        const std::vector<uint8_t> &fields2) {
    for (int i = 0; i < fields1.size(); ++i) {
        uint8_t p1 = fields1[i];
        uint8_t p2 = fields2[i];
        if ((*vectors1[p1])[i1] < (*vectors2[p2])[i2]) {
            return -1;
        } else if ((*vectors1[p1])[i1] > (*vectors2[p2])[i2]) {
            return 1;
        }
    }
    return 0;
}

bool JoinExecutor::sameAs(const std::vector<const std::vector<Term_t> *> &vectors, size_t i1, size_t i2,
        const std::vector<uint8_t> &fields) {
    for (int i = 0; i < fields.size(); ++i) {
        uint8_t p = fields[i];
        if ((*vectors[p])[i1] != (*vectors[p])[i2]) {
            return false;
        }
    }
    return true;
}

void JoinExecutor::do_merge_join_classicalgo(const std::vector<const std::vector<Term_t> *> &vectors1, size_t l1, size_t u1,
        const std::vector<const std::vector<Term_t> *> &vectors2, size_t l2, size_t u2,
        const std::vector<uint8_t> &fields1,
        const std::vector<uint8_t> &fields2,
        const uint8_t posBlocks,
        const Term_t *valBlocks,
        Output * output) {

    LOG(TRACEL) << "mergejoin classical, vector version, l1 = " << l1 << ", u1 = " << u1 << ", l2 = " << l2 << ", u2 = " << u2 << ", fields1.size = " << fields1.size() << ", fields2.size = " << fields2.size();

    if (l1 >= u1 || l2 >= u2) {
        return;
    }

    if (fields1.size() > 0 && fields2.size() > 0) {
        uint8_t p1 = fields1[0];
        uint8_t p2 = fields2[0];

        if ((*vectors1[p1])[l1] > (*vectors2[p2])[u2 - 1]) {
            LOG(TRACEL) << "No possible results: begin value of range larger than end value of vector2";
            return;
        } else if ((*vectors1[p1])[u1 - 1] < (*vectors2[p2])[l2]) {
            LOG(TRACEL) << "No possible results: end value of range smaller than first value of vector2";
            return;
        }

        // Use binary search to find starting points
        size_t u = u2;
        while (l2 < (u - 1)) {
            size_t m = (l2 + u) / 2;
            int res = 0;

            for (int i = 0; i < fields1.size(); ++i) {
                uint8_t p1 = fields1[i];
                uint8_t p2 = fields2[i];

                if ((*vectors1[p1])[l1] < (*vectors2[p2])[m]) {
                    res = -1;
                    break;
                } else if ((*vectors1[p1])[l1] > (*vectors2[p2])[m]) {
                    res = 1;
                    break;
                }
            }
            if (res <= 0) {
                u = m;
            } else {
                l2 = m;
            }
        }
        u = u1;
        while (l1 < (u - 1)) {
            size_t m = (l1 + u) / 2;
            int res = 0;

            for (int i = 0; i < fields1.size(); ++i) {
                uint8_t p1 = fields1[i];
                uint8_t p2 = fields2[i];

                if ((*vectors1[p1])[m] < (*vectors2[p2])[l2]) {
                    res = -1;
                    break;
                } else if ((*vectors1[p1])[m] > (*vectors2[p2])[l2]) {
                    res = 1;
                    break;
                }
            }
            if (res < 0) {
                l1 = m;
            } else {
                u = m;
            }
        }
        LOG(TRACEL) << "found start points, l1 = " << l1 << ", l2 = " << l2;
    }

    int res = 0;

    //Special case. There is no merge join
    if (fields1.size() == 0 && l1 < u1 && l2 < u2) {
        if (vectors1.size() == 0) {
            assert(vectors2.size() != 0);
            for (size_t i = l2; i < u2; i++) {
                output->processResults(0, vectors1, l1, vectors2, i, false);
            }
            return;
        } else if (vectors2.size() == 0) {
            for (size_t i = l1; i < u1; i++) {
                output->processResults(0, vectors1, i, vectors2, l2, false);
            }
            return;
        }
    }

    std::chrono::system_clock::time_point startL = std::chrono::system_clock::now();

    size_t total = 0;
    size_t max = 65536;

    while (l1 < u1 && l2 < u2) {
        //Are they matching?
        while (l1 < u1 && (res = JoinExecutor::cmp(vectors1, l1, vectors2, l2, fields1, fields2)) < 0) {
            l1++;
        }

        if (l1 == u1) break;

        if (res > 0) {
            l2++;
            while (l2 < u2 && (res = JoinExecutor::cmp(vectors1, l1, vectors2, l2, fields1, fields2)) > 0) {
                l2++;
            }
        }

        if (res > 0) { //The second iterator is finished
            break;
        } else if (res < 0) {
            l1++;
            continue;
        }

        assert(res == 0);

        //Go to the end of the first iterator
        size_t count1 = 1;
        size_t count2 = 1;

        while (l1 + count1 < u1) {
            if (! JoinExecutor::sameAs(vectors1, l1, l1 + count1, fields1)) {
                break;
            }
            count1++;
        }
        while (l2 + count2 < u2) {
            if (! JoinExecutor::sameAs(vectors2, l2, l2 + count2, fields2)) {
                break;
            }
            count2++;
        }

        for (size_t j = 0; j < count2; j++) {
            uint8_t idxBlock = 0;
            for (size_t i = 0; i < count1; i++) {
                if (valBlocks != NULL) {
                    Term_t currentValue = (*vectors1[posBlocks])[l1 + i];
                    while (valBlocks[idxBlock] < currentValue) {
                        idxBlock++;
                    }
                    assert(currentValue == valBlocks[idxBlock]);
                }

                output->processResults(idxBlock, vectors1, l1 + i, vectors2, l2 + j, false);
            }
            total += count1;

            while (total >= max) {
                LOG(TRACEL) << "Count = " << count1 << ", total = " << total;
                max = max + max;
            }
        }
        l1 += count1;
        l2 += count2;
    }
    LOG(TRACEL) << "Total = " << total;
#if DEBUG
    std::chrono::duration<double> secL = std::chrono::system_clock::now() - startL;
    LOG(TRACEL) << "do_merge_join: time loop: " << secL.count() * 1000 << ", fields.size()=" << fields1.size();
#endif
}

void JoinExecutor::do_merge_join_fasteralgo(FCInternalTableItr * sortedItr1,
        FCInternalTableItr * sortedItr2,
        const std::vector<uint8_t> &fields1,
        const std::vector<uint8_t> &fields2,
        const uint8_t posBlocks,
        const uint8_t nValBlocks,
        const Term_t *valBlocks,
        ResultJoinProcessor * output) {

    //First grouping the keys on the left side adding information on the bucket to add
    std::vector<std::pair<Term_t, uint32_t>> keys;
    Term_t currentKey = 0;
    uint32_t currentValue = 0;
    bool isFirst = true;
    const uint8_t posKey = fields1[0];
    const uint8_t posKeyInS = fields2[0];
    while (sortedItr1->hasNext()) {
        sortedItr1->next();
        Term_t v = sortedItr1->getCurrentValue(posKey);
        if (isFirst) {
            currentKey = v;
            isFirst = false;
        } else {
            if (currentKey != v) {
                keys.push_back(std::make_pair(currentKey, currentValue));
                currentKey = v;
                currentValue = 0;
            }
        }

        uint8_t idxBlock = 0;
        while (valBlocks[idxBlock] < sortedItr1->getCurrentValue(posBlocks)) {
            idxBlock++;
        }
        currentValue |= 1 << idxBlock;
    }
    if (!isFirst) {
        keys.push_back(std::make_pair(currentKey, currentValue));
    }

    //Do the merge join. For each match, add the derivation in all blocks
    assert(output->getNCopyFromSecond() == 1);
    std::vector<std::pair<Term_t, uint32_t>>::const_iterator itr = keys.begin();
    const uint8_t posToCopy = output->getPosFromSecond()[0].second;
    std::vector<size_t> counts(256);
    for (int i = 0; i < counts.size(); i++) {
        counts[i] = 0;
    }
    while (itr != keys.end() && sortedItr2->hasNext()) {
        sortedItr2->next();
        while (sortedItr2->getCurrentValue(posKeyInS) < itr->first && sortedItr2->hasNext()) {
            sortedItr2->next();
        }
        while (itr != keys.end() && itr->first < sortedItr2->getCurrentValue(posKeyInS)) {
            itr++;
        }
        if (itr != keys.end() && itr->first == sortedItr2->getCurrentValue(posKeyInS)) {
            uint64_t v = itr->second;
            uint8_t idx = 0;
            while (v != 0) {
                if (v & 1) {
                    //Output the derivation
                    output->processResultsAtPos(idx, 0, sortedItr2->getCurrentValue(posToCopy), false);
                    counts[idx]++;
                }
                v = (v >> 1);
                idx++;
            }
        }
    }

    //Add the constant in the resultcontainer to avoid continuos additions
    assert(output->getNCopyFromFirst() == 1);
    const uint8_t posBlocksInResult = output->getPosFromFirst()[0].first;
    for (uint8_t i = 0; i < nValBlocks; ++i) {
        if (counts[i] != 0) {
            std::shared_ptr<Column> column(new CompressedColumn(valBlocks[i], counts[i]));
            output->addColumn(i, posBlocksInResult, column, false, false);
        }
    }

    //Add all other constants that were set in row but do not come from the two sides
    for (uint8_t i = 0; i < output->getRowSize(); ++i) {
        if (i != posBlocksInResult) {
            //Does it come from the literal?
            bool found = false;
            for (uint8_t j = 0; j < output->getNCopyFromSecond() && !found; ++j) {
                if (i == output->getPosFromSecond()[j].first) {
                    found = true;
                }
            }
            if (!found) {
                //Add it as constant
                for (uint8_t m = 0; m < nValBlocks; ++m) {
                    if (counts[m] != 0) {
                        std::shared_ptr<Column> column(new CompressedColumn(output->getRawRow()[i], counts[m]));
                        output->addColumn(m, i, column, false, true);
                    }
                }
            }
        }
    }
}

struct CreateParallelMergeJoiner {
    const std::vector<const std::vector<Term_t> *> vectors;
    FCInternalTableItr *sortedItr2;
    const std::vector<uint8_t> &fields1;
    const std::vector<uint8_t> &fields2;
    const uint8_t posBlocks;
    const uint8_t nValBlocks;
    const Term_t *valBlocks;
    ResultJoinProcessor *output;
    std::mutex *m;

    CreateParallelMergeJoiner(const std::vector<const std::vector<Term_t> *> &vectors,
            FCInternalTableItr *sortedItr2,
            const std::vector<uint8_t> &fields1,
            const std::vector<uint8_t> &fields2,
            const uint8_t posBlocks,
            const uint8_t nValBlocks,
            const Term_t *valBlocks,
            ResultJoinProcessor *output,
            std::mutex *m) :
        vectors(vectors), sortedItr2(sortedItr2),
        fields1(fields1), fields2(fields2), posBlocks(posBlocks),
        nValBlocks(nValBlocks), valBlocks(valBlocks), output(output), m(m) {
        }

    void operator()(const ParallelRange& r) const {
        LOG(TRACEL) << "Parallel merge joiner: r.begin = " << r.begin() << ", r.end = " << r.end();
        FCInternalTableItr *itr1 = new VectorFCInternalTableItr(vectors, r.begin(), r.end());
        Output out(output, m);
        FCInternalTableItr *itr2 = sortedItr2->copy();

        JoinExecutor::do_merge_join_classicalgo(itr1, itr2, fields1,
                fields2, posBlocks, valBlocks, &out);

        out.flush();
        delete itr2;
        delete itr1;
    }
};

struct CreateParallelMergeJoinerVectors {
    const std::vector<const std::vector<Term_t> *> vectors;
    const std::vector<const std::vector<Term_t> *> vectors2;
    const std::vector<uint8_t> &fields1;
    const std::vector<uint8_t> &fields2;
    const uint8_t posBlocks;
    const uint8_t nValBlocks;
    const Term_t *valBlocks;
    ResultJoinProcessor *output;
    std::mutex *m;

    CreateParallelMergeJoinerVectors(const std::vector<const std::vector<Term_t> *> &vectors,
            const std::vector<const std::vector<Term_t> *> vectors2,
            const std::vector<uint8_t> &fields1,
            const std::vector<uint8_t> &fields2,
            const uint8_t posBlocks,
            const uint8_t nValBlocks,
            const Term_t *valBlocks,
            ResultJoinProcessor *output,
            std::mutex *m) :
        vectors(vectors), vectors2(vectors2),
        fields1(fields1), fields2(fields2), posBlocks(posBlocks),
        nValBlocks(nValBlocks), valBlocks(valBlocks), output(output), m(m) {
        }

    void operator()(const ParallelRange& r) const {
        LOG(TRACEL) << "Parallel vector merge joiner: r.begin = " << r.begin() << ", r.end = " << r.end();
        Output out(output, m);

        JoinExecutor::do_merge_join_classicalgo(vectors, r.begin(), r.end(),
                vectors2, 0, vectors2[0]->size(),
                fields1, fields2,
                posBlocks, valBlocks, &out);

        out.flush();
    }
};

void JoinExecutor::do_mergejoin(const FCInternalTable * filteredT1,
        std::vector<uint8_t> &fieldsToSortInMap,
        std::vector<std::shared_ptr<const FCInternalTable>> &tables2,
        const std::vector<uint8_t> &fields1, const uint8_t *posOtherVars,
        const std::vector<Term_t> *valuesOtherVars,
        const std::vector<uint8_t> &fields2, ResultJoinProcessor * output,
        int nthreads) {

    //Only one additional variable is allowed to have low cardinality
    const uint8_t posBlocks = posOtherVars == NULL ? 0 : posOtherVars[0];
    const uint8_t nValBlocks = (valuesOtherVars == NULL) ? 0 : (uint8_t) valuesOtherVars[0].size();
    const Term_t *valBlocks = (valuesOtherVars == NULL) ? NULL : &(valuesOtherVars[0][0]);

    int processedTables = 0;
    bool first = true;

    if (tables2.size() == 0) {
        return;
    }

    FCInternalTableItr *sortedItr1 = NULL;
    std::chrono::system_clock::time_point startS;
    std::chrono::duration<double> secS;
    startS = std::chrono::system_clock::now();

    size_t chunks = 0;
    std::mutex m;

    size_t totalsize2 = 0;
    for (auto t2 : tables2) {
        size_t sz = t2->getNRows();
        LOG(TRACEL) << "tables2 entry size = " << sz;
        totalsize2 += sz;
    }

    //It can be that there are no fields to join.

    //Sort t1
    if (fieldsToSortInMap.size() > 0) {
        sortedItr1 = (InmemoryFCInternalTableItr*)filteredT1->sortBy(fieldsToSortInMap, nthreads);
    } else {
        sortedItr1 = (InmemoryFCInternalTableItr*)filteredT1->getIterator();
    }

    /*
       std::vector<std::shared_ptr<Column>> cols = sortedItr1->getAllColumns();

       bool vectorSupported = true;
       int ncols = (int) sortedItr1->getNColumns();
       for (int i = 0; i < ncols; i++) {
       if (! cols[i]->isBackedByVector()) {
       vectorSupported = false;
       break;
       }
       }
       */

    std::vector<const std::vector<Term_t> *> vectors;
    vectors = sortedItr1->getAllVectors(nthreads);

    size_t totalsize1 = filteredT1->getNRows();

    // if (vectorSupported) {
    // Possibility to parallelize, but also a possibility to create a faster
    // iterator.
    if (nthreads > 1) {
        // chunks = (totalsize1 + nthreads - 1) / nthreads;
        chunks = (totalsize1 + 2 * nthreads - 1) / (2 * nthreads);
    }
    // for (int i = 0; i < ncols; i++) {
    // vectors.push_back(&cols[i]->getVectorRef());
    // }
    // }
    VectorFCInternalTableItr *itr1 = new VectorFCInternalTableItr(vectors, 0, totalsize1);

    secS = std::chrono::system_clock::now() - startS;

#if DEBUG
    LOG(TRACEL) << "do_merge_join: time sorting the left relation: " << secS.count() * 1000;
    LOG(TRACEL) << "filteredT1->size = " << totalsize1 << ", tables2.size() = " << tables2.size() << ", total t2 size = " << totalsize2;
#endif

    bool faster = (fields1.size() == 1 && valBlocks != NULL && output->getNCopyFromFirst() == 1
            && output->getPosFromFirst()[0].second == posBlocks && output->getNCopyFromSecond() == 1);

    Output *out = new Output(output, NULL);

    for (auto t2 : tables2) {
        if (! first) {
            itr1->reset();
        }
        first = false;
        LOG(TRACEL) << "Main loop of do_mergejoin";
        processedTables++;

        //Sort t2
        startS = std::chrono::system_clock::now();
        //Also in this case, there might be no join fields
        FCInternalTableItr *sortedItr2 = NULL;
        if (fields2.size() > 0) {
            LOG(TRACEL) << "t2->sortBy";
            sortedItr2 = t2->sortBy(fields2, nthreads);
        } else {
            sortedItr2 = t2->getIterator();
        }
        bool vector2Supported = true;
        std::vector<const std::vector<Term_t> *> vectors2 = sortedItr2->getAllVectors(nthreads);
        /*
           std::vector<std::shared_ptr<Column>> cols = sortedItr2->getAllColumns();
           int ncols = (int) sortedItr2->getNColumns();
           for (int i = 0; i < ncols; i++) {
           if (! cols[i]->isBackedByVector()) {
           vector2Supported = false;
           break;
           }
           }
           if (vector2Supported) {
           for (int i = 0; i < ncols; i++) {
           vectors2.push_back(&cols[i]->getVectorRef());
           }
           }
           */
        secS = std::chrono::system_clock::now() - startS;
        FCInternalTableItr *itr2 = sortedItr2;
        size_t t2Size = t2->getNRows();
        if (faster) {
            sortedItr2 = new VectorFCInternalTableItr(vectors2, 0, t2Size);
            LOG(TRACEL) << "Faster algo";
            JoinExecutor::do_merge_join_fasteralgo(itr1, sortedItr2, fields1,
                    fields2, posBlocks, nValBlocks,
                    valBlocks, output);
            delete sortedItr2;
#if DEBUG
            output->checkSizes();
#endif
        } else {
            LOG(TRACEL) << "Classical algo";
            LOG(TRACEL) << "totalsize1 = " << totalsize1 << ", t2Size = " << t2Size;
            if (/* vectorSupported && */ nthreads > 1 && totalsize1 > 1 && (totalsize1 + t2Size) > 4096 /* ? */) {
                LOG(TRACEL) << "Chunk size = " << chunks << ", t2->getNRows() = " << t2Size;
                if (vector2Supported) {
                    //tbb::parallel_for(tbb::blocked_range<int>(0, totalsize1, chunks),
                    //        CreateParallelMergeJoinerVectors(vectors, vectors2, fields1, fields2, posBlocks, nValBlocks, valBlocks, output, &m));
                    ParallelTasks::parallel_for(0, totalsize1, chunks,
                            CreateParallelMergeJoinerVectors(vectors, vectors2,
                                fields1, fields2, posBlocks, nValBlocks,
                                valBlocks, output, &m));
                } else {
                    //tbb::parallel_for(tbb::blocked_range<int>(0, totalsize1, chunks),
                    //        CreateParallelMergeJoiner(vectors, sortedItr2, fields1, fields2, posBlocks, nValBlocks, valBlocks, output, &m));
                    ParallelTasks::parallel_for(0, totalsize1, chunks,
                            CreateParallelMergeJoiner(vectors, sortedItr2,
                                fields1, fields2, posBlocks, nValBlocks,
                                valBlocks, output, &m));
                }
            } else {
                JoinExecutor::do_merge_join_classicalgo(vectors, 0, totalsize1,
                        vectors2, 0, t2Size,
                        fields1, fields2,
                        posBlocks, valBlocks, out);
            }
#if DEBUG
            output->checkSizes();
#endif
        }
        itr2->deleteAllVectors(vectors2);
        t2->releaseIterator(itr2);
    }
    delete itr1;
    sortedItr1->deleteAllVectors(vectors);
    filteredT1->releaseIterator(sortedItr1);
    delete out;
#if DEBUG
    LOG(TRACEL) << "Processed tables: " << processedTables;
#endif
}

/*bool JoinExecutor::same(const Segment * segment, const uint32_t idx1, const uint32_t idx2,
  const std::vector<uint8_t> &fields) {
  for (std::vector<uint8_t>::const_iterator itr = fields.cbegin(); itr != fields.cend();
  ++itr) {
//if (segment->at(idx1, *itr) != segment->at(idx2, *itr)) {
//    return false;
//}
}
return true;
}*/

int JoinExecutor::cmp(const Term_t *r1, const Term_t *r2, const std::vector<uint8_t> &fields1,
        const std::vector<uint8_t> &fields2) {
    for (int i = 0; i < fields1.size(); ++i) {
        uint8_t p1 = fields1[i];
        uint8_t p2 = fields2[i];
        if (r1[p1] < r2[p2]) {
            return -1;
        } else if (r1[p1] > r2[p2]) {
            return 1;
        }
    }
    return 0;
}

int JoinExecutor::cmp(FCInternalTableItr * r1, FCInternalTableItr * r2,
        const std::vector<uint8_t> &fields1,
        const std::vector<uint8_t> &fields2) {
    for (int i = 0; i < fields1.size(); ++i) {
        const uint8_t p1 = fields1[i];
        const uint8_t p2 = fields2[i];
        if (r1->getCurrentValue(p1) < r2->getCurrentValue(p2)) {
            return -1;
        } else if (r1->getCurrentValue(p1) > r2->getCurrentValue(p2)) {
            return 1;
        }
    }
    return 0;
}

int JoinExecutor::cmp(const std::vector<Term_t> &r1, FCInternalTableItr * r2,
        const std::vector<uint8_t> &fields1,
        const std::vector<uint8_t> &fields2) {
    for (int i = 0; i < fields1.size(); ++i) {
        const uint8_t p1 = fields1[i];
        const uint8_t p2 = fields2[i];
        if (r1[p1] < r2->getCurrentValue(p2)) {
            return -1;
        } else if (r1[p1] > r2->getCurrentValue(p2)) {
            return 1;
        }
    }
    return 0;
}

bool DuplicateContainers::exists(const Term_t *v) {
    if (ntables == 1) {
        if (nfields == 1) {
            while (firstItr->getCurrentValue(0) < v[0] && firstItr->hasNext()) {
                firstItr->next();
            }

            Term_t currentvalue = firstItr->getCurrentValue(0);
            if (currentvalue < v[0]) {
                //The stream is finished
                empty = true;
            } else if (currentvalue == v[0]) {
                return true;
            }
        } else {
            int resp = cmp(firstItr, v);
            while (resp < 0 && firstItr->hasNext()) {
                firstItr->next();
                resp = cmp(firstItr, v);
            }

            if (resp < 0) {
                empty = true;
            } else {
                if (resp == 0)
                    return true;
            }
        }
    } else {
        if (nfields == 1) {
            uint8_t idxTable = 0;
            size_t emptyTables = 0;
            while (idxTable < ntables) {
                if (tables[idxTable] != NULL) {
                    while (itrs[idxTable]->getCurrentValue(0) < v[0] && itrs[idxTable]->hasNext()) {
                        itrs[idxTable]->next();
                    }

                    Term_t currentvalue = itrs[idxTable]->getCurrentValue(0);
                    if (currentvalue < v[0]) {
                        tables[idxTable]->releaseIterator(itrs[idxTable]);
                        itrs[idxTable] = NULL;
                        tables[idxTable] = NULL;
                    } else if (currentvalue == v[0]) {
                        return true;
                    }
                } else {
                    emptyTables++;
                }
                idxTable++;
            }
            empty = emptyTables == ntables;
        } else {
            uint8_t idxTable = 0;
            size_t emptyTables = 0;
            while (idxTable < ntables) {
                if (tables[idxTable] != NULL) {
                    int resp = cmp(itrs[idxTable], v);
                    while (resp < 0 && itrs[idxTable]->hasNext()) {
                        itrs[idxTable]->next();
                        resp = cmp(itrs[idxTable], v);
                    }

                    if (resp < 0) {
                        tables[idxTable]->releaseIterator(itrs[idxTable]);
                        itrs[idxTable] = NULL;
                        tables[idxTable] = NULL;
                    } else if (resp == 0) {
                        return true;
                    }
                } else {
                    emptyTables++;
                }
                idxTable++;
            }
            empty = emptyTables == ntables;
        }
    }
    return false;
}

int DuplicateContainers::cmp(FCInternalTableItr * itr, const Term_t *v) const {
    for (uint8_t i = 0; i < nfields; ++i) {
        if (itr->getCurrentValue(i) != v[i]) {
            return itr->getCurrentValue(i) - v[i];
        }
    }
    return 0;
}

void DuplicateContainers::clear() {
    if (ntables == 1) {
        if (firstItr != NULL) {
            firstTable->releaseIterator(firstItr);
        }
    } else {
        size_t i = 0;
        while (i < ntables) {
            if (tables[i] != NULL) {
                tables[i]->releaseIterator(itrs[i]);
            }
            i++;
        }
    }

    if (tables != NULL)
        delete[] tables;
    if (itrs != NULL)
        delete[] itrs;
}

DuplicateContainers::DuplicateContainers(FCIterator & itr, const uint8_t sizerow) :
    nfields(sizerow), ntables(itr.getNTables()) {

        if (ntables == 1) {
            firstTable = itr.getCurrentTable().get();
            firstItr = firstTable->getSortedIterator();
            if (firstItr->hasNext()) {
                firstItr->next();
                empty = false;
            } else {
                firstTable->releaseIterator(firstItr);
                firstItr = NULL;
                firstTable = NULL;
                empty = true;
            }
            itrs = NULL;
            tables = NULL;
        } else {
            assert(ntables > 0);
            firstTable = NULL;
            firstItr = NULL;
            tables = new const FCInternalTable*[ntables];
            itrs = new FCInternalTableItr*[ntables];
            size_t i = 0;
            empty = true;
            while (!itr.isEmpty()) {
                if (i >= ntables) {
                    throw 10;
                }
                // Jacopo: valgrind claims an invalid write in the line below! --Ceriel
                tables[i] = itr.getCurrentTable().get();
                itrs[i] = tables[i]->getSortedIterator();
                if (itrs[i]->hasNext()) {
                    itrs[i]->next();
                    empty = false;
                } else {
                    tables[i]->releaseIterator(itrs[i]);
                    itrs[i] = NULL;
                    tables[i] = NULL;
                }
                itr.moveNextCount();
                i++;
            }
        }
    }
