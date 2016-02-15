#include <vlog/filterer.h>
#include <vlog/joinprocessor.h>
#include <vlog/seminaiver.h>
#include <vlog/filterhashjoin.h>
#include <trident/model/table.h>

#include <google/dense_hash_map>
#include <limits.h>
#include <vector>

bool JoinExecutor::isJoinTwoToOneJoin(const RuleExecutionPlan &plan,
                                      const int currentLiteral) {
    return plan.joinCoordinates[currentLiteral].size() == 1 &&
           plan.posFromFirst[currentLiteral].size() == 0 &&
           plan.posFromSecond[currentLiteral].size() == 1;
}

void _joinTwoToOne_prev(std::shared_ptr<Column> firstColumn,
                        std::shared_ptr<const FCInternalTable> table,
                        const RuleExecutionPlan &plan,
                        const int currentLiteral,
                        ResultJoinProcessor *output) {
    //I need a sorted table for the merge join
    std::vector<uint8_t> joinFields;
    joinFields.push_back(plan.joinCoordinates[currentLiteral][0].second);
    assert(plan.joinCoordinates[currentLiteral].size() == 1);
    FCInternalTableItr *itr = table->sortBy(joinFields);

    //Get the second columns. One is for joining and the other for the
    //output.
    uint8_t columns[2];
    columns[0] = plan.joinCoordinates[currentLiteral][0].second; //Join col.
    columns[1] = plan.posFromSecond[currentLiteral][0].second;
    assert(columns[0] != columns[1]);
    std::vector<std::shared_ptr<Column>> cols = itr->getColumn(2,
                                      columns);
    //std::shared_ptr<const Column> secondColumn = cols[0]->sort()->unique();
    std::shared_ptr<const Column> secondColumn = cols[0];

    //Get readers
    //boost::chrono::system_clock::time_point startC = timens::system_clock::now();
    std::unique_ptr<ColumnReader> r1 = firstColumn->getReader();
    std::unique_ptr<ColumnReader> r2 = secondColumn->getReader();
    std::unique_ptr<ColumnReader> rout;

    //std::unique_ptr<ColumnReader> rout = cols[1]->getReader();
    //boost::chrono::duration<double> d =
    //    boost::chrono::system_clock::now() - startC;
    //BOOST_LOG_TRIVIAL(info) << "Time getting readers" << d.count() * 1000;

    //Merge join
    long counter1 = 1;
    long counter2 = 1;
    long cout = 0;
    bool identicalJoin = true; //This flags is used to detect when all values
    //are joining. In this case, I can just copy the output column

    Term_t v1, v2, vout;
    Term_t prevout = (Term_t) - 1;

    bool r1valid = r1->hasNext();
    if (r1valid)
        v1 = r1->next();
    bool r2valid = r2->hasNext();
    if (r2valid) {
        v2 = r2->next();
    }

    while (r1valid && r2valid) {
        if (v1 < v2) {
            //Move on with v1
            r1valid = r1->hasNext();
            if (r1valid) {
                v1 = r1->next();
                counter1++;
            }
        } else {
            //Output all rout with the same v2
            if (v1 == v2) {
                cout++;
                if (!identicalJoin) {
                    if (vout != prevout) {
			BOOST_LOG_TRIVIAL(debug) << "vout = " << vout;
                        output->processResultsAtPos(0, 0, vout, false);
                        prevout = vout;
                    }
                }
            } else if (identicalJoin) {
                identicalJoin = false;
                rout = cols[1]->getReader();
                for (size_t i = 0; i < counter2; ++i) {
                    rout->hasNext(); //Should always succeed
                    vout = rout->next();
                    if (i < cout) {
                        if (vout != prevout) {
			    BOOST_LOG_TRIVIAL(debug) << "vout = " << vout;
                            output->processResultsAtPos(0, 0, vout, false);
                        }
                    }
                    prevout = vout;
                }
            }

            //Move on
            r2valid = r2->hasNext();
            if (r2valid) {
                v2 = r2->next();
                counter2++;
                if (!identicalJoin) {
                    //Move also the output column
                    rout->hasNext();
                    vout = rout->next();
                }
            }
        }
    }
    if (identicalJoin && cout > 0) {
        //Copy the output column in the output
        assert(rout == NULL);
        assert(cout == counter2);
	BOOST_LOG_TRIVIAL(debug) << "Identical join!";
        output->addColumn(0, 0, cols[1]->sort()->unique(), false,  true);
    }
    table->releaseIterator(itr);
}

void _joinTwoToOne_cur(std::shared_ptr<Column> firstColumn,
                       std::shared_ptr<const FCInternalTable> table,
                       const RuleExecutionPlan &plan,
                       const int currentLiteral,
                       ResultJoinProcessor *output) {
    boost::chrono::system_clock::time_point startC = timens::system_clock::now();

    // First see if the joinColumn is a subset of the first column, because in that case we can just
    // copy the output column.
    // Commented out: almost never seems to happen.
    /*
    const uint8_t joinField = plan.joinCoordinates[currentLiteral][0].second;
    assert(plan.joinCoordinates[currentLiteral].size() == 1);

    std::shared_ptr<Column> joinColumn = table->getColumn(joinField);
    joinColumn = joinColumn->sort()->unique();

    bool subsumes = Column::subsumes(firstColumn, joinColumn);
    BOOST_LOG_TRIVIAL(debug) << "joinTwoToOne_cur: subsumes = " << subsumes;
    if (subsumes) {
	const uint8_t outputColumn = plan.posFromSecond[currentLiteral][0].second;
	std::shared_ptr<Column> col = table->getColumn(outputColumn);

	output->addColumn(0, 0, col->sort()->unique(), false,  true);

	boost::chrono::duration<double> d =
	    boost::chrono::system_clock::now() - startC;
	BOOST_LOG_TRIVIAL(debug) << "Time joining the two columns " << d.count() * 1000;
	return;
    }
    */

    //I need a sorted table for the merge join
    std::vector<uint8_t> joinFields;
    joinFields.push_back(plan.joinCoordinates[currentLiteral][0].second);
    assert(plan.joinCoordinates[currentLiteral].size() == 1);
    FCInternalTableItr *itr = table->sortBy(joinFields);

    //Get the second columns. One is for joining and the other for the
    //output.
    uint8_t columns[2];
    columns[0] = plan.joinCoordinates[currentLiteral][0].second; //Join col.
    columns[1] = plan.posFromSecond[currentLiteral][0].second;
    assert(columns[0] != columns[1]);
    std::vector<std::shared_ptr<Column>> cols = itr->getColumn(2,
                                      columns);

    //Get readers
    std::unique_ptr<ColumnReader> r1 = firstColumn->getReader();
    std::unique_ptr<ColumnReader> r2 = cols[0]->getReader();
    std::unique_ptr<ColumnReader> rout = cols[1]->getReader();

    //Merge join
    Term_t v1, v2, vout;
    Term_t prevout = (Term_t) - 1;

    bool r1valid = r1->hasNext();
    if (r1valid)
        v1 = r1->next();
    bool r2valid = r2->hasNext();
    if (r2valid) {
	rout->hasNext(); //Should always succeed
	vout = rout->next();
        v2 = r2->next();
    }

    while (r1valid && r2valid) {
        if (v1 < v2) {
            //Move on with v1
            r1valid = r1->hasNext();
            if (r1valid) {
                v1 = r1->next();
            }
        } else {
            //Output all rout with the same v2
            if (v1 == v2) {
		// BOOST_LOG_TRIVIAL(debug) << "v1 = " << v1 << ", vout = " << vout;
		if (vout != prevout) {
		    output->processResultsAtPos(0, 0, vout, false);
		    prevout = vout;
		}
            }

            //Move on
            r2valid = r2->hasNext();
            if (r2valid) {
                v2 = r2->next();
		//Move also the output column
		rout->hasNext();
		vout = rout->next();
            }
        }
    }

    table->releaseIterator(itr);
    boost::chrono::duration<double> d =
	boost::chrono::system_clock::now() - startC;
    BOOST_LOG_TRIVIAL(debug) << "Time joining the two columns " << d.count() * 1000;
}

void JoinExecutor::joinTwoToOne(
    SemiNaiver *naiver,
    const FCInternalTable *intermediateResults,
    const Literal &literal,
    const size_t min,
    const size_t max,
    ResultJoinProcessor *output,
    const RuleExecutionPlan &plan,
    const int currentLiteral) {

    //Get the first column to join. I need it sorted and only the unique els.
    assert(plan.posFromFirst[currentLiteral].size() == 0);
    std::shared_ptr<Column> firstColumn = intermediateResults->getColumn(
                plan.joinCoordinates[currentLiteral][0].first);
    //boost::chrono::system_clock::time_point startC = timens::system_clock::now();
    firstColumn = firstColumn->sort()->unique();
    //boost::chrono::duration<double> d =
    //    boost::chrono::system_clock::now() - startC;
    //BOOST_LOG_TRIVIAL(info) << "Time sorting and unique " << d.count() * 1000;

    FCIterator tableItr = naiver->getTable(literal, min, max);
    while (!tableItr.isEmpty()) {
        std::shared_ptr<const FCInternalTable> table = tableItr.
                getCurrentTable();

        //Newer faster version. It is not completely tested. I leave the old
        //version commented in case we still need it.
        _joinTwoToOne_cur(firstColumn, table, plan, currentLiteral, output);
        // _joinTwoToOne_prev(firstColumn, table, plan, currentLiteral, output);

        tableItr.moveNextCount();
    }
}

bool JoinExecutor::isJoinVerificative(
    const FCInternalTable *t1,
    const RuleExecutionPlan &plan,
    const int currentLiteral) {
    //All the fields in the result belong to the existing relation
    return plan.posFromSecond[currentLiteral].size() == 0 &&
           plan.joinCoordinates[currentLiteral].size() == 1 &&
           (t1->supportsDirectAccess() ||
            (plan.posFromFirst[currentLiteral].size() > 0 && plan.posFromFirst[currentLiteral][0].second ==
             plan.joinCoordinates[currentLiteral][0].second && currentLiteral == plan.posFromFirst.size() - 1));
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
    const RuleExecutionPlan &plan,
    const int currentLiteral) {

    //Get the first column to join. I need it sorted and only the unique els.
    std::shared_ptr<Column> firstColumn = intermediateResults->getColumn(
            plan.joinCoordinates[currentLiteral][0].first);
    //boost::chrono::system_clock::time_point startC = timens::system_clock::now();
    firstColumn = firstColumn->sort()->unique();

    // BOOST_LOG_TRIVIAL(debug) << "firstColumn: size = " << firstColumn->size();

    //Get the second column to join
    FCIterator tableItr = naiver->getTable(literal, min, max);
    if (tableItr.getNTables() == 1) {
        // BOOST_LOG_TRIVIAL(debug) << "tableItr.getNTables() == 1";
        ColumnWriter writer;
        while (!tableItr.isEmpty()) {
            std::shared_ptr<const FCInternalTable> table = tableItr.
                    getCurrentTable();
            //I need a sorted table for the merge join
            std::vector<uint8_t> joinFields;
            joinFields.push_back(plan.joinCoordinates[currentLiteral][0].second);
            FCInternalTableItr *itr = table->sortBy(joinFields);

            uint8_t columns[1];
            columns[0] = plan.joinCoordinates[currentLiteral][0].second; //Join col.
            std::vector<std::shared_ptr<Column>> cols = itr->getColumn(1,
                                              columns);
            std::shared_ptr<Column> secondColumn = cols[0]->sort()->unique();
            // BOOST_LOG_TRIVIAL(debug) << "secondColumn: size = " << cols[0]->size();

            //Now I have two columns
            Column::intersection(firstColumn, secondColumn, writer);

            table->releaseIterator(itr);
            tableItr.moveNextCount();
        }
        if (!writer.isEmpty()) {
            BOOST_LOG_TRIVIAL(debug) << "writer not empty, size = " << writer.getColumn()->size();
            output->addColumn(0, 0, writer.getColumn()->sort()->unique(), false,  true);
        }
    } else {
        std::vector<std::shared_ptr<Column>> allColumns;
        while (!tableItr.isEmpty()) {
            std::shared_ptr<const FCInternalTable> table = tableItr.getCurrentTable();
            std::shared_ptr<Column> column =
                table->getColumn(plan.joinCoordinates[currentLiteral][0].second);
            //column = column->sort()->unique();
            allColumns.push_back(column);
            tableItr.moveNextCount();
        }

        ColumnWriter secondColumnCreator;
        for (auto t : allColumns) {
            secondColumnCreator.concatenate(t.get());
        }
        std::shared_ptr<Column> secondColumn =
            secondColumnCreator.getColumn()->sort()->unique();
        bool isSubsumed = secondColumn->size() >= firstColumn->size() && Column::subsumes(secondColumn, firstColumn);
        if (isSubsumed) {
            BOOST_LOG_TRIVIAL(debug) << "Subsumed!";
            output->addColumn(0, 0, firstColumn, false,  true);
        } else {
            BOOST_LOG_TRIVIAL(debug) << "Finished sorting the columns";
            //Now I have two columns. We merge-join them
            ColumnWriter writer;
            Column::intersection(firstColumn, secondColumn, writer);
            if (!writer.isEmpty()) {
                BOOST_LOG_TRIVIAL(debug) << "writer not empty, size = " << writer.getColumn()->size();
                output->addColumn(0, 0, writer.getColumn()->sort()->unique(), false,  true);
            }
        }
    }
}

void JoinExecutor::verificativeJoinOneColumn(
    SemiNaiver * naiver,
    const FCInternalTable * intermediateResults,
    const Literal & literal,
    const size_t min,
    const size_t max,
    ResultJoinProcessor * output,
    const RuleExecutionPlan & plan,
    const int currentLiteral) {

    assert(output->getNCopyFromSecond() == 0);

    //1- Sort the existing results by the join field
    std::vector<uint8_t> joinField;
    joinField.push_back(plan.joinCoordinates[currentLiteral][0].first);

    FCInternalTableItr *itr = intermediateResults->sortBy(joinField);
    const uint8_t intResSizeRow = intermediateResults->getRowSize();

    //2- For each distinct key, create an entry in a vector. We'll use
    //these entries to filter the table
    if (!intermediateResults->supportsDirectAccess()) {
        throw 10; //I need direct access to fetch the rows
        // No longer true, I think. --Ceriel
    }
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
                                          getColumn(
                                              plan.joinCoordinates[currentLiteral][0]
                                              .second);
        BOOST_LOG_TRIVIAL(debug) << "Count = " << count;
        FCInternalTableItr *itr = intermediateResults->sortBy(joinField);
        if (column->isEDB()) {
            //Offload a merge join to the EDB layer
            std::vector<Term_t> possibleKeys;
            for (auto v : keys) {
                possibleKeys.push_back(v.first);
            }

            EDBLayer &layer = naiver->getEDBLayer();
            EDBColumn *edbColumn = (EDBColumn*)column.get();
            size_t sizeColumn = 0;
            std::shared_ptr<Column> matchedKeys = layer.checkIn(possibleKeys,
                                                  edbColumn->getLiteral(),
                                                  edbColumn->posColumnInLiteral(),
                                                  sizeColumn);

            //TODO: seems wasteful. We already sorted the intermediate results once. --Ceriel
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
                Term_t outputRow[MAX_ROWSIZE];
                std::unique_ptr<ColumnReader> matchedKeysR = matchedKeys->getReader();

                if (matchedKeysR->hasNext()) {
                    Term_t matchedKeyValue = matchedKeysR->next();
                    // BOOST_LOG_TRIVIAL(debug) << "matchedKeyValue = " << matchedKeyValue;

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
                                    BOOST_LOG_TRIVIAL(error) << "Oops, outputRow["
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

        } else { //Column is not EDB
            if (!itr->hasNext())
                throw 10;

            itr->next();
            Term_t outputRow[MAX_ROWSIZE];
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
                                BOOST_LOG_TRIVIAL(error) << "Oops, outputRow["
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
        }
        intermediateResults->releaseIterator(itr);
        keys.swap(newKeys);
        tableItr.moveNextCount();
    }
    BOOST_LOG_TRIVIAL(debug) << "Loop executed " << count << " times";
}

void JoinExecutor::verificativeJoin(
    SemiNaiver * naiver,
    const FCInternalTable * intermediateResults,
    const Literal & literal,
    const size_t min,
    const size_t max,
    ResultJoinProcessor * output,
    const RuleExecutionPlan & plan,
    const int currentLiteral) {

    //BOOST_LOG_TRIVIAL(debug) << "plan.posFromFirst[currentLiteral][0].second = "
//  << plan.posFromFirst[currentLiteral][0].second
//  << ", plan.joinCoordinates[currentLiteral][0].second = "
//       << plan.joinCoordinates[currentLiteral][0].second;

    if (plan.posFromFirst[currentLiteral][0].second ==
            plan.joinCoordinates[currentLiteral][0].second &&
            currentLiteral == plan.posFromFirst.size() - 1) { //The last literal checks that this is the last join we execute
        BOOST_LOG_TRIVIAL(debug) << "Verificative join one column same output";
        verificativeJoinOneColumnSameOutput(naiver, intermediateResults,
                                            literal, min,
                                            max, output, plan, currentLiteral);
    } else if (plan.joinCoordinates[currentLiteral].size() == 1) {
        BOOST_LOG_TRIVIAL(debug) << "Verificative join one column";
        verificativeJoinOneColumn(naiver, intermediateResults, literal, min,
                                  max, output, plan, currentLiteral);
    } else {
        //not yet supported. Should never occur.
        throw 10;
    }
}

void JoinExecutor::join(SemiNaiver * naiver, const FCInternalTable * t1,
                        const Literal * outputLiteral, const Literal & literal,
                        const size_t min, const size_t max,
                        const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
                        std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
                        ResultJoinProcessor * output, const bool lastLiteral
                        , const RuleExecutionDetails & ruleDetails,
                        const RuleExecutionPlan & plan,
                        int &processedTables,
                        const int currentLiteral) {

    //First I calculate whether the join is verificative or explorative.
    if (JoinExecutor::isJoinVerificative(t1, plan, currentLiteral)) {
        BOOST_LOG_TRIVIAL(debug) << "Executing verificativeJoin. t1->getNRows()=" << t1->getNRows();
        verificativeJoin(naiver, t1, literal, min, max, output, plan,
                         currentLiteral);
    } else {
        //Is the join of the like (A),(A,B)=>(A|B). Then we can speed up the merge join
        if (JoinExecutor::isJoinTwoToOneJoin(plan, currentLiteral)) {
            BOOST_LOG_TRIVIAL(debug) << "Executing joinTwoToOne";
            joinTwoToOne(naiver, t1, literal, min, max, output, plan,
                         currentLiteral);
        } else {
            //This code is to execute more generic joins. We do hash join if
            //keys are few and there is no ordering. Otherwise, merge join.
            if (t1->estimateNRows() <= THRESHOLD_HASHJOIN
                    && joinsCoordinates.size() < 3
                    && (joinsCoordinates.size() > 1 ||
                        joinsCoordinates[0].first != joinsCoordinates[0].second ||
                        joinsCoordinates[0].first != 0)) {
                BOOST_LOG_TRIVIAL(debug) << "Executing hashjoin. t1->getNRows()=" << t1->getNRows();
                hashjoin(t1, naiver, outputLiteral, literal, min, max, filterValueVars,
                         joinsCoordinates, output,
                         lastLiteral, ruleDetails, plan, processedTables);
            } else {
                BOOST_LOG_TRIVIAL(debug) << "Executing mergejoin. t1->getNRows()=" << t1->getNRows();
                mergejoin(t1, naiver, outputLiteral, literal, min, max,
                          joinsCoordinates, output);
            }
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
    // BOOST_LOG_TRIVIAL(debug) << "Optimizer: Total cardinality " << totalCardinality
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
        ResultJoinProcessor * out, const uint8_t njoinfields,
        const uint8_t idxJoinField1, const uint8_t idxJoinField2,
        const Literal * outputLiteral, const Literal & literal, const uint8_t rowSize,
        const std::vector<uint8_t> &posToSort, std::vector<Term_t> &values,
        const bool literalSharesVarsWithHead,
        const size_t min, const size_t max,
        const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
        int &processedTables) {

    //Change the position in the output container since we replace a number of variables with constants.
    std::pair<uint8_t, uint8_t> *posFromSecond = out->getPosFromSecond();
    for (int i = 0; i < out->getNCopyFromSecond(); ++i) {
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
        const std::pair<uint8_t, uint8_t> *posFromFirst = out->getPosFromFirst();
        const uint8_t nPosFromFirst = out->getNCopyFromFirst();
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
                boost::chrono::system_clock::time_point startRetr = boost::chrono::system_clock::now();
                FCIterator tableItr = naiver->getTable(literalToQuery, min, max, &queryFilterer);
#if DEBUG
                boost::chrono::duration<double> secRetr = boost::chrono::system_clock::now() - startRetr;
                BOOST_LOG_TRIVIAL(debug) << "Time retrieving table " << secRetr.count() * 1000;
#endif
                if (tableItr.isEmpty()) {
                    BOOST_LOG_TRIVIAL(debug) << "Empty table!";
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
                BOOST_LOG_TRIVIAL(debug) << "rowSize = " << (int) rowSize << ", start = " << start << ", end = " << end;
                const size_t nderivations = (end - start) / rowSize;
                while (!tableItr.isEmpty()) {
                    tables.push_back(std::make_pair(tableItr.getCurrentBlock(), nderivations));
                    tableItr.moveNextCount();
                }

                //Query the head of the rule to see whether there is previous data to check for duplicates
                //boost::chrono::system_clock::time_point startD = boost::chrono::system_clock::now();
                bool emptyIterals = true;
#if DEBUG
                BOOST_LOG_TRIVIAL(trace) << "Check " << (end - start) / rowSize << " duplicates";
#endif
                while (outputLiteral != NULL && start < end) {
                    VTuple t = outputLiteral->getTuple();
                    for (uint8_t i = 0; i < nPosFromFirst; ++i) {
                        t.set(VTerm(0, values[start + posFromFirst[i].second]), posFromFirst[i].first);
                    }
                    Literal l(outputLiteral->getPredicate(), t);

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
                                          out->getNCopyFromSecond()));
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
                    //boost::chrono::duration<double> secD = boost::chrono::system_clock::now() - startD;

#if DEBUG
                    BOOST_LOG_TRIVIAL(debug) << "Start actual join...";
#endif
                    //boost::chrono::system_clock::time_point startJ = boost::chrono::system_clock::now();
                    {
                        std::vector<uint8_t> ps = newPosToSort;

                        FilterHashJoin exec(out, &map, &doublemap, &values, rowSize, njoinfields,
                                            idxJoinField1, idxJoinField2,
                                            &literalToQuery, true, false, (emptyIterals) ? NULL : &existingTuples,
                                            0, NULL, NULL); //The last three parameters are
                        //not set because the flag 'isDerivationUnique' is set to false
                        BOOST_LOG_TRIVIAL(debug) << "Retained table size = " << retainedTables.size();
                        if (retainedTables.size() > 0) {
                            if (njoinfields == 1) {
                                // BOOST_LOG_TRIVIAL(debug) << "first = " << (int) itr1->second.first << ", second = " << (int) itr1->second.second;
                                // BOOST_LOG_TRIVIAL(debug) << "idxJoinField = " << (int) idxJoinField1;
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
                    //boost::chrono::duration<double> secJ = boost::chrono::system_clock::now() - startJ;
                } //retained tables

                for (std::vector<DuplicateContainers>::iterator itr = existingTuples.begin();
                        itr != existingTuples.end();
                        ++itr) {
                    itr->clear();
                }

                //size_t uniqueDerivation = out->getUniqueDerivation();
                //size_t unfilteredDerivation = out->getUnfilteredDerivation();
                //boost::chrono::system_clock::time_point startC = boost::chrono::system_clock::now();
                out->consolidate(false);
                //boost::chrono::duration<double> secC = boost::chrono::system_clock::now() - startC;

#if DEBUG
                /*** LOGGING ***/
                std::string sMapValues = "";
                /*
                for (int i = itr->second.first; i < itr->second.second; ++i) {
                    sMapValues += to_string(values[i]) + " ";
                }
                */
                //BOOST_LOG_TRIVIAL(trace) << "HashJoin: ntables=" << tableItr.getNTables() << " exitingTuples=" << existingTuples.size() << " GetRetrTime=" << secRetr.count() * 1000 << " GetDuplTime=" << secD.count() * 1000 << " JoinTime=" << secJ.count() * 1000 << "ms ConsolidationTime=" << secC.count() * 1000 << "ms. Input=" << exec.getProcessedElements() << " Output(f)=" << uniqueDerivation << " Output(nf)=" << unfilteredDerivation << " JoinKey=" << itr->first << " MapValues=" << sMapValues;
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
                            const Literal * outputLiteral, const Literal & literal,
                            const size_t min, const size_t max,
                            const std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
                            std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
                            ResultJoinProcessor * output, const bool lastLiteral,
                            const RuleExecutionDetails & ruleDetails, const RuleExecutionPlan & plan,
                            int &processedTables) {

    const bool literalSharesVarsWithHead = plan.lastLiteralSharesWithHead;
    std::vector<uint8_t> lastPosToSort;
    if (lastLiteral)
        lastPosToSort = plan.lastSorting;

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
            bool filterRowsInhashMap = lastLiteral && plan.filterLastHashMap;
            uint8_t filterRowsPosJoin, filterRowsPosOther = 0;
            if (filterRowsInhashMap) {
                filterRowsPosJoin = joinsCoordinates[0].first;
                FinalTableJoinProcessor* o = (FinalTableJoinProcessor*)output;
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
                if (first) {
                    currentKey = t2->getCurrentValue(keyField);
                    first = false;
                } else if (t2->getCurrentValue(keyField) != currentKey) {
                    size_t end = values.size();
                    map.insert(std::make_pair(currentKey, std::make_pair(startpos, end)));
                    currentKey = t2->getCurrentValue(keyField);
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
                return;
            }

        } else {
            const uint8_t keyField1 = joinsCoordinates[0].first;
            const uint8_t keyField2 = joinsCoordinates[1].first;
            std::pair<Term_t, Term_t> currentKey;
            while (t2->hasNext()) {
                //if (t2->hasNext()) {
                t2->next();
                //}

                if (first) {
                    currentKey.first = t2->getCurrentValue(keyField1);
                    currentKey.second = t2->getCurrentValue(keyField2);
                    first = false;
                } else if (t2->getCurrentValue(keyField1) != currentKey.first ||
                           t2->getCurrentValue(keyField2) != currentKey.second) {
                    size_t end = values.size();
                    doublemap.insert(std::make_pair(currentKey, std::make_pair(startpos, end)));
                    currentKey.first = t2->getCurrentValue(keyField1);
                    currentKey.second = t2->getCurrentValue(keyField2);
                    startpos = values.size();
                }
                for (uint8_t j = 0; j < t1->getRowSize(); ++j)
                    values.push_back(t2->getCurrentValue(j));
            }

            if (!first) {
                size_t end = values.size();
                doublemap.insert(std::make_pair(currentKey, std::make_pair(startpos, end)));
            }

            if (doublemap.size() == 0) {
                return;
            }
        }

        t1->releaseIterator(t2);
    }

#if DEBUG
    if (joinsCoordinates.size() < 2) {
        BOOST_LOG_TRIVIAL(trace) << "Hashmap size = " << map.size();
    } else {
        BOOST_LOG_TRIVIAL(trace) << "Hashmap size = " << doublemap.size();
    }
#endif
    //Perform as many joins as the rows in the hashmap
    execSelectiveHashJoin(ruleDetails, naiver, map, doublemap, output, (uint8_t) joinsCoordinates.size(),
                          (joinsCoordinates.size() > 0) ? joinsCoordinates[0].second : 0,
                          (joinsCoordinates.size() > 1) ? joinsCoordinates[1].second : 0,
                          outputLiteral, literal, t1->getRowSize(), lastPosToSort, values,
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
                             const Literal *outputLiteral,
                             const Literal &literalToQuery,
                             const uint32_t min, const uint32_t max,
                             std::vector<std::pair<uint8_t, uint8_t>> joinsCoordinates,
                             ResultJoinProcessor * output) {
    //Find whether some of the join fields have a very low cardinality. We can group them.
    std::vector<uint8_t> idxColumnsLowCardInMap;
    std::vector<uint8_t> idxColumnsLowCardInLiteral;
    std::vector<std::vector<Term_t>> bagValuesColumns;
    //std::vector<uint8_t> idxOtherPos;
    std::vector<uint8_t> fields1;
    std::vector<uint8_t> fields2;

    for (uint32_t i = 0; i < joinsCoordinates.size(); ++i) {
        std::vector<Term_t> dinstinctValues =
            ((InmemoryFCInternalTable*)t1)->getDistinctValues(joinsCoordinates[i].first, 20);
        if (dinstinctValues.size() < 20) {
            //if (isJoin) {
            idxColumnsLowCardInMap.push_back(joinsCoordinates[i].first);
            idxColumnsLowCardInLiteral.push_back(joinsCoordinates[i].second);
            bagValuesColumns.push_back(dinstinctValues);
            //} else {
            //    idxOtherPos.push_back(i);
            //}
        } else { /*if (isJoin)*/
            fields1.push_back(joinsCoordinates[i].first);
            fields2.push_back(joinsCoordinates[i].second);
        }
    }
    //BOOST_LOG_TRIVIAL(debug) << "MergeJoin: ExistingResults " << t1->getNRows()
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
        BOOST_LOG_TRIVIAL(debug) << "Calling do_mergejoin";

        TableFilterer filterer(naiver);
        std::vector<std::shared_ptr<const FCInternalTable>> tablesToMergeJoin;
        FCIterator it = naiver->getTable(literalToQuery, min, max,
                                         &filterer);

        while (!it.isEmpty()) {
            std::shared_ptr<const FCInternalTable> t = it.getCurrentTable();
            bool ok = true;

            //The first condition tests we are evaluating the last literal
            bool isEligibleForPruning = outputLiteral != NULL &&
                                        filterer.isEligibleForPartialSubs(
                                            it.getCurrentBlock(),
                                            *outputLiteral,
                                            t1,
                                            output->getNCopyFromFirst(),
                                            joinsCoordinates.size());
            if (isEligibleForPruning) {
                if (filterer.producedDerivationInPreviousStepsWithSubs(
                            it.getCurrentBlock(),
                            *outputLiteral, literalToQuery, t1,
                            output->getNCopyFromFirst(),
                            output->getPosFromFirst(),
                            joinsCoordinates.size(),
                            &joinsCoordinates[0])) {

                    BOOST_LOG_TRIVIAL(debug) << "REMOVED" <<
                                             outputLiteral->tostring(NULL, NULL) << " "
                                             << literalToQuery.tostring(NULL, NULL);

                    ok = false;
                }
            }
            if (ok)
                tablesToMergeJoin.push_back(t);
            it.moveNextCount();
        }

        if (tablesToMergeJoin.size() > 0)
            do_mergejoin(t1, fields1, tablesToMergeJoin, fields1, NULL, NULL,
                         fields2, output);
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
            boost::chrono::system_clock::time_point startFiltering = boost::chrono::system_clock::now();
            std::shared_ptr<const FCInternalTable> filteredT1 = t1->filter((uint8_t) posToCopy.size(),
                    &(posToCopy[0]), (uint8_t) idxColumnsLowCardInMap.size(), &(idxColumnsLowCardInMap[0]),
                    &(valuesToFilterInFirstSide[0]), 0, NULL);
            boost::chrono::duration<double> secFiltering = boost::chrono::system_clock::now() - startFiltering;
            BOOST_LOG_TRIVIAL(debug) << "Time filtering " << secFiltering.count() * 1000;

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
                boost::chrono::system_clock::time_point startGI = boost::chrono::system_clock::now();
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
                boost::chrono::duration<double> secGI = boost::chrono::system_clock::now() - startGI;
                BOOST_LOG_TRIVIAL(debug) << "Time getting iterator " << secGI.count() * 1000;
#endif

                //boost::chrono::system_clock::time_point startJ = boost::chrono::system_clock::now();
                if (idxOtherPos.size() > 0 && valueOtherPos[0].size() > 1) {
                    do_mergejoin(filteredT1.get(), fieldsToSortInMap, tablesToMergeJoin,
                                 fields1, &(idxOtherPos[0]), &(valueOtherPos[0]), fields2, output);
                } else {
                    do_mergejoin(filteredT1.get(), fieldsToSortInMap, tablesToMergeJoin,
                                 fields1, NULL, NULL, fields2, output);
                }
                //boost::chrono::duration<double> secJ = boost::chrono::system_clock::now() - startJ;

                //size_t uniqueDerivation = output->getUniqueDerivation();
                //size_t unfilteredDerivation = output->getUnfilteredDerivation();
                //boost::chrono::system_clock::time_point startC = boost::chrono::system_clock::now();
                output->consolidate(false);
                //boost::chrono::duration<double> secC = boost::chrono::system_clock::now() - startC;

                /*** LOGGING ***/
//#if DEBUG
//                BOOST_LOG_TRIVIAL(debug) << "MergeJoin: JoinTime=" << secJ.count() * 1000 << "ms. ConsTime="
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

void JoinExecutor::do_merge_join_classicalgo(InmemoryFCInternalTableItr * sortedItr1,
        FCInternalTableItr * sortedItr2,
        const std::vector<uint8_t> &fields1,
        const std::vector<uint8_t> &fields2,
        const uint8_t posBlocks,
        const Term_t *valBlocks,
        ResultJoinProcessor * output) {

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
                // sortedItr2->next();
                // active2 = sortedItr2->hasNext();
                // Is this correct??? Replaced with the following. --Ceriel
                active2 = sortedItr2->hasNext();
                if (active2) {
                    sortedItr2->next();
                }
            } while (active2);
        } else if (sortedItr2->getNColumns() == 0) {
            do {
                output->processResults(0, sortedItr1, sortedItr2, false);
                // sortedItr1->next();
                // active1 = sortedItr1->hasNext();
                // Is this correct??? Replaced with the following. --Ceriel
                active1 = sortedItr1->hasNext();
                if (active1) {
                    sortedItr1->next();
                }
            } while (active1);
        }
    }

    boost::chrono::system_clock::time_point startL = boost::chrono::system_clock::now();

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
                BOOST_LOG_TRIVIAL(debug) << "Count = " << count << ", total = " << total;
                max = max + max;
            }

            if (!sortedItr2->hasNext()) {
                break;
            } else {
                sortedItr2->next();
            }
        } while (JoinExecutor::cmp(rowsToJoin, sortedItr2, fields1, fields2) == 0);
    }
    BOOST_LOG_TRIVIAL(debug) << "Total = " << total;
#if DEBUG
    boost::chrono::duration<double> secL = boost::chrono::system_clock::now() - startL;
    BOOST_LOG_TRIVIAL(debug) << "do_merge_join: time loop: " << secL.count() * 1000 << ", fields.size()=" << fields1.size();
#endif
}

void JoinExecutor::do_merge_join_fasteralgo(InmemoryFCInternalTableItr * sortedItr1,
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
        if (isFirst) {
            currentKey = sortedItr1->getCurrentValue(posKey);
            isFirst = false;
        } else {
            if (currentKey != sortedItr1->getCurrentValue(posKey)) {
                keys.push_back(std::make_pair(currentKey, currentValue));
                currentKey = sortedItr1->getCurrentValue(posKey);
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
        if (!output->isBlockEmpty(i, false)) {
            const uint32_t size = output->getRowsInBlock(i, false);
            std::shared_ptr<Column> column(new CompressedColumn(valBlocks[i], size));
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
                    if (!output->isBlockEmpty(m, false)) {
                        const uint32_t size = output->getRowsInBlock(m, false);
                        std::shared_ptr<Column> column(new CompressedColumn(output->getRawRow()[i], size));
                        output->addColumn(m, i, column, false, true);
                    }
                }
            }
        }
    }
}

void JoinExecutor::do_mergejoin(const FCInternalTable * filteredT1,
                                std::vector<uint8_t> &fieldsToSortInMap,
                                std::vector<std::shared_ptr<const FCInternalTable>> &tables2,
                                const std::vector<uint8_t> &fields1, const uint8_t *posOtherVars,
                                const std::vector<Term_t> *valuesOtherVars,
                                const std::vector<uint8_t> &fields2, ResultJoinProcessor * output) {

    //Only one additional variable is allowed to have low cardinality
    const uint8_t posBlocks = posOtherVars == NULL ? 0 : posOtherVars[0];
    const uint8_t nValBlocks = (valuesOtherVars == NULL) ? 0 : (uint8_t) valuesOtherVars[0].size();
    const Term_t *valBlocks = (valuesOtherVars == NULL) ? NULL : &(valuesOtherVars[0][0]);

    int processedTables = 0;

    for (auto t2 : tables2) {
        InmemoryFCInternalTableItr *sortedItr1 = NULL;
        BOOST_LOG_TRIVIAL(debug) << "Main loop of do_mergejoin";
        processedTables++;

        //Sort t1
        boost::chrono::system_clock::time_point startS;
        boost::chrono::duration<double> secS;
        startS = boost::chrono::system_clock::now();
        //It can be that there are no fields to join.
        // Pity that we have to sort filtered1 for each itr2 entry ... --Ceriel
        if (fieldsToSortInMap.size() > 0) {
            sortedItr1 = (InmemoryFCInternalTableItr*)filteredT1->sortBy(fieldsToSortInMap);
        } else {
            sortedItr1 = (InmemoryFCInternalTableItr*)filteredT1->getIterator();
        }
        secS = boost::chrono::system_clock::now() - startS;
#if DEBUG
        BOOST_LOG_TRIVIAL(debug) << "do_merge_join: time sorting the left relation: " << secS.count() * 1000;
#endif
        //Sort t2
        startS = boost::chrono::system_clock::now();
        //Also in this case, there might be no join fields
        FCInternalTableItr *sortedItr2 = NULL;
        if (fields2.size() > 0) {
            BOOST_LOG_TRIVIAL(debug) << "t2->sortBy";
            sortedItr2 = t2->sortBy(fields2);
        } else {
            sortedItr2 = t2->getIterator();
        }
        secS = boost::chrono::system_clock::now() - startS;
#if DEBUG
        BOOST_LOG_TRIVIAL(debug) << "do_merge_join: time sorting the right relation: " << secS.count() * 1000;
#endif

        if (fields1.size() == 1 && valBlocks != NULL && output->getNCopyFromFirst() == 1
                && output->getPosFromFirst()[0].second == posBlocks && output->getNCopyFromSecond() == 1) {
            BOOST_LOG_TRIVIAL(debug) << "Faster algo";
            JoinExecutor::do_merge_join_fasteralgo(sortedItr1, sortedItr2, fields1,
                                                   fields2, posBlocks, nValBlocks,
                                                   valBlocks, output);
        } else {
            BOOST_LOG_TRIVIAL(debug) << "Classical algo";
            JoinExecutor::do_merge_join_classicalgo(sortedItr1, sortedItr2, fields1,
                                                    fields2, posBlocks, valBlocks, output);
        }
        t2->releaseIterator(sortedItr2);
        if (sortedItr1 != NULL) {
            filteredT1->releaseIterator(sortedItr1);
        }
    }
#if DEBUG
    BOOST_LOG_TRIVIAL(debug) << "Processed tables: " << processedTables;
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
