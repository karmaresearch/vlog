#include <vlog/seminaiver.h>
#include <vlog/seminaiver_threaded.h>
#include <vlog/wizard.h>
#include <vlog/reasoner.h>
#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/qsqquery.h>
#include <vlog/qsqr.h>

#include <trident/kb/consts.h>
#include <trident/model/table.h>

#include <boost/log/trivial.hpp>
#include <boost/thread.hpp>

#include <string>
#include <vector>

long cmpRow(std::vector<uint8_t> *posJoins, const Term_t *row1, const uint64_t *row2) {
    for (int i = 0; i < posJoins->size(); ++i) {
	long r = (row1[i] - row2[(*posJoins)[i]]);
	// BOOST_LOG_TRIVIAL(debug) << "cmpRow: i = " << i << ", row1[i] = " << row1[i]
	//     << ", row2[(*posJoins)[i]] = " << row2[(*posJoins)[i]] << ", r = " << r;
	if (r != 0) {
            return r;
	}
    }
    return 0;
}

void Reasoner::cleanBindings(std::vector<Term_t> &possibleValuesJoins, std::vector<uint8_t> * posJoins, TupleTable *input) {
    if (input != NULL) {
        const size_t sizeBindings = posJoins->size();
        if (possibleValuesJoins.size() / sizeBindings == input->getNRows()) {
            possibleValuesJoins.clear();
        } else {
            /*
                uint8_t posJoinsCopy[sizeBindings];
                for (int j = sizeBindings - 1; j >= 0; j--) {
                    posJoinsCopy[j] = posJoins->at(j);
                }
            */

            std::vector<Term_t> outputBindings;

            //I assume input and bindings are already sorted
            size_t idxInput = 0;
            const uint64_t *currentRowInput = input->getRow(idxInput);
            size_t idxBindings = 0;
            const Term_t *currentRowBindings = &possibleValuesJoins[0];
            while (idxInput < input->getNRows() && idxBindings < possibleValuesJoins.size()) {
                //Compare the two rows
                const long cmpResult = cmpRow(posJoins, currentRowBindings, currentRowInput);
                if (cmpResult < 0) {
                    for (int j = 0; j < sizeBindings; ++j)
                        outputBindings.push_back(currentRowBindings[j]);
                    idxBindings += sizeBindings;
                    if (idxBindings < possibleValuesJoins.size())
                        currentRowBindings = &possibleValuesJoins[idxBindings];
                } else if (cmpResult >= 0) {
                    idxInput++;
                    if (idxInput < input->getNRows())
                        currentRowInput = input->getRow(idxInput);
                }
		if (cmpResult == 0) {
                    idxBindings += sizeBindings;
                    if (idxBindings < possibleValuesJoins.size())
                        currentRowBindings = &possibleValuesJoins[idxBindings];
		}
            }

            //Write all elements that are left
            while (idxBindings < possibleValuesJoins.size()) {
                outputBindings.push_back(possibleValuesJoins[idxBindings++]);
            }


            /*
            for (int i = input->getNRows() - 1; i >= 0; i--) {
                const Term_t *rowToRemove = input->getRow(i);
                //Go through the bindings
                for (std::vector<Term_t>::reverse_iterator itr = possibleValuesJoins.rbegin();
                        itr != possibleValuesJoins.rend();) {
                    bool same = true;
                    for (int j = sizeBindings - 1; j >= 0; j--) {
                        if (same && rowToRemove[posJoinsCopy[j]] != *itr) {
                            same = false;
                        }
                        itr++;
                    }
                    //Remove the row
                    if (same) {
                        possibleValuesJoins.erase(itr.base(), itr.base() + sizeBindings);
                        break;
                    }
                }
            }*/
            possibleValuesJoins.swap(outputBindings);
        }
    }
}

//Function no longer used
/*TupleTable *Reasoner::getVerifiedBindings(QSQQuery &query,
        std::vector<uint8_t> *posJoins,
        std::vector<Term_t> *possibleValuesJoins,
        EDBLayer &layer, Program &program, DictMgmt *dict,
        bool returnOnlyVars) {

    QSQR evaluator(layer, &program);
    TupleTable *output = NULL;

    std::vector<Rule> *originalRules = program.getAllRulesByPredicate(query.getLiteral()->getPredicate().getId());

    //Create a smaller program
    Program clonedProgram = program.clone();
    std::vector<Rule> *clonedRules = clonedProgram.getAllRulesByPredicate(query.getLiteral()->getPredicate().getId());
    //Clean any rule with IDB predicates
    while (clonedRules->size() > 0) {
        if (clonedRules->back().getNIDBPredicates() != 0)
            clonedRules->pop_back();
        else
            break;
    }

    //Execute the smaller program
    evaluator.deallocateAllRules();
    evaluator.cleanAllInputs();
    evaluator.setProgram(&clonedProgram);
#ifdef LINEAGE
    std::vector<LineageInfo> info;
    output = evaluator.evaluateQuery(QSQR_EVAL, &query, posJoins, possibleValuesJoins,
                                     returnOnlyVars, info);
#else
    output = evaluator.evaluateQuery(QSQR_EVAL, &query, posJoins, possibleValuesJoins,
                                     returnOnlyVars);
#endif
    if (output != NULL) {
        cleanBindings(*possibleValuesJoins, posJoins, output);
        if (possibleValuesJoins->size() == 0) {
            return output;
        }
    }

    //First execute all simple rules with one IDB in a sequence
    for (std::vector<Rule>::iterator itr = originalRules->begin(); itr != originalRules->end(); ++itr) {
        if (itr->getNIDBPredicates() == 0) {
            continue;
        } else if (itr->getNIDBPredicates() > 1 || possibleValuesJoins->size() == 0) {
            break;
        }

        //Add the rule
        evaluator.deallocateAllRules();
        clonedRules->push_back(*itr);

        //Launch only the single rule
#ifdef LINEAGE
        std::vector<LineageInfo> info;
        TupleTable *tmpTable = evaluator.evaluateQuery(QSQR_EVAL, &query, posJoins,
                               possibleValuesJoins, returnOnlyVars, info);
#else
        TupleTable *tmpTable = evaluator.evaluateQuery(QSQR_EVAL, &query, posJoins,
                               possibleValuesJoins, returnOnlyVars);
#endif

        if (tmpTable != NULL) {
            //Clean the bindings
            cleanBindings(*possibleValuesJoins, posJoins, tmpTable);

            //Add the temporary bindings to the table
            if (output == NULL) {
                output = tmpTable;
            } else {
                output->addAll(tmpTable);
                delete tmpTable;
            }
        }

        //Remove the rule
        clonedRules->pop_back();
    }

    if (possibleValuesJoins->size() > 0) {
        evaluator.deallocateAllRules();
        evaluator.setProgram(&program);

#ifdef LINEAGE
        std::vector<LineageInfo> info;
        TupleTable *tmpTable = evaluator.evaluateQuery(QSQR_EVAL, &query, posJoins,
                               possibleValuesJoins, returnOnlyVars, info);
#else
        TupleTable *tmpTable = evaluator.evaluateQuery(QSQR_EVAL, &query, posJoins,
                               possibleValuesJoins, returnOnlyVars);
#endif

        if (output == NULL) {
            //Replace container
            output = tmpTable;
        } else {
            //Add all the data
            if (tmpTable != NULL) {
                output->addAll(tmpTable);
                delete tmpTable;
            }
        }
    }
    return output;
}*/

size_t Reasoner::estimate(Literal &query, std::vector<uint8_t> *posBindings,
                          std::vector<Term_t> *valueBindings, EDBLayer &layer,
                          Program &program) {

    QSQQuery rootQuery(query);
    std::unique_ptr<QSQR> evaluator = std::unique_ptr<QSQR>(
                                          new QSQR(layer, &program));
    TupleTable *cardTable = NULL;
    cardTable = evaluator->evaluateQuery(QSQR_EST, &rootQuery,
                                         posBindings, valueBindings, true);
    size_t estimate = cardTable->getRow(0)[0];
    delete cardTable;
    return estimate;
}

FCBlock Reasoner::getBlockFromQuery(Literal constantsQuery, Literal &boundQuery,
                                    std::vector<uint8_t> *posJoins,
                                    std::vector<Term_t> *possibleValuesJoins) {
    uint8_t nconstants = (uint8_t) constantsQuery.getTupleSize();

    VTuple constantsTuple = constantsQuery.getTuple();

    std::shared_ptr<FCInternalTable> table;
    if (nconstants == 0) {
        table = std::shared_ptr<FCInternalTable>(new SingletonTable(0));
    } else {
        SegmentInserter inserter(nconstants);
        Term_t tuple[3];
        uint8_t nPosToCopy = 0;
        uint8_t posToCopy[3];
        assert(boundQuery.getTupleSize() <= 3);
        for (uint8_t i = 0; i < (uint8_t) boundQuery.getTupleSize(); ++i) {
            if (!boundQuery.getTermAtPos(i).isVariable()) {
                posToCopy[nPosToCopy++] = i;
                tuple[i] = boundQuery.getTermAtPos(i).getValue();
            }
        }

        //Add the input tuples
        if (posJoins != NULL) {
            const uint8_t addSize = (uint8_t) posJoins->size();

            for (size_t i = 0; i < possibleValuesJoins->size(); i += addSize) {
                for (uint8_t j = 0; j < addSize; ++j) {
                    tuple[posJoins->at(j)] = possibleValuesJoins->at(i + j);
                }
                inserter.addRow(tuple, posToCopy);
            }
        } else {
            inserter.addRow(tuple, posToCopy);
        }

        std::shared_ptr<const Segment> seg;
        if (inserter.isSorted()) {
            seg = inserter.getSegment();
        } else {
            seg = inserter.getSegment()->sortBy(NULL);
        }
        table =
            std::shared_ptr<FCInternalTable>(
                new InmemoryFCInternalTable(nconstants, 0, true, seg));

        //change the constantsQuery
        if (possibleValuesJoins != NULL && possibleValuesJoins->size() > 1) {
            uint8_t varId = 1;
            for (uint8_t i = 0; i < nconstants; ++i) {
                if (!inserter.getSegment()->getColumn(i)->isConstant()) {
                    constantsTuple.set(VTerm(varId++, 0), i);
                }
            }
        }
    }
    //iteration==1
    return FCBlock(1, table, Literal(constantsQuery.getPredicate(), constantsTuple),
                   NULL, 0, true);
}

TupleIterator *Reasoner::getIterator(Literal &query,
        std::vector<uint8_t> *posJoins,
        std::vector<Term_t> *possibleValuesJoins,
        EDBLayer &edb, Program &program, bool returnOnlyVars,
        std::vector<uint8_t> *sortByFields) {
    if (posJoins != NULL && possibleValuesJoins != NULL) {
	/* No, let's keep them. --Ceriel
	// Check if there are'nt too many values to check.
	if (possibleValuesJoins->size() > 0) {
	    size_t nvals = possibleValuesJoins->size() / posJoins->size();
	    if (nvals > 1000) {
		possibleValuesJoins = NULL;
		posJoins = NULL;
	    }
	}
	*/
    }
    if (query.getPredicate().getType() == EDB) {
	BOOST_LOG_TRIVIAL(info) << "Using edb for " << query.tostring(&program, &edb);
	return Reasoner::getEDBIterator(query, posJoins, possibleValuesJoins, edb,
				    returnOnlyVars, sortByFields);
    }
    if (posJoins == NULL || posJoins->size() < query.getNVars() || returnOnlyVars || posJoins->size() > 1) {
	ReasoningMode mode = chooseMostEfficientAlgo(query, edb, program, posJoins, possibleValuesJoins);
	if (mode == MAGIC) {
	    BOOST_LOG_TRIVIAL(info) << "Using magic for " << query.tostring(&program, &edb);
	    return Reasoner::getMagicIterator(
					query, posJoins, possibleValuesJoins, edb, program,
					returnOnlyVars, sortByFields);
	}
	//top-down
	BOOST_LOG_TRIVIAL(info) << "Using top-down for " << query.tostring(&program, &edb);
	return Reasoner::getTopDownIterator(
		       query, posJoins, possibleValuesJoins, edb, program,
		       returnOnlyVars, sortByFields);
    }

    BOOST_LOG_TRIVIAL(info) << "Using incremental reasoning for " << query.tostring(&program, &edb);
    return getIncrReasoningIterator(query, posJoins, possibleValuesJoins, edb, program, returnOnlyVars, sortByFields); 
}

TupleIterator *Reasoner::getIncrReasoningIterator(Literal &query,
        std::vector<uint8_t> *posJoins,
        std::vector<Term_t> *possibleValuesJoins,
        EDBLayer &edb, Program &program, bool returnOnlyVars,
        std::vector<uint8_t> *sortByFields) {

    // For now, only works when returnOnlyVars == false;
    assert(! returnOnlyVars);

    // For now, it also only works when posJoins->size() == 1.
    assert(posJoins->size() == 1);
    std::sort(possibleValuesJoins->begin(), possibleValuesJoins->end());

    std::vector<uint8_t> newPosJoins;
    if (posJoins != NULL) {
        newPosJoins = *posJoins;
        for (int j = 0; j < query.getTupleSize(); ++j) {
            if (!query.getTermAtPos(j).isVariable()) {
                //Increment all newPosToJoin
                for (int m = 0; m < newPosJoins.size(); ++m) {
                    if (newPosJoins[m] >= j)
                        newPosJoins[m]++;
                }
            } else {
            }
        }
    }

    //First try an explicit search
    std::string te("TE");
    VTuple t = query.getTuple();
    QSQQuery explQuery(Literal(program.getPredicate(te,
                               Predicate::calculateAdornment(t)), t));


    BOOST_LOG_TRIVIAL(debug) << "Expl query = " << explQuery.getLiteral()->tostring(&program, &edb);

    // edb.query only returns variables, so do the query and add constants.
    TupleTable *tempTable = new TupleTable(query.getNVars());
    edb.query(&explQuery, tempTable, posJoins, possibleValuesJoins);
    TupleTable *tempTable1 = tempTable->sortBy(*posJoins);
    delete tempTable;

    TupleTable *outputTable;
    if (returnOnlyVars) {
	outputTable = tempTable;
    } else {
	outputTable = new TupleTable(3);
	uint64_t val[3];
	val[0] = t.get(0).getValue();
	val[1] = t.get(1).getValue();
	val[2] = t.get(2).getValue();
	for (int idx = 0; idx < tempTable1->getNRows(); idx++) {
	    const uint64_t *current = tempTable1->getRow(idx);
	    for (int i = 0; i < newPosJoins.size(); i++) {
		val[newPosJoins[i]] = current[i];
	    }
	}
	delete tempTable1;
    }


    BOOST_LOG_TRIVIAL(debug) << "Expl query done";

    QSQQuery rootQuery(query);

    //Did I get everything?
    if (outputTable->getNRows() < possibleValuesJoins->size() / posJoins->size()) {
        //Continue with the search
        BOOST_LOG_TRIVIAL(debug) << "Found bindings " << outputTable->getNRows();
        if (outputTable->getNRows() > 0) {
            cleanBindings(*possibleValuesJoins, &newPosJoins, outputTable);
        }

	BOOST_LOG_TRIVIAL(info) << "number of possible values = " << (possibleValuesJoins->size() / posJoins->size());
        if (possibleValuesJoins->size() / posJoins->size() <= 1000) {
	    // TODO: experiment with threshold on when to use this ...
            //I use QSQR
            QSQR evaluator(edb, &program);
            std::vector<Rule> *originalRules = program.getAllRulesByPredicate(
                                                   rootQuery.getLiteral()->getPredicate().getId());

            //Create a smaller program
            Program clonedProgram = program.clone();
            std::vector<Rule> *clonedRules = clonedProgram.getAllRulesByPredicate(
                                                 rootQuery.getLiteral()->getPredicate().getId());
            //Clean any rule with IDB predicates
            while (clonedRules->size() > 0) {
                if (clonedRules->back().getNIDBPredicates() != 0)
                    clonedRules->pop_back();
                else
                    break;
            }

            BOOST_LOG_TRIVIAL(debug) << "Rules in the cloned program " << clonedRules->size();

            //Execute all simple rules with one IDB in a sequence
            for (std::vector<Rule>::iterator itr = originalRules->begin();
                    itr != originalRules->end() && possibleValuesJoins->size() > 0;
                    ++itr) {

                if (itr->getNIDBPredicates() == 0) {
                    continue;
                } else if (itr->getNIDBPredicates() > 1
                           || possibleValuesJoins->size() == 0) {
                    break;
                }

                //Add the rule
                evaluator.deallocateAllRules();
                BOOST_LOG_TRIVIAL(debug) << "Executing the rule " << itr->tostring(NULL, NULL);
                clonedRules->push_back(*itr);
                evaluator.setProgram(&clonedProgram);

                //Launch only the single rule
                TupleTable *tmpTable = evaluator.evaluateQuery(QSQR_EVAL, &rootQuery, &newPosJoins,
                                       possibleValuesJoins, returnOnlyVars);

                if (tmpTable != NULL) {
                    //Clean the bindings
                    TupleTable *tmp1 = tmpTable->sortBy(newPosJoins);
                    delete tmpTable;
                    tmpTable = tmp1;
                    if (tmpTable->getNRows() > 0) {
			BOOST_LOG_TRIVIAL(debug) << "#values = " << tmpTable->getNRows();
                        cleanBindings(*possibleValuesJoins, &newPosJoins, tmpTable);

			//Add the temporary bindings to the table
			tmp1 = outputTable->merge(tmpTable);
			delete outputTable;
			delete tmpTable;
			outputTable = tmp1;
		    }
                }

                //Remove the rule
                clonedRules->pop_back();
            }
        }

        if (possibleValuesJoins->size() > 0) {
            // Decide between magic or QSQ-R, to verify the remaining values.
            int algo = chooseMostEfficientAlgo(query, edb, program,
                                               posJoins,
                                               possibleValuesJoins);

            TupleIterator *itr = NULL;

            if (algo == TOPDOWN) {
                itr = getTopDownIterator(query, posJoins, possibleValuesJoins,
                                         edb, program, returnOnlyVars, &newPosJoins);
            } else {
                itr = getMagicIterator(query, posJoins, possibleValuesJoins,
                                       edb, program, returnOnlyVars, &newPosJoins);
            }

            //Add the bindings to a temporary container.
            int rowSize = outputTable->getSizeRow();
            TupleTable *tmp = new TupleTable(rowSize);
            while (itr->hasNext()) {
                itr->next();
                for (int i = 0; i < rowSize; ++i) {
                    tmp->addValue(itr->getElementAt(i));
                }
            }
            //Merge the temporary container into the final one.
            //Keeps the output table sorted.
            TupleTable *tmp1 = outputTable->merge(tmp);
	    delete tmp;
            delete outputTable;
            outputTable = tmp1;

            if (itr != NULL)
                delete itr;
        }
    }

    //Return an iterator of the bindings
    std::shared_ptr<TupleTable> pFinalTable(outputTable);

    //Add sort by if requested
    if (sortByFields != NULL && !sortByFields->empty()) {
        std::shared_ptr<TupleTable> sortTab = std::shared_ptr<TupleTable>(
                pFinalTable->sortBy(*sortByFields));
        return new TupleTableItr(sortTab);
    } else {
        return new TupleTableItr(pFinalTable);
    }
}

TupleIterator *Reasoner::getMagicIterator(Literal &query,
        std::vector<uint8_t> *posJoins,
        std::vector<Term_t> *possibleValuesJoins,
        EDBLayer &edb, Program &program, bool returnOnlyVars,
        std::vector<uint8_t> *sortByFields) {


    //To use if the flag returnOnlyVars is set to false
    uint64_t outputTuple[3];    // Used in trident method, so no Term_t
    uint8_t nPosToCopy = 0;
    uint8_t posToCopy[3];
    std::vector<uint8_t> newPosJoins; //This is used because I need the posJoins in the original triple, and not on the variables
    if (posJoins != NULL) {
        newPosJoins = *posJoins;
        for (int j = 0; j < query.getTupleSize(); ++j) {
            if (!query.getTermAtPos(j).isVariable()) {
                //Increment all newPosToJoin
                for (int m = 0; m < newPosJoins.size(); ++m) {
                    if (newPosJoins[m] >= j)
                        newPosJoins[m]++;
                }
		// Line below added (quite important ...) --Ceriel
                outputTuple[j] = query.getTermAtPos(j).getValue();
            } else {
                posToCopy[nPosToCopy++] = j;
            }
        }
    } else {
        for (int j = 0; j < query.getTupleSize(); ++j) {
            if (!query.getTermAtPos(j).isVariable()) {
                outputTuple[j] = query.getTermAtPos(j).getValue();
            } else {
                posToCopy[nPosToCopy++] = j;
            }
        }
    }

    //Replace variables with constants if posJoins != NULL.
    VTuple t = query.getTuple();
    VTuple boundTuple = t;
    if (posJoins != NULL) {
        //The posjoins do not include constants
        int j = 0;
        for (std::vector<uint8_t>::const_iterator itr = newPosJoins.begin();
                itr != newPosJoins.end(); ++itr) {
            boundTuple.set(VTerm(0, possibleValuesJoins->at(j++)), *itr);
        }

    }

    Predicate pred1(query.getPredicate(), Predicate::calculateAdornment(boundTuple));
    Literal query1(pred1, boundTuple);

    //Get all adorned rules
    std::unique_ptr<Wizard> wizard = std::unique_ptr<Wizard>(new Wizard());
    std::shared_ptr<Program> adornedProgram = wizard->getAdornedProgram(query1, program);
    //Print all rules
#if DEBUG
    BOOST_LOG_TRIVIAL(debug) << "Adorned program:";
    std::vector<Rule> newRules = adornedProgram->getAllRules();
    for (std::vector<Rule>::iterator itr = newRules.begin(); itr != newRules.end(); ++itr) {
        BOOST_LOG_TRIVIAL(debug) << itr->tostring(adornedProgram.get(), &edb);
    }
#endif

    //Rewrite and add the rules
    std::pair<PredId_t, PredId_t> inputOutputRelIDs;
    std::shared_ptr<Program> magicProgram = wizard->doMagic(query1, adornedProgram,
                                            inputOutputRelIDs);

#if DEBUG
    BOOST_LOG_TRIVIAL(debug) << "Magic program:";
    newRules = magicProgram->getAllRules();
    for (std::vector<Rule>::iterator itr = newRules.begin(); itr != newRules.end(); ++itr) {
        BOOST_LOG_TRIVIAL(debug) << itr->tostring(magicProgram.get(), &edb);
    }
#endif

    SemiNaiver *naiver = new SemiNaiver(magicProgram->getAllRules(),
                                        edb, magicProgram.get(), true, true, false, -1, false) ;

    //Add all the input tuples in the input relation
    Predicate pred = magicProgram->getPredicate(inputOutputRelIDs.first);
    VTuple onlyConstsTuple(pred.getCardinality());
    int j = 0;
    for (int i = 0; i < t.getSize(); ++i) {
        if (!boundTuple.get(i).isVariable()) {
            onlyConstsTuple.set(VTerm(0, t.get(i).getValue()), j++);
        }
    }

    Literal unboundQuery(pred, onlyConstsTuple);
    BOOST_LOG_TRIVIAL(debug) << "unboundQuery = " << unboundQuery.tostring(magicProgram.get(), &edb);
    naiver->addDataToIDBRelation(magicProgram->getPredicate(inputOutputRelIDs.first),
                                 getBlockFromQuery(unboundQuery, query1,
                                         newPosJoins.size() != 0 ? &newPosJoins : NULL, possibleValuesJoins));

    //Exec the materialization
    naiver->run(1, 2);

    //Extract the tuples from the output relation
    Literal outputLiteral(magicProgram->getPredicate(inputOutputRelIDs.second), t);
    BOOST_LOG_TRIVIAL(debug) << "outputLiteral = " << outputLiteral.tostring(magicProgram.get(), &edb);
    BOOST_LOG_TRIVIAL(debug) << "returnOnlyVars = " << returnOnlyVars;
    BOOST_LOG_TRIVIAL(debug) << "sortByFields->empty() = " << (sortByFields == NULL ? true : sortByFields->empty());

    FCIterator itr = naiver->getTable(outputLiteral, 0, (size_t) - 1);

    TupleTable *finalTable;
    if (returnOnlyVars) {
        finalTable = new TupleTable(outputLiteral.getNVars());
    } else {
        finalTable = new TupleTable(query.getTupleSize());
    }

    std::vector<uint8_t> posVars = outputLiteral.getPosVars();
    while (!itr.isEmpty()) {
        std::shared_ptr<const FCInternalTable> table = itr.getCurrentTable();
	// BOOST_LOG_TRIVIAL(debug) << "table empty? " << table->isEmpty();
        FCInternalTableItr *itrTable = table->getIterator();

	// itrTable contains only variables. 
        if (returnOnlyVars) {
            //const uint8_t rowSize = table->getRowSize();
            while (itrTable->hasNext()) {
                itrTable->next();
                for (uint8_t j = 0; j < posVars.size(); ++j) {
                    finalTable->addValue(itrTable->getCurrentValue(j));
                }
                // Not sure about this. Was:
                // for (uint8_t j = 0; j < rowsize; ++j) {
                //     finalTable->addValue(itrTable->getCurrentValue(j));
                // }
                // TODO!
            }
        } else {
            while (itrTable->hasNext()) {
                itrTable->next();
                for (uint8_t j = 0; j < nPosToCopy; ++j) {
                    outputTuple[posToCopy[j]] = itrTable->getCurrentValue(j);
                }
                finalTable->addRow(outputTuple);
		// BOOST_LOG_TRIVIAL(debug) << "Adding row " << outputTuple[0] << ", " << outputTuple[1] << ", " << outputTuple[2];
            }
        }

        table->releaseIterator(itrTable);
        itr.moveNextCount();
    }

    std::shared_ptr<TupleTable> pFinalTable(finalTable);
    delete naiver;

    if (sortByFields != NULL && !sortByFields->empty()) {
        std::shared_ptr<TupleTable> sortTab = std::shared_ptr<TupleTable>(
                pFinalTable->sortBy(*sortByFields));
        return new TupleTableItr(sortTab);

    } else {
        return new TupleTableItr(pFinalTable);
    }
}

TupleIterator *Reasoner::getMaterializationIterator(Literal &query,
        std::vector<uint8_t> *posJoins,
	std::vector<Term_t> *possibleValuesJoins,
	EDBLayer &edb, Program &program, bool returnOnlyVars,
	std::vector<uint8_t> *sortByFields) {

    Predicate pred = query.getPredicate();
    VTuple tuple = query.getTuple();
    if (pred.getType() == EDB) {
	BOOST_LOG_TRIVIAL(info) << "Using edb for " << query.tostring(&program, &edb);
	return Reasoner::getEDBIterator(query, posJoins, possibleValuesJoins, edb,
				returnOnlyVars, sortByFields);
    }

    if (posJoins != NULL) {
	BOOST_LOG_TRIVIAL(info) << "getMaterializationIterator with joins not implemented yet";
	throw 10;
    }
    
    // Run materialization
   SemiNaiver *sn = new SemiNaiver(program.getAllRules(),
		      edb, &program, true, true,
		      false, -1, false);

    sn->run();

    //To use if the flag returnOnlyVars is set to false
    uint64_t outputTuple[3];    // Used in trident method, so no Term_t
    uint8_t nPosToCopy = 0;
    uint8_t posToCopy[3];
    for (int j = 0; j < query.getTupleSize(); ++j) {
	if (!query.getTermAtPos(j).isVariable()) {
	    outputTuple[j] = query.getTermAtPos(j).getValue();
	} else {
	    posToCopy[nPosToCopy++] = j;
	}
    }

    FCIterator tableIt = sn->getTable(pred.getId());

    TupleTable *finalTable;
    if (returnOnlyVars) {
	finalTable = new TupleTable(query.getNVars());
    } else {
	finalTable = new TupleTable(query.getTupleSize());
    }
    while (! tableIt.isEmpty()) {
	std::shared_ptr<const FCInternalTable> table = tableIt.getCurrentTable();
	FCInternalTableItr *itrTable = table->getIterator();
	while (itrTable->hasNext()) {
	    itrTable->next();
	    bool copy = true;
	    for (int i = 0; i < tuple.getSize(); i++) {
		if (! tuple.get(i).isVariable()) {
		    if (itrTable->getCurrentValue(i) != tuple.get(i).getValue()) {
			copy = false;
			break;
		    }
		}
	    }
	    if (! copy) {
		continue;
	    }
	    for (int i = 0; i < tuple.getSize(); i++) {
		if (! returnOnlyVars || tuple.get(i).isVariable()) {
		    finalTable->addValue(itrTable->getCurrentValue(i));
		}
	    }

	}
	table->releaseIterator(itrTable);
	tableIt.moveNextCount();
    }

    std::shared_ptr<TupleTable> pFinalTable(finalTable);
    delete sn;

    if (sortByFields != NULL && !sortByFields->empty()) {
        std::shared_ptr<TupleTable> sortTab = std::shared_ptr<TupleTable>(
                pFinalTable->sortBy(*sortByFields));
        return new TupleTableItr(sortTab);

    } else {
        return new TupleTableItr(pFinalTable);
    }
}

ReasoningMode Reasoner::chooseMostEfficientAlgo(Literal &query,
        EDBLayer &layer, Program &program,
        std::vector<uint8_t> *posBindings,
        std::vector<Term_t> *valueBindings) {
    uint64_t cost = 0;
    if (posBindings != NULL) {
        //Create a new query with the values substituted
        int idxValues = 0;
        VTuple newTuple = query.getTuple();
        for (std::vector<uint8_t>::iterator itr = posBindings->begin(); itr != posBindings->end();
                ++itr) {
            newTuple.set(VTerm(0, valueBindings->at(idxValues)), *itr);
            idxValues++;
        }
	// Fixed adornments in predicate of literal below.
	Predicate pred1(query.getPredicate(), Predicate::calculateAdornment(newTuple));
        Literal newLiteral(pred1, newTuple);
        size_t singleCost = estimate(newLiteral, NULL, NULL, layer, program);
        BOOST_LOG_TRIVIAL(debug) << "SingleCost is " <<
                                 singleCost << " nBindings " << (valueBindings->size() / posBindings->size());

        //Are bindings less than 10? Then singleCost is probably about right
        uint64_t nValues = valueBindings->size() / posBindings->size();
        if (nValues > 10) {
            //Copy the first 10 values
            std::vector<Term_t> limitedValueBindings;
            for (int i = 0; i < 10 * posBindings->size(); ++i) {
                limitedValueBindings.push_back(valueBindings->at(i));
            }
            uint64_t tenCost = estimate(query, posBindings,
                                        &limitedValueBindings, layer, program);
            BOOST_LOG_TRIVIAL(debug) << "TenCost is " << tenCost;

            //y = mx + b. m is slope, b is constant.
            //I assume the singleCost is the cost at position 0. Otherwise it's not a line
            double m = (double)(tenCost - singleCost) / (10);   // 9? --Ceriel
            long b = singleCost;
            cost = (uint64_t) (m * nValues + b);
        } else {
            cost = singleCost;
        }
    } else {
        cost = estimate(query, NULL, NULL, layer, program);
    }
    ReasoningMode mode = cost < threshold ? TOPDOWN : MAGIC;
    BOOST_LOG_TRIVIAL(debug) << "Deciding whether I should resolve " <<
                             query.tostring(&program, &layer) <<
                             " with magic or QSQR. Estimated cost: " <<
                             cost << " threshold for QSQ-R is " << threshold;
    return mode;
}

TupleIterator *Reasoner::getEDBIterator(Literal &query,
        std::vector<uint8_t> *posJoins,
        std::vector<Term_t> *possibleValuesJoins,
        EDBLayer &edb, bool returnOnlyVars,
        std::vector<uint8_t> *sortByFields) {
    QSQQuery qsqquery(query);
    int nVars = query.getNVars();
    TupleTable *table = new TupleTable(nVars);
    edb.query(&qsqquery, table, posJoins, possibleValuesJoins);
    std::shared_ptr<TupleTable> ptable = std::shared_ptr<TupleTable>(table);
    if (! returnOnlyVars && nVars != 3) {
	VTuple v = query.getTuple();
	TupleTable *newTable = new TupleTable(3);
	TupleIterator *itr = new TupleTableItr(ptable);
	while (itr->hasNext()) {
	    itr->next();
	    uint64_t row[3];
	    int cnt = 0;
	    for (int i = 0; i < 3; i++) {
		if (v.get(i).isVariable()) {
		    row[i] = itr->getElementAt(cnt);
		    cnt++;
		} else {
		    row[i] = v.get(i).getValue();
		}
	    }
	    newTable->addRow(row);
	}
	ptable = std::shared_ptr<TupleTable>(newTable);
    }
    //Add sort by if requested
    if (sortByFields != NULL && !sortByFields->empty()) {
        std::shared_ptr<TupleTable> sortTab = std::shared_ptr<TupleTable>(
                ptable->sortBy(*sortByFields));
        return new TupleTableItr(sortTab);
    } else {
        return new TupleTableItr(ptable);
    }
}

TupleIterator *Reasoner::getTopDownIterator(Literal &query,
        std::vector<uint8_t> *posJoins,
        std::vector<Term_t> *possibleValuesJoins,
        EDBLayer &edb, Program &program, bool returnOnlyVars,
        std::vector<uint8_t> *sortByFields) {

    BOOST_LOG_TRIVIAL(debug) << "Get topdown iterator for query " << query.tostring(&program, &edb);
    std::vector<uint8_t> newPosJoins;
    if (posJoins != NULL) {
        newPosJoins = *posJoins;
        for (int j = 0; j < query.getTupleSize(); ++j) {
            if (!query.getTermAtPos(j).isVariable()) {
                //Increment all newPosToJoin
                for (int m = 0; m < newPosJoins.size(); ++m) {
                    if (newPosJoins[m] >= j)
                        newPosJoins[m]++;
                }
            } else {
            }
        }
    }

    QSQQuery rootQuery(query);
    BOOST_LOG_TRIVIAL(debug) << "QSQQuery = " << rootQuery.tostring();
    std::unique_ptr<QSQR> evaluator = std::unique_ptr<QSQR>(new QSQR(edb, &program));
    TupleTable *finalTable;
    finalTable = evaluator->evaluateQuery(QSQR_EVAL, &rootQuery, newPosJoins.size() > 0 ? &newPosJoins : NULL,
                                          possibleValuesJoins, returnOnlyVars);

    //Return an iterator of the bindings
    std::shared_ptr<TupleTable> pFinalTable(finalTable);

    //Add sort by if requested
    if (sortByFields != NULL && !sortByFields->empty()) {
        std::shared_ptr<TupleTable> sortTab = std::shared_ptr<TupleTable>(
                pFinalTable->sortBy(*sortByFields));
        return new TupleTableItr(sortTab);
    } else {
        return new TupleTableItr(pFinalTable);
    }

}

std::shared_ptr<SemiNaiver> Reasoner::getSemiNaiver(EDBLayer &layer,
        Program *p, bool opt_intersect, bool opt_filtering, bool opt_threaded,
        int nthreads, int interRuleThreads, bool shuffleRules) {
    BOOST_LOG_TRIVIAL(debug) << "interRuleThreads = " << interRuleThreads << ", shuffleRules = " << shuffleRules;
    if (interRuleThreads > 0) {
        std::shared_ptr<SemiNaiver> sn(new SemiNaiverThreaded(p->getAllRules(),
                                       layer, p, opt_intersect, opt_filtering,
                                       shuffleRules, nthreads, interRuleThreads));
        return sn;
    } else {
        std::shared_ptr<SemiNaiver> sn(new SemiNaiver(p->getAllRules(),
                                       layer, p, opt_intersect, opt_filtering,
                                       opt_threaded, nthreads, shuffleRules));
        return sn;
    }
}

std::shared_ptr<SemiNaiver> Reasoner::fullMaterialization(EDBLayer &layer,
        Program *p, bool opt_intersect, bool opt_filtering, bool opt_threaded, int nthreads, int interRuleThreads, bool shuffleRules) {
    BOOST_LOG_TRIVIAL(info) << "Starting full materialization";
    timens::system_clock::time_point start = timens::system_clock::now();
    std::shared_ptr<SemiNaiver> sn = getSemiNaiver(layer,
                                     p, opt_intersect, opt_filtering, opt_threaded, nthreads, interRuleThreads, shuffleRules);
    sn->run();
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(info) << "Runtime materialization = " << sec.count() * 1000 << " milliseconds";
    sn->printCountAllIDBs();
    return sn;
}
