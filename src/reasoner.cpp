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

#include <vlog/seminaiver.h>
#include <vlog/wizard.h>
#include <vlog/reasoner.h>
#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/qsqquery.h>
#include <vlog/qsqr.h>

#include <trident/kb/consts.h>
#include <trident/model/table.h>

#include <boost/log/trivial.hpp>

#include <string>
#include <vector>

long cmpRow(const uint8_t size, const Term_t *row1, const uint64_t *row2) {
    for (int i = 0; i < size; ++i) {
        if (row1[i] != row2[i])
            return row1[i] - (Term_t) row2[i];
    }
    return 0;
}

void Reasoner::cleanBindings(std::vector<Term_t> &bindings, std::vector<uint8_t> * posJoins, TupleTable *input) {
    if (input != NULL) {
        const size_t sizeBindings = posJoins->size();
        if (bindings.size() / sizeBindings == input->getNRows()) {
            bindings.clear();
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
            const Term_t *currentRowBindings = &bindings[0];
            while (idxInput < input->getNRows() && idxBindings < bindings.size()) {
                //Compare the two rows
                const long cmpResult = cmpRow((uint8_t) sizeBindings, currentRowBindings, currentRowInput);
                if (cmpResult < 0) {
                    for (int j = 0; j < sizeBindings; ++j)
                        outputBindings.push_back(currentRowBindings[j]);
                    idxBindings += sizeBindings;
                    if (idxBindings < bindings.size())
                        currentRowBindings = &bindings[idxBindings];
                } else if (cmpResult >= 0) {
                    idxInput++;
                    if (idxInput < input->getNRows())
                        currentRowInput = input->getRow(idxInput);
                }
            }

            //Write all elements that are left
            while (idxBindings < bindings.size()) {
                outputBindings.push_back(bindings[idxBindings++]);
            }


            /*
            for (int i = input->getNRows() - 1; i >= 0; i--) {
                const Term_t *rowToRemove = input->getRow(i);
                //Go through the bindings
                for (std::vector<Term_t>::reverse_iterator itr = bindings.rbegin();
                        itr != bindings.rend();) {
                    bool same = true;
                    for (int j = sizeBindings - 1; j >= 0; j--) {
                        if (same && rowToRemove[posJoinsCopy[j]] != *itr) {
                            same = false;
                        }
                        itr++;
                    }
                    //Remove the row
                    if (same) {
                        bindings.erase(itr.base(), itr.base() + sizeBindings);
                        break;
                    }
                }
            }*/
            bindings.swap(outputBindings);

        }
    }
}

/*
TupleIterator *Reasoner::getIncrReasoningIterator(Pattern *pattern,
        std::vector<uint8_t> *posJoins,
        std::vector<Term_t> *possibleValuesJoins,
        EDBLayer &edb, Program &program, DictMgmt *dict, bool returnOnlyVars) {
    Tuple t(3);
    std::vector<uint8_t> newPosJoins;
    if (posJoins != NULL)
        newPosJoins = *posJoins;

    //s
    int nvars = 0;
    if (pattern->subject() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 0);
    } else {
        //Constant
        t.set(Term(0, pattern->subject()), 0);
        if (newPosJoins.size() == 2) {
            newPosJoins[0]++;
            newPosJoins[1]++;
        }

        if (newPosJoins.size() == 1) {
            newPosJoins[0]++;
        }
    }
    //p
    if (pattern->predicate() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 1);
    } else {
        //Constant
        t.set(Term(0, pattern->predicate()), 1);
        if (newPosJoins.size() == 1 && newPosJoins[0] == 1) {
            newPosJoins[0]++;
        }
        if (newPosJoins.size() == 2 && newPosJoins[1] == 1) {
            newPosJoins[1]++;
        }
    }
    //o
    if (pattern->object() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 2);
    } else {
        //Constant
        t.set(Term(0, pattern->object()), 2);
    }

    //Create query
    std::string ti("TI");
    QSQQuery rootQuery(Literal(program.getPredicate(ti,
                               Predicate::calculateAdornment(t)), t));

    //Create container
    TupleTable *outputTable = new TupleTable(returnOnlyVars ?
            pattern->getNVars() : 3);

    //First try an explicit search
    std::string te("TE");
    QSQQuery explQuery(Literal(program.getPredicate(te,
                               Predicate::calculateAdornment(t)), t));
    edb.query(&explQuery, outputTable, &newPosJoins, possibleValuesJoins);

    //Did I get everything?
    TupleTable *outputTable1 = outputTable;
    outputTable = outputTable1->sortByAll();
    delete outputTable1;

    if (outputTable->getNRows() < possibleValuesJoins->size() / posJoins->size()) {
        //Continue with the search
        BOOST_LOG_TRIVIAL(debug) << "Found1 bindings " << outputTable->getNRows();
        if (outputTable->getNRows() > 0) {
            cleanBindings(*possibleValuesJoins, posJoins, outputTable);
        }

        if (possibleValuesJoins->size() / posJoins->size() <= 1000) {
            //I use QSQR
            QSQR evaluator(edb, &program, dict);
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
#ifdef LINEAGE
                std::vector<LineageInfo> info;
                TupleTable *tmpTable = evaluator.evaluateQuery(QSQR_EVAL, &rootQuery, &newPosJoins,
                                       possibleValuesJoins, returnOnlyVars, info);
#else
                TupleTable *tmpTable = evaluator.evaluateQuery(QSQR_EVAL, &rootQuery, &newPosJoins,
                                       possibleValuesJoins, returnOnlyVars);
#endif

                if (tmpTable != NULL) {
                    //Clean the bindings
                    TupleTable *tmp1 = tmpTable->sortByAll();
                    delete tmpTable;
                    tmpTable = tmp1;
                    if (tmpTable->getNRows() > 0) {
                        cleanBindings(*possibleValuesJoins, posJoins, tmpTable);
                    }

                    //Add the temporary bindings to the table
                    tmp1 = outputTable->merge(tmpTable);
                    delete outputTable;
                    delete tmpTable;
                    outputTable = tmp1;
                }

                //Remove the rule
                clonedRules->pop_back();
            }
        }

        if (possibleValuesJoins->size() > 0) {

            //Decide between magic or QSQ-R
            int algo = chooseMostEfficientAlgo(pattern, edb, program, dict,
                                               posJoins,
                                               possibleValuesJoins);

            TupleIterator *itr = NULL;
            if (algo == TOPDOWN) {
                itr = getTopDownIterator(pattern, posJoins, possibleValuesJoins,
                                         edb, program, dict, returnOnlyVars);
            } else {
                itr = getMagicIterator(pattern, posJoins, possibleValuesJoins,
                                       edb, program, dict, returnOnlyVars);
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
            TupleTable *tmp1 = tmp->sortByAll();
            delete tmp;
            //Merge the temporary container into the final one.
            //Keeps the output table sorted.
            tmp = outputTable->merge(tmp1);
            delete tmp1;
            delete outputTable;
            outputTable = tmp;

            if (itr != NULL)
                delete itr;
        }
    }
    return new TupleTableItr(std::shared_ptr<TupleTable>(outputTable));
}
*/

//Function no longer used
TupleTable *Reasoner::getVerifiedBindings(QSQQuery &query,
        std::vector<uint8_t> *posJoins,
        std::vector<Term_t> *possibleValuesJoins,
        EDBLayer &layer, Program &program, DictMgmt *dict,
        bool returnOnlyVars) {

    QSQR evaluator(layer, &program, dict);
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
}

size_t Reasoner::estimate(Pattern *pattern, std::vector<uint8_t> *posBindings,
                        std::vector<Term_t> *valueBindings, EDBLayer &layer,
                        Program &program, DictMgmt *dict) {
    Tuple t(3);
    //s
    int nvars = 0;
    if (pattern->subject() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 0);
    } else {
        //Constant
        t.set(Term(0, pattern->subject()), 0);
    }
    //p
    if (pattern->predicate() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 1);
    } else {
        //Constant
        t.set(Term(0, pattern->predicate()), 1);
    }
    //o
    if (pattern->object() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 2);
    } else {
        //Constant
        t.set(Term(0, pattern->object()), 2);
    }

    std::string ti("TI");
    QSQQuery rootQuery(Literal(program.getPredicate(ti, Predicate::calculateAdornment(t)), t));
    QSQR evaluator(layer, &program, dict);
#ifdef LINEAGE
    std::vector<LineageInfo> info;
    TupleTable *cardTable = evaluator.evaluateQuery(QSQR_EST, &rootQuery, posBindings, valueBindings, true, info);
#else
    TupleTable *cardTable = evaluator.evaluateQuery(QSQR_EST, &rootQuery, posBindings, valueBindings, true);
#endif
    size_t estimate = cardTable->getRow(0)[0];
    delete cardTable;
    return estimate;
}

FCBlock Reasoner::getBlockFromQuery(Literal constantsQuery, Literal &boundQuery,
                                    std::vector<uint8_t> *posJoins,
                                    std::vector<Term_t> *possibleValuesJoins) {
    uint8_t nconstants = (uint8_t) constantsQuery.getTupleSize();

    Tuple constantsTuple = constantsQuery.getTuple();

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
                    constantsTuple.set(Term(varId++, 0), i);
                }
            }
        }
    }
    //iteration==1
    return FCBlock(1, table, Literal(constantsQuery.getPredicate(), constantsTuple),
                   NULL, 0, true);
}

TupleIterator *Reasoner::getMagicIterator(Pattern *pattern,
        std::vector<uint8_t> *posJoins,
        std::vector<Term_t> *possibleValuesJoins,
        EDBLayer &edb, Program &program, DictMgmt *dict, bool returnOnlyVars) {

    BOOST_LOG_TRIVIAL(debug) << "Get magic iterator for pattern " << pattern->toString();
    //Transfor pattern in query
    Tuple t(3);
    int nvars = 0;

    //To use if the flag returnOnlyVars is set to false
    uint64_t outputTuple[3];	// Used in trident method, so no Term_t
    uint8_t nPosToCopy = 0;
    uint8_t posToCopy[3];
    std::vector<uint8_t> newPosJoins; //This is used because I need the posJoins in the original triple, and not on the variables
    if (posJoins != NULL)
        newPosJoins = *posJoins;

    if (pattern->subject() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 0);
        posToCopy[nPosToCopy++] = 0;
    } else {
        //Constant
        t.set(Term(0, pattern->subject()), 0);
        outputTuple[0] = pattern->subject();

        if (newPosJoins.size() == 2) {
            newPosJoins[0]++;
            newPosJoins[1]++;
        }

        if (newPosJoins.size() == 1) {
            newPosJoins[0]++;
        }
    }
    if (pattern->predicate() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 1);
        posToCopy[nPosToCopy++] = 1;
    } else {
        //Constant
        t.set(Term(0, pattern->predicate()), 1);
        outputTuple[1] = pattern->predicate();

        if (newPosJoins.size() == 1 && newPosJoins[0] == 1) {
            newPosJoins[0]++;
        }
        if (newPosJoins.size() == 2 && newPosJoins[1] == 1) {
            newPosJoins[1]++;
        }
    }
    if (pattern->object() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 2);
        posToCopy[nPosToCopy++] = 2;
    } else {
        //Constant
        t.set(Term(0, pattern->object()), 2);
        outputTuple[2] = pattern->object();
    }

    //Replace variables with constants if posJoins != NULL.
    Tuple boundTuple = t;
    if (posJoins != NULL) {
        //The posjoins do not include constants
        int j = 0;
        for (std::vector<uint8_t>::const_iterator itr = newPosJoins.begin();
                itr != newPosJoins.end(); ++itr) {
            boundTuple.set(Term(0, possibleValuesJoins->at(j++)), *itr);
        }
    }


    std::string ti("TI");
    Literal query(program.getPredicate(ti, Predicate::calculateAdornment(boundTuple)),
                  boundTuple);

    //Get all adorned rules
    Wizard wizard;
    std::shared_ptr<Program> adornedProgram = wizard.getAdornedProgram(query, program);

#ifdef DEBUG
    /*
        //Print all rules
        BOOST_LOG_TRIVIAL(debug) << "Original program for query " << query.tostring() << ":";
        std::vector<Rule> newRules = program.getAllRules();
        for (std::vector<Rule>::iterator itr = newRules.begin(); itr != newRules.end(); ++itr) {
            BOOST_LOG_TRIVIAL(debug) << itr->tostring(&program, dict);
        }

        BOOST_LOG_TRIVIAL(debug) << "Adorned program for query " << query.tostring() << ":";
        newRules = adornedProgram->getAllRules();
        for (std::vector<Rule>::iterator itr = newRules.begin(); itr != newRules.end(); ++itr) {
            BOOST_LOG_TRIVIAL(debug) << itr->tostring(adornedProgram.get(), dict);
        }
        */
#endif

    //Rewrite and add the rules
    std::pair<PredId_t, PredId_t> inputOutputRelIDs;
    std::shared_ptr<Program> magicProgram = wizard.doMagic(query, adornedProgram,
                                            inputOutputRelIDs);
#ifdef DEBUG

    //Print all rules
    BOOST_LOG_TRIVIAL(debug) << "Rewritten program:";
    std::vector<Rule> newRules = magicProgram->getAllRules();
    for (std::vector<Rule>::iterator itr = newRules.begin(); itr != newRules.end(); ++itr) {
        BOOST_LOG_TRIVIAL(debug) << itr->tostring(magicProgram.get(), dict);
    }

#endif

    SemiNaiver naiver(magicProgram->getAllRules(), edb, dict, magicProgram.get(), true, true) ;

    //Add all the input tuples in the input relation
    Predicate pred = magicProgram->getPredicate(inputOutputRelIDs.first);
    Tuple onlyConstsTuple(pred.getCardinality());
    int j = 0;
    for (int i = 0; i < t.getSize(); ++i) {
        if (!boundTuple.get(i).isVariable()) {
            onlyConstsTuple.set(Term(0, t.get(i).getValue()), j++);
        }
    }

    Literal unboundQuery(pred, onlyConstsTuple);
    naiver.addDataToIDBRelation(magicProgram->getPredicate(inputOutputRelIDs.first),
                                getBlockFromQuery(unboundQuery, query,
                                        newPosJoins.size() != 0 ? &newPosJoins : NULL, possibleValuesJoins));

    //Exec the materialization
    naiver.run(1, 2);

    //Extract the tuples from the output relation
    Literal outputLiteral(magicProgram->getPredicate(inputOutputRelIDs.second), t);
    FCIterator itr = naiver.getTable(outputLiteral, 0, (size_t) - 1);

    TupleTable *finalTable;
    if (returnOnlyVars) {
        finalTable = new TupleTable(outputLiteral.getNVars());
    } else {
        finalTable = new TupleTable(3);
    }

    while (!itr.isEmpty()) {
        std::shared_ptr<const FCInternalTable> table = itr.getCurrentTable();
        FCInternalTableItr *itrTable = table->getIterator();

        if (returnOnlyVars) {
            const uint8_t rowSize = table->getRowSize();
            while (itrTable->hasNext()) {
                itrTable->next();
                for (uint8_t j = 0; j < rowSize; ++j) {
                    finalTable->addValue(itrTable->getCurrentValue(j));
                }
            }
        } else {
            while (itrTable->hasNext()) {
                itrTable->next();
                for (uint8_t j = 0; j < nPosToCopy; ++j) {
                    outputTuple[posToCopy[j]] = itrTable->getCurrentValue(j);
                }
                finalTable->addRow(outputTuple);
            }
        }

        table->releaseIterator(itrTable);
        itr.moveNextCount();
    }

    std::shared_ptr<TupleTable> pFinalTable(finalTable);
    return new TupleTableItr(pFinalTable);
}

ReasoningMode Reasoner::chooseMostEfficientAlgo(Pattern *pattern,
        EDBLayer &layer, Program &program, DictMgmt *dict,
        std::vector<uint8_t> *posBindings,
        std::vector<Term_t> *valueBindings) {
    uint64_t cost = 0;
    if (posBindings != NULL) {
        //Create a new pattern with the values substituted
        Pattern newPattern(*pattern);
        int idxValues = 0;
        for (std::vector<uint8_t>::iterator itr = posBindings->begin(); itr != posBindings->end();
                ++itr) {
            switch (*itr) {
            case 0:
                newPattern.subject(valueBindings->at(idxValues));
                break;
            case 1:
                newPattern.predicate(valueBindings->at(idxValues));
                break;
            case 2:
                newPattern.object(valueBindings->at(idxValues));
                break;
            default:
                throw 10; //cannot happen!
            }
            idxValues++;
        }
        size_t singleCost = estimate(&newPattern, NULL, NULL, layer, program, dict);
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
            uint64_t tenCost = estimate(pattern, posBindings, &limitedValueBindings, layer, program, dict);
            BOOST_LOG_TRIVIAL(debug) << "TenCost is " << tenCost;

            //y = mx + b. m is slope, b is constant.
            //I assume the singleCost is the cost at position 0. Otherwise it's not a line
            double m = (double)(tenCost - singleCost) / (10);
            long b = singleCost;
            cost = (uint64_t) (m * nValues + b);
        } else {
            cost = singleCost;
        }
    } else {
        cost = estimate(pattern, NULL, NULL, layer, program, dict);
    }
    ReasoningMode mode = cost < threshold ? TOPDOWN : MAGIC;
    BOOST_LOG_TRIVIAL(debug) << "Deciding whether I should resolve " <<
                             pattern->toString() << " with magic or QSQR. Estimated cost: " <<
                             cost << " threshold for QSQ-R is " << threshold;
    return mode;
}

TupleIterator *Reasoner::getTopDownIterator(Pattern *pattern,
        std::vector<uint8_t> *posJoins,
        std::vector<Term_t> *possibleValuesJoins,
        EDBLayer &edb, Program &program, DictMgmt *dict, bool returnOnlyVars) {

    BOOST_LOG_TRIVIAL(debug) << "Get topdown iterator for pattern " << pattern->toString();
    Tuple t(3);
    std::vector<uint8_t> newPosJoins;
    if (posJoins != NULL)
        newPosJoins = *posJoins;

    //s
    int nvars = 0;
    if (pattern->subject() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 0);
    } else {
        //Constant
        t.set(Term(0, pattern->subject()), 0);
        if (newPosJoins.size() == 2) {
            newPosJoins[0]++;
            newPosJoins[1]++;
        }

        if (newPosJoins.size() == 1) {
            newPosJoins[0]++;
        }
    }
    //p
    if (pattern->predicate() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 1);
    } else {
        //Constant
        t.set(Term(0, pattern->predicate()), 1);
        if (newPosJoins.size() == 1 && newPosJoins[0] == 1) {
            newPosJoins[0]++;
        }
        if (newPosJoins.size() == 2 && newPosJoins[1] == 1) {
            newPosJoins[1]++;
        }
    }
    //o
    if (pattern->object() < 0) {
        //Variable
        uint8_t id = (uint8_t)program.getIDVar(pattern->getVar(nvars++));
        t.set(Term(id, 0), 2);
    } else {
        //Constant
        t.set(Term(0, pattern->object()), 2);
    }

    //Create query
    std::string ti("TI");
    QSQQuery rootQuery(Literal(program.getPredicate(ti,
                               Predicate::calculateAdornment(t)), t));

    /*if (posJoins != NULL && posJoins->size() == pattern->getNVars()) {
        finalTable = getVerifiedBindings(rootQuery, newPosJoins.size() > 0 ? &newPosJoins : NULL, possibleValuesJoins,
                                         edb, program, dict, returnOnlyVars);
    } else {*/
    QSQR evaluator(edb, &program, dict);
    TupleTable *finalTable;
#ifdef LINEAGE
    std::vector<LineageInfo> info;
    finalTable = evaluator.evaluateQuery(QSQR_EVAL, &rootQuery, newPosJoins.size() > 0 ? &newPosJoins : NULL,
                                         possibleValuesJoins, returnOnlyVars, info);
#else
    finalTable = evaluator.evaluateQuery(QSQR_EVAL, &rootQuery, newPosJoins.size() > 0 ? &newPosJoins : NULL,
                                         possibleValuesJoins, returnOnlyVars);
#endif
    //}

    //Return an iterator of the bindings
    std::shared_ptr<TupleTable> pFinalTable(finalTable);
    return new TupleTableItr(pFinalTable);
}

std::shared_ptr<SemiNaiver> Reasoner::fullMaterialization(KB *kb, EDBLayer &layer,
        Program *p, bool opt_intersect, bool opt_filtering) {
    BOOST_LOG_TRIVIAL(info) << "Starting full materialization";
    timens::system_clock::time_point start = timens::system_clock::now();
    std::shared_ptr<SemiNaiver> sn(new SemiNaiver(p->getAllRules(), layer, kb->getDictMgmt(), p, opt_intersect, opt_filtering));
    sn->run();
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(info) << "Runtime materialization = " << sec.count() * 1000 << " milliseconds";
    sn->printCountAllIDBs();
    return sn;
}
