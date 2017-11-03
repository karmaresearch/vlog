#include <vlog/ruleexecutor.h>
#include <vlog/bindingstable.h>
#include <vlog/edb.h>
#include <vlog/qsqr.h>

#include <trident/model/table.h>

RuleExecutor::RuleExecutor(const Rule &rule, uint8_t headAdornment
                           , Program *program,
                           EDBLayer &layer
                          ) :
    adornedRule(rule.createAdornment(headAdornment))
    , program(program), layer(layer)
{
    calculateJoinsSizeIntermediateRelations();
}

void RuleExecutor::calculateJoinsSizeIntermediateRelations() {
    std::vector<Literal> body = adornedRule.getBody();

    headVarsInEDB.clear();
    repeatedBoundVarsInHead.clear();
    njoins.clear();
    startJoins.clear();
    joins.clear();
    posFromHeadToFirstSupplRelation.clear();

    std::vector<uint8_t> boundVars;

    int nBoundInAdornment = 0;
    int nBoundVarInAdornment = 0;
    Literal head = adornedRule.getFirstHead();
    uint8_t headAdornment = head.getPredicate().getAdorment();
    for (size_t i = 0; i < head.getTupleSize(); ++i) {
        if (headAdornment >> i & 1) {
            VTerm t = head.getTermAtPos(i);
            if (t.isVariable()) {
                int prevOccurrence = -1;
                for (size_t j = 0; j < boundVars.size(); ++j) {
                    if (boundVars[j] == t.getId()) {
                        prevOccurrence = j;
                        break;
                    }
                }
                if (prevOccurrence == -1) {
                    boundVars.push_back(t.getId());
                    posFromHeadToFirstSupplRelation.push_back(nBoundInAdornment);
                    nBoundVarInAdornment++;
#ifdef PRUNING_QSQR
                    //Check if this variable appears in EDB body literals
                    for (size_t j = 0; j < body.size(); ++j) {
                        Literal literal = body.at(j);
                        if (literal.getPredicate().getType() == EDB) {
                            for (uint8_t m = 0; m < literal.getTupleSize(); ++m) {
                                VTerm term = literal.getTermAtPos(m);
                                if (term.isVariable() && term.getId() == t.getId()) {
                                    //match
                                    headVarsInEDB.push_back(std::make_pair(nBoundInAdornment,
                                                                           std::make_pair(literal.getPredicate().getId(), m)));
                                }
                            }
                        }
                    }
#endif
                } else {
                    repeatedBoundVarsInHead.push_back(std::make_pair(prevOccurrence, nBoundInAdornment));
                }
            }
            nBoundInAdornment++;
        }
    }
    sizeSupplRelations.push_back(nBoundVarInAdornment);

    for (size_t j = 0; j < body.size(); ++j) {
        Literal literal = body.at(j);
        uint8_t adornmentLiteral = literal.getPredicate().getAdorment();
        std::vector<uint8_t> currentSignature;
        std::vector<int> posToSupplRel;
        std::vector<std::pair<uint8_t, uint8_t>> boundFromLiteral;
        std::vector<std::pair<uint8_t, uint8_t>> boundFromSuppl;

        //Add the variables in the current atom that are used later on.
        //Calculate also the number of joins to perform.
        int njoins = 0;
        startJoins.push_back((short) joins.size());
        uint8_t nAdornments = 0;
        uint8_t nvars = 0;
        for (uint8_t i = 0; i < literal.getTupleSize(); ++i) {
            VTerm t = literal.getTermAtPos(i);
            if (t.isVariable()) {
                if (adornedRule.doesVarAppearsInFollowingPatterns(j + 1, t.getId())) {
                    posToSupplRel.push_back(nvars);
                    currentSignature.push_back(t.getId());
                }
                //if it exists in boundVars, then there is a join
                for (uint8_t j = 0; j < boundVars.size(); ++j) {
                    if (boundVars[j] == t.getId()) {
                        joins.push_back(std::make_pair(nvars, j));
                        njoins++;
                        boundFromSuppl.push_back(std::make_pair(j, nAdornments));
                    }
                }
                nvars++;
            }

            if (adornmentLiteral & 1) {
                if (!t.isVariable())
                    boundFromLiteral.push_back(std::make_pair(i, nAdornments));
                nAdornments++;
            }
            adornmentLiteral >>= 1;
        }
        this->njoins.push_back((unsigned char) njoins);
        this->posFromLiteral.push_back(boundFromLiteral);
        this->posFromSupplRelation.push_back(boundFromSuppl);

        //Add to currentSignature all variables in the previous rounds that are used later
        int pos = nvars;
        for (std::vector<uint8_t>::iterator itr = boundVars.begin(); itr != boundVars.end();
                ++itr) {
            //Check the variables is not already in the current signature
            bool found = false;
            for (std::vector<uint8_t>::iterator itr2 = currentSignature.begin();
                    itr2 != currentSignature.end();
                    ++itr2) {
                if (*itr2 == *itr) {
                    found = true;
                    break;
                }
            }
            //Does this variable appear in the head or in the following patterns?
            if (!found && adornedRule.doesVarAppearsInFollowingPatterns(j + 1, *itr)) {
                posToSupplRel.push_back(pos);
                currentSignature.push_back(*itr);
            }
            pos++;
        }

        //Add the coordinates to the supplementary relation
        sizeSupplRelations.push_back(posToSupplRel.size());
        posToCopyFromPreviousStep.push_back(posToSupplRel);

        //Copy current signature in the previous
        boundVars = currentSignature;
    }

    //Add the projection to produce the final derivation
    for (size_t i = 0; i < head.getTupleSize(); ++i) {
        VTerm t = head.getTermAtPos(i);
        if (t.isVariable()) {
            //Search position of the variable in boundVars
            uint8_t k = 0;
            bool found = false;
            for (std::vector<uint8_t>::iterator itr = boundVars.begin();
                    itr != boundVars.end(); ++itr) {
                if (*itr == t.getId()) {
                    projectionLastSuppl.push_back(k);
                    found = true;
                    break;
                }
                k++;
            }
            if (!found) {
                LOG(ERRORL) << "Variable not found!";
                throw 10;
            }
        }
    }
}

BindingsTable **RuleExecutor::createSupplRelations() {
    size_t n = adornedRule.getBody().size();
    //Create the suppl. relations
    BindingsTable **supplRelations = new BindingsTable*[n + 1];

    //Add the first relation. Get card. first relation
    supplRelations[0] = new BindingsTable((uint8_t) sizeSupplRelations[0],
                                          posFromHeadToFirstSupplRelation);

    //Add all the others
    for (size_t i = 0; i < n; ++i) {
        supplRelations[i + 1] = new BindingsTable((uint8_t) sizeSupplRelations[i + 1], posToCopyFromPreviousStep[i]);
    }
    return supplRelations;
}

void RuleExecutor::deleteSupplRelations(BindingsTable **supplRelations) {
    size_t n = adornedRule.getBody().size() + 1;
    for (size_t i = 0; i < n; ++i) {
        delete supplRelations[i];
    }
    delete[] supplRelations;
}

bool RuleExecutor::isUnifiable(const Term_t * const value, const size_t sizeTuple,
                               const size_t *posInAdorment, const EDBLayer &layer) {
    //Check with the head of the rule to see whether there is a match

    // LOG(DEBUGL) << "isUnifiable: adornedRule = " << adornedRule.tostring();
    for (size_t i = 0; i < sizeTuple; ++i) {
        size_t pos = posInAdorment[i];
        VTerm t = adornedRule.getFirstHead().getTermAtPos(pos);
        if (!t.isVariable()) {
            // LOG(DEBUGL) << "isUnifiable: value check: " << t.getValue() << ", " << value[i];
            if (t.getValue() != value[i]) {
                // LOG(DEBUGL) << "not unifiable";
                return false;
            }
        } else {
#ifdef PRUNING_QSQR
            for (std::vector<std::pair<uint8_t, std::pair<uint8_t, uint8_t>>>::iterator itr = headVarsInEDB.begin();
                    itr != headVarsInEDB.end(); ++itr) {
                // LOG(DEBUGL) << "isUnifiable: pruning check, itr->first = " << (int) itr->first << ", pos = " << (int) pos;
                if (itr->first == pos && !layer.checkValueInTmpRelation(itr->second.first,
                        itr->second.second, value[i])) {
                    // LOG(DEBUGL) << "not unifiable";
                    return false;
                }
            }
#endif
        }
    }

    //If the head has repeated variables, and these are not instantiated
    //correctly by the tuple, then this is not instantiable.
    if (repeatedBoundVarsInHead.size() != 0) {
        // LOG(DEBUGL) << "isUnifiable: repeatedbounds check";
        for (size_t i = 0; i < repeatedBoundVarsInHead.size(); ++i) {
            std::pair<int, int> pair = repeatedBoundVarsInHead[i];
            // LOG(DEBUGL) << "value1 = " << value[pair.first] << ", value2 = " << value[pair.second];
            if (value[pair.first] != value[pair.second]) {
                // LOG(DEBUGL) << "not unifiable";
                return false;
            }
        }
    }

    // LOG(DEBUGL) << "unifiable!";
    return true;
}

size_t RuleExecutor::estimateRule(const int depth, const uint8_t bodyAtom,
                                  BindingsTable **supplRelations, QSQR* qsqr, EDBLayer &layer) {
    TupleTable *retrievedBindings = NULL;
    Literal l = adornedRule.getBody()[bodyAtom];
    uint8_t nCurrentJoins = this->njoins[bodyAtom];
    std::vector<uint8_t> posJoinsSupplRel;
    std::vector<uint8_t> posJoinsLiteral;
    if (nCurrentJoins > 0) {
        int startIdx = startJoins[bodyAtom];
        for (int i = 0; i < nCurrentJoins; ++i) {
            posJoinsSupplRel.push_back(joins[startIdx + i].second);
            posJoinsLiteral.push_back(joins[startIdx + i].first);
        }
    }

    if (l.getPredicate().getType() == EDB) {
        if (bodyAtom < adornedRule.getBody().size() - 1) {
            retrievedBindings = new TupleTable(l.getNVars());
            QSQQuery query(l);
            if (nCurrentJoins > 0) {
                std::vector<Term_t> bindings = supplRelations[bodyAtom]->getProjection(posJoinsSupplRel);
                layer.query(&query, retrievedBindings, &posJoinsLiteral, &bindings);
            } else {
                layer.query(&query, retrievedBindings, NULL, NULL);
            }
        } else { //Last one
            //Pass parameters from supplRelations
            if (nCurrentJoins > 0) {
                std::vector<Term_t> bindings = supplRelations[bodyAtom]->getProjection(posJoinsSupplRel);
                size_t card = 0;
                for (int i = 0; i < bindings.size(); i += nCurrentJoins) {
                    //Modify l
                    VTuple t = l.getTuple();
                    for (int j = 0; j < nCurrentJoins; ++j) {
                        t.set(VTerm(0, bindings[i + j]), posJoinsLiteral[j]);
                    }
                    Literal query(l.getPredicate(), t);
                    card += layer.estimateCardinality(query);
                }
                return card;
            } else {
                //QSQQuery query(l);
                return layer.estimateCardinality(l);
            }
        }
    } else {
        QSQQuery query(l);
        //Copy in input the query that we are about to launch
        BindingsTable *table = qsqr->getInputTable(query.getLiteral()->getPredicate());
        //size_t offsetInput = table->getNTuples();
        if (posFromSupplRelation[bodyAtom].size() == 0) {
            if (posFromLiteral[bodyAtom].size() == 0) {
                table->addRawTuple(NULL);
            } else {
                Term_t tmpRow[SIZETUPLE];
                vector<std::pair<uint8_t, uint8_t>> pairs = posFromLiteral[bodyAtom];

                for (size_t i = 0; i < pairs.size(); ++i) {
                    tmpRow[pairs[i].second] = query.getLiteral()->getTermAtPos(pairs[i].first).getValue();
                }
                table->addRawTuple(tmpRow);
            }
        } else {
            Term_t tmpRow[SIZETUPLE];
            if (posFromLiteral[bodyAtom].size() > 0) {
                vector<std::pair<uint8_t, uint8_t>> pairs = posFromLiteral[bodyAtom];
                for (size_t i = 0; i < pairs.size(); ++i) {
                    tmpRow[pairs[i].second] = query.getLiteral()->getTermAtPos(pairs[i].first).getValue();
                }
            }
            std::vector<std::pair<uint8_t, uint8_t>> pairs = posFromSupplRelation[bodyAtom];
            for (size_t i = 0; i < supplRelations[bodyAtom]->getNTuples(); ++i) {
                const Term_t *tuple = supplRelations[bodyAtom]->getTuple(i);
                for (std::vector<std::pair<uint8_t, uint8_t>>::iterator itr = pairs.begin();
                        itr != pairs.end(); ++itr) {
                    tmpRow[itr->second] = tuple[itr->first];
                }
                table->addRawTuple(tmpRow);
            }
        }
        //Call the query if there are new queries
        //if (table->getNTuples() > offsetInput) {
        Predicate pred = query.getLiteral()->getPredicate();
        return qsqr->estimate(depth, pred, table/*, offsetInput*/);
        //} else {
        //    return 0;
        //}
    }

    //I arrive here only if the query is EDB and it is not the last one
    if (retrievedBindings == NULL || retrievedBindings->getNRows() == 0) {
        if (retrievedBindings != NULL)
            delete retrievedBindings;
        return 0;
    }

    size_t card = retrievedBindings->getNRows();
    if (nCurrentJoins > 0) {
        //Sort the current TupleBindings to perform a merge sort with the just-retrieved tuples
        TupleTable *sortedBindings2 = supplRelations[bodyAtom]->sortBy(posJoinsSupplRel);

        //Sort also the retrieved tuples
        TupleTable *sortedBindings1 = retrievedBindings->sortBy(posJoinsLiteral);

        //Do the join and copy the results in the following suppl. relation
        RuleExecutor::join(sortedBindings1, sortedBindings2, &(joins.at(startJoins[bodyAtom])),
                           nCurrentJoins, supplRelations[bodyAtom + 1]);

        delete sortedBindings1;
        delete sortedBindings2;
    } else {
        if (supplRelations[bodyAtom]->getSizeTuples() == 0) {
            //Simply copy all retrieved elements in the following relation
            for (size_t i = 0; i < retrievedBindings->getNRows(); ++i) {
                supplRelations[bodyAtom + 1]->addTuple(retrievedBindings->getRow(i));
            }
        } else {
            LOG(ERRORL) << "Need to perform the cardinal product. Not yet supported";
            throw 10;
        }
    }

    if (retrievedBindings != NULL) {
        delete retrievedBindings;
    }
    return card;
}

void RuleExecutor::evaluateRule(const uint8_t bodyAtom,
                                BindingsTable **supplRelations,
                                QSQR* qsqr, EDBLayer &layer) {

    Literal l(adornedRule.getBody()[bodyAtom]);

    // LOG(DEBUGL) << "evaluateRule: literal = " << l.tostring(program, &layer);

    //Do the computation to produce bindings for the next suppl. relation.
    uint8_t nCurrentJoins = this->njoins[bodyAtom];
    TupleTable *retrievedBindings = NULL;

    std::vector<uint8_t> posJoinsSupplRel;
    std::vector<uint8_t> posJoinsLiteral;
    if (nCurrentJoins > 0) {
        int startIdx = startJoins[bodyAtom];
        for (int i = 0; i < nCurrentJoins; ++i) {
            posJoinsSupplRel.push_back(joins[startIdx + i].second);
            posJoinsLiteral.push_back(joins[startIdx + i].first);
        }
    }

    QSQQuery query(l);
    if (l.getPredicate().getType() == EDB) {
        //LOG(DEBUGL) << "Atom " << (int)bodyAtom << " is EDB";
        retrievedBindings = new TupleTable(l.getNVars());
        //std::chrono::system_clock::time_point startEDB =
        //    std::chrono::system_clock::now();
        if (nCurrentJoins > 0) {
            std::vector<Term_t> bindings = supplRelations[bodyAtom]->
                                           getUniqueSortedProjection(
                                               posJoinsSupplRel);
	    // posJoinsLiteral is variable position, so for instance 1 means second variable.
	    // However, in edb layer, it means position in query, which may also contain constants.
	    // So, replace by variable positions in query.
	    for (int i = 0; i < posJoinsLiteral.size(); i++) {
		posJoinsLiteral[i] = l.getPosVars()[posJoinsLiteral[i]];
	    }
            layer.query(&query, retrievedBindings,
                        &posJoinsLiteral, &bindings);
        } else {
            layer.query(&query, retrievedBindings, NULL, NULL);
        }
        //durationEDB += std::chrono::system_clock::now() - startEDB;
        // LOG(DEBUGL) << "EDB, query " << query.tostring() << ", retrieved " << retrievedBindings->getNRows();
    } else {
        //Copy in input the query that we are about to launch
        BindingsTable *table = qsqr->getInputTable(query.getLiteral()->getPredicate());
        //LOG(DEBUGL) << "ENRICH TABLE " << table->getNTuples();
        size_t offsetInput = table->getNTuples();
        if (posFromSupplRelation[bodyAtom].size() == 0) {
            if (posFromLiteral[bodyAtom].size() == 0) {
                table->addRawTuple(NULL);
            } else {
                Term_t tmpRow[SIZETUPLE];
                vector<std::pair<uint8_t, uint8_t>> pairs = posFromLiteral[bodyAtom];

                for (size_t i = 0; i < pairs.size(); ++i) {
                    tmpRow[pairs[i].second] = query.getLiteral()->getTermAtPos(pairs[i].first).getValue();
                }
                table->addRawTuple(tmpRow);
            }
        } else {
            Term_t tmpRow[SIZETUPLE];
            if (posFromLiteral[bodyAtom].size() > 0) {
                vector<std::pair<uint8_t, uint8_t>> pairs = posFromLiteral[bodyAtom];
                for (size_t i = 0; i < pairs.size(); ++i) {
                    tmpRow[pairs[i].second] = query.getLiteral()->getTermAtPos(pairs[i].first).getValue();
                }
            }
            std::vector<std::pair<uint8_t, uint8_t>> pairs = posFromSupplRelation[bodyAtom];
            for (size_t i = 0; i < supplRelations[bodyAtom]->getNTuples(); ++i) {
                const Term_t *tuple = supplRelations[bodyAtom]->getTuple(i);
                for (std::vector<std::pair<uint8_t, uint8_t>>::iterator itr = pairs.begin();
                        itr != pairs.end(); ++itr) {
                    tmpRow[itr->second] = tuple[itr->first];
                }
                table->addRawTuple(tmpRow);
            }
        }

        //Call the query if there are new queries
        if (table->getNTuples() > offsetInput) {
            Predicate pred = query.getLiteral()->getPredicate();
#ifdef RECURSIVE_QSQR
            qsqr->evaluate(pred, table, offsetInput);
#else
            //Do not execute it recursively. Instead, create a new task and exit
            QSQR_Task task(RULE_QUERY, pred);
            task.executor = this;
            task.inputTable = table;
            task.supplRelations = supplRelations;
            task.offsetInput = offsetInput;
            task.currentRuleIndex = bodyAtom;
            task.qsqr = qsqr;
            qsqr->pushTask(task);
            qsqr->evaluate(pred, table, offsetInput);
            return;
#endif
        }

        //Get previous answers
        BindingsTable *answer = qsqr->getAnswerTable(query.getLiteral());
        retrievedBindings = answer->projectAndFilter(l, NULL, NULL);
    }

    if (retrievedBindings == NULL || retrievedBindings->getNRows() == 0) {
        if (retrievedBindings != NULL)
            delete retrievedBindings;
        return;
    }

    if (nCurrentJoins > 0) {
        //Sort the current TupleBindings to perform a merge sort with the just-retrieved tuples
        TupleTable *sortedBindings2 = supplRelations[bodyAtom]->sortBy(posJoinsSupplRel);

        //Sort also the retrieved tuples
        TupleTable *sortedBindings1 = retrievedBindings->sortBy(posJoinsLiteral);

        //Do the join and copy the results in the following suppl. relation
        RuleExecutor::join(sortedBindings1, sortedBindings2, &(joins.at(startJoins[bodyAtom])),
                           nCurrentJoins, supplRelations[bodyAtom + 1]);

        delete sortedBindings1;
        delete sortedBindings2;
    } else {
        if (supplRelations[bodyAtom]->getSizeTuples() == 0) {
            //Simply copy all retrieved elements in the following relation
            for (size_t i = 0; i < retrievedBindings->getNRows(); ++i) {
                supplRelations[bodyAtom + 1]->addTuple(retrievedBindings->getRow(i));
            }
        } else {
            LOG(ERRORL) << "Need to perform the cardinal product. Not yet supported";
            throw 10;
        }
    }

    if (retrievedBindings != NULL) {
        delete retrievedBindings;
    }
}

#ifdef LINEAGE
void RuleExecutor::printLineage(std::vector<LineageInfo> &lineage) {
    for (int i = 0; i < lineage.size(); ++i) {
        cout << lineage[i].bodyAtomId << " " << lineage[i].nQueries << " " << lineage[i].adornedRule->tostring(program, layer) << endl;
        //print the queries
        cout << "              " << lineage[i].offset << " " << (void*) lineage[i].pointerToInput << endl;
        if (lineage[i].nQueries > 0) {
            for (int j = 0; j < lineage[i].nQueries && j < 14; ++j) {
                cout << "            ";
                for (int m = 0; m < lineage[i].sizeQuery; ++m) {
                    char text[256];
                    layer->getDictText(lineage[i].queries[m + j * lineage[i].sizeQuery], text);
                    cout << text << " ";
                }
                cout << endl;
            }
        }
    }
}
#endif

size_t RuleExecutor::estimate(const int depth, BindingsTable * input,/* size_t offsetInput,*/ QSQR * qsqr,
                              EDBLayer &layer) {
    LOG(DEBUGL) << "Estimating rule " << adornedRule.tostring(NULL,NULL) << ", depth = " << depth;
    size_t output = 0;
    //if (input->getNTuples() > offsetInput) {
    //Get the new tuples. All the tuples that merge with the head of the
    //adorned rule are being copied in the first supplementary relation
    BindingsTable **supplRelations = createSupplRelations();

    //Copy all the tuples that are unifiable with the head in the first
    //supplementary relation.
    for (size_t i = /*offsetInput*/0; i < input->getNTuples(); ++i) {
        const Term_t* tuple = input->getTuple(i);
        if (isUnifiable(tuple, input->getSizeTuples(), input->getPosFromAdornment(), layer)) {
            supplRelations[0]->addTuple(tuple);
        }
    }

    if (supplRelations[0]->getNTuples() > 0) {
        uint8_t bodyAtomIdx = 0;
	output = 1;
        do {
            //LOG(INFOL) << "Atom " << (int) bodyAtomIdx;
	    uint8_t nCurrentJoins = this->njoins[bodyAtomIdx];
	    size_t r = estimateRule(depth, bodyAtomIdx, supplRelations, qsqr, layer);
            if (nCurrentJoins != 0) {
                output = r;
            } else {
                output *= r;
            }
	    LOG(DEBUGL) << "Atom: " << (int) bodyAtomIdx << ", estimate: " << r << ", output: " << output;
	    bodyAtomIdx++;
        } while (output != 0 && bodyAtomIdx < adornedRule.getBody().size()
                 && supplRelations[bodyAtomIdx]->getNTuples() > 0);
	if (bodyAtomIdx < adornedRule.getBody().size()) {
	    output = 0;
	}

    }

    // Leaked supplRelations. Added line below. --Ceriel
    deleteSupplRelations(supplRelations);
    //}
    LOG(DEBUGL) << "Estimate for rule " << adornedRule.tostring(program,&layer) << ", depth = " << depth << " = " << output;
    return output;
}

void RuleExecutor::copyLastRelInAnswers(QSQR *qsqr,
                                        size_t nTuples,
                                        BindingsTable **supplRelations,
                                        BindingsTable *lastSupplRelation) {
    if (nTuples > 0) {
        Literal l = adornedRule.getFirstHead();
        BindingsTable *answer = qsqr->getAnswerTable(&l);

        //Copy the head in the tuple
        Term_t tuple[SIZETUPLE];
        uint8_t nvars = 0;
        uint8_t posVars[SIZETUPLE];
        for (uint8_t i = 0; i < adornedRule.getFirstHead().getTupleSize(); ++i) {
            VTerm t = adornedRule.getFirstHead().getTermAtPos(i);
            if (t.isVariable()) {
                posVars[nvars++] = i;
            } else {
                tuple[i] = t.getValue();
            }
        }

        for (size_t i = 0; i < nTuples; ++i) {
            const Term_t *supplRow = lastSupplRelation->getTuple(i);
            for (uint8_t j = 0; j < nvars; ++j) {
                tuple[posVars[j]] = supplRow[projectionLastSuppl
                                             [j]];
            }
            answer->addRawTuple(tuple);
        }
    }

    //Delete supplRelations
    deleteSupplRelations(supplRelations);
}

void RuleExecutor::evaluate(BindingsTable * input, size_t offsetInput,
                            QSQR * qsqr,
                            EDBLayer &layer) {


    //Evaluate the rule
    if (input->getNTuples() > offsetInput) {
        //Get the new tuples. All the tuples that merge with the head of the
        //adorned rule are being copied in the first supplementary relation
        BindingsTable **supplRelations = createSupplRelations();

        //Copy all the tuples that are unifiable with the head in the first
        //supplementary relation.
        for (size_t i = offsetInput; i < input->getNTuples(); ++i) {
            const Term_t* tuple = input->getTuple(i);
            if (isUnifiable(tuple, input->getSizeTuples(),
                            input->getPosFromAdornment(), layer)) {
                supplRelations[0]->addTuple(tuple);
            }
        }

        size_t cnt = supplRelations[0]->getNTuples();
        if (cnt > 0) {
#ifdef RECURSIVE_QSQR
            uint8_t bodyAtomIdx = 0;
            do {
                evaluateRule(bodyAtomIdx++, supplRelations, qsqr, layer);
            } while (bodyAtomIdx < adornedRule.getBody().size()
                     && supplRelations[bodyAtomIdx]->getNTuples() > 0);

            BindingsTable *lastSupplRelation = supplRelations[adornedRule.getBody().size()];
            size_t nTuples = lastSupplRelation->getNTuples();
            if (nTuples > 10000) {
                LOG(WARNL) << "The last supplRelation contains " << nTuples;
            }

            copyLastRelInAnswers(qsqr, nTuples, supplRelations,
                                 lastSupplRelation);
#else
            //Create a task to execute
            QSQR_Task task(QSQR_TaskType::RULE);
            task.supplRelations = supplRelations;
            task.currentRuleIndex = 1;
            task.qsqr = qsqr;
            task.layer = &layer;
            task.executor = this;
            qsqr->pushTask(task);
            evaluateRule(0, supplRelations, qsqr, layer);
#endif
        } else {
            //Delete supplRelations
            deleteSupplRelations(supplRelations);
        }
    }
}

#ifndef RECURSIVE_QSQR
void RuleExecutor::processTask(QSQR_Task *t) {
    QSQR_Task &task = *t;
    switch (task.type) {
    case RULE: {
	size_t sz = adornedRule.getBody().size();
        if (task.currentRuleIndex < sz && task.
                supplRelations[task.currentRuleIndex]->getNTuples() > 0) {
            QSQR_Task newTask(QSQR_TaskType::RULE);
            newTask.supplRelations = task.supplRelations;
            newTask.currentRuleIndex = task.currentRuleIndex + 1;
            newTask.qsqr = task.qsqr;
            newTask.layer = task.layer;
            newTask.executor = this;
            task.qsqr->pushTask(newTask);
            //Move to the next atom
            evaluateRule((uint8_t) task.currentRuleIndex, task.supplRelations,
                         task.qsqr, *task.layer);
        } else {
            BindingsTable *lastSupplRelation = task.supplRelations[sz];
            size_t nTuples = lastSupplRelation->getNTuples();
            if (nTuples > 10000) {
                LOG(DEBUGL) << "The last supplRelation contains " << nTuples;
            }
            copyLastRelInAnswers(task.qsqr, nTuples, task.supplRelations,
                                 lastSupplRelation);
        }
        }
        break;
    case RULE_QUERY: {
        //LOG(DEBUGL) << "Process RULE_QUERY";
        Literal l(adornedRule.getBody()[task.currentRuleIndex]);
        QSQQuery query(l);
        BindingsTable *answer = task.qsqr->getAnswerTable(query.getLiteral());
        TupleTable *retrievedBindings = answer->
                                        projectAndFilter(l, NULL, NULL);
        const uint8_t nCurrentJoins = this->njoins[task.currentRuleIndex];
        std::vector<uint8_t> posJoinsSupplRel;
        std::vector<uint8_t> posJoinsLiteral;
        if (nCurrentJoins > 0) {
            int startIdx = startJoins[task.currentRuleIndex];
            for (int i = 0; i < nCurrentJoins; ++i) {
                posJoinsSupplRel.push_back(joins[startIdx + i].second);
                posJoinsLiteral.push_back(joins[startIdx + i].first);
            }
        }

        if (retrievedBindings == NULL || retrievedBindings->getNRows() == 0) {
            if (retrievedBindings != NULL) {
                delete retrievedBindings;
                retrievedBindings = NULL;
            }
        } else if (nCurrentJoins > 0) {
            TupleTable *sortedBindings2 = task.supplRelations[task.
                                          currentRuleIndex]->
                                          sortBy(posJoinsSupplRel);

            //Sort also the retrieved tuples
            TupleTable *sortedBindings1 = retrievedBindings->
                                          sortBy(posJoinsLiteral);

            //Do the join and copy the results in the following suppl. relation
            RuleExecutor::join(sortedBindings1, sortedBindings2,
                               &(joins.at(startJoins[task.currentRuleIndex])),
                               nCurrentJoins, task.supplRelations[task.
                                       currentRuleIndex + 1]);

            delete sortedBindings1;
            delete sortedBindings2;
        } else {
            if (task.supplRelations[task.
                                    currentRuleIndex]->getSizeTuples() == 0) {
                //Simply copy all retrieved elements in the following relation
                for (size_t i = 0; i < retrievedBindings->getNRows(); ++i) {
                    task.supplRelations[task.currentRuleIndex + 1]->
                    addTuple(retrievedBindings->getRow(i));
                }
            } else {
                //Not supported yet
                throw 10;
            }
        }

        if (retrievedBindings != NULL) {
            delete retrievedBindings;
        }
    }
    break;
    default:
        throw 10;
    }
}
#endif

RuleExecutor::~RuleExecutor() {
}

char RuleExecutor::cmp(const uint64_t *row1, const uint64_t *row2, const std::pair<uint8_t, uint8_t> *joins, const uint8_t njoins) {
    for (uint8_t i = 0; i < njoins; ++i) {
        Term_t v1 = (Term_t) row1[joins[i].first];
        Term_t v2 = row2[joins[i].second];
        if (v1 < v2) {
            return -1;
        }
        if (v2 < v1) {
            return 1;
        }
    }
    return 0;
}

void RuleExecutor::join(TupleTable * r1, TupleTable * r2,
                        std::pair<uint8_t, uint8_t> *j, uint8_t nj, BindingsTable * output) {
    //Perform a merge join and copy the data into a row to be copied in the output
    size_t indexR1 = 0;
    size_t indexR2 = 0;

    while (true) {
        //Exit condition
        if (indexR1 >= r1->getNRows() || indexR2 >= r2->getNRows()) {
#ifdef DEBUG
	    output->statistics();
#endif
            return;
        }

        //Compare the fields in the two relations.
        char res = RuleExecutor::cmp(r1->getRow(indexR1), r2->getRow(indexR2), j, nj);
        if (res == 0) {
            //Determine the range on both sides.
            size_t startJoin1 = indexR1;
            size_t startJoin2 = indexR2;
            //Find first end
            size_t tmpIndex = startJoin2+1;
            while (tmpIndex < r2->getNRows() &&
                    RuleExecutor::cmp(r1->getRow(indexR1), r2->getRow(tmpIndex), j, nj) == 0) {
                tmpIndex++;
            }
            size_t endJoin2 = tmpIndex;
            //Determine second end
            tmpIndex = startJoin1+1;
            while (tmpIndex < r1->getNRows() &&
                    RuleExecutor::cmp(r1->getRow(tmpIndex), r2->getRow(indexR2), j, nj) == 0) {
                tmpIndex++;
            }
            size_t endJoin1 = tmpIndex;

            for (size_t i = startJoin1; i < endJoin1; ++i) {
                for (size_t m = startJoin2; m < endJoin2; ++m) {
                    output->addTuple(r1->getRow(i), (uint8_t)r1->getSizeRow(), r2->getRow(m), (uint8_t)r2->getSizeRow());
                }
            }
            indexR1 = endJoin1;
            indexR2 = endJoin2;
        } else if (res < 0) {
            //The first relation is smaller than the other. Increase it until we catch up
            indexR1++;
        } else {
            // The opposite case
            indexR2++;
        }
    }
}
