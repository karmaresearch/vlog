#include <vlog/ruleexecdetails.h>

void RuleExecutionDetails::rearrangeLiterals(std::vector<const Literal*> &vector, const size_t idx) {
    //First go through all the elements before, to make sure that there is always at least one shared variable.
    std::vector<const Literal*> subset;
    for (int i = idx - 1; i >= 0; --i) {
        subset.push_back(vector[i]);
    }
    const Literal *idxPointer = vector[idx];
    std::vector<uint8_t> startVars = idxPointer->getAllVars();
    std::vector<const Literal*> leftLiterals;

    //Group the left side (the one that has EDB literals)
    groupLiteralsBySharedVariables(startVars, subset, leftLiterals);

    //If there are elements that are not linked, then I add them after
    std::vector<const Literal*> subset2;
    if (leftLiterals.size() > 0) {
        std::copy(leftLiterals.begin(), leftLiterals.end(), std::back_inserter(subset2));
    }
    std::copy(vector.begin() + idx + 1, vector.end(), std::back_inserter(subset2));

    for (std::vector<const Literal*>::iterator itr = subset.begin(); itr != subset.end();
            ++itr) {
        std::vector<uint8_t> newvars = (*itr)->getNewVars(startVars);
        std::copy(newvars.begin(), newvars.end(), std::back_inserter(startVars));
    }
    std::vector<const Literal*> leftLiterals2;
    groupLiteralsBySharedVariables(startVars, subset2, leftLiterals2);

    vector.clear();
    std::copy(subset.begin(), subset.end(), std::back_inserter(vector));
    vector.push_back(idxPointer);
    std::copy(subset2.begin(), subset2.end(), std::back_inserter(vector));

    //assert(leftLiterals2.size() == 0);
    while (leftLiterals2.size() > 0) {
        const Literal *lit = leftLiterals2.back();
        leftLiterals2.pop_back();

        /*if (lit->getNVars() > 0) {
            assert(false); //not supported
        } else {*/
        //just add it
        vector.push_back(lit);
        //}
    }
}

void RuleExecutionDetails::groupLiteralsBySharedVariables(std::vector<uint8_t> &startVars,
        std::vector<const Literal *> &set, std::vector<const Literal*> &leftelements) {
    if (set.size() == 0)
        return;
    if (set.size() == 1) {
        if (set[0]->getSharedVars(startVars).size() == 0) {
            leftelements.push_back(set[0]);
            set.clear();
        }
        std::vector<uint8_t> newvars = set[0]->getNewVars(startVars);
        std::copy(newvars.begin(), newvars.end(), std::back_inserter(startVars));
        return;
    }

    std::vector<uint8_t> varsBestMatching(startVars);
    std::vector<const Literal*> bestMatching;
    std::vector<const Literal*> bestMatchingLeft(set);

    for (std::vector<const Literal*>::iterator itr = set.begin(); itr != set.end();
            ++itr) {
        std::vector<uint8_t> sharedVars = (*itr)->getSharedVars(startVars);
        if (sharedVars.size() > 0 || startVars.size() == 0) {
            //copy the remaining
            std::vector<const Literal*> newSet;
            std::vector<const Literal*> newleftElements;
            for (std::vector<const Literal*>::iterator itr2 = set.begin(); itr2 != set.end();
                    ++itr2) {
                if (*itr2 != *itr) {
                    newSet.push_back(*itr2);
                }
            }

            //copy the new vars
            std::vector<uint8_t> newvars(startVars);
            std::vector<uint8_t> nv = (*itr)->getNewVars(newvars);
            std::copy(nv.begin(), nv.end(), std::back_inserter(newvars));

            groupLiteralsBySharedVariables(newvars, newSet, newleftElements);
            if (newleftElements.size() == 0) {
                startVars.clear();
                set.clear();
                leftelements.clear();
                set.push_back(*itr);
                std::copy(newSet.begin(), newSet.end(), std::back_inserter(set));
                std::copy(newvars.begin(), newvars.end(), std::back_inserter(startVars));
                return;
            } else {
                if (newSet.size() > bestMatching.size()) {
                    varsBestMatching.clear();
                    bestMatching.clear();
                    bestMatchingLeft.clear();
                    std::copy(newvars.begin(), newvars.end(), std::back_inserter(varsBestMatching));
                    std::copy(newSet.begin(), newSet.end(), std::back_inserter(bestMatching));
                    std::copy(newleftElements.begin(), newleftElements.end(), std::back_inserter(bestMatchingLeft));
                }
            }
        }
    }

    //I did not find any good matching. Return best matching
    set.clear();
    leftelements.clear();
    startVars.clear();
    std::copy(bestMatching.begin(), bestMatching.end(), std::back_inserter(set));
    std::copy(bestMatchingLeft.begin(), bestMatchingLeft.end(), std::back_inserter(leftelements));
    std::copy(varsBestMatching.begin(), varsBestMatching.end(), std::back_inserter(startVars));
}

void RuleExecutionDetails::extractAllEDBPatterns(std::vector<const Literal*> &output, const std::vector<Literal> &input) {
    for (std::vector<Literal>::const_iterator itr = input.begin(); itr != input.end();
            ++itr) {
        if (itr->getPredicate().getType() == EDB) {
            output.push_back(&(*itr));
        }
    }
}

void RuleExecutionDetails::checkFilteringStrategy(RuleExecutionPlan &outputPlan, const Literal &literal, const Literal &head) {
    //Two conditions: head and last literals must be compatible, and they must share at least one variable in the same position
    Substitution substitutions[SIZETUPLE];
    int nsubs = Literal::subsumes(substitutions, literal, head);
    if (nsubs != -1) {
        outputPlan.lastLiteralSubsumesHead = true;
        for (uint32_t j = 0; j < nsubs; ++j) {
            if (!substitutions[j].destination.isVariable()) {
                //Find pos of the variable in the literal
                uint8_t idVar = substitutions[j].origin;
                for (uint8_t m = 0; m < literal.getTupleSize(); ++m) {
                    if (literal.getTermAtPos(m).getId() == idVar) {
                        outputPlan.lastLiteralPosConstsInHead.push_back(m);
                        outputPlan.lastLiteralValueConstsInHead.push_back(substitutions[j].destination.getValue());
                    }
                }
            }
        }
    } else {
        outputPlan.lastLiteralSubsumesHead = false;
    }

    std::vector<uint8_t> vars = literal.getAllVars();
    std::vector<uint8_t> sharedVars = head.getSharedVars(vars);
    if (sharedVars.size() > 0) {
        outputPlan.lastLiteralSharesWithHead = true;
    } else {
        outputPlan.lastLiteralSharesWithHead = false;
    }

    outputPlan.lastSorting.clear();
    if (sharedVars.size() > 0) {
        //set the sorting by the position of the sharedVars in the literal
        for (std::vector<uint8_t>::iterator itr = sharedVars.begin(); itr != sharedVars.end();
                ++itr) {
            uint8_t posVar = 0;
            for (uint8_t pos = 0; pos < literal.getTupleSize(); ++pos) {
                VTerm t = literal.getTermAtPos(pos);
                if (t.isVariable()) {
                    if (t.getId() == *itr) {
                        outputPlan.lastSorting.push_back(posVar);
                    }
                    posVar++;
                }
            }
        }
    }
}

void RuleExecutionDetails::calculateNVarsInHeadFromEDB() {
    for (uint8_t i = 0; i < rule.getHead().getTupleSize(); ++i) {
        VTerm t = rule.getHead().getTermAtPos(i);
        if (t.isVariable()) {
            //Check if this variable appears on some edb terms
            std::vector<std::pair<uint8_t, uint8_t>> edbLiterals;
            uint8_t idxLiteral = 0;
            for (std::vector<Literal>::iterator itr = bodyLiterals.begin(); itr != bodyLiterals.end();
                    ++itr) {
                if (itr->getPredicate().getType() == EDB) {
                    for (uint8_t j = 0; j < itr->getTupleSize(); ++j) {
                        VTerm t2 = itr->getTermAtPos(j);
                        if (t2.isVariable() && t.getId() == t2.getId()) {
                            edbLiterals.push_back(std::make_pair(idxLiteral, j));
                            break;
                        }
                    }
                }
                idxLiteral++;
            }

            if (edbLiterals.size() > 0) {
                //add the position and the occurrences
                posEDBVarsInHead.push_back(i);
                occEDBVarsInHead.push_back(edbLiterals);
            }
        }
    }

    //Group the occurrences by EDB literal
    for (uint8_t idxVar = 0; idxVar < posEDBVarsInHead.size(); ++idxVar) {
        uint8_t var = posEDBVarsInHead[idxVar];
        for (uint8_t idxPattern = 0; idxPattern < occEDBVarsInHead[idxVar].size(); ++idxPattern) {
            std::pair<uint8_t, uint8_t> patternAndPos = occEDBVarsInHead[idxVar][idxPattern];
            bool found = false;
            for (uint8_t i = 0; i < edbLiteralPerHeadVars.size() && !found; ++i) {
                std::pair<uint8_t, std::vector<std::pair<uint8_t, uint8_t>>> el = edbLiteralPerHeadVars[i];
                if (el.first == patternAndPos.first) {
                    edbLiteralPerHeadVars[i].second.push_back(std::make_pair(var, patternAndPos.second));
                    found = true;
                }
            }
            if (!found) {
                edbLiteralPerHeadVars.push_back(std::make_pair(patternAndPos.first, std::vector<std::pair<uint8_t, uint8_t>>()));
                edbLiteralPerHeadVars.back().second.push_back(std::make_pair(var, patternAndPos.second));
            }
        }
    }
}

void RuleExecutionDetails::checkWhetherEDBsRedundantHead(RuleExecutionPlan &p, const Literal &head) {
    //Check whether some EDBs can lead to redundant derivation
    for (int i = 0; i < p.plan.size(); ++i) {
        const Literal *literal = p.plan[i];
	// Should this not check if literal is an EDB?
	// Otherwise, I don't understand why this method has the name it has. --Ceriel
	if (literal->getPredicate().getType() != EDB) {
	    continue;
	}

        std::vector<uint8_t> edbVars = literal->getAllVars();
        //Get all IDBs with the same predicate
        for (int j = 0; j < p.plan.size(); ++j) {
            if (i != j) {
                const Literal *body = p.plan[j];
                if (head.getPredicate().getId() == body->getPredicate().getId()) {
                    //Do the two patterns share the same variables at the
                    //same position?
                    bool match = true;
                    std::vector<std::pair<uint8_t, uint8_t>> positions;
                    for (int l = 0; l < head.getTupleSize(); ++l) {
                        VTerm headTerm = head.getTermAtPos(l);
                        VTerm bodyTerm = body->getTermAtPos(l);
                        if (headTerm.isVariable() && bodyTerm.isVariable()) {
                            if (headTerm.getId() != bodyTerm.getId()) {
                                bool found1 = false;
                                bool found2 = false;
                                for (std::vector<uint8_t>::iterator itr = edbVars.begin();
                                        itr != edbVars.end(); ++itr) {
                                    if (*itr == headTerm.getId()) {
                                        found1 = true;
                                    }
                                    if (*itr == bodyTerm.getId()) {
                                        found2 = true;
                                    }
                                }

                                if (found1 && found2) {
                                    //Add in positions the vars in the edb literal

                                    uint8_t pos1 = 255, pos2 = 255;
                                    for (uint8_t x = 0; x < literal->getTupleSize(); ++x) {
                                        if (literal->getTermAtPos(x).isVariable()) {
                                            if (literal->getTermAtPos(x).getId() == headTerm.getId())
                                                pos1 = x;
                                            if (literal->getTermAtPos(x).getId() == bodyTerm.getId())
                                                pos2 = x;
                                        }
                                    }
                                    assert(pos1 != 255 && pos2 != 255);
                                    if (pos1 > pos2) {
                                        positions.push_back(std::make_pair(pos2, pos1));
                                    } else {
                                        positions.push_back(std::make_pair(pos1, pos2));
                                    }
                                } else {
                                    match = false;
                                    break; //Mismatch
                                }
                            }
                        } else if (!headTerm.isVariable() && !bodyTerm.isVariable()) {
                            if (headTerm.getValue() != bodyTerm.getValue()) {
                                match = false;
                                break; //Mismatch
                            }
                        } else {
                            match = false;
                            break;//Mismatch
                        }
                    }

                    if (match && positions.size() > 0) {
                        RuleExecutionPlan::MatchVariables r;
                        r.posLiteralInOrder = (uint8_t) i;
                        r.matches = positions;
                        p.matches.push_back(r);
                        break;
                    }
                }
            }
        }
    }
}

void RuleExecutionDetails::createExecutionPlans() {
    //Init
    std::vector<Literal> bl = rule.getBody();
    bodyLiterals.clear();
    for (std::vector<Literal>::iterator itr = bl.begin(); itr != bl.end(); ++itr) {
        bodyLiterals.push_back(*itr);
    }
    orderExecutions.clear();

    if (nIDBs > 0) {
        //Collect the IDB predicates
        std::vector<uint8_t> posMagicAtoms;
        std::vector<uint8_t> posIdbLiterals;
        uint8_t i = 0;

        for (std::vector<Literal>::const_iterator itr = bodyLiterals.begin();
                itr != bodyLiterals.end();
                ++itr) {
            if (itr->getPredicate().getType() == IDB) {
                posIdbLiterals.push_back(i);
		if (itr->getPredicate().isMagic()) {
		    posMagicAtoms.push_back(i);
		}
            }
            i++;
        }

        int order = 0;
        for (std::vector<uint8_t>::iterator itr = posIdbLiterals.begin();
                itr != posIdbLiterals.end();
                ++itr) {
            RuleExecutionPlan p;
            size_t idx = 0;

	    if (posMagicAtoms.size() > 0) {
		assert(posMagicAtoms.size() == 1);
                p.plan.push_back(&bodyLiterals[posMagicAtoms[0]]);
                extractAllEDBPatterns(p.plan, bodyLiterals);
		if (! bodyLiterals[*itr].isMagic()) {
		    p.plan.push_back(&bodyLiterals[*itr]);
		}
	    } else {
                extractAllEDBPatterns(p.plan, bodyLiterals);
                idx = p.plan.size();
                p.plan.push_back(&bodyLiterals[*itr]);
	    }

            //Add all others
            for (std::vector<uint8_t>::iterator itr2 = posIdbLiterals.begin();
                    itr2 != posIdbLiterals.end();
                    ++itr2) {
                if (*itr2 != *itr && ! bodyLiterals[*itr2].isMagic()) {
                    p.plan.push_back(&bodyLiterals[*itr2]);
                }
            }

            rearrangeLiterals(p.plan, idx);

            RuleExecutionDetails::checkFilteringStrategy(
                p, *p.plan[p.plan.size() - 1], rule.getHead());

            RuleExecutionDetails::checkWhetherEDBsRedundantHead(p,
                    rule.getHead());

            p.checkIfFilteringHashMapIsPossible(rule.getHead());

            p.calculateJoinsCoordinates(rule.getHead());

            //New version. Should be able to catch everything
            for (int i = 0; i < p.plan.size(); ++i) {
                if (p.plan[i]->getPredicate().getType() == EDB) {
                    p.ranges.push_back(std::make_pair(0, (size_t) - 1));
                } else {
                    const Literal *l = p.plan[i];
                    if (l == &bodyLiterals[*itr]) {
                        p.ranges.push_back(std::make_pair(1, (size_t) - 1));
                    } else if (l < &bodyLiterals[*itr]) {
                        p.ranges.push_back(std::make_pair(0, 1));
                    } else {
                        p.ranges.push_back(std::make_pair(0, (size_t) - 1));
                    }
                }
            }

            //Add all the appropriate ranges
            /*if (order == 0) {
                int nIDBs = 0;
                for (int i = 0; i < p.plan.size(); ++i) {
                    if (p.plan[i]->getPredicate().getType() == EDB) {
                        p.ranges.push_back(std::make_pair(0, (size_t) - 1));
                    } else {
                        if (nIDBs == 0) {
                            p.ranges.push_back(std::make_pair(1, (size_t) - 1));
                        } else {
                            p.ranges.push_back(std::make_pair(0, (size_t) - 1));
                        }
                        nIDBs++;
                    }
                }
            } else if (order == 1) {
                int nIDBs = 0;
                for (int i = 0; i < p.plan.size(); ++i) {
                    if (p.plan[i]->getPredicate().getType() == EDB) {
                        p.ranges.push_back(std::make_pair(0, (size_t) - 1));
                    } else {
                        if (nIDBs == 0) {
                            p.ranges.push_back(std::make_pair(1, (size_t) - 1));
                        } else if (nIDBs == 1) {
                            p.ranges.push_back(std::make_pair(0, 1));
                        } else {
                            p.ranges.push_back(std::make_pair(0, (size_t) - 1));
                        }
                        nIDBs++;
                    }
                }
            } else if (order == 2) {
                int nIDBs = 0;
                for (int i = 0; i < p.plan.size(); ++i) {
                    if (p.plan[i]->getPredicate().getType() == EDB) {
                        p.ranges.push_back(std::make_pair(0, (size_t) - 1));
                    } else {
                        if (nIDBs == 0) {
                            p.ranges.push_back(std::make_pair(1, (size_t) - 1));
                        } else if (nIDBs == 1) {
                            p.ranges.push_back(std::make_pair(0, 1));
                        } else {
                            p.ranges.push_back(std::make_pair(0, 1));
                        }
                        nIDBs++;
                    }
                }
            } else {
                //throw 10;
            }*/

            orderExecutions.push_back(p);

            order++;
        }
        assert(orderExecutions.size() == nIDBs);
    } else {
        //Create a single plan. Here they are all EDBs. So the ranges are all the same
        std::vector<const Literal*> v;
        RuleExecutionPlan p;
        for (std::vector<Literal>::const_iterator itr = bodyLiterals.begin(); itr != bodyLiterals.end();
                ++itr) {
            p.plan.push_back(&(*itr));
            p.ranges.push_back(std::make_pair(0, (size_t) - 1));
        }
        RuleExecutionDetails::checkFilteringStrategy(p, bodyLiterals[bodyLiterals.size() - 1], rule.getHead());

        p.calculateJoinsCoordinates(rule.getHead());

        orderExecutions.push_back(p);
    }
}


