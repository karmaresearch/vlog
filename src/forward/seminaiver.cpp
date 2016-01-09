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
#include <vlog/concepts.h>
#include <vlog/joinprocessor.h>
#include <vlog/fctable.h>
#include <vlog/fcinttable.h>
#include <vlog/filterer.h>
#include <trident/model/table.h>

#include <boost/filesystem.hpp>

#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include <sstream>

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

void RuleExecutionPlan::checkIfFilteringHashMapIsPossible(const Literal &head) {
    //2 conditions: the last literal shares the same variables as the head in the same position and has the same constants
    filterLastHashMap = false;
    const Literal *lastLit = plan.back();

    if (head.getPredicate().getId() != lastLit->getPredicate().getId()) {
        return;
    }

    bool differentVar = false;
    for (uint8_t i = 0; i < head.getTupleSize(); ++i) {
        Term th = head.getTermAtPos(i);
        Term tl = lastLit->getTermAtPos(i);
        if (!th.isVariable()) {
            if (tl.isVariable() || tl.getValue() != th.getValue())
                return;
        } else {
            if (!tl.isVariable()) {
                return;
            }
            //All variables except one should be the same
            if (tl.getId() != th.getId()) {
                if (differentVar == false) {
                    differentVar = true;
                } else {
                    return;
                }
            }
        }
    }

    filterLastHashMap = true;
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
                Term t = literal.getTermAtPos(pos);
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
        Term t = rule.getHead().getTermAtPos(i);
        if (t.isVariable()) {
            //Check if this variable appears on some edb terms
            std::vector<std::pair<uint8_t, uint8_t>> edbLiterals;
            uint8_t idxLiteral = 0;
            for (std::vector<Literal>::iterator itr = bodyLiterals.begin(); itr != bodyLiterals.end();
                    ++itr) {
                if (itr->getPredicate().getType() == EDB) {
                    for (uint8_t j = 0; j < itr->getTupleSize(); ++j) {
                        Term t2 = itr->getTermAtPos(j);
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
                        Term headTerm = head.getTermAtPos(l);
                        Term bodyTerm = body->getTermAtPos(l);
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
            }
            i++;
        }

        int order = 0;
        for (std::vector<uint8_t>::iterator itr = posIdbLiterals.begin();
                itr != posIdbLiterals.end();
                ++itr) {
            RuleExecutionPlan p;
            size_t idx = 0;

            if (bodyLiterals[*itr].isMagic()) {
                p.plan.push_back(&bodyLiterals[*itr]);
                extractAllEDBPatterns(p.plan, bodyLiterals);
            } else {
                extractAllEDBPatterns(p.plan, bodyLiterals);
                idx = p.plan.size();
                p.plan.push_back(&bodyLiterals[*itr]);
            }

            //Add all others
            for (std::vector<uint8_t>::iterator itr2 = posIdbLiterals.begin();
                    itr2 != posIdbLiterals.end();
                    ++itr2) {
                if (*itr2 != *itr) {
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

RuleExecutionPlan RuleExecutionPlan::reorder(std::vector<uint8_t> &order, const Literal &headLiteral) {
    RuleExecutionPlan newPlan;
    for (int i = 0; i < order.size(); ++i) {
        newPlan.plan.push_back(plan[order[i]]);
        newPlan.ranges.push_back(ranges[order[i]]);
    }

    RuleExecutionDetails::checkFilteringStrategy(newPlan, *newPlan.plan[order.size() - 1], headLiteral);

    RuleExecutionDetails::checkWhetherEDBsRedundantHead(newPlan, headLiteral);

    newPlan.checkIfFilteringHashMapIsPossible(headLiteral);

    newPlan.calculateJoinsCoordinates(headLiteral);

    return newPlan;
}

void RuleExecutionPlan::calculateJoinsCoordinates(const Literal &headLiteral) {
    std::vector<uint8_t> existingVariables;
    for (uint8_t i = 0; i < plan.size(); ++i) {
        const Literal *currentLiteral = plan[i];

        std::vector<std::pair<uint8_t, uint8_t>> jc;
        std::vector<std::pair<uint8_t, uint8_t>> pf;
        std::vector<std::pair<uint8_t, uint8_t>> ps;
        std::vector<uint8_t> newExistingVariables;

        //Should I copy all the previous variables?
        if (i == plan.size() - 1) {
            //No need to store any new variable. Just copy the old ones in the head
            for (uint8_t headPos = 0; headPos < headLiteral.getTupleSize(); ++headPos) {
                const Term headTerm = headLiteral.getTermAtPos(headPos);
                if (headTerm.isVariable()) {
                    for (uint8_t m = 0; m < existingVariables.size(); ++m) {
                        if (existingVariables[m] == headTerm.getId()) {
                            // BOOST_LOG_TRIVIAL(debug) << "Adding [" << (int) headPos << ", " << (int) m << "] to pf";
                            pf.push_back(make_pair(headPos, m));
                            break;
                        }
                    }
                }
            }
        } else {
            //copy only the ones that will be used later on
            for (uint8_t j = 0; j < existingVariables.size(); ++j) {
                bool isVarNeeded = false;

                for (uint8_t m = i + 1; m < plan.size() && !isVarNeeded; ++m) {
                    std::vector<uint8_t> allVars = plan[m]->getAllVars();
                    for (uint8_t n = 0; n < allVars.size() && !isVarNeeded; ++n) {
                        if (allVars[n] == existingVariables[j]) {
                            isVarNeeded = true;
                        }
                    }
                }

                if (!isVarNeeded) {
                    std::vector<uint8_t> varsHead = headLiteral.getAllVars();
                    for (uint8_t n = 0; n < varsHead.size() && !isVarNeeded; ++n) {
                        if (varsHead[n] == existingVariables[j]) {
                            isVarNeeded = true;
                        }
                    }
                }

                if (isVarNeeded) {
                    // Maps from the new position to the old.
                    // BOOST_LOG_TRIVIAL(debug) << "Adding [" << (int) newExistingVariables.size() << ", " << (int) j << "] to pf";
                    pf.push_back(make_pair(newExistingVariables.size(), j));
                    // BOOST_LOG_TRIVIAL(debug) << "Adding variable " << existingVariables[j];
                    newExistingVariables.push_back(existingVariables[j]);
                }
            }
        }


        //Put in join coordinates between the previous and the current literal
        uint8_t litVars = 0;
        std::set<uint8_t> varSoFar; //This set is used to avoid that repeated
        //variables in the literal produce multiple copies in the head
        for (uint8_t x = 0; x < currentLiteral->getTupleSize(); ++x) {
            const Term t = currentLiteral->getTermAtPos(x);
            if (t.isVariable()) {
                //Is join?
                bool found = false;
                uint8_t j = 0;
                for (; j < existingVariables.size() && !found; ++j) {
                    if (existingVariables[j] == t.getId()) {
                        found = true;
                        break;
                    }
                }

                // BOOST_LOG_TRIVIAL(debug) <<  "Considering variable " << (int) t.getId() << ", litVars = " << (int) litVars;
                if (found) {
                    jc.push_back(std::make_pair(j, litVars));
                } else {
                    // Check if we still need this variable. We need it if it occurs
                    // in any of the next literals in the pattern, or if it occurs
                    // in the select clause (the head).
                    // Note that, since it is not an existing variable, this is the
                    // first occurrence.
                    bool isVariableNeeded = false;
                    //Check next literals
                    for (uint8_t m = i + 1; m < plan.size() && !isVariableNeeded; ++m) {
                        std::vector<uint8_t> allVars = plan[m]->getAllVars();
                        for (uint8_t n = 0; n < allVars.size() && !isVariableNeeded; ++n) {
                            if (allVars[n] == t.getId()) {
                                isVariableNeeded = true;
                            }
                        }

                    }

                    //Check the head
                    if (!isVariableNeeded) {
                        std::vector<uint8_t> varsHead = headLiteral.getAllVars();
                        for (uint8_t n = 0; n < varsHead.size() && !isVariableNeeded; ++n) {
                            if (varsHead[n] == t.getId()) {
                                isVariableNeeded = true;
                            }
                        }
                    }

                    if (isVariableNeeded) {
                        if (i == plan.size() - 1) {
                            // Here, the variable can only be needed if it occurs in the head.
                            // The "ps" map in this case maps from the head position to
                            // the variable number in the pattern.
                            uint8_t headPos = 0;
                            for (; headPos < headLiteral.getTupleSize(); ++headPos) {
                                const Term headTerm = headLiteral.getTermAtPos(headPos);
                                if (headTerm.isVariable() && headTerm.getId() == t.getId()
                                        && !varSoFar.count(t.getId())) {
                                    // BOOST_LOG_TRIVIAL(debug) << "Adding [" << (int) headPos << ", " << (int) litVars << "] to ps";
                                    ps.push_back(make_pair(headPos, litVars));
                                }
                            }
                            varSoFar.insert(t.getId());
                        } else {
                            //Add it to the next list of bindings if it is not already present
                            bool isNew = true;
                            for (std::vector<uint8_t>::iterator itr = newExistingVariables.begin();
                                    itr != newExistingVariables.end() && isNew; ++itr) {
                                if (*itr == t.getId()) {
                                    isNew = false;
                                    break;
                                }
                            }

                            if (isNew) {
                                // BOOST_LOG_TRIVIAL(debug) << "Adding [" << (int) newExistingVariables.size() << ", " << (int) litVars << "] to ps";
                                ps.push_back(make_pair(newExistingVariables.size(), litVars));
                                newExistingVariables.push_back(t.getId());
                                // BOOST_LOG_TRIVIAL(debug) << "Adding variable " << (int) t.getId();
                            }
                        }
                    }
                }
                litVars++;
            }
        }


        if (i == plan.size() - 1) {
            sizeOutputRelation.push_back((uint8_t) headLiteral.getTupleSize());
        } else {
            existingVariables = newExistingVariables;
            sizeOutputRelation.push_back((uint8_t) existingVariables.size());
        }
        joinCoordinates.push_back(jc);
        posFromFirst.push_back(pf);
        posFromSecond.push_back(ps);
        // BOOST_LOG_TRIVIAL(debug) << "Pushing pf, ps";
    }

}

/*void SemiNaiver::calculateJoinsCoordinates(std::vector<uint8_t> &existingBindings,
        const Literal & literal, std::vector<std::pair<uint8_t, uint8_t>> &outputJoins,
        std::vector<uint8_t> &posVarsInLiteralToCopy, std::vector<uint8_t> &varsInLiteralToCopy) {
    uint8_t idxVar = 0;
    for (int i = 0; i < literal.getTupleSize(); ++i) {
        Term t = literal.getTermAtPos(i);
        if (t.isVariable()) {
            //Search in the previous array if we should join
            int j = 0;
            bool found = false;
            for (std::vector<uint8_t>::iterator itr = existingBindings.begin();
                    itr != existingBindings.end(); ++itr) {
                if (*itr == t.getId()) {
                    //Found matching
                    outputJoins.push_back(std::make_pair(j, idxVar));
                    found = true;
                    break;
                }
                j++;
            }

            if (!found) {
                //I should copy it
                posVarsInLiteralToCopy.push_back(idxVar);
                varsInLiteralToCopy.push_back(t.getId());
            }
            idxVar++;
        }
    }
}*/

SemiNaiver::SemiNaiver(std::vector<Rule> ruleset, EDBLayer &layer, DictMgmt *dict,
                       Program *program, bool opt_intersect, bool opt_filtering) : layer(layer), dict(dict), program(program), opt_intersect(opt_intersect), opt_filtering(opt_filtering) {

    TableFilterer::setOptIntersect(opt_intersect);
    memset(predicatesTables, 0, sizeof(TupleTable*)*MAX_NPREDS);

    BOOST_LOG_TRIVIAL(info) << "Running SemiNaiver, opt_intersect = " << opt_intersect << ", opt_filtering = " << opt_filtering;

    for (std::vector<Rule>::iterator itr = ruleset.begin(); itr != ruleset.end();
            ++itr) {
        RuleExecutionDetails d(*itr);
        std::vector<Literal> bodyLiterals = itr->getBody();
        for (std::vector<Literal>::iterator itr = bodyLiterals.begin();
                itr != bodyLiterals.end(); ++itr) {
            if (itr->getPredicate().getType() == IDB)
                d.nIDBs++;
        }
        if (d.nIDBs != 0)
            this->ruleset.push_back(d);
        else
            this->edbRuleset.push_back(d);
    }
}

typedef struct StatIteration {

    size_t iteration;
    const Rule *rule;
    double time;
    bool derived;

    bool operator <(const StatIteration &it) const {
        return time > it.time;
    }


} StatIteration;

void SemiNaiver::run(size_t lastExecution, size_t iteration) {
    listDerivations.clear();

    //Prepare for the execution
#if DEBUG
    boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
    BOOST_LOG_TRIVIAL(debug) << "Optimizing ruleset...";
#endif
    for (std::vector<RuleExecutionDetails>::iterator itr = ruleset.begin(); itr != ruleset.end();
            ++itr) {
        BOOST_LOG_TRIVIAL(debug) << "Optimizing rule " << itr->rule.tostring(NULL, NULL);
        itr->createExecutionPlans();
        itr->calculateNVarsInHeadFromEDB();
        itr->lastExecution = lastExecution;

        for (int i = 0; i < itr->orderExecutions.size(); ++i) {
            string plan = "";
            for (int j = 0; j < itr->orderExecutions[i].plan.size(); ++j) {
                plan += string(" ") + itr->orderExecutions[i].plan[j]->tostring(program, dict);
            }
            BOOST_LOG_TRIVIAL(debug) << "-->" << plan;
        }
    }
    for (std::vector<RuleExecutionDetails>::iterator itr = edbRuleset.begin(); itr != edbRuleset.end();
            ++itr) {
        itr->createExecutionPlans();
    }
#if DEBUG
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(debug) << "Runtime ruleset optimization ms = " << sec.count() * 1000;

    start = boost::chrono::system_clock::now();
#endif
    for (int i = 0; i < edbRuleset.size(); ++i) {
        executeRule(edbRuleset[i], iteration++);
    }
#if DEBUG
    sec = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(debug) << "Runtime EDB rules ms = " << sec.count() * 1000;
#endif
    for (auto el : ruleset)
        BOOST_LOG_TRIVIAL(debug) << el.rule.tostring(program, dict);

    //int lastPosWithDerivation = 0;
    size_t currentRule = 0;
    uint32_t rulesWithoutDerivation = 0;

    //Used for statistics
    std::vector<StatIteration> costRules;
    size_t nRulesOnePass = 0;
    size_t lastIteration = 0;

    if (ruleset.size() > 0) {
        do {
            BOOST_LOG_TRIVIAL(info) << "Iteration " << iteration;
            boost::chrono::system_clock::time_point start = timens::system_clock::now();
            bool response = executeRule(ruleset[currentRule], iteration);
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;

            StatIteration stat;
            stat.iteration = iteration;
            stat.rule = &ruleset[currentRule].rule;
            stat.time = sec.count() * 1000;
            stat.derived = response;
            costRules.push_back(stat);
            ruleset[currentRule].lastExecution = iteration++;

            if (response) {
                if (ruleset[currentRule].rule.isRecursive()) {
                    //Is the rule recursive? Go until saturation...
                    int recursiveIterations = 0;
                    do {
                        BOOST_LOG_TRIVIAL(info) << "Iteration " << iteration;
                        start = timens::system_clock::now();
                        recursiveIterations++;
                        response = executeRule(ruleset[currentRule], iteration);
                        ruleset[currentRule].lastExecution = iteration++;
                        sec = boost::chrono::system_clock::now() - start;
                        ++recursiveIterations;
                        stat.iteration = iteration;
                        stat.rule = &ruleset[currentRule].rule;
                        stat.time = sec.count() * 1000;
                        stat.derived = response;
                        costRules.push_back(stat);
                        /*if (++recursiveIterations % 10 == 0) {
                            BOOST_LOG_TRIVIAL(info) << "Saturating rule " <<
                                                    ruleset[currentRule].rule.tostring(program, dict) <<
                                                    " " << recursiveIterations;
                        }*/
                    } while (response);
                    BOOST_LOG_TRIVIAL(debug) << "Rules " <<
                                             ruleset[currentRule].rule.tostring(program, dict) <<
                                             "  required " << recursiveIterations << " to saturate";
                }

                //lastPosWithDerivation = currentRule;
                rulesWithoutDerivation = 0;
                nRulesOnePass++;
            } else {
                rulesWithoutDerivation++;
            }

            currentRule = (currentRule + 1) % ruleset.size();

#ifdef DEBUG
            //CODE FOR Statistics
            if (currentRule == 0) {
                BOOST_LOG_TRIVIAL(info) << "Finish pass over the rules. Step=" << iteration << ". RulesWithDerivation=" <<
                                        nRulesOnePass << " out of " << ruleset.size() << " Derivations so far " << countAllIDBs();
                nRulesOnePass = 0;

                //Get the top 10 rules in the last iteration
                std::sort(costRules.begin(), costRules.end());
                string out = "";
                int n = 0;
                for (const auto &exec : costRules) {
                    if (exec.iteration >= lastIteration) {
                        //Get the cardinalities of all body literals
                        /*std::vector<size_t> cardinalities;
                        for (auto &literal : exec.rule->getBody()) {
                            FCIterator itr = this->getTable(literal, 0, ~0lu);
                            long count = 0;
                            while (!itr.isEmpty()) {
                                count += itr.getCurrentBlock()->table->getNRows();
                                itr.moveNextCount();
                            }
                            cardinalities.push_back(count);
                        }*/

                        if (n < 10 || exec.derived) {
                            out += "Iteration " + to_string(exec.iteration) + " runtime " + to_string(exec.time);
                            out += " " + exec.rule->tostring(program, dict) + " response " + to_string(exec.derived);
                            /*if (exec.derived) {

                                boost::chrono::system_clock::time_point start = timens::system_clock::now();
                                PredId_t predid = exec.rule->getHead().getPredicate().getId();
                                assert(predicatesTables[predid] != NULL);
                                FCTable *t = predicatesTables[predid];
                                FCIterator itr = t->read(exec.iteration);
                                assert(!itr.isEmpty());
                                std::shared_ptr<const FCInternalTable> table =
                                    itr.getCurrentTable();
                                FCInternalTableItr *itr2 = table->getIterator();
                                out += "\nContent table pred " + to_string(predid) + "\n";
                                int i = 0;
                                char supportText[MAX_TERM_SIZE];
                                while (itr2->hasNext()) {
                                    i++;
                                    itr2->next();
                                    for (int i = 0; i < itr2->getNColumns(); ++i) {
                                        out += to_string(itr2->getCurrentValue(i)) + " ";
                                    }

                                    //Add the text
                                    for (int i = 0; i < itr2->getNColumns(); ++i) {
                                        if (dict->getText(itr2->getCurrentValue(i), supportText)) {
                                            out += string(supportText) + " ";
                                        }
                                    }


                                    out += "\n";
                                    if (i > 100)
                                        break;
                                }
                                table->releaseIterator(itr2);
                                boost::chrono::duration<double> d =
                                    boost::chrono::system_clock::now() - start;
                                out += "\nTime printing results: " + to_string(d.count() * 1000) + " ms.\n";

                            }*/
                            /*for (auto c : cardinalities) {
                                out += to_string(c) + " ";
                            }*/
                            out += "\n";
                        }
                        n++;
                    }
                }
                BOOST_LOG_TRIVIAL(info) << "Rules with the highest cost\n\n" << out;
                lastIteration = iteration;
            }
            //END CODE STATISTICS
#endif
        } while (rulesWithoutDerivation != ruleset.size());
    }
    BOOST_LOG_TRIVIAL(info) << "Finished process. Iterations=" << iteration;

#ifdef DEBUG
    //DEBUGGING CODE -- needed to see which rules cost the most
    //Sort the iteration costs
    std::sort(costRules.begin(), costRules.end());
    int i = 0;
    double sum = 0;
    double sum10 = 0;
    for (auto &el : costRules) {
        BOOST_LOG_TRIVIAL(info) << "Cost iteration " << el.iteration << " " <<
                                el.time;
        i++;
        if (i >= 20)
            break;

        sum += el.time;
        if (i <= 10)
            sum10 += el.time;
    }
    BOOST_LOG_TRIVIAL(info) << "Sum first 20 rules: " << sum
                            << " first 10:" << sum10;
#endif
}

void SemiNaiver::storeOnFiles(std::string path, const bool decompress,
		const int minLevel) {
    //Create a directory if necessary
    boost::filesystem::create_directories(boost::filesystem::path(path));
    char buffer[MAX_TERM_SIZE];

    //I create a new file for every idb predicate
    for (PredId_t i = 0; i < MAX_NPREDS; ++i) {
        FCTable *table = predicatesTables[i];
        if (table != NULL && !table->isEmpty()) {
            FCIterator itr = table->read(minLevel); //1 contains all explicit facts
            if (!itr.isEmpty()) {
                std::ofstream streamout(path + "/" + program->getPredicateName(i));
                const uint8_t sizeRow = table->getSizeRow();
                while (!itr.isEmpty()) {
                    std::shared_ptr<const FCInternalTable> t = itr.getCurrentTable();
                    FCInternalTableItr *iitr = t->getIterator();
                    while (iitr->hasNext()) {
                        iitr->next();
                        std::string row = to_string(iitr->getCurrentIteration()) + "\t";
                        for (uint8_t m = 0; m < sizeRow; ++m) {
                            if (decompress) {
				if (dict->getText(iitr->getCurrentValue(m), buffer)) {
				    row += string(buffer) + "\t";
				} else {
				    std::string t = program->getFromAdditional(iitr->getCurrentValue(m));
				    if (t == std::string("")) {
					t = std::to_string(iitr->getCurrentValue(m));
				    }
				    row += t + "\t";
				}
                            } else {
                                row += to_string(iitr->getCurrentValue(m)) + "\t";
                            }
                        }
                        streamout << row << std::endl;
                    }
                    t->releaseIterator(iitr);
                    itr.moveNextCount();
                }
                streamout.close();
            }
        }
    }
}

bool _sortCards(const std::pair<uint8_t, size_t> &v1, const std::pair<uint8_t, size_t> &v2) {
    return v1.second < v2.second;
}

void SemiNaiver::addDataToIDBRelation(const Predicate pred,
                                      FCBlock block) {
    FCTable *table = NULL;
    BOOST_LOG_TRIVIAL(trace) << "Adding block to " << (int) pred.getId();
    if (predicatesTables[pred.getId()] != NULL) {
        table = predicatesTables[pred.getId()];
    } else {
        table = new FCTable(pred.getCardinality());
        predicatesTables[pred.getId()] = table;
    }
    table->addBlock(block);
}

bool SemiNaiver::executeRule(RuleExecutionDetails &ruleDetails,
                             const uint32_t iteration) {
    //Set up timers
    const boost::chrono::system_clock::time_point startRule = timens::system_clock::now();
    boost::chrono::duration<double> durationJoin(0);
    boost::chrono::duration<double> durationConsolidation(0);
    boost::chrono::duration<double> durationFirstAtom(0);

    Rule rule = ruleDetails.rule;
    Literal headLiteral = rule.getHead();
#if DEBUG
    BOOST_LOG_TRIVIAL(info) << "Rule: " << rule.tostring(program, dict);
#endif

    //Get table to contain the results
    PredId_t idHeadPredicate = headLiteral.getPredicate().getId();
    FCTable *endTable = NULL;
    if (predicatesTables[idHeadPredicate] != NULL) {
        endTable = predicatesTables[idHeadPredicate];
    } else {
        uint8_t card = headLiteral.getPredicate().getCardinality();
        endTable = new FCTable(card);
        predicatesTables[idHeadPredicate] = endTable;
    }

    //In case the rule has many IDBs predicates, I calculate several combinations of countings.
    const std::vector<RuleExecutionPlan> *orderExecutions = &ruleDetails.orderExecutions;

    //Start executing all possible combinations of rules
    int orderExecution = 0;
    int processedTables = 0;

    //If the last iteration the rule failed because an atom was empty, I record this
    //because I might use this info to skip some computation later on
    const bool failEmpty = ruleDetails.failedBecauseEmpty;
    const Literal *atomFail = ruleDetails.atomFailure;
    ruleDetails.failedBecauseEmpty = false;

    BOOST_LOG_TRIVIAL(debug) << "orderExecutions.size() = " << orderExecutions->size();

    for (; orderExecution < orderExecutions->size() &&
            (ruleDetails.lastExecution > 0 || orderExecution == 0); ++orderExecution) {

        //Auxiliary relations to perform the joins
        RuleExecutionPlan plan = orderExecutions->at(orderExecution);
        const uint8_t nBodyLiterals = (uint8_t) plan.plan.size();

        //**** Should I skip the evaluation because some atoms are empty? ***
        bool isOneRelEmpty = false;
        if (ruleDetails.nIDBs > 0) {
            //First I check if there are tuples in each relation. And if there are, then I count how many
            std::vector<size_t> cards;
            //Get the cardinality of all relations
            for (int i = 0; i < nBodyLiterals; ++i) {
                size_t min = plan.ranges[i].first, max = plan.ranges[i].second;
                if (min == 1)
                    min = ruleDetails.lastExecution;
                if (max == 1)
                    max = ruleDetails.lastExecution - 1;

                cards.push_back(estimateCardTable(*plan.plan[i], min, max));
                BOOST_LOG_TRIVIAL(debug) << "Estimation of the atom " <<
                                         plan.plan[i]->tostring(program, dict) <<
                                         " is " << cards.back() << " in the range " <<
                                         min << " " << max;
                if (cards.back() == 0) {
                    isOneRelEmpty = true;
                    break;
                }
            }

            if (!isOneRelEmpty) {
                //Reorder the atoms in terms of cardinality.
                std::vector<std::pair<uint8_t, size_t>> positionCards;
                for (uint8_t i = 0; i < cards.size(); ++i) {
                    BOOST_LOG_TRIVIAL(debug) << "Atom " << (int) i << " has card " << cards[i];
                    positionCards.push_back(std::make_pair(i, cards[i]));
                }
                sort(positionCards.begin(), positionCards.end(), _sortCards);

                //Ensure there are always variables
                std::vector<std::pair<uint8_t, size_t>> adaptedPosCards;
                adaptedPosCards.push_back(positionCards.front());
                std::vector<uint8_t> vars = plan.plan[
                                                positionCards[0].first]
                                            ->getAllVars();
                positionCards.erase(positionCards.begin());

                while (positionCards.size() > 0) {
                    //Look for the smallest pattern which shares a variable
                    bool found = false;
                    for (int i = 0; i < positionCards.size(); ++i) {
                        if (plan.plan[positionCards[i].first]
                                ->getSharedVars(vars).size() != 0) {
                            found = true;
                            adaptedPosCards.push_back(positionCards[i]);
                            positionCards.erase(positionCards.begin() + i);
                            std::vector<uint8_t> newvars = plan.plan[positionCards[i].first]->getAllVars();
                            std::copy(newvars.begin(), newvars.end(), std::back_inserter(vars));
                            break;
                        }
                    }

                    if (!found) {
                        break;
                    }
                }

                //If the order is not the original, then I must reorder it
                bool toReorder = positionCards.size() == 0;
                if (toReorder) {
                    int idx = 0;
                    toReorder = false;
                    for (auto el : adaptedPosCards) {
                        if (el.first != idx) {
                            toReorder = true;
                            break;
                        }
                        idx++;
                    }
                }
                if (toReorder) {
                    std::vector<uint8_t> orderLiterals;
                    for (int i = 0; i < adaptedPosCards.size(); ++i) {
                        BOOST_LOG_TRIVIAL(debug) << "Reordered plan is " << (int)adaptedPosCards[i].first;
                        orderLiterals.push_back(adaptedPosCards[i].first);
                    }
                    plan = plan.reorder(orderLiterals, headLiteral);
                }
            }
        }
        if (isOneRelEmpty) {
            BOOST_LOG_TRIVIAL(debug) << "Aborting this combination";
            continue;
        }

#ifdef DEBUG
        std::string listLiterals = "EXEC COMB: ";
        for (std::vector<const Literal*>::iterator itr = plan.plan.begin(); itr != plan.plan.end();
                ++itr) {
            listLiterals += (*itr)->tostring(program, dict);
        }
        BOOST_LOG_TRIVIAL(debug) << listLiterals;
#endif

        /*******************************************************************/

        std::shared_ptr<const FCInternalTable> currentResults;
        int optimalOrderIdx = 0;

        while (optimalOrderIdx < nBodyLiterals) {
            const Literal *bodyLiteral = plan.plan[optimalOrderIdx];

            //This data structure is used to filter out rows where different columns
            //lead to the same derivation
            std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars = NULL;
            if (plan.matches.size() > 0) {
                for (int i = 0; i < plan.matches.size(); ++i) {
                    if (plan.matches[i].posLiteralInOrder == optimalOrderIdx) {
                        filterValueVars = &plan.matches[i].matches;
                    }
                }
            }

            //BEGIN -- Determine where to put the results of the query
            ResultJoinProcessor *joinOutput = NULL;
            const bool lastLiteral = optimalOrderIdx == (nBodyLiterals - 1);
            if (!lastLiteral) {
                joinOutput = new InterTableJoinProcessor(plan.sizeOutputRelation[optimalOrderIdx],
                        plan.posFromFirst[optimalOrderIdx], plan.posFromSecond[optimalOrderIdx]);
            } else {
                joinOutput = new FinalTableJoinProcessor(plan.posFromFirst[optimalOrderIdx],
                        plan.posFromSecond[optimalOrderIdx], listDerivations, endTable,
                        headLiteral, &ruleDetails, (uint8_t) orderExecution, iteration);
            }
            //END --  Determine where to put the results of the query

            //Calculate range for the retrieval of the triples
            size_t min = plan.ranges[optimalOrderIdx].first;
            size_t max = plan.ranges[optimalOrderIdx].second;
            if (min == 1)
                min = ruleDetails.lastExecution;
            if (max == 1)
                max = ruleDetails.lastExecution - 1;
            BOOST_LOG_TRIVIAL(debug) << "Evaluating atom " << optimalOrderIdx << " min=" << min << " max=" << max;

            if (optimalOrderIdx == 0) {
                boost::chrono::system_clock::time_point start = timens::system_clock::now();
                //If the rule has only one body literal, has the same bindings list of the head,
                //and the current head relation is empty, then I can simply copy the table
                FCIterator literalItr = getTable(*bodyLiteral, min, max);
                TableFilterer queryFilterer(this);
                //,
                //*bodyLiteral);

                if (bodyLiteral->getPredicate().getType() == IDB) {
                    processedTables += literalItr.getNTables();
                }

                if (lastLiteral && endTable->isEmpty() &&
                        literalItr.getNTables() == 1 &&
                        headLiteral.sameVarSequenceAs(*bodyLiteral) &&
                        bodyLiteral->getTupleSize() == headLiteral.getTupleSize()) {
                    while (!literalItr.isEmpty()) {
                        std::shared_ptr<const FCInternalTable> table =
                            literalItr.getCurrentTable();


                        if (/*!queryFilterer.
                                producedDerivationInFollowingSteps(headLiteral,
                                        literalItr.getCurrentIteration()) &&*/
                            !queryFilterer.
                            producedDerivationInPreviousSteps(
                                headLiteral, *bodyLiteral,
                                literalItr.getCurrentBlock())) {

                            endTable->add(table->cloneWithIteration(iteration),
                                          headLiteral, &ruleDetails,
                                          (uint8_t) orderExecution, iteration, true);

                        }

                        literalItr.moveNextCount();
                    }
                } else if (nBodyLiterals == 1) {
                    const bool uniqueResults = headLiteral.getNUniqueVars()
                                               == bodyLiteral->getNUniqueVars()
                                               && literalItr.getNTables() == 1;
                    while (!literalItr.isEmpty()) {
                        //Add the columns to the output container
                        std::shared_ptr<const FCInternalTable> table =
                            literalItr.getCurrentTable();
                        FCInternalTableItr *interitr = table->getIterator();

                        if (!lastLiteral ||
                                (/*!queryFilterer.
                                 producedDerivationInFollowingSteps(headLiteral,
                                         literalItr.getCurrentIteration()) &&*/
                                    !queryFilterer.
                                    producedDerivationInPreviousSteps(
                                        headLiteral,
                                        *bodyLiteral,
                                        literalItr.getCurrentBlock())) ) {

                            joinOutput->addColumns(0, interitr, endTable->isEmpty()
                                                   && uniqueResults,
                                                   uniqueResults && headLiteral.
                                                   sameVarSequenceAs(*bodyLiteral),
                                                   literalItr.getNTables() == 1);
                        }

                        table->releaseIterator(interitr);
                        literalItr.moveNextCount();
                    }
                } else {
                    //Copy the iterator in the tmp container.
                    //This process cannot derive duplicates if the number of variables is equivalent.
                    const bool uniqueResults = headLiteral.getNUniqueVars() == bodyLiteral->getNUniqueVars();
                    while (!literalItr.isEmpty()) {
                        std::shared_ptr<const FCInternalTable> table = literalItr.getCurrentTable();
                        BOOST_LOG_TRIVIAL(debug) << "Creating iterator";
                        FCInternalTableItr *interitr = table->getIterator();
                        BOOST_LOG_TRIVIAL(debug) << "Created iterator";
                        while (interitr->hasNext()) {
                            interitr->next();
                            if (filterValueVars != NULL) {
                                assert(filterValueVars->size() == 1);
                                const std::pair<uint8_t, uint8_t> psColumnsToFilter =
                                    removePosConstants(filterValueVars->at(0),
                                                       *bodyLiteral);
                                //otherwise I miss others
                                if (interitr->getCurrentValue(
                                            psColumnsToFilter.first) ==
                                        interitr->getCurrentValue(
                                            psColumnsToFilter.second)) {
                                    continue;
                                }
                            }

                            if (lastLiteral) {
                                joinOutput->processResults(0,
                                                           (FCInternalTableItr*)NULL,
                                                           interitr, endTable->isEmpty()
                                                           && uniqueResults);
                            } else {
                                joinOutput->processResults(0,
                                                           (FCInternalTableItr*)NULL,
                                                           interitr, uniqueResults);
                            }
                        }
                        BOOST_LOG_TRIVIAL(debug) << "Releasing iterator";
                        table->releaseIterator(interitr);
                        literalItr.moveNextCount();
                    }
                }
                durationFirstAtom += boost::chrono::system_clock::now() - start;
            } else {
                //Perform the join
                boost::chrono::system_clock::time_point start = timens::system_clock::now();
                JoinExecutor::join(this, currentResults.get(), lastLiteral ? &headLiteral : NULL,
                                   *bodyLiteral, min, max, filterValueVars,
                                   plan.joinCoordinates[optimalOrderIdx], joinOutput,
                                   lastLiteral, ruleDetails, plan, processedTables,
                                   optimalOrderIdx);
                boost::chrono::duration<double> d =
                    boost::chrono::system_clock::now() - start;
                durationJoin += d;
            }

            //BEGIN -- delete intermediate data structures
            boost::chrono::system_clock::time_point startC = timens::system_clock::now();

            joinOutput->consolidate(true);
            boost::chrono::duration<double> d =
                boost::chrono::system_clock::now() - startC;
            durationConsolidation += d;


            if (!lastLiteral) {
                currentResults = ((InterTableJoinProcessor*)joinOutput)->getTable();
            } else {
#ifdef DEBUG
                /*                if (endTable->getNRows(iteration) > 0) {
                                    //Print them
                                    FCIterator itr = endTable->read(iteration);
                                    while (!itr.isEmpty()) {
                                        std::shared_ptr<const FCInternalTable> t = itr.getCurrentTable();
                                        FCInternalTableItr *itrTable = t->getIterator();
                                        while (itrTable->hasNext()) {
                                            itrTable->next();
                                            for (int i = 0; i < itrTable->getNColumns(); ++i) {
                                                cerr << itrTable->getCurrentValue(i) << " ";
                                            }
                                            cerr << endl;
                                        }
                                        t->releaseIterator(itrTable);
                                        itr.moveNextCount();
                                    }
                                }
                */
#endif

            }
            delete joinOutput;
            optimalOrderIdx++;

            if (lastLiteral && !endTable->isEmpty()) {
                FCBlock block = endTable->getLastBlock();
                if (block.iteration == iteration && (listDerivations.size() == 0 ||
                                                     listDerivations.back().iteration != iteration)) {
                    listDerivations.push_back(block);
                }
            }

            if (! lastLiteral && (currentResults == NULL || currentResults->isEmpty())) {
                BOOST_LOG_TRIVIAL(debug) << "The evaluation of atom " << (optimalOrderIdx - 1) << " returned no result";

                //If the range was 0 to MAX_INT, then also other combinations
                //will never fire anything
                if ((min == 0 || (failEmpty && atomFail == bodyLiteral))
                        && max == (size_t) - 1) {
                    orderExecution = orderExecutions->size();
                    ruleDetails.failedBecauseEmpty = true;
                    ruleDetails.atomFailure = bodyLiteral;
                }
                break;
            }
        }
    }

    boost::chrono::duration<double> totalDuration =
        boost::chrono::system_clock::now() - startRule;
    double td = totalDuration.count() * 1000;
    std::stringstream stream;
    std::string sTd = "";
    if (td > 1000) {
        td = td / 1000;
        stream << td << "sec";
    } else {
        stream << td << "ms";
    }

#ifdef DEBUG
    if (!endTable->isEmpty(iteration)) {
        BOOST_LOG_TRIVIAL(info) << "Iteration " << iteration << ". Rule derived new tuples. Combinations " << orderExecution << ", Processed IDB Tables=" <<
                                processedTables << ", Total runtime " << stream.str()
                                << ", join " << durationJoin.count() * 1000 << "ms, consolidation " <<
                                durationConsolidation.count() * 1000 << "ms, retrieving first atom " << durationFirstAtom.count() * 1000 << "ms.";
    } else {
        BOOST_LOG_TRIVIAL(info) << "Iteration " << iteration << ". Rule derived NO new tuples. Combinations " << orderExecution << ", Processed IDB Tables=" <<
                                processedTables << ", Total runtime " << stream.str()
                                << ", join " << durationJoin.count() * 1000 << "ms, consolidation " <<
                                durationConsolidation.count() * 1000 << "ms, retrieving first atom " << durationFirstAtom.count() * 1000 << "ms.";
    }
#endif

    return !endTable->isEmpty(iteration);
}

size_t SemiNaiver::estimateCardTable(const Literal &literal, const size_t minIteration,
                                       const size_t maxIteration) {
    PredId_t id = literal.getPredicate().getId();
    FCTable *table = predicatesTables[id];
    if (table == NULL || table->isEmpty() ||
            table->getMaxIteration() < minIteration ||
            table->getMinIteration() > maxIteration) {
        if (table == NULL && literal.getPredicate().getType() == EDB) {
            //It might be because the table is not loaded. Try to load it and repeat the process
            FCIterator itr = getTable(literal, minIteration, maxIteration);
            if (itr.isEmpty()) {
                return 0;
            } else {
                return estimateCardTable(literal, minIteration, maxIteration);
            }
        } else {
            return 0;
        }
    } else {
        return table->estimateCardinality(literal, minIteration, maxIteration);
        // Was: return table->estimateCardInRange(minIteration, maxIteration);
    }
}

FCIterator SemiNaiver::getTableFromIDBLayer(const Literal &literal, const size_t minIteration, TableFilterer *filter) {

    PredId_t id = literal.getPredicate().getId();
    BOOST_LOG_TRIVIAL(debug) << "SemiNaiver::getTableFromIDBLayer: id = " << (int) id
                             << ", minIter = " << minIteration << ", literal=" << literal.tostring(NULL, NULL);
    FCTable *table = predicatesTables[id];
    if (table == NULL || table->isEmpty() || table->getMaxIteration() < minIteration) {
        BOOST_LOG_TRIVIAL(trace) << "Return empty iterator";
        return FCIterator();
    } else {
        return table->filter(literal, minIteration, filter)->read(minIteration);
    }
}

FCIterator SemiNaiver::getTableFromIDBLayer(const Literal &literal, const size_t minIteration,
        const size_t maxIteration, TableFilterer *filter) {
    PredId_t id = literal.getPredicate().getId();
    BOOST_LOG_TRIVIAL(debug) << "SemiNaiver::getTableFromIDBLayer: id = " << (int) id
                             << ", minIter = " << minIteration << ", maxIteration = " << maxIteration << ", literal=" << literal.tostring(NULL, NULL);
    FCTable *table = predicatesTables[id];
    if (table == NULL || table->isEmpty() || table->getMaxIteration() < minIteration) {
        BOOST_LOG_TRIVIAL(trace) << "Return empty iterator";
        return FCIterator();
    } else {
        if (literal.getNUniqueVars() < literal.getTupleSize()) {
            return table->filter(literal, minIteration, filter)->read(minIteration, maxIteration);
        } else {
            return table->read(minIteration, maxIteration);
        }
    }
}

size_t SemiNaiver::estimateCardinality(const Literal &literal, const size_t minIteration,
        const size_t maxIteration) {
    FCTable *table = predicatesTables[literal.getPredicate().getId()];
    if (table == NULL) {
        return 0;
    } else {
        return table->estimateCardinality(literal, minIteration, maxIteration);
    }
}

FCIterator SemiNaiver::getTableFromEDBLayer(const Literal & literal) {
    FCTable *table = predicatesTables[literal.getPredicate().getId()];
    if (table == NULL) {
        table = new FCTable((uint8_t) literal.getTupleSize());
        predicatesTables[literal.getPredicate().getId()] = table;

        Tuple t = literal.getTuple();
        //Add all different variables
        for (uint8_t i = 0; i < t.getSize(); ++i) {
            t.set(Term(i + 1, 0), i);
        }
        Literal mostGenericLiteral(literal.getPredicate(), t);

        std::shared_ptr<FCInternalTable> ptrTable(new EDBFCInternalTable(0,
                mostGenericLiteral, &layer));
        table->add(ptrTable, mostGenericLiteral, NULL, 0, 0, true);
    }
    if (literal.getNUniqueVars() < literal.getTupleSize()) {
        return table->filter(literal)->read(0);
    } else {
        return table->read(0);
    }
}

FCIterator SemiNaiver::getTable(const Literal & literal,
                                const size_t min, const size_t max, TableFilterer *filter) {
    //BEGIN -- Get the table that correspond to the current literal
    //boost::chrono::system_clock::time_point start = timens::system_clock::now();
    if (literal.getPredicate().getType() == EDB) {
        return getTableFromEDBLayer(literal);
    } else {
        /*if (currentIDBpred == 0) {
            literalItr = getTableFromIDBLayer(literal, ruleDetails.lastExecution);
        } else {
            if (orderExecution == 0) {
                literalItr = getTableFromIDBLayer(literal, 0);
            } else {
                literalItr = getTableFromIDBLayer(literal, 0,
                                                  ruleDetails.lastExecution);
            }
        }
        currentIDBpred++;*/
        return getTableFromIDBLayer(literal, min, max, filter);
    }
    //boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    //BOOST_LOG_TRIVIAL(debug) << "Runtime retrieving literal ms = " << sec.count() * 1000;
    //END -- Get the table that correspond to the literal

}

SemiNaiver::~SemiNaiver() {
    for (int i = 0; i < MAX_NPREDS; ++i) {
        if (predicatesTables[i] != NULL)
            delete predicatesTables[i];
    }

    /*for (EDBCache::iterator itr = edbCache.begin(); itr != edbCache.end(); ++itr) {
        delete itr->second;
    }*/
}

size_t SemiNaiver::countAllIDBs() {
    long c = 0;
    for (PredId_t i = 0; i < MAX_NPREDS; ++i) {
        if (predicatesTables[i] != NULL) {
            if (program->isPredicateIDB(i)) {
                long count = predicatesTables[i]->getNAllRows();
                c += count;
            }
        }
    }
    return c;
}

void SemiNaiver::printCountAllIDBs() {
    long c = 0;
    long emptyRel = 0;
    for (PredId_t i = 0; i < MAX_NPREDS; ++i) {
        if (predicatesTables[i] != NULL) {
            if (program->isPredicateIDB(i)) {
                long count = predicatesTables[i]->getNAllRows();
                if (count > 0) {
                    string predname = program->getPredicateName(i);
                    BOOST_LOG_TRIVIAL(debug) << "Cardinality of " <<
                                             predname << ": " << count;
                } else {
                    emptyRel++;
                }
                c += count;
            }
        }
    }
    BOOST_LOG_TRIVIAL(debug) << "Predicates without derivation: " << emptyRel;
    BOOST_LOG_TRIVIAL(info) << "Total # derivations: " << c;
}

std::pair<uint8_t, uint8_t> SemiNaiver::removePosConstants(
    std::pair<uint8_t, uint8_t> columns,
    const Literal &literal) {

    std::pair<uint8_t, uint8_t> newcols;
    //Fix first pos
    newcols.first = columns.first;
    for (int i = 0; i < columns.first; ++i) {
        if (!literal.getTermAtPos(i).isVariable())
            newcols.first--;
    }
    newcols.second = columns.second;
    for (int i = 0; i < columns.second; ++i) {
        if (!literal.getTermAtPos(i).isVariable())
            newcols.second--;
    }
    return newcols;
}
