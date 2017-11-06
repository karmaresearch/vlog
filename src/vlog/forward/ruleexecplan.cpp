#include <vlog/ruleexecplan.h>
#include <vlog/ruleexecdetails.h>

#include <kognac/logs.h>

#include <set>

void RuleExecutionPlan::checkIfFilteringHashMapIsPossible(const Literal &head,
        HeadVars &output) {
    //2 conditions: the last literal shares the same variables as the head in the same position and has the same constants
    output.filterLastHashMap = false;
    const Literal *lastLit = plan.back();

    if (head.getPredicate().getId() != lastLit->getPredicate().getId()) {
        return;
    }

    bool differentVar = false;
    for (uint8_t i = 0; i < head.getTupleSize(); ++i) {
        VTerm th = head.getTermAtPos(i);
        VTerm tl = lastLit->getTermAtPos(i);
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

    output.filterLastHashMap = true;
}

RuleExecutionPlan RuleExecutionPlan::reorder(std::vector<uint8_t> &order,
        const Literal &headLiteral, int posHead) const {
    RuleExecutionPlan newPlan;
    for (int i = 0; i < order.size(); ++i) {
        newPlan.plan.push_back(plan[order[i]]);
        newPlan.ranges.push_back(ranges[order[i]]);
    }

    HeadVars hv;
    RuleExecutionDetails::checkFilteringStrategy(*newPlan.plan[order.size() - 1],
            headLiteral, hv);
    RuleExecutionDetails::checkWhetherEDBsRedundantHead(newPlan, headLiteral, hv);
    newPlan.checkIfFilteringHashMapIsPossible(headLiteral, hv);
    newPlan.calculateJoinsCoordinates(headLiteral, hv);
    newPlan.infoHeads = infoHeads;
    newPlan.infoHeads[posHead] = hv;

    return newPlan;
}

void RuleExecutionPlan::calculateJoinsCoordinates(const Literal &headLiteral,
        HeadVars &output) {
    std::vector<uint8_t> existingVariables;
    for (uint8_t i = 0; i < plan.size(); ++i) {
        const Literal *currentLiteral = plan[i];

        std::vector<std::pair<uint8_t, uint8_t>> jc;
        std::vector<std::pair<uint8_t, uint8_t>> pf;
        std::vector<std::pair<uint8_t, uint8_t>> ps;
        std::vector<uint8_t> newExistingVariables;

        //Should I copy all the previous variables
        if (i == plan.size() - 1) {
            std::map<uint8_t, std::vector<uint8_t>> extvars2pos;

            //No need to store any new variable. Just copy the old ones in the head
            for (uint8_t headPos = 0; headPos < headLiteral.getTupleSize(); ++headPos) {
                const VTerm headTerm = headLiteral.getTermAtPos(headPos);
                if (headTerm.isVariable()) {
                    bool found = false;
                    for (uint8_t m = 0; m < existingVariables.size(); ++m) {
                        if (existingVariables[m] == headTerm.getId()) {
                            found = true;
                            pf.push_back(make_pair(headPos, m));
                            break;
                        }
                    }
                    if (!found) {
                        //The variable is existential
                        if (!dependenciesExtVars.count(headTerm.getId())) {
                            LOG(ERRORL) << "An existential variable is not found! Should not happen...";
                            throw 10;
                        } else {
                            std::vector<uint8_t> posOfDependenciesInBody;
                            for(auto var : dependenciesExtVars[headTerm.getId()]) {
                                for(uint8_t i = 0; i < existingVariables.size(); ++i) {
                                    if (existingVariables[i] == var) {
                                        posOfDependenciesInBody.push_back(i);
                                    }
                                }
                            }
                            extvars2pos[headTerm.getId()] = posOfDependenciesInBody;
                        }
                    }
                }
            }
            output.extvars2posFromSecond = extvars2pos;
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
                    // LOG(DEBUGL) << "Adding [" << (int) newExistingVariables.size() << ", " << (int) j << "] to pf";
                    pf.push_back(make_pair(newExistingVariables.size(), j));
                    // LOG(DEBUGL) << "Adding variable " << (int) existingVariables[j];
                    newExistingVariables.push_back(existingVariables[j]);
                }
            }
        }

        //Put in join coordinates between the previous and the current literal
        uint8_t litVars = 0;
        std::set<uint8_t> varSoFar; //This set is used to avoid that repeated
        //variables in the literal produce multiple copies in the head
        // LOG(DEBUGL) << "currentLiteral = " << currentLiteral->tostring(NULL, NULL);
        for (uint8_t x = 0; x < currentLiteral->getTupleSize(); ++x) {
            const VTerm t = currentLiteral->getTermAtPos(x);
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

                // LOG(DEBUGL) <<  "Considering variable " << (int) t.getId() << ", litVars = " << (int) litVars << ", found = " << found;
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
                                const VTerm headTerm = headLiteral.getTermAtPos(headPos);
                                if (headTerm.isVariable() && headTerm.getId() == t.getId()
                                        && !varSoFar.count(t.getId())) {
                                    // LOG(DEBUGL) << "Adding [" << (int) headPos << ", " << (int) litVars << "] to ps";
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
                                // LOG(DEBUGL) << "Adding [" << (int) newExistingVariables.size() << ", " << (int) litVars << "] to ps";
                                ps.push_back(make_pair(newExistingVariables.size(), litVars));
                                newExistingVariables.push_back(t.getId());
                                // LOG(DEBUGL) << "Adding new variable " << (int) t.getId();
                            }
                        }
                    }
                }
                litVars++;
            }
        }

        if (i == plan.size() - 1) {
            output.sizeOutputRelation.push_back((uint8_t) headLiteral.getTupleSize());
        } else {
            existingVariables = newExistingVariables;
            output.sizeOutputRelation.push_back((uint8_t) existingVariables.size());
        }
        output.joinCoordinates.push_back(jc);
        output.posFromFirst.push_back(pf);
        output.posFromSecond.push_back(ps);
        // LOG(DEBUGL) << "Pushing pf, ps";
    }

}

bool RuleExecutionPlan::hasCartesian(HeadVars &hv) {
    for (int i = 1; i < hv.joinCoordinates.size(); i++) {
        LOG(DEBUGL) << "joinCoordinates[" << i << "]: size = " << hv.joinCoordinates[i].size();
        if (hv.joinCoordinates[i].size() == 0) {
            return true;
        }
    }
    return false;
}
