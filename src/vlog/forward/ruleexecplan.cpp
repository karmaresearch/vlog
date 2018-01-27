#include <vlog/ruleexecplan.h>
#include <vlog/ruleexecdetails.h>

#include <kognac/logs.h>

#include <set>

void RuleExecutionPlan::checkIfFilteringHashMapIsPossible(const Literal &head) {
    //2 conditions: the last literal shares the same variables as the head in the same position and has the same constants
    filterLastHashMap = false;
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

    filterLastHashMap = true;
}

RuleExecutionPlan RuleExecutionPlan::reorder(std::vector<uint8_t> &order,
        const std::vector<Literal> &heads) const {
    RuleExecutionPlan newPlan;
    for (int i = 0; i < order.size(); ++i) {
        newPlan.plan.push_back(plan[order[i]]);
        newPlan.ranges.push_back(ranges[order[i]]);
    }
    newPlan.dependenciesExtVars = dependenciesExtVars;

    if (heads.size() == 1) {
        RuleExecutionDetails::checkFilteringStrategy(*newPlan.plan[order.size() - 1],
                heads[0], newPlan);
        newPlan.checkIfFilteringHashMapIsPossible(heads[0]);
    }
    //RuleExecutionDetails::checkWhetherEDBsRedundantHead(newPlan, headLiteral, hv);
    newPlan.calculateJoinsCoordinates(heads);
    return newPlan;
}

void RuleExecutionPlan::calculateJoinsCoordinates(const std::vector<Literal> &heads) {
    std::vector<uint8_t> existingVariables;

    //Get all variables in the head, and dependencies if they are existential
    std::map<uint8_t,std::vector<uint8_t>> variablesNeededForHead;
    uint32_t countVars = 0;
    for (auto &headLiteral : heads) {
        for (uint8_t headPos = 0; headPos < headLiteral.getTupleSize(); ++headPos) {
            const VTerm headTerm = headLiteral.getTermAtPos(headPos);
            if (headTerm.isVariable()) {
                variablesNeededForHead[headTerm.getId()].push_back(countVars + headPos);
            }
        }
        countVars += headLiteral.getTupleSize();
    }

    for (uint8_t i = 0; i < plan.size(); ++i) {
        const Literal *currentLiteral = plan[i];

        std::vector<std::pair<uint8_t, uint8_t>> jc;
        std::vector<std::pair<uint8_t, uint8_t>> pf;
        std::vector<std::pair<uint8_t, uint8_t>> ps;
        std::vector<uint8_t> newExistingVariables;

        //Should I copy all the previous variables
        if (i == plan.size() - 1) {
            //No need to store any new variable. Just copy the old ones in the head
            for (uint8_t m = 0; m < existingVariables.size(); ++m) {
                if (variablesNeededForHead.count(existingVariables[m])) {
                    auto p = variablesNeededForHead.find(existingVariables[m]);
                    for (auto &el : p->second)
                        pf.push_back(make_pair(el, m));
                }
            }
        } else {
            //copy only the ones that will be used later on
            for (uint8_t j = 0; j < existingVariables.size(); ++j) {
                bool isVarNeeded = false;

                //Check in the rest of the body if the variable is mentioned
                for (uint8_t m = i + 1; m < plan.size() && !isVarNeeded; ++m) {
                    std::vector<uint8_t> allVars = plan[m]->getAllVars();
                    for (uint8_t n = 0; n < allVars.size() && !isVarNeeded; ++n) {
                        if (allVars[n] == existingVariables[j]) {
                            isVarNeeded = true;
                        }
                    }
                }

                //Can be used in the head or as dependency for the chase
                if (!isVarNeeded) {
                    isVarNeeded = variablesNeededForHead.count(existingVariables[j]);
                }

                if (isVarNeeded) {
                    // Maps from the new position to the old.
                    pf.push_back(make_pair(newExistingVariables.size(), j));
                    newExistingVariables.push_back(existingVariables[j]);
                }
            }
        }

        //Put in join coordinates between the previous and the current literal
        uint8_t litVars = 0;
        std::set<uint8_t> varSoFar; //This set is used to avoid that repeated
        //variables in the literal produce multiple copies in the head
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

                if (found) {
                    jc.push_back(std::make_pair(j, litVars));
                } else {
                    // Check if we still need this variable. We need it if it occurs
                    // in any of the next literals in the pattern, or if it occurs
                    // in the select clause (the head) or if it is used for slolemization.
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

                    //Check the head or chase-related dependencies
                    if (!isVariableNeeded) {
                        isVariableNeeded = variablesNeededForHead.count(t.getId());
                    }

                    if (isVariableNeeded) {
                        if (i == plan.size() - 1) {
                            // Here, the variable can only be needed if it occurs in the head.
                            // The "ps" map in this case maps from the head position to
                            // the variable number in the pattern.
                            if (!varSoFar.count(t.getId())) {
                                auto p = variablesNeededForHead.find(t.getId());
                                for(auto &el : p->second)
                                    ps.push_back(make_pair(el, litVars));
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
                                ps.push_back(make_pair(newExistingVariables.size(), litVars));
                                newExistingVariables.push_back(t.getId());
                            }
                        }
                    }
                }
                litVars++;
            }
        }

        if (i == plan.size() - 1) {
            //output.sizeOutputRelation.push_back((uint8_t) headLiteral.getTupleSize());
            sizeOutputRelation.push_back(~0);

            //Calculate the positions of the dependencies for the chase
            std::map<uint8_t, std::vector<uint8_t>> extvars2pos;
            for(const auto &pair : dependenciesExtVars) {
                for(auto v : pair.second) {
                    //Search first the existingVariables
                    bool found = false;
                    for(uint8_t j = 0; j < existingVariables.size(); ++j) {
                        if (existingVariables[j] == v) {
                            extvars2pos[pair.first].push_back(j);
                            found = true;
                            break;
                        }
                    }

                    //Then search among the last literal
                    if (!found) {
                        uint8_t litVars = 0;
                        for (uint8_t x = 0; x < currentLiteral->getTupleSize(); ++x) {
                            const VTerm t = currentLiteral->getTermAtPos(x);
                            if (t.isVariable()) {
                                if (t.getId() == v) {
                                    extvars2pos[pair.first].push_back(
                                            existingVariables.size() + litVars);
                                    found = true;
                                    break;
                                }
                                litVars++;
                            }
                        }
                    }
                }
            }
            extvars2posFromSecond = extvars2pos;

        } else {
            existingVariables = newExistingVariables;
            sizeOutputRelation.push_back((uint8_t) existingVariables.size());
        }
        joinCoordinates.push_back(jc);
        posFromFirst.push_back(pf);
        posFromSecond.push_back(ps);
    }

}

bool RuleExecutionPlan::hasCartesian() {
    for (int i = 1; i < joinCoordinates.size(); i++) {
        LOG(DEBUGL) << "joinCoordinates[" << i << "]: size = " << joinCoordinates[i].size();
        if (joinCoordinates[i].size() == 0) {
            return true;
        }
    }
    return false;
}
