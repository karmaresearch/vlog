#include <vlog/ruleexecplan.h>
#include <vlog/ruleexecdetails.h>

#include <kognac/logs.h>

#include <set>
#include <algorithm>

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
        const std::vector<Literal> &heads, bool copyAllVars) const {
    RuleExecutionPlan newPlan;
    newPlan.lastLiteralSharesWithHead = false;
    newPlan.filterLastHashMap = false;
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
    newPlan.calculateJoinsCoordinates(heads, copyAllVars);
    return newPlan;
}

void RuleExecutionPlan::calculateJoinsCoordinates(const std::vector<Literal> &heads,
        bool copyAllVars) {
    std::vector<uint8_t> existingVariables;

    //Get all variables in the head, and dependencies if they are existential
    //{head_variable_id:[idx_in_term_list_in_head]}
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
        /*if (copyAllVars) {
        //Update pf
        newExistingVariables = existingVariables;
        int idx = 0;
        for(const auto &p : existingVariables) {
        pf.push_back(std::make_pair(idx, idx));
        idx += 1;
        }
        } else */
        if (i == (plan.size() - 1)) {
            //No need to store any new variable. Just copy the old ones in the head
            //if the variables in "existingVariables" are needed for the head, then
            //save in "pf" [_idx_in_term_list_in_head_,_idx_in_existingVariables_]
            for (uint8_t m = 0; m < existingVariables.size(); ++m) {
                if (variablesNeededForHead.count(existingVariables[m])) {
                    auto p = variablesNeededForHead.find(existingVariables[m]);
                    for (auto &el : p->second) {
                        pf.push_back(std::make_pair(el, m));
                    }
                }
            }
        } else {
            //copy only the ones that will be used later on
            for (uint8_t j = 0; j < existingVariables.size(); ++j) {
                // first check if the variable existingVariables[j] is needed in the future
                bool isVarNeeded = false;
                //Check in the rest of the body if the variable is mentioned
                for (uint8_t m = i + 1; m < plan.size(); ++m) {
                    if (plan[m]->containsVariable(existingVariables[j])){
                        isVarNeeded = true;
                        break;
                    }
                }
                //Can be used in the head or as dependency for the chase
                isVarNeeded = isVarNeeded || variablesNeededForHead.count(existingVariables[j]);

                if (isVarNeeded || copyAllVars) {
                    // Maps from the new position to the old.
                    // add to pf the pair [idx_in_newExistingVariables, idx_in_existingVariables]
                    pf.push_back(std::make_pair(newExistingVariables.size(), j));
                    // add to newExistingVariables the existingVariable[j]
                    newExistingVariables.push_back(existingVariables[j]);
                }
            }
        }

        //Record whether the value of a variable should be retrieved from previous atoms
        std::vector<std::pair<uint8_t,uint8_t>> v2p;
        //Put in join coordinates between the previous and the current literal
        uint8_t litVars = 0;
        std::set<uint8_t> varSoFar; //This set is used to avoid that repeated
        //variables in the literal produce multiple copies in the head
        for (uint8_t x = 0; x < currentLiteral->getTupleSize(); ++x) {
            const VTerm t = currentLiteral->getTermAtPos(x);

            if (t.isVariable()) {
                //Is it join?
                auto itr = std::find(existingVariables.begin(),existingVariables.end(),t.getId());
                if (itr != existingVariables.end()) { //found
                    uint8_t position = itr - existingVariables.begin();
                    //Maybe I join with a repeated variable. In this case, I don't add it
                    bool repeatedFound = false;
                    for(auto &c : jc) {
                        if (c.first == position) {
                            repeatedFound = true;
                            break;
                        }
                    }
                    if (!repeatedFound) {
                        jc.push_back(std::make_pair(position, litVars));
                        v2p.push_back(std::make_pair(x, position));
                    }
                } else {
                    // Check if we still need this variable. We need it if it occurs
                    // in any of the next literals in the pattern, or if it occurs
                    // in the select clause (the head) or if it is used for slolemization.
                    // Note that, since it is not an existing variable, this is the
                    // first occurrence.
                    bool isVariableNeeded = false;
                    //Check next literals
                    for (uint8_t m = i + 1; m < plan.size(); ++m) {
                        if (plan[m]->containsVariable(t.getId())){
                            isVariableNeeded = true;
                            break;
                        }
                    }

                    //Check the head or chase-related dependencies
                    isVariableNeeded = isVariableNeeded || variablesNeededForHead.count(t.getId());

                    if (i == (plan.size() - 1)) {
                        if (isVariableNeeded) {
                            // Here, the variable can only be needed if it occurs in the head.
                            // The "ps" map in this case maps from the head position to
                            // the variable number in the pattern.
                            if (!varSoFar.count(t.getId())) {
                                auto p = variablesNeededForHead.find(t.getId());
                                for(auto &el : p->second) {
                                    ps.push_back(std::make_pair(el, litVars));
                                }
                                varSoFar.insert(t.getId());
                            }
                        }
                        v2p.push_back(std::make_pair(x, newExistingVariables.size() + litVars));
                    } else if (isVariableNeeded || copyAllVars) {
                        //Add it to the next list of bindings if it is not already present
                        std::vector<uint8_t>::iterator iter2 = std::find(newExistingVariables.begin(), newExistingVariables.end(), t.getId());
                        if (iter2 != newExistingVariables.end()){ //found
                            v2p.push_back(std::make_pair(x, (uint8_t)(iter2 - newExistingVariables.begin())));
                        } else {
                            ps.push_back(std::make_pair(newExistingVariables.size(), litVars));
                            v2p.push_back(std::make_pair(x, newExistingVariables.size()));
                            newExistingVariables.push_back(t.getId());
                            LOG(TRACEL) << "New variable: " << (int) t.getId();
                        }
                    }
                }
                litVars++; //counter of variables
            }
        }
        vars2pos.push_back(v2p);

        if (i == plan.size() - 1) {
            //TODO this code can be simplified.
            //output.sizeOutputRelation.push_back((uint8_t) headLiteral.getTupleSize());
            sizeOutputRelation.push_back(~0);

            //Calculate the positions of the dependencies for the chase
            std::map<uint8_t, std::vector<uint8_t>> extvars2pos;
            for(const auto &pair : dependenciesExtVars) {
                for(const auto& v : pair.second) {
                    //Search first the existingVariables
                    bool found = false;
                    LOG(TRACEL) << "v = " << (int) v;
                    for(int j = 0; j < existingVariables.size(); ++j) {
                        LOG(TRACEL) << "existingvars[" << j << "] = " << (int) existingVariables[j];
                        if (existingVariables[j] == v) {
                            extvars2pos[pair.first].push_back(j);
                            LOG(TRACEL) << "Position = " << j;
                            found = true;
                            break;
                        }
                    }

                    //Then search among the last literal
                    if (!found) {
                        int litVars = 0;
                        for (int x = 0; x < currentLiteral->getTupleSize(); ++x) {
                            const VTerm t = currentLiteral->getTermAtPos(x);
                            if (t.isVariable()) {
                                if (t.getId() == v) {
                                    extvars2pos[pair.first].push_back(
                                            existingVariables.size() + litVars);
                                    LOG(TRACEL) << "Position = " << (int) (existingVariables.size() + litVars);
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

