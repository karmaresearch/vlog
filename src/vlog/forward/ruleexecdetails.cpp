#include <vlog/ruleexecdetails.h>

#include <set>
#include <map>
#include <cassert>
#include <algorithm>

void RuleExecutionDetails::rearrangeLiterals(std::vector<const Literal*> &vector, const size_t idx) {
    //First go through all the elements before, to make sure that there is always at least one shared variable.
    std::vector<const Literal*> subset;
    for (int i = idx - 1; i >= 0; --i) {
        subset.push_back(vector[i]);
    }
    const Literal *idxPointer = vector[idx];
    std::vector<Var_t> startVars = idxPointer->getAllVars();
    std::vector<const Literal*> leftLiterals;

    //Group the left side (the one that has EDB literals)
    groupLiteralsBySharedVariables(startVars, subset, leftLiterals);

    //If there are elements that are not linked, then I add them after
    std::vector<const Literal*> subset2;
    if (!leftLiterals.empty()) {
        std::copy(leftLiterals.begin(), leftLiterals.end(), std::back_inserter(subset2));
    }
    std::copy(vector.begin() + idx + 1, vector.end(), std::back_inserter(subset2));

    for (const auto& literalPointer : subset) {
        std::vector<Var_t> newVars = literalPointer->getNewVars(startVars);
        std::copy(newVars.begin(), newVars.end(), std::back_inserter(startVars));
    }
    std::vector<const Literal*> leftLiterals2;
    groupLiteralsBySharedVariables(startVars, subset2, leftLiterals2);

    vector.clear();
    std::copy(subset.begin(), subset.end(), std::back_inserter(vector));
    vector.push_back(idxPointer);
    std::copy(subset2.begin(), subset2.end(), std::back_inserter(vector));

    //assert(leftLiterals2.size() == 0);
    while (!leftLiterals2.empty()) {
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

void RuleExecutionDetails::groupLiteralsBySharedVariables(std::vector<Var_t> &startVars,
        std::vector<const Literal *> &set, std::vector<const Literal*> &leftelements) {
    if (set.size() == 0)
        return;
    if (set.size() == 1) {
        if (set[0]->getSharedVars(startVars).size() == 0) {
            leftelements.push_back(set[0]);
            set.clear();
        }
        std::vector<Var_t> newVars = set[0]->getNewVars(startVars);
        std::copy(newVars.begin(), newVars.end(), std::back_inserter(startVars));
        return;
    }

    std::vector<const Literal *> set1;

    for (const auto& literalPointer : set) {
        if (literalPointer->getAllVars().size() == 0) {
            leftelements.push_back(literalPointer);
        } else {
            set1.push_back(literalPointer);
        }
    }

    std::vector<Var_t> varsBestMatching(startVars);
    std::vector<const Literal*> bestMatching;
    std::vector<const Literal*> bestMatchingLeft(set);

    for (const auto& literalPointer : set1) {
        std::vector<Var_t> sharedVars = literalPointer->getSharedVars(startVars);
        if (sharedVars.size() > 0 || startVars.size() == 0) {
            //copy the remaining
            std::vector<const Literal*> newSet;
            std::vector<const Literal*> newLeftElements;
            for (const auto& literalPointer2 : set1) {
                if (literalPointer2 != literalPointer) {
                    newSet.push_back(literalPointer2);
                }
            }

            //copy the new vars
            std::vector<Var_t> newVars(startVars);
            std::vector<Var_t> nv = literalPointer->getNewVars(newVars);
            std::copy(nv.begin(), nv.end(), std::back_inserter(newVars));

            groupLiteralsBySharedVariables(newVars, newSet, newLeftElements);
            if (newLeftElements.empty()) {
                startVars.clear();
                set1.clear();
                leftelements.clear();
                set1.push_back(literalPointer);
                std::copy(newSet.begin(), newSet.end(), std::back_inserter(set1));
                std::copy(newVars.begin(), newVars.end(), std::back_inserter(startVars));
                return;
            } else {
                if (newSet.size() > bestMatching.size()) {
                    varsBestMatching.clear();
                    bestMatching.clear();
                    bestMatchingLeft.clear();
                    std::copy(newVars.begin(), newVars.end(), std::back_inserter(varsBestMatching));
                    std::copy(newSet.begin(), newSet.end(), std::back_inserter(bestMatching));
                    std::copy(newLeftElements.begin(), newLeftElements.end(), std::back_inserter(bestMatchingLeft));
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
    for (const auto& literal : input) {
        if (literal.getPredicate().getType() == EDB) {
            output.push_back(&literal);
        }
    }
}

void RuleExecutionDetails::checkFilteringStrategy(
        const Literal &literal, const Literal &head, RuleExecutionPlan &hv) {
    //Two conditions: head and last literals must be compatible, and they must
    //share at least one variable in the same position

    std::vector<Var_t> vars = literal.getAllVars();
    std::vector<Var_t> sharedVars = head.getSharedVars(vars);
    if (!sharedVars.empty()) {
        hv.lastLiteralSharesWithHead = true;
    } else {
        hv.lastLiteralSharesWithHead = false;
    }

    hv.lastSorting.clear();
    if (!sharedVars.empty()) {
        //set the sorting by the position of the sharedVars in the literal
        for (const auto& sharedVar : sharedVars) {
            uint8_t posVar = 0;
            for (int pos = 0; pos < literal.getTupleSize(); ++pos) {
                VTerm t = literal.getTermAtPos(pos);
                if (t.isVariable()) {
                    if (t.getId() == sharedVar) {
                        hv.lastSorting.push_back(posVar);
                    }
                    posVar++;
                }
            }
        }
    }
}

void RuleExecutionDetails::calculateNVarsInHeadFromEDB() {
    int globalCounter = 0;
    for (auto head : rule.getHeads()) {
        for (int i = 0; i < head.getTupleSize(); ++i) {
            VTerm t = head.getTermAtPos(i);
            if (t.isVariable()) {
                //Check if this variable appears on some edb terms
                std::vector<std::pair<int, uint8_t>> edbLiterals;
                int idxLiteral = 0;
                for (const auto & literal : bodyLiterals) {
                    if (literal.getPredicate().getType() == EDB) {
                        for (int j = 0; j < literal.getTupleSize(); ++j) {
                            VTerm t2 = literal.getTermAtPos(j);
                            if (t2.isVariable() && t.getId() == t2.getId()) {
                                edbLiterals.push_back(std::make_pair(idxLiteral, j));
                                break;
                            }
                        }
                    }
                    idxLiteral++;
                }

                if (!edbLiterals.empty()) {
                    //add the position and the occurrences
                    posEDBVarsInHead.push_back(globalCounter + i);
                    occEDBVarsInHead.push_back(edbLiterals);
                }
            }
        }
        globalCounter += head.getTupleSize();

        //Group the occurrences by EDB literal
        for (int idxVar = 0; idxVar < posEDBVarsInHead.size(); ++idxVar) {
            Var_t var = posEDBVarsInHead[idxVar];
            for (int idxPattern = 0;
                    idxPattern < occEDBVarsInHead[idxVar].size();
                    ++idxPattern) {
                std::pair<uint8_t, uint8_t> patternAndPos =
                    occEDBVarsInHead[idxVar][idxPattern];
                bool found = false;
                for (int i = 0; i < edbLiteralPerHeadVars.size() &&
                        !found; ++i) {
                    std::pair<uint8_t,
                        std::vector<std::pair<int, uint8_t>>> el =
                            edbLiteralPerHeadVars[i];
                    if (el.first == patternAndPos.first) {
                        edbLiteralPerHeadVars[i].second.push_back(
                                std::make_pair(var, patternAndPos.second));
                        found = true;
                    }
                }
                if (!found) {
                    edbLiteralPerHeadVars.push_back(std::make_pair(patternAndPos.first, std::vector<std::pair<int, uint8_t>>()));
                    edbLiteralPerHeadVars.back().second.push_back(std::make_pair(var, patternAndPos.second));
                }
            }
        }
    }
}

/*void RuleExecutionDetails::checkWhetherEDBsRedundantHead(RuleExecutionPlan &p,
  const Literal &head) {
//Check whether some EDBs can lead to redundant derivation
for (int i = 0; i < p.plan.size(); ++i) {
const Literal *literal = p.plan[i];
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
for (int x = 0; x < literal->getTupleSize(); ++x) {
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
hv.matches.push_back(r);
break;
}
}
}
}
}
}*/
void RuleExecutionDetails::createExecutionPlans(
        std::vector<std::pair<size_t, size_t>> &ranges,
        bool copyAllVars) {
    bodyLiterals.clear();
    orderExecutions.clear();

    //Init
    for (auto& literal : rule.getBody()) {
        bodyLiterals.push_back(literal);
    }

    //Calculate the dependencies of the existential variables to the variables in the body
    std::map<Var_t, std::vector<Var_t>> dependenciesExtVars = rule.calculateDependencies();

    //Create a single execution plan
    RuleExecutionPlan p;
    p.filterLastHashMap = false;
    std::vector<const Literal*> v;
    p.dependenciesExtVars = dependenciesExtVars;
    int rangeId = 0;
    for (const auto& literal : bodyLiterals) {
        p.plan.push_back(&literal);
        p.ranges.push_back(std::make_pair(ranges[rangeId].first, ranges[rangeId].second));
    }

    auto &heads = rule.getHeads();
    p.calculateJoinsCoordinates(heads, copyAllVars);
    orderExecutions.push_back(p);
}

void RuleExecutionDetails::createExecutionPlans(bool copyAllVars) {
    bodyLiterals.clear();
    orderExecutions.clear();

    for (auto& literal : rule.getBody()) {
        bodyLiterals.push_back(literal);
    }

    //Calculate the dependencies of the existential variables to the variables in the body
    std::map<Var_t, std::vector<Var_t>> dependenciesExtVars = rule.calculateDependencies();

    if (nIDBs > 0) {
        //Collect the IDB predicates
        std::vector<int> posMagicAtoms;
        std::vector<int> posIdbLiterals;
        orderExecutions.resize(nIDBs);

        for (int i = 0; i < bodyLiterals.size(); ++i){
            Predicate predicate = bodyLiterals.at(i).getPredicate();
            if(predicate.getType() == IDB){
                posIdbLiterals.push_back(i);
                if(predicate.isMagic()) {
                    posMagicAtoms.push_back(i);
                }
            }
        }

        int order = 0;
        for (const auto& pos : posIdbLiterals) {
            RuleExecutionPlan *p = &orderExecutions[order];
            p->filterLastHashMap = false;
            p->dependenciesExtVars = dependenciesExtVars;
            size_t idx = 0;

            if (posMagicAtoms.size() > 0) {
                assert(posMagicAtoms.size() == 1);
                p->plan.push_back(&bodyLiterals[posMagicAtoms[0]]);
                extractAllEDBPatterns(p->plan, bodyLiterals);
                if (! bodyLiterals[pos].isMagic()) {
                    p->plan.push_back(&bodyLiterals[pos]);
                }
            } else {
                extractAllEDBPatterns(p->plan, bodyLiterals);
                idx = p->plan.size();
                p->plan.push_back(&bodyLiterals[pos]);
            }

            //Add all others
            for (const auto& pos2 : posIdbLiterals) {
                if (pos2 != pos && ! bodyLiterals[pos2].isMagic()) {
                    p->plan.push_back(&bodyLiterals[pos2]);
                }
            }
            rearrangeLiterals(p->plan, idx);

            //Do some checks
            auto &heads = rule.getHeads();
            if (heads.size() == 1) {
                RuleExecutionDetails::checkFilteringStrategy(
                        *p->plan[p->plan.size() - 1], heads[0], *p);
                p->checkIfFilteringHashMapIsPossible(heads[0]);
            }

            //RuleExecutionDetails::checkWhetherEDBsRedundantHead(p, h);

            //Calculate all join coordinates
            p->calculateJoinsCoordinates(heads, copyAllVars);

            //New version. Should be able to catch everything
            for (const auto& literal : p->plan) {
                if (literal->getPredicate().getType() == EDB || literal->isNegated()) {
                    p->ranges.push_back(std::make_pair(0, (size_t) - 1));
                } else {
                    if (literal == &bodyLiterals[pos]) {
                        p->ranges.push_back(std::make_pair(1, (size_t) - 1));
                    } else if (literal < &bodyLiterals[pos]) {
                        p->ranges.push_back(std::make_pair(0, 1));
                    } else {
                        p->ranges.push_back(std::make_pair(0, (size_t) - 1));
                    }
                }
            }

            order++;
        }
        assert(orderExecutions.size() == nIDBs);
    } else {
        //Create a single plan. Here they are all EDBs. So the ranges are all the same
        std::vector<const Literal*> v;
        orderExecutions.resize(1);
        RuleExecutionPlan *p = &orderExecutions[0];
        p->filterLastHashMap = false;
        p->dependenciesExtVars = dependenciesExtVars;
        for (std::vector<Literal>::const_iterator itr = bodyLiterals.begin();
                itr != bodyLiterals.end();
                ++itr) {
            p->plan.push_back(&(*itr));
            p->ranges.push_back(std::make_pair(0, (size_t) - 1));
        }

        auto &heads = rule.getHeads();
        if (heads.size() == 1) {
            RuleExecutionDetails::checkFilteringStrategy(
                    bodyLiterals[bodyLiterals.size() - 1], heads[0], *p);
        }
        p->calculateJoinsCoordinates(heads, copyAllVars);
    }
}
