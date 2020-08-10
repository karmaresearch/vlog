#include <vlog/optimizer.h>
#include <vlog/concepts.h>

std::vector<const Literal*> Optimizer::calculateBestPlan(
    std::vector<const Literal*> &existingPlan,
    std::vector<Var_t> boundVars, std::vector<Var_t> &existingVars,
    std::vector<const Literal*> &remainingLiterals) {

    if (remainingLiterals.size() == 0) {
        return existingPlan;
    }

    if (remainingLiterals.size() == 1) {
        //No choice is possible
        if (remainingLiterals[0]->getSharedVars(existingVars).size() == 0 && existingPlan.size() != 0) {
            throw 10;
        }
        existingPlan.push_back(remainingLiterals[0]);
        return existingPlan;
    }

    int nBounds = -1;
    bool nBoundsQuery = false;
    int type = IDB; //IDB=1, EDB=0
    const Literal *chosenLiteral = NULL;

    //Choose the best between the remaining literals
    for (std::vector<const Literal*>::const_iterator itr = remainingLiterals.begin();
            itr != remainingLiterals.end(); ++itr) {
        int litBoundVars = 0;
        int allBounds = 0;
        bool litBoundVarsQuery = false;

        const Literal *l = *itr;
        for (int i = 0; i < l->getTupleSize(); ++i) {
            if (l->getTermAtPos(i).isVariable()) {
                Var_t id = l->getTermAtPos(i).getId();
                for (std::vector<Var_t>::iterator itr = existingVars.begin();
                        itr != existingVars.end();
                        ++itr) {
                    if (id == *itr) {
                        litBoundVars++;
                        allBounds++;
                    }
                }
                for (std::vector<Var_t>::iterator itr = boundVars.begin();
                        itr != boundVars.end();
                        ++itr) {
                    if (id == *itr) {
                        litBoundVarsQuery = true;
                    }
                }
            } else {
                allBounds++;
            }
        }

        if (litBoundVars > 0) {
            if (l->getPredicate().getType() < type ||
                    (l->getPredicate().getType() == type && allBounds > nBounds) ||
                    (l->getPredicate().getType() == type && allBounds == nBounds && litBoundVarsQuery && !nBoundsQuery)) {
                chosenLiteral = l;
                nBounds = allBounds;
                nBoundsQuery = litBoundVarsQuery;
                type = l->getPredicate().getType();
            }
        }
    }

    if (chosenLiteral == NULL) {
        assert(existingPlan.size() == 0);
        chosenLiteral = remainingLiterals[0];
    }

    //Remove from the remaining list the chosen literal
    std::vector<const Literal*> newRemainingLiterals;
    for (std::vector <
            const Literal* >::const_iterator itr = remainingLiterals.begin();
            itr != remainingLiterals.end(); ++itr) {
        if (*itr != chosenLiteral) {
            newRemainingLiterals.push_back(*itr);
        }
    }

    //Add it to the existing list
    assert(chosenLiteral != NULL);
    existingPlan.push_back(chosenLiteral);

    //add all new variables
    for (int i = 0; i < chosenLiteral->getTupleSize(); ++i) {
        if (chosenLiteral->getTermAtPos(i).isVariable()) {
            Var_t varid = chosenLiteral->getTermAtPos(i).getId();
            bool isNew = true;
            for (std::vector<Var_t>::iterator itr = existingVars.begin();
                    itr != existingVars.end();
                    ++itr) {
                if (*itr == varid) {
                    isNew = false;
                    break;
                }
            }
            if (isNew) {
                existingVars.push_back(varid);
            }
        }
    }

    return calculateBestPlan(existingPlan, boundVars, existingVars, newRemainingLiterals);
}

bool selectivitySorter(const Literal *l1, const Literal *l2) {
    uint8_t t1 = l1->getPredicate().getType();
    uint8_t t2 = l2->getPredicate().getType();
    if (t1 != t2) {
        return t1 < t2;
    }
    return l1->getNVars() < l2->getNVars();
}

std::vector<const Literal*> Optimizer::rearrangeBodyAfterAdornment(std::vector<Var_t>
        &boundVars, const std::vector<Literal> &body) {
    std::vector<const Literal*> initialPlan;
    std::vector<const Literal*> remainingLiterals;
    std::vector<Var_t> existingVars;
    std::copy(boundVars.begin(), boundVars.end(), std::back_inserter(existingVars));

    std::vector<const Literal*> output;

    //Magic predicates are not rearranged
    for (int i = 0; i < body.size(); ++i) {
        if (body.at(i).isMagic()) {
            output.push_back(&(body.at(i)));
        } else {
            remainingLiterals.push_back(&(body.at(i)));
        }
    }

    //First I order the literals by selectivity
    std::sort(remainingLiterals.begin(), remainingLiterals.end(), selectivitySorter);

    //Add the variables of the first pattern in case there is no bound variable
    if (boundVars.size() == 0 && remainingLiterals.size() > 1) {
        initialPlan.push_back(remainingLiterals.front());
        std::vector<Var_t> vars = remainingLiterals.front()->getAllVars();
        std::copy(vars.begin(), vars.end(), std::back_inserter(existingVars));
        remainingLiterals.erase(remainingLiterals.begin());
    }

    //Now I select those plans where the bindings in the head are propagated to the body
    std::vector<const Literal*> rearrangedLiterals = calculateBestPlan(initialPlan, boundVars,
            existingVars, remainingLiterals);
    std::copy(rearrangedLiterals.begin(), rearrangedLiterals.end(), std::back_inserter(output));
    return output;
}
