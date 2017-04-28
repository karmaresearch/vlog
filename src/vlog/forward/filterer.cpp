#include <vlog/filterer.h>
#include <vlog/concepts.h>
#include <vlog/fctable.h>
#include <vlog/fcinttable.h>

#include <vector>

TableFilterer::TableFilterer(SemiNaiver *naiver) : naiver(naiver) {}

bool TableFilterer::opt_intersection;

bool TableFilterer::intersection(const Literal &currentQuery,
                                 const FCBlock &block) {
    if (!getOptIntersect()) {
        BOOST_LOG_TRIVIAL(debug) << "intersection disabled";
        return true;
    }
    Substitution subs[SIZETUPLE];
    if (Literal::getSubstitutionsA2B(subs, block.query, currentQuery) != -1) {
        return true;
    } else {
        return false;
    }
}

bool TableFilterer::producedDerivationInPreviousSteps(
    const Literal &outputQuery,
    const Literal &currentQuery,
    const FCBlock *block) {

    const RuleExecutionDetails *rule = block->rule;
    if (rule == NULL || rule->nIDBs == 0) {
        return false;
    }

    //Easy case: the body of the current rule is equal to our rule
    Substitution subs[10];
    int nsubs = Literal::getSubstitutionsA2B(subs,
                rule->rule.getHead(), currentQuery);
    assert(nsubs != -1);

    for (const auto &lit : rule->rule.getBody()) {
        if (lit.getPredicate().getType() == IDB) {
            const Literal subsChild = lit.substitutes(subs, nsubs);
            if (subsChild == outputQuery) {
                //BOOST_LOG_TRIVIAL(info) << "SIMPLEPRUNING ok";
                return true;
            }

        }
    }
    return false;
}

bool TableFilterer::isEligibleForPartialSubs(
    const FCBlock *block,
    const Literal &headRule,
    const FCInternalTable *currentResults,
    const int nPosFromFirst,
    const int nPosFromSecond) {

    if (! naiver->opt_filter()) {
        BOOST_LOG_TRIVIAL(debug) << "isEligibleForPartialSubs disabled";
        return false;
    }

    //Minimum requirements for the rule at hand
    if ((currentResults != NULL && currentResults->getNRows() > 10000000) ||
            nPosFromFirst != 1  || nPosFromSecond == 0) {
        //if (currentResults->getNRows() > 10000000)
        //    BOOST_LOG_TRIVIAL(warning) << "Current results too large: " << currentResults->getNRows();
        return false;
    }

    //Requirements for the block we are going to check
    if (block == NULL || block->rule == NULL) {
        return false;
    }

    const Rule &rule = block->rule->rule;
    const std::vector<Literal> &bodyLiterals = rule.getBody();
    if (bodyLiterals.size() > 2) {
        return false;
    }
    if (bodyLiterals.size() == 2) {
	// Check the join: may have only one join position.
	int count = 0;
	std::vector<uint8_t> v1 = bodyLiterals[0].getAllVars();
	std::vector<uint8_t> v2 = bodyLiterals[1].getAllVars();
	for (int i = 0; i < v1.size(); i++) {
	    for (int j = 0; j < v2.size(); j++) {
		if (v1[i] == v2[j]) {
		    count++;
		    if (count > 1) {
			return false;
		    }
		    break;
		}
	    }
	}
    }

    // One body literal must match the head of the rule at hand
    bool foundRecursive = false;
    int idxRecursive = -1;
    bool foundSmall = false;
    int idxSmall = -1;
    int i = 0;
    for (const auto el : bodyLiterals) {
        if (!foundRecursive && el.getPredicate().getId() == headRule.getPredicate().getId()) {
            long estimate = naiver->estimateCardinality(el, 0, block->iteration);
            if (estimate < 30000000) { //I must be quick to query...
		foundRecursive = true;
                idxRecursive = i;
            } else {
		// Recursive but too large
		return false;
	    }
        }
        if (!foundSmall && el.getPredicate().getId() != headRule.getPredicate().getId()) {
            //Check if the size is small
            long estimate = naiver->estimateCardinality(el, 0, block->iteration);
            if (estimate < 10000000) { //I must be quick to query...
                foundSmall = true;
                idxSmall = i;
            } else {
                EDBLayer &layer = naiver->getEDBLayer();
                BOOST_LOG_TRIVIAL(debug) << "Card of " << el.tostring(naiver->getProgram(), &layer) << estimate << "<- too large";
            }
        }
        i++;
    }

    if (!foundRecursive) {
        //Is the rule linear. Than I can look at the block tree of the antecedent...
        if (rule.getNIDBPredicates() == 1 && bodyLiterals.size() == 1 &&
                headRule.getPredicate().getId() ==
                rule.getHead().getPredicate().getId()) {
            //BOOST_LOG_TRIVIAL(info) << "THE RULE OF THE block is linear. Try to look at the child...";
            //Get the last block (before the iteration) of the child predicate

            //Check if the rule is recursive. If so, then we can use the block,
            //but only if the previous block in that tree occurs before the
            //previous execution of this rule. In this case, I know that the
            //content of our block is
            //an exact replica of the last block of the child tree.

            FCIterator childItr = naiver->getTable(bodyLiterals[0], 0, block->iteration);
            std::vector<const FCBlock*> childBlocks;
            //There must be such a block, otherwise block would be empty
            while (!childItr.isEmpty()) {
                const FCBlock *childBlock = childItr.getCurrentBlock();
                childBlocks.push_back(childBlock);
                childItr.moveNextCount();
            }

            if (childBlocks.empty()) {
                BOOST_LOG_TRIVIAL(info) << "The child predicate is empty. Let it go";
                return false;
            }

            //Is the rule in that block recursive?
            if (childBlocks.back()->rule->rule.isRecursive()) {
                //BOOST_LOG_TRIVIAL(info) << "THE LAST BLOCK in the prev predicate is recursive!";
                //Ok now check the previous block (if any). I must be sure
                //that the block occurred before the previous execution of this
                //rule (if any).
                size_t prevIteration = 0;
                if (childBlocks.size() > 1) {
                    prevIteration = childBlocks[childBlocks.size() - 2]->iteration;
                }
                ///BOOST_LOG_TRIVIAL(info) << "THE PREV ITERATION IS " <<
                //                        prevIteration <<
                //                        "The block prev iteration was " <<
                //                        block->rule->lastExecution;

                if (prevIteration == 0 || block->rule->lastExecution > prevIteration) {
                    // Ok, I'm in business. Now I'm sure block contains only
                    // the content of this rule. Now I can check whether this rule
                    // satisfies the eligibility criteria.
                    bool resp = isEligibleForPartialSubs(childBlocks.back(),
                                                         bodyLiterals[0], NULL, 1, 1);
                    //BOOST_LOG_TRIVIAL(info) << "Here I would have returned " << resp;
                    return resp;
                } else {
                    //BOOST_LOG_TRIVIAL(info) << "No matching with the intervals";
                    return false;
                }

            } else {
                //BOOST_LOG_TRIVIAL(info) << "I was looking at the child predicate, but the last rule was not recursive";
                return false;
            }
        } else {
            return false;
        }
    }

//BOOST_LOG_TRIVIAL(info) << "TODERIVE=" << headRule.tostring(naiver->getProgram(), naiver->getDict()) << " rule=" << rule.tostring(naiver->getProgram(), naiver->getDict()) << "recursive=" << foundRecursive << " small=" << foundSmall;

//Check if the small literal shares variables with the recursive literal
    if (foundSmall) {
        std::vector<uint8_t> allvars = bodyLiterals[idxRecursive].getAllVars();
        if (bodyLiterals[idxSmall].getSharedVars(allvars).size() == 0)
            return false;
    }

    //BOOST_LOG_TRIVIAL(info) << "True!";

    return true;
}

bool TableFilterer::producedDerivationInPreviousStepsWithSubs(
    const FCBlock *block,
    const Literal &outputQuery,
    const Literal &currentQuery,
    const FCInternalTable *currentResults,
    const int nPosForHead,
    const std::pair<uint8_t, uint8_t> *posHead,
    const int nPosForLit,
    const std::pair<uint8_t, uint8_t> *posLiteral) {

    //Create a map that points from all subs in the body to the head
    if (nPosForHead != 1) {
        throw 10;
    }
    if (nPosForLit != 1) {
        throw 10;
    }

    map<Term_t, std::vector<Term_t>> mapSubstitutions;
    std::shared_ptr<Column> vLitCol = currentResults->getColumn(posLiteral[0].first);
    std::shared_ptr<Column> vHeadCol = currentResults->getColumn(posHead[0].second);
    if (vLitCol->isBackedByVector() && vHeadCol->isBackedByVector()) {
	const std::vector<Term_t> *vLitVec = &vLitCol->getVectorRef();
	const std::vector<Term_t> *vHeadVec = &vHeadCol->getVectorRef();

	for (size_t i = 0; i < vLitVec->size(); i++) {
	    const Term_t vLit = (*vLitVec)[i];
	    if (!mapSubstitutions.count(vLit)) {
		mapSubstitutions.insert(std::make_pair(vLit,
						       std::vector<Term_t>()));
	    }
	    mapSubstitutions[vLit].push_back((*vHeadVec)[i]);
	}
    } else {
	FCInternalTableItr *itr = currentResults->getIterator();
	while (itr->hasNext()) {
	    itr->next();
	    const Term_t vLit = itr->getCurrentValue(posLiteral[0].first);
	    if (!mapSubstitutions.count(vLit)) {
		mapSubstitutions.insert(std::make_pair(vLit,
						       std::vector<Term_t>()));
	    }
	    const Term_t vHead = itr->getCurrentValue(posHead[0].second);
	    mapSubstitutions[vLit].push_back(vHead);
	}
	currentResults->releaseIterator(itr);
    }
    // finished creating the map

    // std::vector<uint8_t> posVarsInHead = outputQuery.getPosVars();
    std::vector<uint8_t> posVarsInLit = currentQuery.getPosVars();

    // I think using posVarsInHead[posHead[0].first] is incorrect.
    // What we need here is the position of the variable in the head (outputQuery),
    // which is given by posHead[0].first.
    // posLiterals is different: these are in fact the join coordinates,
    // so the name is confusing to say the least. But, posLiteral[0].second
    // thus is the number of the variable in the currentQuery, so we do indeed
    // need to use posVarsInLit[posLiteral[0].second] to get its position in
    // currentQuery.
    // --Ceriel
    return producedDerivationInPreviousStepsWithSubs_rec(block,
            mapSubstitutions, outputQuery, currentQuery,
            // posVarsInHead[posHead[0].first], posVarsInLit[posLiteral[0].second]);
            posHead[0].first, posVarsInLit[posLiteral[0].second]);
}

bool TableFilterer::producedDerivationInPreviousStepsWithSubs_rec(
    const FCBlock *block,
    const map<Term_t, std::vector<Term_t>> &mapSubstitutions,
    const Literal &outputQuery,
    const Literal &currentQuery,
    const size_t posHead_first,
    const size_t posLit_second
) {

    const Rule &blockRule = block->rule->rule;
    //Get the body atom that matches with our head. This atom must exists.
    std::unique_ptr<Literal> rLit = getRecursiveLiteral(blockRule, outputQuery);
    //Get possibly the other atom which we can use to retrieve the subs
    std::unique_ptr<Literal> nrLit = getNonRecursiveLiteral(blockRule,
                                     outputQuery);

    //Recursive call
    if (rLit == NULL) {
        if (blockRule.getBody().size() != 1) {
            throw 10;
        }
        /*** This code calculates the new current query and pos of the subs ***/
        Substitution subs[10];
        const int nsubs = Literal::getSubstitutionsA2B(subs,
                          blockRule.getHead(), currentQuery);
        if (nsubs == -1) {
            throw 10;
        }
        VTerm tAtCurQuery = currentQuery.getTermAtPos(posLit_second);
        uint8_t idVarCurQuery = tAtCurQuery.getId();
        for (int i = 0; i < nsubs; ++i) {
            if (subs[i].origin == idVarCurQuery) {
                idVarCurQuery = subs[i].destination.getId();
            }
        }
        const Literal childCurrentQuery = blockRule.getBody()[0].substitutes(
                                              subs, nsubs);
        uint8_t childPosLit_second;
        for (int i = 0; i < childCurrentQuery.getTupleSize(); ++i) {
            if (childCurrentQuery.getTermAtPos(i).getId() == idVarCurQuery)
                childPosLit_second = i;
        }
        /*** End ***/

        /*** This code calculates the new output query and pos of the subs ***/
        Substitution subs2[10];
        const int nsubs2 = Literal::getSubstitutionsA2B(subs2,
                           blockRule.getHead(), outputQuery);
        VTerm tAtOutQuery = outputQuery.getTermAtPos(posHead_first);
        uint8_t idVarOutQuery = tAtOutQuery.getId();
        for (int i = 0; i < nsubs2; ++i) {
            if (subs2[i].origin == idVarOutQuery) {
                idVarOutQuery = subs2[i].destination.getId();
            }
        }
        const Literal childOutputQuery = blockRule.getBody()[0].substitutes(
                                             subs2, nsubs2);
        uint8_t childPosHead_first;
        for (int i = 0; i < childOutputQuery.getTupleSize(); ++i) {
            if (childOutputQuery.getTermAtPos(i).getId() == idVarOutQuery)
                childPosHead_first = i;
        }
        /*** End ***/

        //Get the last block of the first body
        FCIterator itr = naiver->getTable(childCurrentQuery,
                                          0, block->iteration);
        const FCBlock *recursiveBlock = NULL;
        while (!itr.isEmpty()) {
            recursiveBlock = itr.getCurrentBlock();
            itr.moveNextCount();
        }
        if (recursiveBlock == NULL)
            throw 10;

        //Get the new outputQuery

        return producedDerivationInPreviousStepsWithSubs_rec(recursiveBlock,
                mapSubstitutions, childOutputQuery, childCurrentQuery,
                childPosHead_first,
                childPosLit_second);
    }

    /*** START THE COMPUTATION ***/
    Literal headBlockRule = blockRule.getHead();
    //Pos var to be joined with the one in the head
    std::pair<uint8_t, uint8_t> joinHeadAndNRLits;
    //Pos of the variables to be joined with the recursive predicate
    std::pair<uint8_t, uint8_t> joinRandNRLits;

    std::map<Term_t, std::set<Term_t>> mapSubstitutionsInBlockQuery;
    if (nrLit != NULL) {
        // Calculate the join position
        bool foundJoin = false;
        bool foundHead = false;
        for (int j = 0; j < nrLit->getTupleSize(); ++j) {
            VTerm t = nrLit->getTermAtPos(j);
            if (t.isVariable()) {
                for (int i = 0; i < rLit->getTupleSize(); ++i) {
                    VTerm t2 = rLit->getTermAtPos(i);
                    if (t2.isVariable() && t.getId() == t2.getId()) {
                        if (foundJoin)
                            throw 10; //there should be only one
                        joinRandNRLits.first = j;
                        joinRandNRLits.second = i;
                        foundJoin = true;
                    }
                }

                for (int i = 0; i < headBlockRule.getTupleSize(); ++i) {
                    VTerm t2 = headBlockRule.getTermAtPos(i);
                    if (t2.isVariable() && t.getId() == t2.getId()) {
                        if (foundHead)
                            throw 10;
                        joinHeadAndNRLits.first = i;
                        joinHeadAndNRLits.second = j;
                        foundHead = true;
                    }
                }
            }
        }

        //Some extra contols
        if (!foundJoin) {
            throw 10;
        }
        if (!foundHead) {
            throw 10;
        }

        //Load all the substitutions of the non recursive literal. Set as key
        //the value that comes from the head. The values are all values that
        //should be joined with the recursive predicate.
        FCIterator itr = naiver->getTable(*nrLit.get(), 0, block->iteration);
        while (!itr.isEmpty()) {
            std::shared_ptr<const FCInternalTable> t = itr.getCurrentTable();
            FCInternalTableItr *itr2 = t->getIterator();
            while (itr2->hasNext()) {
                itr2->next();
                const Term_t key = itr2->getCurrentValue(
                                       joinHeadAndNRLits.second);
                if (!mapSubstitutionsInBlockQuery.count(key)) {
                    mapSubstitutionsInBlockQuery[key] = std::set<Term_t>();
                }
                //Copy the value
                mapSubstitutionsInBlockQuery[key].insert(itr2->
                        getCurrentValue(joinRandNRLits.first));
            }
            t->releaseIterator(itr2);
            itr.moveNextCount();
        }
    }

    bool response = true;

    Substitution subs[SIZETUPLE];
    long counterProcessedSubs = 0;
    int countEmpty = 0;
    for (const auto &el : mapSubstitutions) {
        counterProcessedSubs++;

        //If one query leads to more instantiations of the head, but there is no
        //additional query that can create more instantiations on the body
        //of block query, then I just quit
        if (el.second.size() > 1 && nrLit == NULL) {
            response = false;
            break;
        }

        // Calculate the set of all possible heads that we should match
        set<Term_t> possibleHeads;
        for (const auto &el2 : el.second)
            possibleHeads.insert(el2);

        // Substitute the value in the body
        VTuple t = currentQuery.getTuple();
        t.set(VTerm(0, el.first), posLit_second);
        Literal substitutedLiteral(currentQuery.getPredicate(), t);

        // Copy the substitutions in the head of the block rule
        int nsubs = Literal::getSubstitutionsA2B(subs, headBlockRule,
                    substitutedLiteral);
        // If nsubs == -1, then the content of the block cannot match the query
        // we will ask. I'll count these cases. If this occurs for any subs.
        // then I can exclude the block.
        if (nsubs == -1) {
            countEmpty++;
            continue;
        }
        // assert(nsubs != -1);

        //Copy the substitutions in the body literal of block rule that
        //should match with our head
        const Literal srLit = rLit->substitutes(subs, nsubs);
        assert(srLit.getPredicate().getId() ==
               outputQuery.getPredicate().getId());

        if (nrLit != NULL) {
            //Substitutes also the other non-recursive atom

            //This is the value that from the head query propagated to the head
            //of this query and now to the body literal.
            const Term_t k = el.first;
            if (mapSubstitutionsInBlockQuery.count(k)) {
                set<Term_t> &localBindings = mapSubstitutionsInBlockQuery[k];

                //For each binding in the head, test if there it exists a
                //corresponding query in the localbindings
                for (auto posH : possibleHeads) {
                    if (!localBindings.count(posH)) {
                        response = false;
                        break;
                    } else {
                        //Create an atom with how our head will look like
                        VTuple t = outputQuery.getTuple();
                        t.set(VTerm(0, posH), joinRandNRLits.second);
                        Literal newHead(outputQuery.getPredicate(), t);

                        //Create the body
                        VTuple t2 = srLit.getTuple();
                        t2.set(VTerm(0, posH), joinRandNRLits.second);
                        Literal newBody(srLit.getPredicate(), t2);

                        //Check if they are equivalent
                        if (!(newBody == newHead)) {
                            response = false;
                            break;
                        }

                    }
                }

                if (!response) {
                    break;
                }
            }

        } else {
            //Instantiate the head with the only value in possible heads and
            //check whether the local body and the head are the same
            VTuple t = outputQuery.getTuple();
            assert(possibleHeads.size() == 1);
            t.set(VTerm(0, *possibleHeads.begin()), posHead_first);
            Literal newHead(outputQuery.getPredicate(), t);
            if (!(newHead == srLit)) {
                response = false;
                break;
            }
        }
    }

    if (countEmpty == mapSubstitutions.size()) {
        //All substituted queries did not match the head of the rule.
        //I flag response to true, because I want the block to be ignored.
        response = true;
    }

    return response;
}
