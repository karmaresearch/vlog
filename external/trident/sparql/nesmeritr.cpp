#include <trident/sparql/joins.h>
#include <boost/log/trivial.hpp>

#include <iostream>

using namespace std;
namespace timens = boost::chrono;

#define GO_BACK(x) -x -1;

PairItr *NestedMergeJoinItr::getFirstIterator(Pattern p) {
    return q->get(p.idx(), p.subject(), p.predicate(), p.object());
}

long NestedMergeJoinItr::executePlan() {
    assert(outputTuples == 0);
    if (currentItr == NULL) {
        return 0;
    }

    //  boost::chrono::duration<double> timings[10];
    //  for (int i = 0; i < plan->nPatterns; ++i) {
    //      timings[i] = timings[i].zero();
    //  }
    //  timens::duration<double> timeLookup = timeLookup.zero();
    //  timens::duration<double> finalWaitTime = timeLookup.zero();

    while (true) {
        /* Get the next value of the current iterator. If the current
         * iterator does not have values anymore, then we move one level below.
         */
//nextPair:

        if (currentItr->hasNext()) {
            currentItr->next();
        } else {
            /* If the initial iterator is finished, then the join is terminated*/
            if (idxCurrentPattern == 0) {
                break;
            } else {
                //Go one pattern below
                idxCurrentPattern--;
                currentItr = iterators[idxCurrentPattern];
                currentVarsPos = plan->posVarsToCopyInIdx[idxCurrentPattern];
                nCurrentVarsPos = plan->nPosVarsToCopyInIdx[idxCurrentPattern];
                continue;
            }
        }

skipNext:

        //cerr << "Process pair " << idxCurrentPattern << "-" << currentItr->getValue1() << " "
        //     << currentItr->getValue2() << endl;

        /***** Fill the row with the variables of the pattern just read *****/
        idxCurrentRow = startingVarPerPattern[idxCurrentPattern];
        for (int i = 0; i < nCurrentVarsPos; ++i) {
            long val = 0;
            switch (currentVarsPos[i]) {
            case 0:
                val = currentItr->getKey();
                break;
            case 1:
                val = currentItr->getValue1();
                break;
            case 2:
                val = currentItr->getValue2();
                break;
            }
            compressedRow[idxCurrentRow++] = val;
        }

        /***** IF THIS WAS THE LAST PATTERN ... *****/
        if (idxCurrentPattern == idxLastPattern) {
            //nElements++;
            for (int j = 0; j < sVarsToReturn; ++j) {
                outputResults->addValue(compressedRow[varsToReturn[j]]);
            }
            outputTuples++;
            if (outputTuples == maxTuplesInBuffer) {
                outputTuples = 0;
                return maxTuplesInBuffer;
            }
        } else {
            /***** PERFORM THE JOIN *****/
            //          timens::system_clock::time_point start =
            //                  timens::system_clock::now();
            int return_code = executeJoin(compressedRow, plan->patterns,
                                          idxCurrentPattern + 1, iterators,
                                          allJoins[idxCurrentPattern + 1],
                                          nJoins[idxCurrentPattern + 1]);
            //          timings[idxCurrentPattern + 1] += timens::system_clock::now()
            //                  - start;

            switch (return_code) {
            case JOIN_FAILED:
                //The join on these values has failed. Read the next value of
                //the current pattern
                continue;
            case JOIN_SUCCESSFUL:
                //Good, I can move on with reading the next pattern.
                idxCurrentPattern++;
                currentItr = iterators[idxCurrentPattern];
                currentVarsPos = plan->posVarsToCopyInIdx[idxCurrentPattern];
                nCurrentVarsPos = plan->nPosVarsToCopyInIdx[idxCurrentPattern];
                //The join has already read the first valid element for the
                //pattern. I don't need to call a next() in the next loop.
                goto skipNext;

            case NOMORE_JOIN:       //The join algorithm told me that it
                //is not possible to construct a join anymore...
                goto exit;
            default:
                // Move back of n patterns
                idxCurrentPattern = -return_code - 1;
                currentItr = iterators[idxCurrentPattern];
                currentVarsPos = plan->posVarsToCopyInIdx[idxCurrentPattern];
                nCurrentVarsPos = plan->nPosVarsToCopyInIdx[idxCurrentPattern];
            }
        }
    }

exit:

    cleanup();
    long results = outputTuples;
    outputTuples = 0;
    currentItr = NULL;
    return results;
}

int NestedMergeJoinItr::executeJoin(long *row, Pattern *patterns, int idxPattern,
                                    PairItr **iterators, JoinPoint *joins, const int nJoins) {

    /***** DETERMINE WHETHER WE NEED A NEW PATTERN *****/
    bool need_new_iterator = false;
    int performed_joins = 0;
    if (nJoins > 0) {
        if (joins[0].posIndex == 0) {
            if (row[joins[0].posRow] != joins[0].lastValue) {
                joins[0].lastValue = row[joins[0].posRow];
                need_new_iterator = true;
            } else {
                need_new_iterator = false;
                performed_joins++; //One join is already performed.
            }
        }
    } else {
        //If it is a cartesian product we always request a new pattern.
        need_new_iterator = true;
    }
    /***** RELEASE THE OLD ITERATOR IF I NEED TO *****/
    PairItr *itr = iterators[idxPattern];
    if (need_new_iterator && itr != NULL) {
        q->releaseItr(itr);
        itr = NULL;
    }
    /***** GET THE NEW ITERATOR *****/
    if (itr == NULL) {
        long s = patterns[idxPattern].subject();
        long p = patterns[idxPattern].predicate();
        long o = patterns[idxPattern].object();

        //Replace first term
        performed_joins = 0;
        if (nJoins > 0 && joins[0].posIndex == 0) {
            performed_joins++;
            switch (joins[0].posPattern) {
            case 0:
                s = compressedRow[joins[0].posRow];
                break;
            case 1:
                p = compressedRow[joins[0].posRow];
                break;
            case 2:
                o = compressedRow[joins[0].posRow];
                break;
            }
        }
        itr = iterators[idxPattern] = q->get(patterns[idxPattern].idx(), s, p,
                                             o);
        if (!itr->hasNext()) {
            if (!need_new_iterator) {
                return NOMORE_JOIN;
            } else {
                return JOIN_FAILED;
            }
        }

        itr->mark();

        for (int i = performed_joins; i < nJoins; ++i) {
            long v = compressedRow[joins[i].posRow];
            if (joins[i].posIndex == 1) {
                itr->setConstraint1(v);
                itr->gotoFirstTerm(v);
            } else { // can only be 2
                if (itr->hasNext()) {
                    itr->setConstraint2(v);
                    itr->gotoSecondTerm(v);
                }
            }
            joins[i].lastValue = v;
        }

        if (itr->hasNext()) {
            itr->next();
            return JOIN_SUCCESSFUL;
        } else {
            return try_merge_join(iterators, idxPattern,
                                  joins + performed_joins, nJoins - performed_joins);
        }
    } else {
        //If there are new joins then we move the iterator.
        int rem_joins = nJoins - performed_joins;
        if (rem_joins == 0) {
            //I only need to reset the iterator
            itr->reset(0);
        } else if (rem_joins == 1) {
            long value_to_join_with =
                compressedRow[joins[performed_joins].posRow];

            //The value is smaller than the current one. I need to reset
            //the iterator first...

            if (joins[performed_joins].lastValue > value_to_join_with
                    || !itr->hasNext() || itr->getValue1() == LONG_MIN) {
                itr->reset(0);
            } else {
                if (joins[performed_joins].lastValue == value_to_join_with) {
                    int nvars = patterns[idxPattern].getNVars();
                    if ((nvars == 2 && performed_joins == 0) || nvars == 3) {
                        itr->reset(1);
                    } else {
                        itr->reset(0);
                    }
                }
            }

            joins[performed_joins].lastValue = value_to_join_with;

            if (joins[performed_joins].posIndex == 1) {
                itr->setConstraint1(value_to_join_with);
                itr->gotoFirstTerm(value_to_join_with);
            } else {
                itr->setConstraint2(value_to_join_with);
                itr->gotoSecondTerm(value_to_join_with);
            }
        } else if (rem_joins == 2) {
            long value_to_join_with1 =
                compressedRow[joins[performed_joins].posRow];
            long value_to_join_with2 =
                compressedRow[joins[performed_joins + 1].posRow];

            //Reset the iterator if the range is smaller than the current pos
            int comp1 = (int)(joins[performed_joins].lastValue - value_to_join_with1);
            int comp2 = (int)(joins[performed_joins + 1].lastValue
                              - value_to_join_with2);

            if (comp1 > 0 || !itr->hasNext() || itr->getValue1() == LONG_MIN) {
                itr->reset(0);
            } else if (comp1 == 0 && comp2 >= 0) {
                itr->reset(1);
            }

            itr->setConstraint1(value_to_join_with1);
            itr->setConstraint2(-1);
            itr->gotoFirstTerm(value_to_join_with1);
            if (itr->hasNext()) {
                itr->setConstraint2(value_to_join_with2);
                itr->gotoSecondTerm(value_to_join_with2);
            }

            joins[performed_joins].lastValue = value_to_join_with1;
            joins[performed_joins + 1].lastValue = value_to_join_with2;
        }

        if (itr->hasNext()) {
            itr->next();
            return JOIN_SUCCESSFUL;
        } else {
            return try_merge_join(iterators, idxPattern,
                                  joins + performed_joins, rem_joins);
        }
    }
    return JOIN_SUCCESSFUL;
}

int NestedMergeJoinItr::try_merge_join(PairItr **iterators, int idxCurrentPattern,
                                       const JoinPoint *joins, const int nJoins) {
    if (nJoins == 0) {
        //I cannot do a merge join if there is no join to perform
        return JOIN_FAILED;
    } else if (nJoins == 1) {
        if (!joins[0].merge) {
            return JOIN_FAILED;
        }

        PairItr *sourceItr = iterators[joins[0].sourcePattern];
        PairItr *currentItr = iterators[idxCurrentPattern];

        /*if (!sourceItr->allowMerge() || !currentItr->allowMerge())
            return JOIN_FAILED;*/

        long value_to_join_with =
            joins[0].sourcePosIndex == 1 ?
            sourceItr->getValue1() : sourceItr->getValue2();
        long current_value =
            joins[0].posIndex == 1 ?
            currentItr->getValue1() : currentItr->getValue2();
        //Two cases: the current value is smaller or larger.
        if (value_to_join_with < current_value) {
            if (joins[0].sourcePosIndex == 1) {
                sourceItr->gotoFirstTerm(current_value);
            } else {
                sourceItr->gotoSecondTerm(current_value);
            }

            if (joins[0].posIndex == 1) {
                currentItr->setConstraint1(current_value);
            } else {
                currentItr->setConstraint2(current_value);
            }

            return GO_BACK(joins[0].sourcePattern);
        } else {
            //In this case, there are no more values in current Itr that
            //can produce a join. Because of this, we have to either go back to
            //the previous pattern than source or stop the join.
            if (joins[0].sourcePattern == 0) {
                return NOMORE_JOIN;
            } else {
                int idx = joins[0].sourcePattern - 1;
                return GO_BACK(idx);
            }
        }
        return JOIN_FAILED;

    } else {
        //nJoins == 2
        //Here we could further optimize the join
        return JOIN_FAILED;
    }
}

void NestedMergeJoinItr::cleanup() {
    // Release the iterators
    for (int i = 0; i < plan->nPatterns; ++i) {
        if (iterators[i] != NULL)
            q->releaseItr(iterators[i]);
    }

    // Remove references to the iterators
    int lastPattern = plan->nPatterns - 1;
    for (int i = 0; i <= lastPattern; ++i) {
        iterators[i] = NULL;
    }
}

void NestedMergeJoinItr::init(PairItr *firstIterator, TupleTable *outputR, long limitOutputTuple) {
    /***** INIT VARIABLES *****/
    //nElements = 0; //# rows printed
    allJoins = plan->joins; //pointer to all joins to perform
    nJoins = plan->nJoins; //number of joins
    idxCurrentPattern = 0;
    currentVarsPos = plan->posVarsToCopyInIdx[idxCurrentPattern];
    nCurrentVarsPos = plan->nPosVarsToCopyInIdx[idxCurrentPattern];

    // Variables used to fulfill the values in the row to print
    startingVarPerPattern = plan->sizeRowsPatterns;
    idxLastPattern = plan->nPatterns - 1;
    idxCurrentRow = 0;

    // Variables used to print the output
    varsToReturn = plan->posVarsToReturn;
    sVarsToReturn = plan->nPosVarsToReturn;
    if (limitOutputTuple == 0) {
        maxTuplesInBuffer = OUTPUT_BUFFER_SIZE / sVarsToReturn;
    } else {
        maxTuplesInBuffer = limitOutputTuple;
    }
    outputTuples = 0;
    if (outputR == NULL) {
        this->outputResults = new TupleTable(sVarsToReturn);
        deleteOutputResults = true;
    } else {
        this->outputResults = outputR;
        deleteOutputResults = false;
    }

    for (int i = 0; i < plan->nPatterns; ++i) {
        iterators[i] = NULL;
    }
    for (int i = 0; i < MAX_N_PATTERNS; ++i) {
        compressedRow[i] = -1;
    }

    /***** GET FIRST ITERATOR *****/
    currentItr = iterators[0] = firstIterator;

    currentBuffer = NULL;
    remainingInBuffer = 0;
}

bool NestedMergeJoinItr::hasNext() {
    if (remainingInBuffer == 0) {
        outputResults->clear();
        long results = executePlan();
        if (results > 0) {
            assert(outputResults->getNRows() > 0);
            currentBuffer = outputResults->getRow(0);
            remainingInBuffer = results * sVarsToReturn;
            return true;
        } else {
            return false;
        }
    }
    return true;
}

void NestedMergeJoinItr::next() {
    assert(remainingInBuffer > 0);
    remainingInBuffer -= sVarsToReturn;
}

uint64_t NestedMergeJoinItr::getElementAt(const int pos) {
    return currentBuffer[remainingInBuffer + pos];
}

size_t NestedMergeJoinItr::getTupleSize() {
    return sVarsToReturn;
}
