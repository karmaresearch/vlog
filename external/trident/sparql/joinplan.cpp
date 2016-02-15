#include <trident/sparql/joinplan.h>
#include <trident/sparql/filter.h>
#include <trident/sparql/sparqloperators.h>
#include <trident/kb/querier.h>

#include <tridentcompr/utils/utils.h>

#include <iostream>
#include <cstdio>
#include <algorithm>

NestedJoinPlan::NestedJoinPlan(Pattern &p2, Querier *q,
                               std::vector<std::vector<int>> &posToCopy,
                               std::vector<std::pair<int, int>> &patternJoins,
                               std::vector<int> &posToReturn) {

    patterns = new Pattern[2];
    patterns[1] = p2;
    nPatterns = 2;

    int idx = q->getIndex(patterns[1].subject(), patterns[1].predicate(), patterns[1].object());
    int *perm = q->getInvOrder(idx);
    patterns[1].idx(idx);

    filters = NULL;
    //erase name variables if there are any
    if (patterns[1].subject() < 0)
        patterns[1].subject(-1);
    if (patterns[1].predicate() < 0)
        patterns[1].predicate(-1);
    if (patterns[1].object() < 0)
        patterns[1].object(-1);


    posVarsToCopyInIdx = new int*[2];
    posVarsToCopyInPattern = new int*[2];
    nPosVarsToCopyInIdx = new int[2];
    nPosVarsToCopyInIdx[0] = posToCopy[0].size();
    posVarsToCopyInIdx[0] = new int[nPosVarsToCopyInIdx[0]];
    posVarsToCopyInPattern[0] = NULL;
    for (int i = 0; i < nPosVarsToCopyInIdx[0]; ++i) {
        posVarsToCopyInIdx[0][i] = posToCopy[0][i];
    }
    nPosVarsToCopyInIdx[1] = posToCopy[1].size();
    posVarsToCopyInIdx[1] = new int[nPosVarsToCopyInIdx[1]];
    posVarsToCopyInPattern[1] = NULL;
    for (int i = 0; i < nPosVarsToCopyInIdx[1]; ++i) {
        posVarsToCopyInIdx[1][i] = perm[posToCopy[1][i]];
    }

    sizeRowsPatterns = new int[2];
    sizeRowsPatterns[0] = 0;
    sizeRowsPatterns[1] = nPosVarsToCopyInIdx[0];

    nJoins = new int[2];
    nJoins[1] = patternJoins.size();

    joins = new JoinPoint*[2];
    joins[0] = NULL;
    joins[1] = new JoinPoint[patternJoins.size()];

    //sort the patternJoins by join position
    if (patternJoins.size() > 1)
        std::sort(patternJoins.begin(), patternJoins.end(), SorterByPerm(perm));

    for (int i = 0; i < patternJoins.size(); ++i) {
        joins[1][i].posRow = patternJoins[i].first;
        joins[1][i].posPattern = patternJoins[i].second;
        joins[1][i].sourcePattern = 0;
        joins[1][i].lastValue = 0;
        if (patternJoins.size() == 2) {
            joins[1][i].sourcePosIndex = 1 + i;
        } else {
            joins[1][i].sourcePosIndex = 2;
        }
        joins[1][i].index = idx;
        joins[1][i].posIndex = perm[patternJoins[i].second];
        joins[1][i].merge = joins[1][i].posIndex == joins[1][i].sourcePosIndex;
    }

    nPosVarsToReturn = posToReturn.size();
    posVarsToReturn = new int[nPosVarsToReturn];
    for (int i = 0; i < nPosVarsToReturn; ++i) {
        posVarsToReturn[i] = posToReturn[i];
    }
}

bool cmp(JoinPoint *o1, JoinPoint *o2) {
    return o1->posIndex < o2->posIndex;
}

bool cmpCard(pair<Pattern *, uint64_t> o1, pair<Pattern *, uint64_t> o2) {
    return o1.second < o2.second;
}

bool cmpJoins(PatternInfo i1, PatternInfo i2) {
    int res = (i1.nvars - i1.njoins) - (i2.nvars - i2.njoins);
    if (res == 0) {
//        if ((i1.nvars - i1.njoins) > 0) {
        return i1.card < i2.card;
//        } else {
//            return i1.nvars > i2.nvars;
//        }
    } else {
        return res < 0;
    }
}

void NestedJoinPlan::rearrange(std::vector<std::pair<Pattern *, uint64_t> > &patterns) {
    //Sort the patterns in ascending order
    std::sort(patterns.begin(), patterns.end(), cmpCard);

    //Construct the new list of patterns. Start from the first and then pick the
    //first that joins with it
    std::vector<std::pair<Pattern *, uint64_t> > newList;
    Pattern *currentPattern = patterns[0].first;
    newList.push_back(patterns[0]);
    vector<string> vars;
    currentPattern->addVarsTo(vars);
    patterns.erase(patterns.begin());

    while (patterns.size() > 0) {
        vector<PatternInfo> possibleJoins;
        for (std::vector<std::pair<Pattern *, uint64_t> >::iterator itr = patterns.begin();
                itr != patterns.end(); ++itr) {
            //Check if the pattern joins with current patterns. If so, then we remove it.
            // int njoins = itr->first->joinsWith(currentPattern); Ceriel: Why not:
            int njoins = itr->first->joinsWith(vars);
            if (njoins > 0) {
                PatternInfo info;
                info.pos = itr;
                info.njoins = njoins;
                info.nvars = itr->first->getNVars();
                info.card = itr->second;
                possibleJoins.push_back(info);
            }
        }

        //Pick the best join
        bool found = false;
        if (possibleJoins.size() > 0) {
            std::sort(possibleJoins.begin(), possibleJoins.end(), cmpJoins);

            vector<std::pair<Pattern *, uint64_t> >::iterator p = possibleJoins[0].pos;
            newList.push_back(*p);
            currentPattern = p->first;
            currentPattern->addVarsTo(vars);
            patterns.erase(p);
            found = true;
//        }
        }

        if (!found) { //Add the first.
            newList.push_back(patterns[0]);
            currentPattern = patterns[0].first;
            currentPattern->addVarsTo(vars);
            patterns.erase(patterns.begin());
        }
    }

    //Copy the content of newList into pattern
    patterns = newList;
}

std::vector<int> NestedJoinPlan::reorder(std::vector<Pattern*> patterns,
        std::vector<std::shared_ptr<SPARQLOperator>> scans) {
    //Rearrange the order execution of the patterns
    //timens::system_clock::time_point start = timens::system_clock::now();

    std::vector<std::pair<Pattern *, uint64_t> > pairs;
    BOOST_LOG_TRIVIAL(debug) << "UNOPTIMIZED ORDER OF PATTERNS:";
    for (int i = 0; i < patterns.size(); ++i) {
        Pattern *p = patterns[i];
        uint64_t card = std::static_pointer_cast<Scan>(scans[i])->estimateCost();
        pairs.push_back(std::make_pair(p, card));
        BOOST_LOG_TRIVIAL(debug) << card << " " << p->toString();
    }

    //Rearrange the patterns making sure that: there is always a join and the
    //smallest are picked first
    NestedJoinPlan::rearrange(pairs);

    BOOST_LOG_TRIVIAL(debug) << "OPTIMIZED ORDER OF PATTERNS:";
    std::vector<int> output;
    for (vector<std::pair<Pattern*, uint64_t>>::iterator itr = pairs.begin();
            itr != pairs.end(); ++itr) {
        Pattern *p = itr->first;

        //Find the ID
        int idx = -1;
        for (int j = 0; j < patterns.size(); ++j) {
            if (patterns[j] == p) {
                idx = j;
                break;
            }
        }
        assert(idx != -1);
        output.push_back(idx);

        BOOST_LOG_TRIVIAL(debug) << " " << itr->first->toString() << " card: " << itr->second;
    }

    //boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
    //                                      - start;
    //BOOST_LOG_TRIVIAL(info) << "Time optimizing the query = " << sec.count() * 1000
    //                        << " milliseconds";

    return output;
}

std::vector<string> JoinPlan::mergeFields(std::vector<string> s1,
        std::vector<string> s2) {
    if (s1.size() == 0) {
        return s2;
    } else {
        for (std::vector<string>::iterator itr = s2.begin(); itr != s2.end();
                ++itr) {
            bool found = false;
            for (std::vector<string>::iterator itr1 = s1.begin(); itr1 != s1.end();
                    ++itr1) {
                if (*itr1 == *itr) {
                    found = true;
                    break;
                }
            }
            if (!found)
                s1.push_back(*itr);
        }
        return s1;
    }
}


void JoinPlan::prepare(std::vector<std::shared_ptr<SPARQLOperator>> children) {
    std::vector<string> empty;
    prepare(children, empty);
}

void JoinPlan::prepare(std::vector<std::shared_ptr<SPARQLOperator>> children,
                       std::vector<string> &projections) {
    std::vector<string> currentVars;
    for (int i = 0; i < children.size(); ++i) {
        joins.push_back(std::vector<JoinPoint>());
        std::vector<uint8_t> pc;

        if (i == 0) {
            //Copy all variables
            const size_t n = children[0]->getOutputTupleSize();
            // BOOST_LOG_TRIVIAL(debug) << "children[0]->getOutputTupleSize() = " << n;
            currentVars = children[0]->getTupleFieldsIDs();
            // BOOST_LOG_TRIVIAL(debug) << "currentVars.size() = " << currentVars.size();
            for (uint8_t j = 0; j < n; ++j)
                pc.push_back(j);
        } else {
            //Calculate the joins etc.
            std::vector<string> newVars = children[i]->getTupleFieldsIDs();
            for (uint8_t j = 0; j < newVars.size(); ++j) {
                bool found = false;
                int idxVar = 0;
                for (; idxVar < currentVars.size(); ++idxVar) {
                    if (currentVars[idxVar] == newVars[j]) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    //Set up join
                    JoinPoint jp;
                    // BOOST_LOG_TRIVIAL(debug) << "posRow = " << idxVar << ", posPattern = " << j;
                    jp.posRow = idxVar;
                    jp.posPattern = j;
                    joins[i].push_back(jp);
                } else {
                    if (i == children.size() - 1 && projections.size() != 0) {
                        bool found = false;
                        for (int m = 0; m < projections.size(); ++m) {
                            if (newVars[j] == projections[m]) {
                                found = true;
                                break;
                            }
                        }
                        if (found)
                            pc.push_back(j);
                    } else {
                        pc.push_back(j);
                    }
                    currentVars.push_back(newVars[j]);
                }
            }
        }

        posVarsToCopy.push_back(pc);
    }

    //Copy in output all variables
    if (projections.size() != 0) {
        for (int j = 0; j < projections.size(); ++j) {
            bool found = false;
            for (uint8_t i = 0; i < currentVars.size(); ++i) {
                if (currentVars[i] == projections[j]) {
		    posVarsToReturn.push_back(i);
                    break;
                }
            }
            assert(found);
        }
    } else {
        //copy all
        for (uint8_t i = 0; i < currentVars.size(); ++i) {
            posVarsToReturn.push_back(i);
        }
    }

}

void NestedJoinPlan::prepare(Querier *q, std::vector<Pattern *> patterns,
                             std::vector<Filter *>filters, std::vector<string> projections) {
    int nPatterns = patterns.size();

    // Prepare for every pattern a list of variables, vars to join, etc.
    int *idxs = new int[nPatterns];
    int *sizeRowPattern = new int[nPatterns];
    memset(sizeRowPattern, 0, sizeof(int) * nPatterns);
    int **posVarsToCopy = new int*[nPatterns];
    int **posVarsToCopyOrig = new int*[nPatterns];
    int *nPosVarsToCopy = new int[nPatterns];

    JoinPoint **joins = new JoinPoint*[nPatterns];
    memset(joins, 0, sizeof(JoinPoint *) * nPatterns);
    int *nJoins = new int[nPatterns];
    memset(nJoins, 0, sizeof(int) * nPatterns);

    std::vector<string> headerRow;
    std::vector<int> pHeaderRow;
    long tmpS, tmpP, tmpO;

    for (int i = 0; i < nPatterns; ++i) {
        Pattern *p = patterns[i];
        if (i == 0) {
            //determine which index to use
            idxs[i] = q->getIndex(p->subject(), p->predicate(), p->object());
            int *inv_perm = q->getInvOrder(idxs[i]);

            posVarsToCopy[i] = new int[p->getNVars()];
            posVarsToCopyOrig[i] = new int[p->getNVars()];
            nPosVarsToCopy[i] = p->getNVars();
            for (int j = 0; j < p->getNVars(); ++j) {
                int pos = p->posVar(j);
                posVarsToCopy[i][j] = inv_perm[pos];
                posVarsToCopyOrig[i][j] = pos;
                headerRow.push_back(p->getVar(j));
                pHeaderRow.push_back(0);
            }
        } else {
            // In this case I need to split the variables between the ones
            // to copy and the ones to join
            int tmpCopyVars[3];
            int sTmpCopyVars = 0;
            std::vector<JoinPoint*> joins_on_pattern;
            sizeRowPattern[i] = (int)headerRow.size();

            for (int j = 0; j < p->getNVars(); ++j) {
                string name = p->getVar(j);

                // Check in the current row whether the join should be made
                int found = -1;
                for (unsigned int m = 0; m < headerRow.size(); ++m) {
                    string currentName = headerRow[m];
                    if (currentName == name) {
                        found = m;
                        break;
                    }
                }
                if (found != -1) {
                    JoinPoint *jp = new JoinPoint();
                    jp->posRow = found;
                    jp->sourcePattern = pHeaderRow[found];
                    jp->posPattern = p->posVar(j);
                    joins_on_pattern.push_back(jp);
                } else {
                    tmpCopyVars[sTmpCopyVars++] = p->posVar(j);
                    headerRow.push_back(p->getVar(j));
                    pHeaderRow.push_back(i);
                }
            }

            // Determine which index to use
            tmpS = p->subject();
            tmpP = p->predicate();
            tmpO = p->object();
            int idxJoin = -2;

            //Order the joins by the order in which the vars appear in the previous patterns
            if (joins_on_pattern.size() > 1) {
                std::sort(joins_on_pattern.begin(), joins_on_pattern.end(), SorterByJoinOrder());
            }

            for (unsigned int r = 0; r < joins_on_pattern.size(); ++r) {
                JoinPoint *jp = joins_on_pattern[r];
                switch (jp->posPattern) {
                case 0:
                    tmpS = idxJoin--;
                    break;
                case 1:
                    tmpP = idxJoin--;
                    break;
                case 2:
                    tmpO = idxJoin--;
                }
            }

            // Determine the index
            idxs[i] = q->getIndex(tmpS, tmpP, tmpO);
            BOOST_LOG_TRIVIAL(debug) << "Selected index " << idxs[i] << " for " << tmpS << " " << tmpP << " " << tmpO;
            int *inv_perm = q->getInvOrder(idxs[i]);

            for (unsigned int r = 0; r < joins_on_pattern.size(); ++r) {
                JoinPoint *jp = joins_on_pattern[r];
                int posVar = jp->posPattern;
                jp->posIndex = inv_perm[posVar];
                jp->index = idxs[i];

                // Determine the position in the source index
                int *source_inv_perm = q->getInvOrder(idxs[jp->sourcePattern]);
                int pos_source = patterns[jp->sourcePattern]->posVar(
                                     headerRow[jp->posRow]);
                jp->sourcePosIndex = source_inv_perm[pos_source];
                // Can I do a merge?
                bool merge = canIApplyMergeJoin(q, &patterns, i, jp, idxs);
                jp->merge = merge;
            }

            // Sort the joins so that they are ordered by position in the
            // index.
            std::sort(joins_on_pattern.begin(), joins_on_pattern.end(), cmp);

            //Copy the data
            joins[i] = new JoinPoint[joins_on_pattern.size()];
            nJoins[i] = (int)joins_on_pattern.size();
            for (int j = 0; j < nJoins[i]; ++j) {
                joins[i][j] = *(joins_on_pattern[j]);
                delete joins_on_pattern[j];
            }

            posVarsToCopy[i] = new int[sTmpCopyVars];
            posVarsToCopyOrig[i] = new int[sTmpCopyVars];
            nPosVarsToCopy[i] = sTmpCopyVars;
            for (int m = 0; m < sTmpCopyVars; ++m) {
                posVarsToCopy[i][m] = inv_perm[tmpCopyVars[m]];
                posVarsToCopyOrig[i][m] = tmpCopyVars[m];
            }
        }


        //Invert the order of the vars to copy to improve the merge joins
        if (nPosVarsToCopy[i] == 2 && posVarsToCopy[i][0] > posVarsToCopy[i][1]) {
            //swap them
            int b = posVarsToCopy[i][0];
            int b2 = posVarsToCopyOrig[i][0];
            posVarsToCopy[i][0] = posVarsToCopy[i][1];
            posVarsToCopyOrig[i][0] = posVarsToCopyOrig[i][1];
            posVarsToCopy[i][1] = b;
            posVarsToCopyOrig[i][1] = b2;

            std::string v = headerRow[headerRow.size() - 2];
            headerRow[headerRow.size() - 2] = headerRow[headerRow.size() - 1];
            headerRow[headerRow.size() - 1] = v;
        }
    }

    // Check vars to return
    int *posVarsToReturn = new int[128];
    int sPosVarsToReturn = 0;
    for (unsigned int i = 0; i < projections.size(); ++i) {
        string name = projections[i];
        for (unsigned int j = 0; j < headerRow.size(); ++j) {
            if (name == headerRow[j]) {
                posVarsToReturn[sPosVarsToReturn++] = j;
                break;
            }
        }
    }

    // Copy the local variables in the global ones
    this->posVarsToReturn = new int[sPosVarsToReturn];
    this->nPosVarsToReturn = sPosVarsToReturn;
    for (int y = 0; y < sPosVarsToReturn; ++y) {
        this->posVarsToReturn[y] = posVarsToReturn[y];
    }
    delete[] posVarsToReturn;

    this->sizeRowsPatterns = sizeRowPattern;

    this->posVarsToCopyInIdx = posVarsToCopy;
    this->posVarsToCopyInPattern = posVarsToCopyOrig;
    this->nPosVarsToCopyInIdx = nPosVarsToCopy;

    this->joins = joins;
    this->nJoins = nJoins;

    this->nPatterns = nPatterns;
    this->patterns = new Pattern[nPatterns];
    this->filters = new Filter*[nPatterns];
    for (int i = 0; i < nPatterns; ++i) {
        this->patterns[i] = *patterns[i];
        this->patterns[i].idx(idxs[i]);
        this->filters[i] = filters[i];
    }

    delete[] idxs;
}

bool NestedJoinPlan::canIApplyMergeJoin(Querier *q, std::vector<Pattern *> *patterns,
                                        int patternId, JoinPoint *j, int *idxs) {
    if (j->posIndex == 0 || j->sourcePosIndex == 0)
        return false;

    if (j->sourcePattern != patternId - 1)
        return false;

    if (j->posIndex == 2 && j->sourcePosIndex != 2) {
        return false;
    }

    //Check if there are other variables before sourcePosIndex that can break the ordering
    //I accept them only if they are already bound
    int* positionsSourceIndex = q->getOrder(idxs[patternId - 1]);
    std::vector<int> *pos_vars = (*patterns)[j->sourcePattern]->getPosVars();
    for (int i = 0; i < j->sourcePosIndex; ++i) {
        int pos = positionsSourceIndex[i];
        int idxVar = -1;
        for (unsigned int j = 0; j < pos_vars->size(); ++j) {
            if ((*pos_vars)[j] == pos) {
                idxVar = j;
            }
        }
        if (idxVar != -1) {
            //Is this variable bound in the previous patterns?
            bool isBound = false;
            int idxSourcePattern = j->sourcePattern;
            string var = (*patterns)[idxSourcePattern]->getVar(idxVar);
            for (int j = 0; j < idxSourcePattern; ++j) {
                if (patterns->at(j)->containsVar(var)) {
                    isBound = true;
                    break;
                }
            }

            if (!isBound)
                return false;
        }
    }

    /*if (j->posIndex == 1) {
        //The first element in the index is a constant
        int firstPos = q->getOrder(j->index)[0];
        bool found = false;
        std::vector<int> *pos_vars = (*patterns)[patternId]->getPosVars();
        for (unsigned int i = 0; i < pos_vars->size(); ++i) {
            if ((*pos_vars)[i] == firstPos) {
                found = true;
                break;
            }
        }
        if (found) {
            return false;
        }

        //In the source index, all elements before are constants
        int *sourceIdxOrder = q->getOrder(idxs[j->sourcePattern]);
        for (int i = 0; i < j->sourcePosIndex; ++i) {
            found = false;
            int pos = sourceIdxOrder[i];
            std::vector<int> *pos_vars = (*patterns)[j->sourcePattern]->getPosVars();
            for (unsigned int i = 0; i < pos_vars->size(); ++i) {
                if ((*pos_vars)[i] == pos) {
                    found = true;
                    break;
                }
            }
            if (found) {
                return false;
            }
        }

    } else if (j->posIndex == 2) {
        int firstPos = q->getOrder(j->index)[0];
        int secondPos = q->getOrder(j->index)[1];
        bool found = false;
        std::vector<int> *pos_vars = (*patterns)[j->sourcePattern]->getPosVars();
        for (unsigned int i = 0; i < pos_vars->size(); ++i) {
            int varPos = (*pos_vars)[i];
            if (varPos == firstPos || varPos == secondPos) {
                found = true;
                break;
            }
        }
        if (found) {
            return false;
        }
    }*/

    return true;
}

void NestedJoinPlan::printInfo() {
    //Print some info on the plan
    for (int i = 0; i < nPatterns; ++i) {
        printf("Pattern %ld %ld %ld\n", patterns[i].subject(),
               patterns[i].predicate(), patterns[i].object());
        printf("->idx=%d\n", patterns[i].idx());

        for (unsigned int j = 0; j < patterns[i].getPosVars()->size(); ++j) {
            printf("Pos var %d\n", (*patterns[i].getPosVars())[j]);
        }

        for (int j = 0; j < nPosVarsToCopyInIdx[i]; ++j) {
            printf("->VarPosIdx=%d\n", posVarsToCopyInIdx[i][j]);
        }
    }

    for (int i = 0; i < nPosVarsToReturn; ++i) {
        printf("Var to return %d\n", posVarsToReturn[i]);
    }

    for (int i = 0; i < nPatterns; ++i) {
        printf("join length=%d\n", nJoins[i]);
        for (int j = 0; j < nJoins[i]; ++j) {
            JoinPoint jp = joins[i][j];
            printf("%d %d\n", jp.sourcePattern, jp.sourcePosIndex);
        }
    }
}
