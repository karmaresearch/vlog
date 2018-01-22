#include <vlog/seminaiver_threaded.h>
#include <vlog/resultjoinproc.h>
#include <vlog/finalresultjoinproc.h>

#include <vector>

bool SemiNaiverThreaded::executeUntilSaturation(
        std::vector<RuleExecutionDetails> &ruleset,
        std::vector<StatIteration> &costRules,
        bool fixpoint) {

    //Create n threads
    std::vector<std::thread> threads(interRuleThreads);
    bool anotherRound;
    bool newDer = false;
    do {
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        //LOG(INFOL) << "Creating threads ...";
        //Create a shared datastructure to record the execution of the rules
        StatusRuleExecution_ThreadSafe status(ruleset.size());

        //Execute the rules on multiple threads
        size_t iterationBeginBlock = iteration;
        for (int i = 0; i < interRuleThreads; ++i) {
            threads[i] = std::thread(&SemiNaiverThreaded::runThread,
                    this,
                    std::ref(ruleset),
                    &status,
                    &costRules,
                    iterationBeginBlock);
        }

        //Wait until all threads are finished
        for (int i = 0; i < interRuleThreads; ++i) {
            threads[i].join();
        }

        //Copy all the derivations produced by the rules in the KB
        anotherRound = false;
        // anotherRound = doGlobalConsolidation(status);
        for (int i = 0; i < MAX_NPREDS; i++) {
            marked[i] = newMarked[i];
            if (marked[i]) {
                anotherRound = true;
            }
            newMarked[i] = false;
        }

        //LOG(INFOL) << "Another round = " << anotherRound;
        std::chrono::duration<double> sec2 = std::chrono::system_clock::now() - start;
        LOG(WARNL) << "--Time round " << sec2.count() * 1000 << " " << iteration;
        newDer |= anotherRound;
    } while (anotherRound);
    return newDer;
}

bool sortByIteration(ResultJoinProcessor *p1, ResultJoinProcessor *p2) {
    return ((SingleHeadFinalRuleProcessor*)p1)->getIteration() <
        ((SingleHeadFinalRuleProcessor*)p2)->getIteration();
}

bool SemiNaiverThreaded::doGlobalConsolidation(
        StatusRuleExecution_ThreadSafe &data) {

    //Sort by iteration
    //LOG(WARNL) << "Start consolidation ...";
    //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    std::sort(data.getTmpDerivations().begin(),
            data.getTmpDerivations().end(),
            sortByIteration);

    bool response = false;
    std::map<PredId_t, std::vector<SingleHeadFinalRuleProcessor*>> allDersByPred;
    for (const auto &el : data.getTmpDerivations()) {
        SingleHeadFinalRuleProcessor *fel = (SingleHeadFinalRuleProcessor*)el;
        if (!el->isEmpty()) {
            PredId_t pid = fel->getLiteral().getPredicate().getId();
            if (allDersByPred.count(pid)) {
                allDersByPred.find(pid)->second.push_back(fel);
            } else {
                std::vector<SingleHeadFinalRuleProcessor*> vec;
                vec.push_back(fel);
                allDersByPred.insert(make_pair(pid, vec));
            }
        }
    }

    //Determine which derivations should be consolidated in parallel
    std::vector<std::pair<PredId_t, std::vector<SingleHeadFinalRuleProcessor*>>> parallelDerivations;
    for (auto p = allDersByPred.begin(); p != allDersByPred.end(); ++p) {
        /*if (p->second.size() == 1) {
        //Is the derivation unique? Or very small?
        if (!p->second[0]->containsUnfilteredDerivation()) {
        p->second[0]->consolidate(true, true);
        LOG(WARNL) << "P " << p->first << " consolidated sequentially ...";
        continue;
        }
        }*/
        parallelDerivations.push_back(make_pair(p->first, p->second));
    }

    //LOG(WARNL) << "Predicates to update in parallel: " << parallelDerivations.size();
    //std::chrono::duration<double> sec1 = std::chrono::system_clock::now() - start;
    //LOG(WARNL) << "Time consolidation so far " << sec1.count() * 1000;

    for (auto p = parallelDerivations.begin(); p != parallelDerivations.end();
            ++p) {
        for (auto cont = p->second.begin(); cont != p->second.end(); ++cont) {
            //std::chrono::system_clock::time_point startS = std::chrono::system_clock::now();
            auto segments = (*cont)->getAllSegments();
            //std::chrono::duration<double> sec1 = std::chrono::system_clock::now() - startS;
            //LOG(WARNL) << "--Time getAllSegms " << sec1.count() * 1000;
            FCTable *table = (*cont)->getTable();
            for (auto segment = segments.begin(); segment != segments.end();
                    ++segment) {
                //std::chrono::system_clock::time_point startR = std::chrono::system_clock::now();
                auto newseg = table->retainFrom(*segment, false, nthreads);
                //std::chrono::duration<double> sec1 = std::chrono::system_clock::now() - startR;
                //LOG(WARNL) << "--Time retain " << sec1.count() * 1000;


                //std::chrono::system_clock::time_point startC = std::chrono::system_clock::now();
                if (!newseg->isEmpty()) {
                    (*cont)->consolidateSegment(newseg);
                    response = true;
                    newMarked[p->first] = true;
                    marked[p->first] = true;
                }
                //std::chrono::duration<double> sec2 = std::chrono::system_clock::now() - startC;
                //LOG(WARNL) << "--Time conso" << sec2.count() * 1000;
            }
        }
    }

    //std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    //LOG(WARNL) << "Time consolidation " << sec.count() * 1000;
    return response;
}

void SemiNaiverThreaded::runThread(
        std::vector<RuleExecutionDetails> &ruleset,
        StatusRuleExecution_ThreadSafe *status,
        std::vector<StatIteration> *costRules,
        size_t lastExec) {

    int ruleToExecute = status->getRuleIDToExecute();
    SemiNaiver_Threadlocal *data = new SemiNaiver_Threadlocal();
    thread_data.reset(data);

    std::vector<ResultJoinProcessor*> res;

    while (ruleToExecute != -1) {

        //Get atomic iteration
        data->iteration = getAtomicIteration();
        Literal headLiteral = ruleset[ruleToExecute].rule.getFirstHead();
        PredId_t idHeadPredicate = headLiteral.getPredicate().getId();
        std::vector<PredId_t> predicates;
        predicates.push_back(idHeadPredicate);
        std::vector<Literal> body = ruleset[ruleToExecute].rule.getBody();
        for (std::vector<Literal>::const_iterator itr = body.begin(); itr != body.end(); ++itr) {
            if (itr->getPredicate().getType() == IDB) {
                predicates.push_back(itr->getPredicate().getId());
            }
        }
        // Sort predicates, to avoid deadlock
        std::sort(predicates.begin(), predicates.end());

        lock(predicates, idHeadPredicate);

        //Execute the rule
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        bool response = executeRule(ruleset[ruleToExecute],
                data->iteration,
                // &res);
        NULL);
        std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
        StatIteration stat;
        stat.iteration = data->iteration;
        stat.rule = &ruleset[ruleToExecute].rule;
        stat.time = sec.count() * 1000;
        stat.derived = response;

        //Add statistics
        mutexInsert.lock();
        costRules->push_back(stat);
        mutexInsert.unlock();

        //Change it to "lastExecution"
        // ruleset[ruleToExecute].lastExecution = lastExec;
        ruleset[ruleToExecute].lastExecution = data->iteration;

        if (response) {
            newMarked[idHeadPredicate] = true;
            marked[idHeadPredicate] = true;
            if (ruleset[ruleToExecute].rule.isRecursive()) {
                int recursiveIterations = 0;
                do {
                    // LOG(INFOL) << "Iteration " << iteration;
                    data->iteration = getAtomicIteration();
                    start = std::chrono::system_clock::now();
                    recursiveIterations++;
                    response = executeRule(ruleset[ruleToExecute],
                            data->iteration,
                            // &res);
                    NULL);

                    ruleset[ruleToExecute].lastExecution = data->iteration;
                    sec = std::chrono::system_clock::now() - start;
                    ++recursiveIterations;
                    stat.iteration = data->iteration;
                    stat.rule = &ruleset[ruleToExecute].rule;
                    stat.time = sec.count() * 1000;
                    stat.derived = response;
                    mutexInsert.lock();
                    costRules->push_back(stat);
                    mutexInsert.unlock();
                    /*if (++recursiveIterations % 10 == 0) {
                      LOG(INFOL) << "Saturating rule " <<
                      ruleset[currentRule].rule.tostring(program, dict) <<
                      " " << recursiveIterations;
                      }*/
                } while (response);
                    LOG(DEBUGL) << "Rule required " << recursiveIterations << " to saturate";
            }
        }

        unlock(predicates, idHeadPredicate);

        ruleToExecute = status->getRuleIDToExecute();
    }

    //Register the derivations
    if (!res.empty()) {
        for (auto &el : res)
            status->registerDerivations(el);
    }
}

void SemiNaiverThreaded::saveDerivationIntoDerivationList(FCTable *endTable) {
    std::lock_guard<std::mutex> lock(mutexListDer);
    SemiNaiver::saveDerivationIntoDerivationList(endTable);
}

long SemiNaiverThreaded::getNLastDerivationsFromList() {
    std::lock_guard<std::mutex> lock(mutexListDer);
    return SemiNaiver::getNLastDerivationsFromList();
}

void SemiNaiverThreaded::saveStatistics(StatsRule &stats) {
    std::lock_guard<std::mutex> lock(mutexStatistics);
    SemiNaiver::saveStatistics(stats);
}

StatusRuleExecution_ThreadSafe::StatusRuleExecution_ThreadSafe(const int nrules)
    : rulecount(0), nrules(nrules) {
    }


int StatusRuleExecution_ThreadSafe::getRuleIDToExecute() {
    //Return -1 if no rule is available. Otherwise return the ID of the rule to
    //execute.
    std::lock_guard<std::mutex> lock(mutexRules);
    if (rulecount == nrules) {
        return -1;
    }
    LOG(DEBUGL) << "Got rule " << rulecount;
    return rulecount++;
}

void StatusRuleExecution_ThreadSafe::registerDerivations(
        ResultJoinProcessor *res) {
    std::lock_guard<std::mutex> lock(mutexRules);
    tmpderivations.push_back(res);
}

FCTable *SemiNaiverThreaded::getTable(const PredId_t pred, const uint8_t card) {
    if (predicatesTables[pred] == NULL) {
        std::lock_guard<std::mutex> lock(mutexGetTable);
        return SemiNaiver::getTable(pred, card);
    }
    return predicatesTables[pred];
}

FCIterator SemiNaiverThreaded::getTableFromEDBLayer(const Literal &literal) {
    PredId_t id = literal.getPredicate().getId();
    FCTable *table = predicatesTables[id];
    if (table == NULL) {
        std::lock_guard<std::mutex> lock(mutexGetTable);
        return SemiNaiver::getTableFromEDBLayer(literal);
    }
    return SemiNaiver::getTableFromEDBLayer(literal);
}

bool SemiNaiverThreaded::tryLock(std::vector<PredId_t> &predicates, PredId_t idHeadPredicate) {
    PredId_t prev = (PredId_t) - 1;
    LOG(DEBUGL) << "Getting locks ...";
    for (std::vector<PredId_t>::const_iterator itr = predicates.begin(); itr != predicates.end(); ++itr) {
        if (*itr != prev) {
            LOG(DEBUGL) << "Getting lock " << *itr;
            bool gotlock;
            if (*itr == idHeadPredicate) {
                gotlock = mutexes[*itr].try_lock();
            } else {
                gotlock = mutexes[*itr].try_lock(); //Previously it was a shared lock
            }
            if (! gotlock) {
                prev = (PredId_t) -1;
                for (std::vector<PredId_t>::const_iterator itr1 = predicates.begin(); itr1 != itr; ++itr1) {
                    if (*itr1 != prev) {
                        if (*itr1 == idHeadPredicate) {
                            mutexes[*itr1].unlock();
                        } else {
                            mutexes[*itr1].unlock(); //Previously it was a shared lock
                        }
                    }
                }
                return false;
            }
        }
        prev = *itr;
    }
    LOG(DEBUGL) << "Got locks!";
    return true;
}

void SemiNaiverThreaded::lock(std::vector<PredId_t> &predicates, PredId_t idHeadPredicate) {

    PredId_t prev = (PredId_t) - 1;
    LOG(DEBUGL) << "Getting locks ...";
    for (std::vector<PredId_t>::const_iterator itr = predicates.begin(); itr != predicates.end(); ++itr) {
        if (*itr != prev) {
            LOG(DEBUGL) << "Getting lock " << *itr;
            if (*itr == idHeadPredicate) {
                mutexes[*itr].lock();
            } else {
                mutexes[*itr].lock(); //Previously it was a shared lock
            }
        }
        prev = *itr;
    }
    LOG(DEBUGL) << "Got locks!";
}

void SemiNaiverThreaded::unlock(std::vector<PredId_t> &predicates, PredId_t idHeadPredicate) {
    PredId_t prev = (PredId_t) - 1;
    LOG(DEBUGL) << "Releasing locks ...";
    for (std::vector<PredId_t>::const_iterator itr = predicates.begin(); itr != predicates.end(); ++itr) {
        if (*itr != prev) {
            LOG(DEBUGL) << "Releasing lock " << *itr;
            if (*itr == idHeadPredicate) {
                mutexes[*itr].unlock();
            } else {
                mutexes[*itr].unlock(); //Previously it was a shared lock
            }
        }
        prev = *itr;
    }
    LOG(DEBUGL) << "Released locks!";
}
