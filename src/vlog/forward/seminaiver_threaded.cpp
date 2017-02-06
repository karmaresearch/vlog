#include <vlog/seminaiver_threaded.h>
#include <vlog/resultjoinproc.h>

#include <boost/thread.hpp>
#include <boost/log/trivial.hpp>
#include <boost/chrono.hpp>

#include <vector>

void SemiNaiverThreaded::executeUntilSaturation(std::vector<StatIteration> &costRules) {

    //Create n threads
    std::vector<boost::thread> threads(interRuleThreads);
    bool anotherRound;
    do {
        boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
        //BOOST_LOG_TRIVIAL(info) << "Creating threads ...";
        //Create a shared datastructure to record the execution of the rules
        StatusRuleExecution_ThreadSafe status(ruleset.size());

        //Execute the rules on multiple threads
        size_t iterationBeginBlock = iteration;
        for (int i = 0; i < interRuleThreads; ++i) {
            threads[i] = boost::thread(&SemiNaiverThreaded::runThread,
                                       this,
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

        //BOOST_LOG_TRIVIAL(info) << "Another round = " << anotherRound;
        boost::chrono::duration<double> sec2 = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(warning) << "--Time round " << sec2.count() * 1000 << " " << iteration;
    } while (anotherRound);
}

bool sortByIteration(ResultJoinProcessor *p1, ResultJoinProcessor *p2) {
    return ((FinalTableJoinProcessor*)p1)->getIteration() <
           ((FinalTableJoinProcessor*)p2)->getIteration();
}

bool SemiNaiverThreaded::doGlobalConsolidation(
    StatusRuleExecution_ThreadSafe &data) {

    //Sort by iteration
    //BOOST_LOG_TRIVIAL(warning) << "Start consolidation ...";
    //boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
    std::sort(data.getTmpDerivations().begin(),
              data.getTmpDerivations().end(),
              sortByIteration);

    bool response = false;
    std::map<PredId_t, std::vector<FinalTableJoinProcessor*>> allDersByPred;
    for (const auto &el : data.getTmpDerivations()) {
        FinalTableJoinProcessor *fel = (FinalTableJoinProcessor*)el;
        if (!el->isEmpty()) {
            PredId_t pid = fel->getLiteral().getPredicate().getId();
            if (allDersByPred.count(pid)) {
                allDersByPred.find(pid)->second.push_back(fel);
            } else {
                std::vector<FinalTableJoinProcessor*> vec;
                vec.push_back(fel);
                allDersByPred.insert(make_pair(pid, vec));
            }
        }
    }

    //Determine which derivations should be consolidated in parallel
    std::vector<std::pair<PredId_t, std::vector<FinalTableJoinProcessor*>>> parallelDerivations;
    for (auto p = allDersByPred.begin(); p != allDersByPred.end(); ++p) {
        /*if (p->second.size() == 1) {
            //Is the derivation unique? Or very small?
            if (!p->second[0]->containsUnfilteredDerivation()) {
                p->second[0]->consolidate(true, true);
                BOOST_LOG_TRIVIAL(warning) << "P " << p->first << " consolidated sequentially ...";
                continue;
            }
        }*/
        parallelDerivations.push_back(make_pair(p->first, p->second));
    }

    //BOOST_LOG_TRIVIAL(warning) << "Predicates to update in parallel: " << parallelDerivations.size();
    //boost::chrono::duration<double> sec1 = boost::chrono::system_clock::now() - start;
    //BOOST_LOG_TRIVIAL(warning) << "Time consolidation so far " << sec1.count() * 1000;

    for (auto p = parallelDerivations.begin(); p != parallelDerivations.end();
            ++p) {
        for (auto cont = p->second.begin(); cont != p->second.end(); ++cont) {
            //boost::chrono::system_clock::time_point startS = boost::chrono::system_clock::now();
            auto segments = (*cont)->getAllSegments();
            //boost::chrono::duration<double> sec1 = boost::chrono::system_clock::now() - startS;
            //BOOST_LOG_TRIVIAL(warning) << "--Time getAllSegms " << sec1.count() * 1000;
            FCTable *table = (*cont)->getTable();
            for (auto segment = segments.begin(); segment != segments.end();
                    ++segment) {
                //boost::chrono::system_clock::time_point startR = boost::chrono::system_clock::now();
                auto newseg = table->retainFrom(*segment, false, nthreads);
                //boost::chrono::duration<double> sec1 = boost::chrono::system_clock::now() - startR;
                //BOOST_LOG_TRIVIAL(warning) << "--Time retain " << sec1.count() * 1000;


                //boost::chrono::system_clock::time_point startC = boost::chrono::system_clock::now();
                if (!newseg->isEmpty()) {
                    (*cont)->consolidateSegment(newseg);
                    response = true;
		    newMarked[p->first] = true;
		    marked[p->first] = true;
                }
                //boost::chrono::duration<double> sec2 = boost::chrono::system_clock::now() - startC;
                //BOOST_LOG_TRIVIAL(warning) << "--Time conso" << sec2.count() * 1000;
            }
        }
    }

    //boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    //BOOST_LOG_TRIVIAL(warning) << "Time consolidation " << sec.count() * 1000;
    return response;
}

void SemiNaiverThreaded::runThread(
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
	Literal headLiteral = ruleset[ruleToExecute].rule.getHead();
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
        boost::chrono::system_clock::time_point start = timens::system_clock::now();
        bool response = executeRule(ruleset[ruleToExecute],
                                    data->iteration,
                                    // &res);
				    NULL);
        boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
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
                    // BOOST_LOG_TRIVIAL(info) << "Iteration " << iteration;
                    data->iteration = getAtomicIteration();
                    start = timens::system_clock::now();
                    recursiveIterations++;
                    response = executeRule(ruleset[ruleToExecute],
                                           data->iteration,
                                           // &res);
					   NULL);

                    ruleset[ruleToExecute].lastExecution = data->iteration;
                    sec = boost::chrono::system_clock::now() - start;
                    ++recursiveIterations;
                    stat.iteration = data->iteration;
                    stat.rule = &ruleset[ruleToExecute].rule;
                    stat.time = sec.count() * 1000;
                    stat.derived = response;
                    mutexInsert.lock();
                    costRules->push_back(stat);
                    mutexInsert.unlock();
                    /*if (++recursiveIterations % 10 == 0) {
                        BOOST_LOG_TRIVIAL(info) << "Saturating rule " <<
                                                ruleset[currentRule].rule.tostring(program, dict) <<
                                                " " << recursiveIterations;
                    }*/
                } while (response);
                BOOST_LOG_TRIVIAL(debug) << "Rule required " << recursiveIterations << " to saturate";
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
    boost::mutex::scoped_lock lock(mutexListDer);
    SemiNaiver::saveDerivationIntoDerivationList(endTable);
}

long SemiNaiverThreaded::getNLastDerivationsFromList() {
    boost::mutex::scoped_lock lock(mutexListDer);
    return SemiNaiver::getNLastDerivationsFromList();
}

void SemiNaiverThreaded::saveStatistics(StatsRule &stats) {
    boost::mutex::scoped_lock lock(mutexStatistics);
    SemiNaiver::saveStatistics(stats);
}

StatusRuleExecution_ThreadSafe::StatusRuleExecution_ThreadSafe(const int nrules)
    : rulecount(0), nrules(nrules) {
}


int StatusRuleExecution_ThreadSafe::getRuleIDToExecute() {
    //Return -1 if no rule is available. Otherwise return the ID of the rule to
    //execute.
    boost::mutex::scoped_lock lock(mutexRules);
    if (rulecount == nrules) {
        return -1;
    }
    BOOST_LOG_TRIVIAL(debug) << "Got rule " << rulecount;
    return rulecount++;
}

void StatusRuleExecution_ThreadSafe::registerDerivations(
    ResultJoinProcessor *res) {
    boost::mutex::scoped_lock lock(mutexRules);
    tmpderivations.push_back(res);
}

FCTable *SemiNaiverThreaded::getTable(const PredId_t pred, const uint8_t card) {
    if (predicatesTables[pred] == NULL) {
        boost::mutex::scoped_lock lock(mutexGetTable);
        return SemiNaiver::getTable(pred, card);
    }
    return predicatesTables[pred];
}

FCIterator SemiNaiverThreaded::getTableFromEDBLayer(const Literal &literal) {
    PredId_t id = literal.getPredicate().getId();
    FCTable *table = predicatesTables[id];
    if (table == NULL) {
        boost::mutex::scoped_lock lock(mutexGetTable);
        return SemiNaiver::getTableFromEDBLayer(literal);
    }
    return SemiNaiver::getTableFromEDBLayer(literal);
}

bool SemiNaiverThreaded::tryLock(std::vector<PredId_t> &predicates, PredId_t idHeadPredicate) {
    PredId_t prev = (PredId_t) - 1;
    BOOST_LOG_TRIVIAL(debug) << "Getting locks ...";
    for (std::vector<PredId_t>::const_iterator itr = predicates.begin(); itr != predicates.end(); ++itr) {
        if (*itr != prev) {
            BOOST_LOG_TRIVIAL(debug) << "Getting lock " << *itr;
	    bool gotlock;
            if (*itr == idHeadPredicate) {
                gotlock = mutexes[*itr].try_lock();
            } else {
                gotlock = mutexes[*itr].try_lock_shared();
            }
	    if (! gotlock) {
		prev = (PredId_t) -1;
		for (std::vector<PredId_t>::const_iterator itr1 = predicates.begin(); itr1 != itr; ++itr1) {
		    if (*itr1 != prev) {
			if (*itr1 == idHeadPredicate) {
			    mutexes[*itr1].unlock();
			} else {
			    mutexes[*itr1].unlock_shared();
			}
		    }
		}
		return false;
	    }
        }
        prev = *itr;
    }
    BOOST_LOG_TRIVIAL(debug) << "Got locks!";
    return true;
}

void SemiNaiverThreaded::lock(std::vector<PredId_t> &predicates, PredId_t idHeadPredicate) {

    PredId_t prev = (PredId_t) - 1;
    BOOST_LOG_TRIVIAL(debug) << "Getting locks ...";
    for (std::vector<PredId_t>::const_iterator itr = predicates.begin(); itr != predicates.end(); ++itr) {
        if (*itr != prev) {
            BOOST_LOG_TRIVIAL(debug) << "Getting lock " << *itr;
            if (*itr == idHeadPredicate) {
                mutexes[*itr].lock();
            } else {
                mutexes[*itr].lock_shared();
            }
        }
        prev = *itr;
    }
    BOOST_LOG_TRIVIAL(debug) << "Got locks!";
}

void SemiNaiverThreaded::unlock(std::vector<PredId_t> &predicates, PredId_t idHeadPredicate) {
    PredId_t prev = (PredId_t) - 1;
    BOOST_LOG_TRIVIAL(debug) << "Releasing locks ...";
    for (std::vector<PredId_t>::const_iterator itr = predicates.begin(); itr != predicates.end(); ++itr) {
        if (*itr != prev) {
            BOOST_LOG_TRIVIAL(debug) << "Releasing lock " << *itr;
            if (*itr == idHeadPredicate) {
                mutexes[*itr].unlock();
            } else {
                mutexes[*itr].unlock_shared();
            }
        }
        prev = *itr;
    }
    BOOST_LOG_TRIVIAL(debug) << "Released locks!";
}
