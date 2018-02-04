#include <vlog/seminaiver.h>
#include <vlog/concepts.h>
#include <vlog/joinprocessor.h>
#include <vlog/fctable.h>
#include <vlog/fcinttable.h>
#include <vlog/filterer.h>
#include <vlog/finalresultjoinproc.h>
#include <vlog/extresultjoinproc.h>
#include <trident/model/table.h>
#include <kognac/consts.h>
#include <kognac/utils.h>

#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include <sstream>
#include <unordered_set>

void SemiNaiver::createGraphRuleDependency(std::vector<int> &nodes,
        std::vector<std::pair<int, int>> &edges) {
    //Add the nodes and edges
    nodes.clear();
    edges.clear();

    std::vector<Rule> rules = program->getAllRules();

    std::vector<int> *definedBy = new std::vector<int>[MAX_NPREDS];
    for (int i = 0; i < rules.size(); i++) {
        Rule ri = rules[i];
        PredId_t pred = ri.getFirstHead().getPredicate().getId();
        std::vector<Literal> body = ri.getBody();
        for (std::vector<Literal>::const_iterator itr = body.begin(); itr != body.end(); ++itr) {
            Predicate p = itr->getPredicate();
            if (p.getType() == IDB) {
                // Only add "interesting" rules: ones that have an IDB predicate in the RHS.
                nodes.push_back(i);
                definedBy[pred].push_back(i);
                LOG(INFOL) << " Rule " << i << ": " << ri.tostring(program, &layer);
                break;
            }
        }
    }
    for (int i = 0; i < rules.size(); ++i) {
        Rule ri = rules[i];
        std::vector<Literal> body = ri.getBody();
        for (std::vector<Literal>::const_iterator itr = body.begin(); itr != body.end(); ++itr) {
            Predicate pred = itr->getPredicate();
            if (pred.getType() == IDB) {
                PredId_t id = pred.getId();
                for (std::vector<int>::const_iterator k = definedBy[id].begin(); k != definedBy[id].end(); ++k) {
                    edges.push_back(make_pair(*k, i));
                }
            }
        }
        /*
        // Also add dependency to other rules defining the same predicate?
        PredId_t id = ri.getHead().getPredicate().getId();
        for (std::vector<int>::const_iterator k = definedBy[id].begin(); k != definedBy[id].end(); ++k) {
        if (*k != i) {
        edges.push_back(make_pair(*k, i));
        }
        }
        */
    }
    delete[] definedBy;
}

string set_to_string(std::unordered_set<int> s) {
    ostringstream oss("");
    for (std::unordered_set<int>::const_iterator k = s.begin(); k != s.end(); ++k) {
        oss << *k << " ";
    }
    return oss.str();
}

SemiNaiver::SemiNaiver(std::vector<Rule> ruleset, EDBLayer &layer,
        Program *program, bool opt_intersect, bool opt_filtering,
        bool multithreaded, bool restrictedChase, int nthreads, bool shuffle) :
    opt_intersect(opt_intersect),
    opt_filtering(opt_filtering),
    multithreaded(multithreaded),
    restrictedChase(restrictedChase),
    running(false),
    layer(layer),
    program(program),
    nthreads(nthreads) {

        TableFilterer::setOptIntersect(opt_intersect);
        memset(predicatesTables, 0, sizeof(TupleTable*)*MAX_NPREDS);

        LOG(DEBUGL) << "Running SemiNaiver, opt_intersect = " << opt_intersect << ", opt_filtering = " << opt_filtering << ", multithreading = " << multithreaded << ", shuffle = " << shuffle;

        int ruleid = 0;
        for (std::vector<Rule>::iterator itr = ruleset.begin(); itr != ruleset.end();
                ++itr) {
            RuleExecutionDetails *d = new RuleExecutionDetails(*itr, ruleid++);
            std::vector<Literal> bodyLiterals = itr->getBody();
            for (std::vector<Literal>::iterator itr = bodyLiterals.begin();
                    itr != bodyLiterals.end(); ++itr) {
                if (itr->getPredicate().getType() == IDB)
                    d->nIDBs++;
            }
            if (d->nIDBs != 0)
                this->allIDBRules.push_back(*d);
            else
                this->allEDBRules.push_back(*d);
            delete d;
        }

        if (multithreaded) {
            // newDetails will ultimately contain the new rule order.
            std::vector<int> newDetails;
            for (size_t i = 0; i < this->allIDBRules.size(); i++) {
                newDetails.push_back(i);
            }

            if (!shuffle) {
                std::vector<int> *definedBy = new std::vector<int>[MAX_NPREDS];
                // First, determine which rules compute which predicate.
                for (int i = 0; i < this->allIDBRules.size(); i++) {
                    PredId_t pred = this->allIDBRules[i].rule.getFirstHead().getPredicate().getId();
                    definedBy[pred].push_back(i);
                }

                // Now, determine, for each rule, which rules cannot be executed concurrently.
                // Two rules cannot be executed concurrently if:
                // - they compute the same predicate, or
                // - the RHS of one contains the predicate computed by the other.
                std::vector<std::unordered_set<int>> excludes;
                std::vector<int> nRulesForPredicate;
                for (int i = 0; i < this->allIDBRules.size(); i++) {
                    const Rule *r = &(this->allIDBRules[i].rule);
                    PredId_t pred = r->getFirstHead().getPredicate().getId();
                    nRulesForPredicate.push_back(definedBy[pred].size());
                    std::unordered_set<int> exclude;
                    // Exclude rules that compute the same predicate.
                    for (std::vector<int>::const_iterator k = definedBy[pred].begin(); k != definedBy[pred].end(); ++k) {
                        exclude.insert(*k);
                    }
                    std::vector<Literal> body = r->getBody();
                    // Exclude rules that compute a predicate that is used in the RHS.
                    for (std::vector<Literal>::const_iterator itr = body.begin(); itr != body.end(); ++itr) {
                        Predicate pred = itr->getPredicate();
                        if (pred.getType() == IDB) {
                            PredId_t id = pred.getId();
                            for (std::vector<int>::const_iterator k = definedBy[id].begin(); k != definedBy[id].end(); ++k) {
                                exclude.insert(*k);
                            }
                        }
                    }
                    excludes.push_back(exclude);
                }

                delete[] definedBy;

                // Now, first sort the rules so that predicates that are computed in the most rules come first.
                std::sort(newDetails.begin(), newDetails.end(), [nRulesForPredicate](int a, int b) {
                        return nRulesForPredicate[a] > nRulesForPredicate[b];
                        });

                // Now, create groups of rules that can be computed concurrently.
                std::unordered_set<int> blocked;
                std::vector<int> newOrder;
                while (newOrder.size() < this->allIDBRules.size()) {
                    LOG(DEBUGL) << "New round";
                    int count = 0;
                    for (int i = 0; i < this->allIDBRules.size(); i++) {
                        auto search = blocked.find(newDetails[i]);
                        if (search == blocked.end()) {
                            // This rule is currently not blocked yet, so we add it to the group.
                            LOG(DEBUGL) << "Adding rule " << newDetails[i];
                            newOrder.push_back(newDetails[i]);
                            // Add the rules that cannot be executed concurrently to the blocked rules.
                            blocked.insert(excludes[newDetails[i]].begin(), excludes[newDetails[i]].end());
                            count++;
                            // Hack, really, to limit the size of the groups, in an attempt to not end up
                            // with a couple of large groups and then a large number of (too) small groups.
                            if (count >= 4) break;
                        }
                    }
                    blocked.clear();
                    blocked.insert(newOrder.begin(), newOrder.end());
                }
                newDetails = newOrder;
            } else {
                // Just shuffle all the rules.
                std::random_shuffle(newDetails.begin(), newDetails.end());
            }
            std::vector<RuleExecutionDetails> saved = this->allIDBRules;
            this->allIDBRules.clear();
            for (size_t i = 0; i < newDetails.size(); i++) {
                this->allIDBRules.push_back(saved[newDetails[i]]);
            }
        }
    }

bool SemiNaiver::executeRules(std::vector<RuleExecutionDetails> &edbRuleset,
        std::vector<RuleExecutionDetails> &ruleset,
        std::vector<StatIteration> &costRules,
        bool fixpoint) {
#if DEBUG
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
#endif
    bool newDer = false;
    for (int i = 0; i < edbRuleset.size(); ++i) {
        newDer |= executeRule(edbRuleset[i], iteration, NULL);
        iteration++;
    }
#if DEBUG
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "Runtime EDB rules ms = " << sec.count() * 1000;
#endif

    if (ruleset.size() > 0) {
        newDer |= executeUntilSaturation(ruleset, costRules, fixpoint);
    }
    return newDer;
}

void SemiNaiver::run(size_t lastExecution, size_t it) {
    running = true;
    iteration = it;
    startTime = std::chrono::system_clock::now();
#ifdef WEBINTERFACE
    statsLastIteration = -1;
#endif
    listDerivations.clear();

    //Prepare for the execution
#if DEBUG
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    LOG(DEBUGL) << "Optimizing ruleset...";
#endif
    for (std::vector<RuleExecutionDetails>::iterator itr = allIDBRules.begin();
            itr != allIDBRules.end();
            ++itr) {
        LOG(DEBUGL) << "Optimizing rule " << itr->rule.tostring(NULL, NULL);
        itr->createExecutionPlans();
        itr->calculateNVarsInHeadFromEDB();
        itr->lastExecution = lastExecution;

        for (int i = 0; i < itr->orderExecutions.size(); ++i) {
            string plan = "";
            for (int j = 0; j < itr->orderExecutions[i].plan.size(); ++j) {
                plan += string(" ") +
                    itr->orderExecutions[i].plan[j]->tostring(program, &layer);
            }
            LOG(DEBUGL) << "-->" << plan;
        }
    }
    for (std::vector<RuleExecutionDetails>::iterator itr = allEDBRules.begin();
            itr != allEDBRules.end();
            ++itr) {
        itr->createExecutionPlans();
    }
    for (auto el : allIDBRules)
        LOG(DEBUGL) << el.rule.tostring(program, &layer);

    //Setup the datastructures to handle the chase
    std::vector<RuleExecutionDetails> allrules;
    std::copy(allEDBRules.begin(), allEDBRules.end(), std::back_inserter(allrules));
    std::copy(allIDBRules.begin(), allIDBRules.end(), std::back_inserter(allrules));
    chaseMgmt = std::shared_ptr<ChaseMgmt>(new ChaseMgmt(allrules,
                restrictedChase));
#if DEBUG
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "Runtime ruleset optimization ms = " << sec.count() * 1000;
#endif

    //Used for statistics
    std::vector<StatIteration> costRules;

    if (restrictedChase && program->areExistentialRules()) {
        //Split the program: First execute the rules without existential
        //quantifiers, then all the others
        std::vector<RuleExecutionDetails> originalEDBruleset = allEDBRules;
        std::vector<RuleExecutionDetails> originalRuleset = allIDBRules;

        //Only non-existential rules
        std::vector<RuleExecutionDetails> tmpEDBRules;
        for(auto &r : originalEDBruleset) {
            if (!r.rule.isExistential())  {
                tmpEDBRules.push_back(r);
            }
        }
        std::vector<RuleExecutionDetails> tmpIDBRules;
        for(auto &r : originalRuleset) {
            if (!r.rule.isExistential()) {
                tmpIDBRules.push_back(r);
            }
        }
        //Now execute the existential rules
        std::vector<RuleExecutionDetails> tmpExtEDBRules;
        for(auto &r : originalEDBruleset) {
            if (r.rule.isExistential())  {
                tmpExtEDBRules.push_back(r);
            }
        }
        std::vector<RuleExecutionDetails> tmpExtIDBRules;
        for(auto &r : originalRuleset) {
            if (r.rule.isExistential()) {
                tmpExtIDBRules.push_back(r);
            }
        }
        int loopNr = 0;
        std::vector<RuleExecutionDetails> emptyRuleset;
        while (true) {
            bool resp1;
            if (loopNr == 0)
                resp1 = executeRules(tmpEDBRules, tmpIDBRules, costRules, true);
            else
                resp1 = executeRules(emptyRuleset, tmpIDBRules, costRules, true);
            bool resp2;
            if (loopNr == 0)
                resp2 = executeRules(tmpExtEDBRules, tmpExtIDBRules, costRules, false);
            else
                resp2 = executeRules(emptyRuleset, tmpExtIDBRules, costRules, false);
            if (!resp1 && !resp2) {
                break; //Fix-point
            }
            loopNr++;
        }
    } else {
        executeRules(allEDBRules, allIDBRules, costRules, true);
    }

    running = false;
    LOG(INFOL) << "Finished process. Iterations=" << iteration;

    //DEBUGGING CODE -- needed to see which rules cost the most
    //Sort the iteration costs
#ifdef DEBUG
    std::sort(costRules.begin(), costRules.end());
    int i = 0;
    double sum = 0;
    double sum10 = 0;
    for (auto &el : costRules) {
        LOG(DEBUGL) << "Cost iteration " << el.iteration << " " <<
            el.time;
        i++;
        if (i >= 20)
            break;

        sum += el.time;
        if (i <= 10)
            sum10 += el.time;
    }
    LOG(DEBUGL) << "Sum first 20 rules: " << sum
        << " first 10:" << sum10;
#endif
}

bool SemiNaiver::executeUntilSaturation(
        std::vector<RuleExecutionDetails> &ruleset,
        std::vector<StatIteration> &costRules,
        bool fixpoint) {
    size_t currentRule = 0;
    uint32_t rulesWithoutDerivation = 0;

    size_t nRulesOnePass = 0;
    size_t lastIteration = 0;
    bool newDer = false;

    std::chrono::system_clock::time_point round_start = std::chrono::system_clock::now();
    do {
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        bool response = executeRule(ruleset[currentRule],
                iteration,
                NULL);
        newDer |= response;
        std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
        StatIteration stat;
        stat.iteration = iteration;
        stat.rule = &ruleset[currentRule].rule;
        stat.time = sec.count() * 1000;
        stat.derived = response;
        costRules.push_back(stat);
        ruleset[currentRule].lastExecution = iteration++;

        if (response) {
            if (ruleset[currentRule].rule.isRecursive()) {
                //Is the rule recursive? Go until saturation...
                int recursiveIterations = 0;
                do {
                    // LOG(INFOL) << "Iteration " << iteration;
                    start = std::chrono::system_clock::now();
                    recursiveIterations++;
                    response = executeRule(ruleset[currentRule],
                            iteration,
                            NULL);
                    newDer |= response;
                    stat.iteration = iteration;
                    ruleset[currentRule].lastExecution = iteration++;
                    sec = std::chrono::system_clock::now() - start;
                    ++recursiveIterations;
                    stat.rule = &ruleset[currentRule].rule;
                    stat.time = sec.count() * 1000;
                    stat.derived = response;
                    costRules.push_back(stat);
                } while (response);
                    LOG(DEBUGL) << "Rules " <<
                        ruleset[currentRule].rule.tostring(program, &layer) <<
                        "  required " << recursiveIterations << " to saturate";
            }
            rulesWithoutDerivation = 0;
            nRulesOnePass++;
        } else {
            rulesWithoutDerivation++;
        }

        currentRule = (currentRule + 1) % ruleset.size();

        if (currentRule == 0) {
#ifdef DEBUG
            std::chrono::duration<double> sec = std::chrono::system_clock::now() - round_start;
            LOG(DEBUGL) << "--Time round " << sec.count() * 1000 << " " << iteration;
            round_start = std::chrono::system_clock::now();
            //CODE FOR Statistics
            LOG(INFOL) << "Finish pass over the rules. Step=" << iteration << ". RulesWithDerivation=" <<
                nRulesOnePass << " out of " << ruleset.size() << " Derivations so far " << countAllIDBs();
	    printCountAllIDBs("After step " + to_string(iteration) + ": ");
            nRulesOnePass = 0;

            //Get the top 10 rules in the last iteration
            std::sort(costRules.begin(), costRules.end());
            string out = "";
            int n = 0;
            for (const auto &exec : costRules) {
                if (exec.iteration >= lastIteration) {
                    if (n < 10 || exec.derived) {
                        out += "Iteration " + to_string(exec.iteration) + " runtime " + to_string(exec.time);
                        out += " " + exec.rule->tostring(program, &layer) + " response " + to_string(exec.derived);
                        out += "\n";
                    }
                    n++;
                }
            }
            LOG(DEBUGL) << "Rules with the highest cost\n\n" << out;
            lastIteration = iteration;
            //END CODE STATISTICS
#endif
            if (!fixpoint)
                break;
        }
    } while (rulesWithoutDerivation != ruleset.size());
                    return newDer;
}

void SemiNaiver::storeOnFiles(std::string path, const bool decompress,
        const int minLevel, const bool csv) {
    //Create a directory if necessary
    Utils::create_directories(path);
    char buffer[MAX_TERM_SIZE];

    //I create a new file for every idb predicate
    for (PredId_t i = 0; i < MAX_NPREDS; ++i) {
        FCTable *table = predicatesTables[i];
        if (table != NULL && !table->isEmpty()) {
            FCIterator itr = table->read(minLevel); //1 contains all explicit facts
            if (!itr.isEmpty()) {
                std::ofstream streamout(path + "/" + program->getPredicateName(i));
                const uint8_t sizeRow = table->getSizeRow();
                while (!itr.isEmpty()) {
                    std::shared_ptr<const FCInternalTable> t = itr.getCurrentTable();
                    FCInternalTableItr *iitr = t->getIterator();
                    while (iitr->hasNext()) {
                        iitr->next();
			std::string row = "";
			if (! csv) {
			    row = to_string(iitr->getCurrentIteration());
			}
			bool first = true;
                        for (uint8_t m = 0; m < sizeRow; ++m) {
                            if (decompress || csv) {
                                if (layer.getDictText(iitr->getCurrentValue(m), buffer)) {
				    if (csv) {
					if (first) {
					    first = false;
					} else {
					    row += ",";
					}
				    } else {
					row += "\t";
				    }
				    row += string(buffer);
                                } else {
                                    std::string t = program->getFromAdditional(iitr->getCurrentValue(m));
                                    if (t == std::string("")) {
					uint64_t v = iitr->getCurrentValue(m);
					t = "" + std::to_string(v >> 40) + "_"
					    + std::to_string((v >> 32) & 0377) + "_"
					    + std::to_string(v & 0xffffffff);
                                        // t = std::to_string(iitr->getCurrentValue(m));
                                    }
				    if (csv) {
					if (first) {
					    first = false;
					} else {
					    row += ",";
					}
				    } else {
					row += "\t";
				    }
                                    row += t;
                                }
                            } else {
                                row += "\t" + to_string(iitr->getCurrentValue(m));
                            }
                        }
                        streamout << row << std::endl;
                    }
                    t->releaseIterator(iitr);
                    itr.moveNextCount();
                }
                streamout.close();
            }
        }
    }
}

bool _sortCards(const std::pair<uint8_t, size_t> &v1, const std::pair<uint8_t, size_t> &v2) {
    return v1.second < v2.second;
}

void SemiNaiver::addDataToIDBRelation(const Predicate pred,
        FCBlock block) {
    LOG(DEBUGL) << "Adding block to " << (int) pred.getId();
    FCTable *table = getTable(pred.getId(), pred.getCardinality());
    table->addBlock(block);
}

bool SemiNaiver::checkIfAtomsAreEmpty(const RuleExecutionDetails &ruleDetails,
        const RuleExecutionPlan &plan,
        std::vector<size_t> &cards) {
    const uint8_t nBodyLiterals = (uint8_t) plan.plan.size();
    bool isOneRelEmpty = false;
    //First I check if there are tuples in each relation.
    //And if there are, then I count how many
    //Get the cardinality of all relations
    for (int i = 0; i < nBodyLiterals; ++i) {
        size_t min = plan.ranges[i].first, max = plan.ranges[i].second;
        if (min == 1)
            min = ruleDetails.lastExecution;
        if (max == 1)
            max = ruleDetails.lastExecution - 1;

        cards.push_back(estimateCardTable(*plan.plan[i], min, max));
        LOG(DEBUGL) << "Estimation of the atom " <<
            plan.plan[i]->tostring(program, &layer) <<
            " is " << cards.back() << " in the range " <<
            min << " " << max;
        if (cards.back() == 0) {
            isOneRelEmpty = true;
            break;
        }
    }
    return isOneRelEmpty;
}

struct CreateParallelFirstAtom {
    const std::vector<const std::vector<Term_t> *> vectors;
    const std::vector<Output *> outputs;
    const size_t chunksz;
    const size_t sz;
    const bool uniqueResults;
    const std::vector<Term_t> *fvfirst;
    const std::vector<Term_t> *fvsecond;

    CreateParallelFirstAtom(const std::vector<const std::vector<Term_t> *> vectors,
            const std::pair<uint8_t, uint8_t> *fv,
            const std::vector<Output *> outputs, const size_t chunksz,
            const size_t sz, const bool uniqueResults) :
        vectors(vectors), outputs(outputs), chunksz(chunksz), sz(sz),
        uniqueResults(uniqueResults) {
            if (fv == NULL) {
                fvfirst = NULL;
                fvsecond = NULL;
            } else {
                fvfirst = vectors[fv->first];
                fvsecond = vectors[fv->second];
            }
        }

    void operator()(const ParallelRange& r) const {

        for (int i = r.begin(); i != r.end(); i++) {
            size_t begin = r.begin() * chunksz;
            size_t end = begin + chunksz;
            if (end > sz) end = sz;
            if (begin == end) {
                return;
            }

            if (fvfirst != NULL) {
                for (size_t j = begin; j < end; j++) {
                    if ((*fvfirst)[j] == (*fvsecond)[j]) {
                        continue;
                    }
                    outputs[i]->processResults(0,
                            vectors, j,
                            vectors, j, uniqueResults);
                }
            } else {
                for (size_t j = begin; j < end; j++) {
                    outputs[i]->processResults(0,
                            vectors, j,
                            vectors, j, uniqueResults);
                }
            }
        }
    }
};

void SemiNaiver::processRuleFirstAtom(const uint8_t nBodyLiterals,
        const Literal *bodyLiteral,
        std::vector<Literal> &heads,
        const size_t min,
        const size_t max,
        int &processedTables,
        const bool lastLiteral,
        const uint32_t iteration,
        const RuleExecutionDetails &ruleDetails,
        const uint8_t orderExecution,
        std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
        ResultJoinProcessor *joinOutput) {
    //If the rule has only one body literal, has the same bindings list of the head,
    //and the current head relation is empty, then I can simply copy the table
    FCIterator literalItr = getTable(*bodyLiteral, min, max);
    TableFilterer queryFilterer(this);
    if (bodyLiteral->getPredicate().getType() == IDB) {
        processedTables += literalItr.getNTables();
    }
    Literal &firstHeadLiteral = heads[0];
    auto idHeadPredicate = firstHeadLiteral.getPredicate().getId();
    FCTable *firstEndTable = getTable(idHeadPredicate, firstHeadLiteral.
            getPredicate().getCardinality());

    //Can I copy the table in the body as is?
    bool rawCopy = lastLiteral && heads.size() == 1 && literalItr.getNTables() == 1;
    if (rawCopy) {
        rawCopy &= firstEndTable->isEmpty() &&
            firstEndTable->getSizeRow() == literalItr.getCurrentTable()->getRowSize() &&
            firstHeadLiteral.sameVarSequenceAs(*bodyLiteral) &&
            bodyLiteral->getTupleSize() == firstHeadLiteral.getTupleSize() &&
            ((SingleHeadFinalRuleProcessor*)joinOutput)->shouldAddToEndTable();
    }

    if (rawCopy) { //The previous check was successful
        while (!literalItr.isEmpty()) {
            std::shared_ptr<const FCInternalTable> table =
                literalItr.getCurrentTable();

            if (!queryFilterer.
                    producedDerivationInPreviousSteps(
                        firstHeadLiteral, *bodyLiteral,
                        literalItr.getCurrentBlock())) {

                firstEndTable->add(table->cloneWithIteration(iteration),
                        firstHeadLiteral, 0, &ruleDetails,
                        orderExecution, iteration, true, nthreads);

            }

            literalItr.moveNextCount();
        }
    } else if (nBodyLiterals == 1) {
        const bool uniqueResults =
	    ! ruleDetails.rule.isExistential()
	    && firstHeadLiteral.getNUniqueVars() == bodyLiteral->getNUniqueVars()
            && literalItr.getNTables() == 1 && heads.size() == 1;
        while (!literalItr.isEmpty()) {
            //Add the columns to the output container
	    // Can lastLiteral be false if nBodyLiterals == 1??? --Ceriel
            if (!lastLiteral || ruleDetails.rule.isExistential() ||
                    heads.size() != 1 || !queryFilterer.
                    producedDerivationInPreviousSteps(
                        firstHeadLiteral,
                        *bodyLiteral,
                        literalItr.getCurrentBlock())) {

                std::shared_ptr<const FCInternalTable> table =
                    literalItr.getCurrentTable();
                FCInternalTableItr *interitr = table->getIterator();

                bool unique = uniqueResults && firstEndTable->isEmpty();
                bool sorted = uniqueResults && firstHeadLiteral.
                    sameVarSequenceAs(*bodyLiteral);
                joinOutput->addColumns(0, interitr,
                        unique,
                        sorted,
                        literalItr.getNTables() == 1);

                table->releaseIterator(interitr);
            }
	    // No else-clause here? Yes, can only be duplicates
            literalItr.moveNextCount();
        }
    } else {
        //Copy the iterator in the tmp container.
        //This process cannot derive duplicates if the number of variables is equivalent.
        const bool uniqueResults = heads.size() == 1
	    && ! ruleDetails.rule.isExistential()
	    && firstHeadLiteral.getNUniqueVars() == bodyLiteral->getNUniqueVars()
	    && (!lastLiteral || firstEndTable->isEmpty());

        while (!literalItr.isEmpty()) {
            std::shared_ptr<const FCInternalTable> table = literalItr.getCurrentTable();
            LOG(DEBUGL) << "Creating iterator";
            FCInternalTableItr *interitr = table->getIterator();
            std::pair<uint8_t, uint8_t> *fv = NULL;
            std::pair<uint8_t, uint8_t> psColumnsToFilter;
            if (filterValueVars != NULL) {
                assert(filterValueVars->size() == 1);
                fv = &(*filterValueVars)[0];
                psColumnsToFilter = removePosConstants(*fv, *bodyLiteral);
                fv = &psColumnsToFilter;
            }

            std::vector<const std::vector<Term_t> *> vectors;
            vectors = interitr->getAllVectors(nthreads);

            if (vectors.size() > 0) {
                size_t sz = vectors[0]->size();
                int chunksz = (sz + nthreads - 1) / nthreads;
                if (nthreads > 1 && chunksz > 1024) {
                    std::mutex m;
                    std::vector<Output *> outputs;
                    for (int i = 0; i < nthreads; i++) {
                        outputs.push_back(new Output(joinOutput, &m));
                    }
                    //tbb::parallel_for(tbb::blocked_range<int>(0, nthreads, 1),
                    //        CreateParallelFirstAtom(vectors, fv, outputs, chunksz, sz, uniqueResults));
                    ParallelTasks::parallel_for(0, nthreads, 1,
                            CreateParallelFirstAtom(vectors, fv, outputs,
                                chunksz, sz, uniqueResults));
                    // Maintain order of outputs, so:
                    for (int i = 0; i < nthreads; i++) {
                        outputs[i]->flush();
                        delete outputs[i];
                    }
                } else {
                    const std::vector<Term_t> *fvfirst = NULL;
                    const std::vector<Term_t> *fvsecond = NULL;
                    if (fv != NULL) {
                        fvfirst = vectors[fv->first];
                        fvsecond = vectors[fv->second];
                        for (int i = 0; i < fvfirst->size(); i++) {
                            if ((*fvfirst)[i] == (*fvsecond)[i]) {
                                continue;
                            }

                            joinOutput->processResults(0,
                                    vectors, i,
                                    vectors, i, uniqueResults);
                        }
                    } else {
                        for (int i = 0; i < vectors[0]->size(); i++) {
                            joinOutput->processResults(0,
                                    vectors, vectors[0]->size(),
                                    vectors, i, uniqueResults);
                        }
                    }
                }
                interitr->deleteAllVectors(vectors);
            } else {
                while (interitr->hasNext()) {
                    interitr->next();
                    if (fv != NULL) {
                        //otherwise I miss others
                        if (interitr->getCurrentValue(fv->first) ==
                                interitr->getCurrentValue(
                                    fv->second)) {
                            continue;
                        }
                    }

                    joinOutput->processResults(0,
                            (FCInternalTableItr*)NULL,
                            interitr, uniqueResults);
                }
            }
            LOG(DEBUGL) << "Releasing iterator";
            table->releaseIterator(interitr);
            literalItr.moveNextCount();
        }
    }
}

void SemiNaiver::reorderPlan(RuleExecutionPlan &plan,
        const std::vector<size_t> &cards,
        const std::vector<Literal> &heads) {
    //Reorder the atoms in terms of cardinality.
    std::vector<std::pair<uint8_t, size_t>> positionCards;
    for (uint8_t i = 0; i < cards.size(); ++i) {
        LOG(DEBUGL) << "Atom " << (int) i << " has card " << cards[i];
        positionCards.push_back(std::make_pair(i, cards[i]));
    }
    sort(positionCards.begin(), positionCards.end(), _sortCards);

    //Ensure there are always variables
    std::vector<std::pair<uint8_t, size_t>> adaptedPosCards;
    adaptedPosCards.push_back(positionCards.front());
    std::vector<uint8_t> vars = plan.plan[
        positionCards[0].first]
            ->getAllVars();
    // LOG(DEBUGL) << "Added vars of " << plan.plan[positionCards[0].first]->tostring(NULL, NULL);
    positionCards.erase(positionCards.begin());

    while (positionCards.size() > 0) {
        //Look for the smallest pattern which shares the most variables
        int saved = -1;
        int savedNShared = 0;
        for (int i = 0; i < positionCards.size(); ++i) {
            // LOG(DEBUGL) << "Checking vars of " << plan.plan[positionCards[i].first]->tostring(NULL, NULL);
            int shared = plan.plan[positionCards[i].first]->getSharedVars(vars).size();
            if (shared > savedNShared) {
                savedNShared = shared;
                saved = i;
            }
        }
        if (saved < 0) {
            // LOG(DEBUGL) << "No shared var found";
            break;
        }
        adaptedPosCards.push_back(positionCards[saved]);
        std::vector<uint8_t> newvars = plan.plan[positionCards[saved].first]->getAllVars();
        std::copy(newvars.begin(), newvars.end(), std::back_inserter(vars));
        // LOG(DEBUGL) << "Added vars of " << plan.plan[positionCards[0].first]->tostring(NULL, NULL);
        positionCards.erase(positionCards.begin() + saved);
    }

    //If the order is not the original, then I must reorder it
    bool toReorder = positionCards.size() == 0;
    if (toReorder) {
        int idx = 0;
        toReorder = false;
        for (auto el : adaptedPosCards) {
            if (el.first != idx) {
                toReorder = true;
                break;
            }
            idx++;
        }
    }
    if (toReorder) {
        std::vector<uint8_t> orderLiterals;
        for (int i = 0; i < adaptedPosCards.size(); ++i) {
            LOG(DEBUGL) << "Reordered plan is " << (int)adaptedPosCards[i].first;
            orderLiterals.push_back(adaptedPosCards[i].first);
        }
        plan = plan.reorder(orderLiterals, heads);
    }
}

FCTable *SemiNaiver::getTable(const PredId_t pred, const uint8_t card) {
    FCTable *endTable;
    if (predicatesTables[pred] != NULL) {
        endTable = predicatesTables[pred];
    } else {
        endTable = new FCTable(NULL, card);
        predicatesTables[pred] = endTable;
    }
    return endTable;
}

void SemiNaiver::saveDerivationIntoDerivationList(FCTable *endTable) {
    LOG(ERRORL) << "Legacy method. Shouldn't be needed anymore ...";
    throw 10;
}

void SemiNaiver::saveStatistics(StatsRule &stats) {
    statsRuleExecution.push_back(stats);
}

bool SemiNaiver::executeRule(RuleExecutionDetails &ruleDetails,
        const uint32_t iteration,
        std::vector<ResultJoinProcessor*> *finalResultContainer) {
    Rule rule = ruleDetails.rule;
    bool answer = true;
    std::vector<Literal> heads = rule.getHeads();
    answer &= executeRule(ruleDetails, heads, iteration, finalResultContainer);
    return answer;
}


bool SemiNaiver::executeRule(RuleExecutionDetails &ruleDetails,
        std::vector<Literal> &heads,
        const uint32_t iteration,
        std::vector<ResultJoinProcessor*> *finalResultContainer) {
    Rule rule = ruleDetails.rule;

#ifdef WEBINTERFACE
    // Cannot run multithreaded in this case.
    currentRule = rule.tostring(program, &layer);
#endif

    LOG(DEBUGL) << "Iteration: " << iteration <<
        " Rule: " << rule.tostring(program, &layer);

    //Set up timers
    const std::chrono::system_clock::time_point startRule = std::chrono::system_clock::now();
    std::chrono::duration<double> durationJoin(0);
    std::chrono::duration<double> durationConsolidation(0);
    std::chrono::duration<double> durationFirstAtom(0);

    //Get table corresponding to the head predicate
    //FCTable *endTable = getTable(idHeadPredicate, headLiteral.
    //        getPredicate().getCardinality());

    //if (headLiteral.getNVars() == 0 && !endTable->isEmpty()) {
    //    LOG(DEBUGL) << "No variables and endtable not empty, so cannot find new derivations";
    //    return false;
    //}

    //In case the rule has many IDBs predicates, I calculate several
    //combinations of countings.
    const std::vector<RuleExecutionPlan> *orderExecutions =
        &ruleDetails.orderExecutions;

    //Start executing all possible combinations of rules
    int orderExecution = 0;
    int processedTables = 0;

    //If the last iteration the rule failed because an atom was empty, I record this
    //because I might use this info to skip some computation later on
    const bool failEmpty = ruleDetails.failedBecauseEmpty;
    const Literal *atomFail = ruleDetails.atomFailure;
    ruleDetails.failedBecauseEmpty = false;

    LOG(DEBUGL) << "orderExecutions.size() = " << orderExecutions->size();

    for (; orderExecution < orderExecutions->size() &&
            (ruleDetails.lastExecution > 0 || orderExecution == 0); ++orderExecution) {
        LOG(DEBUGL) << "orderExecution: " << orderExecution;

        //Auxiliary relations to perform the joins
        std::vector<size_t> cards;
        RuleExecutionPlan plan = orderExecutions->at(orderExecution);
        const uint8_t nBodyLiterals = (uint8_t) plan.plan.size();

        //**** Should I skip the evaluation because some atoms are empty? ***
        bool isOneRelEmpty = checkIfAtomsAreEmpty(ruleDetails, plan, cards);
        if (isOneRelEmpty) {
            LOG(DEBUGL) << "Aborting this combination";
            continue;
        }

        //Reorder the list of atoms depending on the observed cardinalities
        reorderPlan(plan, cards, heads);

#ifdef DEBUG
        std::string listLiterals = "EXEC COMB: ";
        for (std::vector<const Literal*>::iterator itr = plan.plan.begin();
                itr != plan.plan.end();
                ++itr) {
            listLiterals += (*itr)->tostring(program, &layer);
        }
        LOG(DEBUGL) << listLiterals;
#endif

        /*******************************************************************/

        std::shared_ptr<const FCInternalTable> currentResults;
        int optimalOrderIdx = 0;

        bool first = true;
        while (optimalOrderIdx < nBodyLiterals) {
            const Literal *bodyLiteral = plan.plan[optimalOrderIdx];

            //This data structure is used to filter out rows where different columns
            //lead to the same derivation
            std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars = NULL;
            if (heads.size() == 1) {
                if (plan.matches.size() > 0) {
                    for (int i = 0; i < plan.matches.size(); ++i) {
                        if (plan.matches[i].posLiteralInOrder
                                == optimalOrderIdx) {
                            filterValueVars = &plan.matches[i].matches;
                        }
                    }
                }
            }

            //BEGIN -- Determine where to put the results of the query
            ResultJoinProcessor *joinOutput = NULL;
            const bool lastLiteral = optimalOrderIdx == (nBodyLiterals - 1);
            if (!lastLiteral) {
                joinOutput = new InterTableJoinProcessor(
                        plan.sizeOutputRelation[optimalOrderIdx],
                        plan.posFromFirst[optimalOrderIdx],
                        plan.posFromSecond[optimalOrderIdx],
                        ! multithreaded ? -1 : nthreads);
            } else {
                if (ruleDetails.rule.isExistential()) {
                    joinOutput = new ExistentialRuleProcessor(
                            plan.posFromFirst[optimalOrderIdx],
                            plan.posFromSecond[optimalOrderIdx],
                            listDerivations,
                            heads, &ruleDetails,
                            (uint8_t) orderExecution, iteration,
                            finalResultContainer == NULL,
                            !multithreaded ? -1 : nthreads,
                            this,
                            chaseMgmt);
                } else {
                    if (heads.size() == 1) {
                        FCTable *table = getTable(heads[0].getPredicate().getId(),
                                heads[0].getPredicate().getCardinality());
                        joinOutput = new SingleHeadFinalRuleProcessor(
                                plan.posFromFirst[optimalOrderIdx],
                                plan.posFromSecond[optimalOrderIdx],
                                listDerivations,
                                table,
                                heads[0],
                                0,
                                &ruleDetails,
                                (uint8_t) orderExecution,
                                iteration,
                                finalResultContainer == NULL,
                                !multithreaded ? -1 : nthreads);
                    } else {
                        joinOutput = new FinalRuleProcessor(
                                plan.posFromFirst[optimalOrderIdx],
                                plan.posFromSecond[optimalOrderIdx],
                                listDerivations,
                                heads, &ruleDetails,
                                (uint8_t) orderExecution, iteration,
                                finalResultContainer == NULL,
                                !multithreaded ? -1 : nthreads, this);
                    }
                }
            }
            //END --  Determine where to put the results of the query

            //Calculate range for the retrieval of the triples
            size_t min = plan.ranges[optimalOrderIdx].first;
            size_t max = plan.ranges[optimalOrderIdx].second;
            if (min == 1)
                min = ruleDetails.lastExecution;
            if (max == 1)
                max = ruleDetails.lastExecution - 1;
            LOG(DEBUGL) << "Evaluating atom " << optimalOrderIdx << " " << bodyLiteral->tostring() <<
                " min=" << min << " max=" << max;

            if (first) {
                std::chrono::system_clock::time_point startFirstA = std::chrono::system_clock::now();
                if (lastLiteral || bodyLiteral->getNVars() > 0) {
                    processRuleFirstAtom(nBodyLiterals, bodyLiteral,
                            heads, min, max, processedTables,
                            lastLiteral,
                            iteration, ruleDetails,
                            orderExecution,
                            filterValueVars,
                            joinOutput);
                    durationFirstAtom += std::chrono::system_clock::now() - startFirstA;
                    first = false;
                }
            } else {
                //Perform the join
                std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
                JoinExecutor::join(this, currentResults.get(),
                        lastLiteral ? &heads: NULL,
                        *bodyLiteral, min, max, filterValueVars,
                        plan.joinCoordinates[optimalOrderIdx],
                        joinOutput,
                        lastLiteral, ruleDetails,
                        plan,
                        processedTables,
                        optimalOrderIdx,
                        multithreaded ? nthreads : -1);
                std::chrono::duration<double> d =
                    std::chrono::system_clock::now() - start;
                LOG(DEBUGL) << "Time join: " << d.count() * 1000;
                durationJoin += d;
            }

            //Clean up possible duplicates
            std::chrono::system_clock::time_point startC =
                std::chrono::system_clock::now();
            if (! first) {
                joinOutput->consolidate(true);
                std::chrono::duration<double> d =
                    std::chrono::system_clock::now() - startC;
                durationConsolidation += d;
            }

            //Prepare for the processing of the next atom (if any)
            if (!lastLiteral && ! first) {
                currentResults = ((InterTableJoinProcessor*)joinOutput)->getTable();
            }
            if (lastLiteral && finalResultContainer) {
                finalResultContainer->push_back(joinOutput);
            } else {
                delete joinOutput;
            }
            optimalOrderIdx++;

            if (!lastLiteral && ! first && (currentResults == NULL ||
                        currentResults->isEmpty())) {
                LOG(DEBUGL) << "The evaluation of atom " <<
                    (optimalOrderIdx - 1) << " returned no result";
                //If the range was 0 to MAX_INT, then also other combinations
                //will never fire anything
                if (min == 0 && max == (size_t) - 1 && failEmpty && atomFail == bodyLiteral) {
                    orderExecution = orderExecutions->size();
                    ruleDetails.failedBecauseEmpty = true;
                    ruleDetails.atomFailure = bodyLiteral;
                }
                break;
            }
        }
    }

    bool prodDer = false;
    for (auto &h : heads) {
        auto idHeadPredicate = h.getPredicate().getId();
        FCTable *t = getTable(idHeadPredicate, h.
                getPredicate().getCardinality());
        if (!t->isEmpty(iteration)) {
            FCBlock block = t->getLastBlock();
            if (block.iteration == iteration) {
                listDerivations.push_back(block);
            }
            prodDer |= true;
        }
    }

    std::chrono::duration<double> totalDuration =
        std::chrono::system_clock::now() - startRule;
    double td = totalDuration.count() * 1000;

#ifdef WEBINTERFACE
    StatsRule stats;
    stats.iteration = iteration;
    stats.idRule = ruleDetails.ruleid;
    if (!prodDer) {
        stats.derivation = 0;
    } else {
        stats.derivation = getNLastDerivationsFromList();
    }
    //Jacopo: td is not existing anymore...
    stats.timems = (long)td;
    saveStatistics(stats);
    currentPredicate = -1;
    currentRule = "";
#endif

    std::stringstream stream;
    std::string sTd = "";
    if (td > 1000) {
        td = td / 1000;
        stream << td << "sec";
    } else {
        stream << td << "ms";
    }

    if (prodDer) {
        LOG(DEBUGL) << "Iteration " << iteration << ". Rule derived new tuples. Combinations " << orderExecution << ", Processed IDB Tables=" <<
            processedTables << ", Total runtime " << stream.str()
            << ", join " << durationJoin.count() * 1000 << "ms, consolidation " <<
            durationConsolidation.count() * 1000 << "ms, retrieving first atom " << durationFirstAtom.count() * 1000 << "ms.";
    } else {
        LOG(DEBUGL) << "Iteration " << iteration << ". Rule derived NO new tuples. Combinations " << orderExecution << ", Processed IDB Tables=" <<
            processedTables << ", Total runtime " << stream.str()
            << ", join " << durationJoin.count() * 1000 << "ms, consolidation " <<
            durationConsolidation.count() * 1000 << "ms, retrieving first atom " << durationFirstAtom.count() * 1000 << "ms.";
    }

    return prodDer;
}

long SemiNaiver::getNLastDerivationsFromList() {
    return listDerivations.back().table->getNRows();
}

size_t SemiNaiver::estimateCardTable(const Literal &literal,
        const size_t minIteration,
        const size_t maxIteration) {

    PredId_t id = literal.getPredicate().getId();
    FCTable *table = predicatesTables[id];
    if (table == NULL || table->isEmpty() ||
            table->getMaxIteration() < minIteration ||
            table->getMinIteration() > maxIteration) {
        if (table == NULL && literal.getPredicate().getType() == EDB) {
            //It might be because the table is not loaded. Try to load it and repeat the process
            FCIterator itr = getTable(literal, minIteration, maxIteration);
            if (itr.isEmpty()) {
                return 0;
            } else {
                return estimateCardTable(literal, minIteration, maxIteration);
            }
        } else {
            return 0;
        }
    } else {
        size_t estimate = table->estimateCardinality(literal, minIteration, maxIteration);
        if (estimate == 0) {
            return 0;
        }
        return estimate;
        // Was: return table->estimateCardInRange(minIteration, maxIteration);
    }
}

FCIterator SemiNaiver::getTableFromIDBLayer(const Literal &literal, const size_t minIteration, TableFilterer *filter) {

    PredId_t id = literal.getPredicate().getId();
    LOG(TRACEL) << "SemiNaiver::getTableFromIDBLayer: id = " << (int) id
        << ", minIter = " << minIteration << ", literal=" << literal.tostring(NULL, NULL);
    FCTable *table = predicatesTables[id];
    if (table == NULL || table->isEmpty() || table->getMaxIteration() < minIteration) {
        LOG(DEBUGL) << "Return empty iterator";
        return FCIterator();
    } else {
        return table->filter(literal, minIteration, filter, nthreads)->read(minIteration);
    }
}

FCIterator SemiNaiver::getTableFromIDBLayer(const Literal &literal, const size_t minIteration,
        const size_t maxIteration, TableFilterer *filter) {
    PredId_t id = literal.getPredicate().getId();
    LOG(TRACEL) << "SemiNaiver::getTableFromIDBLayer: id = " << (int) id
        << ", minIter = " << minIteration << ", maxIteration = " << maxIteration << ", literal=" << literal.tostring(NULL, NULL);
    FCTable *table = predicatesTables[id];
    if (table == NULL || table->isEmpty() || table->getMaxIteration() < minIteration) {
        LOG(DEBUGL) << "Return empty iterator";
        return FCIterator();
    } else {
        if (literal.getNUniqueVars() < literal.getTupleSize()) {
            return table->filter(literal, minIteration, filter, nthreads)->read(minIteration, maxIteration);
        } else {
            return table->read(minIteration, maxIteration);
        }
    }
}

size_t SemiNaiver::estimateCardinality(const Literal &literal, const size_t minIteration,
        const size_t maxIteration) {
    FCTable *table = predicatesTables[literal.getPredicate().getId()];
    if (table == NULL) {
        return 0;
    } else {
        return table->estimateCardinality(literal, minIteration, maxIteration);
    }
}

FCIterator SemiNaiver::getTableFromEDBLayer(const Literal & literal) {
    PredId_t id = literal.getPredicate().getId();
    FCTable *table = predicatesTables[id];
    if (table == NULL) {
        table = SemiNaiver::getTable(id, (uint8_t) literal.getTupleSize());

        VTuple t = literal.getTuple();
        //Add all different variables
        for (uint8_t i = 0; i < t.getSize(); ++i) {
            t.set(VTerm(i + 1, 0), i);
        }
        Literal mostGenericLiteral(literal.getPredicate(), t);

        std::shared_ptr<FCInternalTable> ptrTable(new EDBFCInternalTable(0,
                    mostGenericLiteral, &layer));
        table->add(ptrTable, mostGenericLiteral, 0, NULL, 0, 0, true, nthreads);
    }
    if (literal.getNUniqueVars() < literal.getTupleSize()) {
        return table->filter(literal, nthreads)->read(0);
    } else {
        return table->read(0);
    }
}

FCIterator SemiNaiver::getTable(const Literal & literal,
        const size_t min, const size_t max, TableFilterer *filter) {
    //BEGIN -- Get the table that correspond to the current literal
    //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    if (literal.getPredicate().getType() == EDB) {
        return getTableFromEDBLayer(literal);
    } else {
        /*if (currentIDBpred == 0) {
          literalItr = getTableFromIDBLayer(literal, ruleDetails.lastExecution);
          } else {
          if (orderExecution == 0) {
          literalItr = getTableFromIDBLayer(literal, 0);
          } else {
          literalItr = getTableFromIDBLayer(literal, 0,
          ruleDetails.lastExecution);
          }
          }
          currentIDBpred++;*/
        return getTableFromIDBLayer(literal, min, max, filter);
    }
    //std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    //LOG(DEBUGL) << "Runtime retrieving literal ms = " << sec.count() * 1000;
    //END -- Get the table that correspond to the literal

}

FCIterator SemiNaiver::getTable(const PredId_t predid) {
    return predicatesTables[predid]->read(0);
}

size_t SemiNaiver::getSizeTable(const PredId_t predid) const {
    return predicatesTables[predid]->getNAllRows();
}

SemiNaiver::~SemiNaiver() {
    for (int i = 0; i < MAX_NPREDS; ++i) {
        if (predicatesTables[i] != NULL)
            delete predicatesTables[i];
    }

    /*for (EDBCache::iterator itr = edbCache.begin(); itr != edbCache.end(); ++itr) {
      delete itr->second;
      }*/
}

size_t SemiNaiver::countAllIDBs() {
    long c = 0;
    for (PredId_t i = 0; i < MAX_NPREDS; ++i) {
        if (predicatesTables[i] != NULL) {
            if (program->isPredicateIDB(i)) {
                long count = predicatesTables[i]->getNAllRows();
                c += count;
            }
        }
    }
    return c;
}

#ifdef WEBINTERFACE
std::vector<std::pair<string, std::vector<StatsSizeIDB>>> SemiNaiver::getSizeIDBs() {
    std::vector<std::pair<string, std::vector<StatsSizeIDB>>> out;
    for (PredId_t i = 0; i < MAX_NPREDS; ++i) {
        if (predicatesTables[i] != NULL && i != currentPredicate) {
            if (program->isPredicateIDB(i)) {
                FCIterator itr = predicatesTables[i]->read(0);
                std::vector<StatsSizeIDB> stats;
                while (!itr.isEmpty()) {
                    std::shared_ptr<const FCInternalTable> t = itr.getCurrentTable();
                    StatsSizeIDB s;
                    s.iteration = itr.getCurrentIteration();
                    s.idRule = itr.getRule()->ruleid;
                    s.derivation = t->getNRows();
                    stats.push_back(s);
                    itr.moveNextCount();
                }

                if (stats.size() > 0) {
                    out.push_back(make_pair(program->getPredicateName(i), stats));
                }

                //long count = predicatesTables[i]->getNAllRows();
                //out.push_back(make_pair(program->getPredicateName(i), count));
            }
        }
    }
    return out;
}
#endif

void SemiNaiver::printCountAllIDBs(string prefix) {
    long c = 0;
    long emptyRel = 0;
    for (PredId_t i = 0; i < MAX_NPREDS; ++i) {
        if (predicatesTables[i] != NULL) {
            if (program->isPredicateIDB(i)) {
                long count = predicatesTables[i]->getNAllRows();
                if (count == 0) {
                    emptyRel++;
		}
		string predname = program->getPredicateName(i);
		LOG(INFOL) << prefix << "Cardinality of " <<
		    predname << ": " << count;
                c += count;
            }
        }
    }
    LOG(DEBUGL) << prefix << "Predicates without derivation: " << emptyRel;
    LOG(INFOL) << prefix << "Total # derivations: " << c;
}

std::pair<uint8_t, uint8_t> SemiNaiver::removePosConstants(
        std::pair<uint8_t, uint8_t> columns,
        const Literal &literal) {

    std::pair<uint8_t, uint8_t> newcols;
    //Fix first pos
    newcols.first = columns.first;
    for (int i = 0; i < columns.first; ++i) {
        if (!literal.getTermAtPos(i).isVariable())
            newcols.first--;
    }
    newcols.second = columns.second;
    for (int i = 0; i < columns.second; ++i) {
        if (!literal.getTermAtPos(i).isVariable())
            newcols.second--;
    }
    return newcols;
}

size_t SemiNaiver::getCurrentIteration() {
    return iteration;
}

#ifdef WEBINTERFACE
string SemiNaiver::getCurrentRule() {
    return currentRule;
}

std::vector<StatsRule> SemiNaiver::getOutputNewIterations() {
    std::vector<StatsRule> out;
    size_t cIt = iteration;
    int nextIteration = statsLastIteration + 1;
    /*for (const auto &el : listDerivations) {
      if (el.iteration > nextIteration && el.iteration < cIt) {
      while (nextIteration < el.iteration) {
      StatsRule r;
      r.iteration = nextIteration;
      r.derivation = 0;
      out.push_back(r);
      nextIteration++;
      }
      StatsRule r;
      r.iteration = el.iteration;
      r.derivation = el.table->getNRows();
      r.idRule = getRuleID(el.rule);
      out.push_back(r);
      nextIteration++;
      }
      }
      while (nextIteration < cIt) {
      StatsRule r;
      r.iteration = nextIteration;
      r.derivation = 0;
      out.push_back(r);
      nextIteration++;
      }*/
    size_t sizeVector = statsRuleExecution.size();
    for (int i = 0; i < sizeVector; ++i) {
        StatsRule el = statsRuleExecution[i];
        if (el.iteration >= nextIteration && el.iteration < cIt) {
            out.push_back(el);
            statsLastIteration = el.iteration;
        }
    }
    return out;
}

bool SemiNaiver::isRunning() {
    return running;
}
#endif

/*int SemiNaiver::getRuleID(const RuleExecutionDetails *rule) {
  if (edbRuleset.size() > 0) {
  RuleExecutionDetails *begin = &(edbRuleset[0]);
  RuleExecutionDetails *end = &(edbRuleset.back());
  if (rule >= begin && rule < end) {
  return rule - begin;
  }
  }
  RuleExecutionDetails *begin = &(ruleset[0]);
  RuleExecutionDetails *end = &(ruleset.back());
  if (rule >= begin && rule < end) {
  return edbRuleset.size() + rule - begin;
  }

  LOG(ERRORL) << "I cannot recognize the rule and hence cannot give it an ID";
  LOG(ERRORL) << "Rule: " << rule->rule.tostring(program, &layer);
  throw 10;
  }*/
