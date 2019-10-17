#include <vlog/seminaiver.h>
#include <vlog/concepts.h>
#include <vlog/joinprocessor.h>
#include <vlog/fctable.h>
#include <vlog/fcinttable.h>
#include <vlog/filterer.h>
#include <vlog/finalresultjoinproc.h>
#include <vlog/extresultjoinproc.h>
#include <vlog/utils.h>
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

    std::vector<int> *definedBy = new std::vector<int>[program->getNPredicates()];
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
                LOG(DEBUGL) << " Rule " << i << ": " << ri.tostring(program, &layer);
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
                    edges.push_back(std::make_pair(*k, i));
                }
            }
        }
        /*
        // Also add dependency to other rules defining the same predicate?
        PredId_t id = ri.getHead().getPredicate().getId();
        for (std::vector<int>::const_iterator k = definedBy[id].begin(); k != definedBy[id].end(); ++k) {
        if (*k != i) {
        edges.push_back(std::make_pair(*k, i));
        }
        }
        */
    }
    delete[] definedBy;
}

std::string set_to_string(std::unordered_set<int> s) {
    ostringstream oss("");
    for (std::unordered_set<int>::const_iterator k = s.begin(); k != s.end(); ++k) {
        oss << *k << " ";
    }
    return oss.str();
}

SemiNaiver::SemiNaiver(EDBLayer &layer,
        Program *program, bool opt_intersect, bool opt_filtering,
        bool multithreaded, TypeChase typeChase, int nthreads, bool shuffle,
        bool ignoreExistentialRules, Program *RMFC_check) :
    opt_intersect(opt_intersect),
    opt_filtering(opt_filtering),
    multithreaded(multithreaded),
    typeChase(typeChase),
    running(false),
    layer(layer),
    program(program),
    nthreads(nthreads),
    checkCyclicTerms(false),
    ignoreExistentialRules(ignoreExistentialRules),
    RMFC_program(RMFC_check) {

        std::vector<Rule> ruleset = program->getAllRules();
        predicatesTables.resize(program->getMaxPredicateId());
        ignoreDuplicatesElimination = false;
        TableFilterer::setOptIntersect(opt_intersect);

        if (! program->stratify(stratification, nStratificationClasses)) {
            LOG(ERRORL) << "Program could not be stratified";
            throw std::runtime_error("Program could not be stratified");
        }
        LOG(DEBUGL) << "nStratificationClasses = " << nStratificationClasses;

        LOG(DEBUGL) << "Running SemiNaiver, opt_intersect = " << opt_intersect << ", opt_filtering = " << opt_filtering << ", multithreading = " << multithreaded << ", shuffle = " << shuffle;


        uint32_t ruleid = 0;
        this->allIDBRules.resize(nStratificationClasses);
        for (int i = 0; i < nStratificationClasses; i++) {
            this->allIDBRules[i].reserve(ruleset.size());
        }
        for (const auto& rule : ruleset){
            RuleExecutionDetails *d = new RuleExecutionDetails(rule, ruleid++);
            std::vector<Literal> bodyLiterals = rule.getBody();
            for (const auto& literal : bodyLiterals){
                if (literal.getPredicate().getType() == IDB) {
                    d->nIDBs++;
                }
            }
            if (d->nIDBs != 0) {
                PredId_t id = rule.getFirstHead().getPredicate().getId();
                this->allIDBRules[nStratificationClasses == 1 ? 0 : stratification[id]].push_back(*d);
            } else
                this->allEDBRules.push_back(*d);
            delete d;
        }
        for (int i = 0; i < nStratificationClasses; i++) {
            this->allIDBRules[i].reserve(this->allIDBRules[i].size());
        }

#if 0
        // Commented out rule-reordering for now. It is only needed for interrule-parallelism.
        if (multithreaded) {
            // newDetails will ultimately contain the new rule order.
            std::vector<int> newDetails;
            for (size_t i = 0; i < this->allIDBRules.size(); i++) {
                newDetails.push_back(i);
            }

            if (!shuffle) {
                std::vector<int> *definedBy = new std::vector<int>[program->getNPredicates()];
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
#endif
    }

bool SemiNaiver::executeRules(std::vector<RuleExecutionDetails> &edbRuleset,
        std::vector<std::vector<RuleExecutionDetails>> &ruleset,
        std::vector<StatIteration> &costRules,
        const size_t limitView,
        bool fixpoint, unsigned long *timeout) {
#if DEBUG
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
#endif
    bool newDer = false;
    for (size_t i = 0; i < edbRuleset.size(); ++i) {
        newDer |= executeRule(edbRuleset[i], iteration, limitView, NULL);
        if (timeout != NULL && *timeout != 0) {
            std::chrono::duration<double> s = std::chrono::system_clock::now() - startTime;
            if (s.count() > *timeout) {
                *timeout = 0;   // To indicate materialization was stopped because of timeout.
                return newDer;
            }
        }
        iteration++;
    }
#if DEBUG
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "Runtime EDB rules ms = " << sec.count() * 1000;
#endif

    for (int i = 0; i < ruleset.size(); i++) {
        if (ruleset[i].size() > 0) {
            newDer |= executeUntilSaturation(ruleset[i], costRules, limitView,  fixpoint, timeout);
        }
    }
    return newDer;
}

void SemiNaiver::prepare(size_t lastExecution, int singleRuleToCheck) {
    //Prepare for the execution
#if DEBUG
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    LOG(DEBUGL) << "Optimizing ruleset...";
#endif
    size_t allRulesSize = 0;
    std::vector<RuleExecutionDetails> allrules;
    for (auto& strata : allIDBRules) {
        for (auto& ruleExecDetails: strata) {
#if DEBUG
            LOG(DEBUGL) << "Optimizing rule " << ruleExecDetails.rule.tostring(NULL, NULL);
#endif
            ruleExecDetails.createExecutionPlans(checkCyclicTerms);
            ruleExecDetails.calculateNVarsInHeadFromEDB();
            ruleExecDetails.lastExecution = lastExecution;
#if DEBUG
            for (const auto& ruleExecPlan : ruleExecDetails.orderExecutions) {
                std::string plan = "";
                for (const auto& literal : ruleExecPlan.plan){
                    plan += " " + literal->tostring(program, &layer);
                }
                LOG(DEBUGL) << "-->" << plan;
            }
            LOG(DEBUGL) << ruleExecDetails.rule.tostring(program, &layer);
#endif
        }
        allRulesSize += strata.size();
    }
    for (auto& ruleExecDetails : allEDBRules) {
        ruleExecDetails.createExecutionPlans(checkCyclicTerms);
    }
    allRulesSize += allEDBRules.size();
    allrules.reserve(allRulesSize);

    //Setup the datastructures to handle the chase
    std::copy(allEDBRules.begin(), allEDBRules.end(), std::back_inserter(allrules));
    for (int k = 0; k < allIDBRules.size(); k++) {
        std::copy(allIDBRules[k].begin(), allIDBRules[k].end(), std::back_inserter(allrules));
    }
    chaseMgmt = std::shared_ptr<ChaseMgmt>(new ChaseMgmt(allrules,
                typeChase, checkCyclicTerms,
                singleRuleToCheck,
                predIgnoreBlock));
#if DEBUG
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(DEBUGL) << "Runtime ruleset optimization ms = " << sec.count() * 1000;
#endif
}

void SemiNaiver::run(size_t lastExecution, size_t it, unsigned long *timeout,
        bool checkCyclicTerms, int singleRuleToCheck, PredId_t predIgnoreBlock) {
    this->checkCyclicTerms = checkCyclicTerms;
    this->foundCyclicTerms = false;
    this->predIgnoreBlock = predIgnoreBlock; //Used in the RMSA

    running = true;
    iteration = it;
    startTime = std::chrono::system_clock::now();
#ifdef WEBINTERFACE
    statsLastIteration = -1;
#endif
    listDerivations.clear();

    prepare(lastExecution, singleRuleToCheck);

    //Used for statistics
    std::vector<StatIteration> costRules;

    if ((typeChase == TypeChase::RESTRICTED_CHASE ||
                typeChase == TypeChase::SUM_RESTRICTED_CHASE)
            && program->areExistentialRules()) {
        //Split the program: First execute the rules without existential
        //quantifiers, then all the others
        std::vector<RuleExecutionDetails> originalEDBruleset = allEDBRules;
        std::vector<std::vector<RuleExecutionDetails>> originalRuleset = allIDBRules;

        //Only non-existential rules
        std::vector<RuleExecutionDetails> tmpEDBRules;
        for(auto &r : originalEDBruleset) {
            if (!r.rule.isExistential())  {
                tmpEDBRules.push_back(r);
            }
        }
        std::vector<std::vector<RuleExecutionDetails>> tmpIDBRules(nStratificationClasses);
        for (int k = 0; k < originalRuleset.size(); k++) {
            for(auto &r : originalRuleset[k]) {
                if (!r.rule.isExistential()) {
                    tmpIDBRules[k].push_back(r);
                }
            }
        }
        //Only existential rules
        std::vector<RuleExecutionDetails> tmpExtEDBRules;
        for(auto &r : originalEDBruleset) {
            if (r.rule.isExistential())  {
                tmpExtEDBRules.push_back(r);
            }
        }
        std::vector<std::vector<RuleExecutionDetails>> tmpExtIDBRules(nStratificationClasses);
        for (int k = 0; k < originalRuleset.size(); k++) {
            for(auto &r : originalRuleset[k]) {
                if (r.rule.isExistential()) {
                    tmpExtIDBRules[k].push_back(r);
                }
            }
        }
        int loopNr = 0;
        std::vector<RuleExecutionDetails> emptyRuleset;
        bool mayHaveTimeout = timeout != NULL && *timeout != 0;
        while (true) {
            bool resp1;
            if (loopNr == 0)
                resp1 = executeRules(tmpEDBRules, tmpIDBRules, costRules, 0, true, timeout);
            else
                resp1 = executeRules(emptyRuleset, tmpIDBRules, costRules, 0, true, timeout);
            bool resp2;
            if (loopNr == 0)
                resp2 = executeRules(tmpExtEDBRules, tmpExtIDBRules,
                        costRules, iteration == 0 ? 1 : iteration, false, timeout);
            else
                resp2 = executeRules(emptyRuleset, tmpExtIDBRules, costRules,
                        iteration == 0 ? 1 : iteration, false, timeout);
            if ((!resp1 && !resp2) || (foundCyclicTerms && typeChase != TypeChase::SUM_RESTRICTED_CHASE)) {
                break; //Fix-point
            }
            loopNr++;
            if (mayHaveTimeout && *timeout == 0) {
                break;
            }
        }
    } else {
        executeRules(allEDBRules, allIDBRules, costRules, 0, true, timeout);
    }

    running = false;
    LOG(DEBUGL) << "Finished process. Iterations=" << iteration;

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
        const size_t limitView,
        bool fixpoint, unsigned long *timeout) {
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
                limitView,
                NULL);
        newDer |= response;
        if (timeout != NULL && *timeout != 0) {
            std::chrono::duration<double> s = std::chrono::system_clock::now() - startTime;
            if (s.count() > *timeout) {
                *timeout = 0;   // To indicate materialization was stopped because of timeout.
                return newDer;
            }
        }
        std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
        StatIteration stat;
        stat.iteration = iteration;
        stat.rule = &ruleset[currentRule].rule;
        stat.time = sec.count() * 1000;
        stat.derived = response;
        costRules.push_back(stat);
        if (limitView > 0) {
            // Don't use iteration here, because lastExecution determines which data we'll look at during the next round,
            // and limitView determines which data we are considering now. There should not be a gap.
            ruleset[currentRule].lastExecution = limitView;
        } else {
            ruleset[currentRule].lastExecution = iteration;
        }
        iteration++;

		if (checkCyclicTerms) {
			foundCyclicTerms = chaseMgmt->checkCyclicTerms(currentRule);
			if (foundCyclicTerms) {
				LOG(DEBUGL) << "Found a cyclic term";
				return newDer;
			}
		}

        if (response) {

            if ((typeChase == TypeChase::RESTRICTED_CHASE ||
                        typeChase == TypeChase::SUM_RESTRICTED_CHASE) &&
                    ruleset[currentRule].rule.isExistential()) {
                return response;
            }

            //I disable this...
            if (false && ruleset[currentRule].rule.isRecursive() && limitView == 0) {
                //Is the rule recursive? Go until saturation...
                int recursiveIterations = 0;
                do {
                    // LOG(DEBUGL) << "Iteration " << iteration;
                    start = std::chrono::system_clock::now();
                    recursiveIterations++;
                    response = executeRule(ruleset[currentRule],
                            iteration,
                            limitView,
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
                    if (timeout != NULL && *timeout != 0) {
                        std::chrono::duration<double> s = std::chrono::system_clock::now() - startTime;
                        if (s.count() > *timeout) {
                            *timeout = 0;   // To indicate materialization was stopped because of timeout.
                            return newDer;
                        }
                    }

                    if (checkCyclicTerms) {
                        foundCyclicTerms = chaseMgmt->checkCyclicTerms(currentRule);
                        if (foundCyclicTerms)
                            return newDer;
                    }

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
            LOG(INFOL) << "Finish pass over the rules. Step=" << iteration << ". IDB RulesWithDerivation=" <<
                nRulesOnePass << " out of " << ruleset.size() << " Derivations so far " << countAllIDBs();
            printCountAllIDBs("After step " + to_string(iteration) + ": ");
            nRulesOnePass = 0;

            //Get the top 10 rules in the last iteration
            std::sort(costRules.begin(), costRules.end());
            std::string out = "";
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

void SemiNaiver::storeOnFile(std::string path, const PredId_t pred, const bool decompress, const int minLevel, const bool csv) {
    FCTable *table = predicatesTables[pred];
    char buffer[MAX_TERM_SIZE];

    std::ofstream streamout(path);
    if (streamout.fail()) {
        throw("Could not open " + path + " for writing");
    }

    if (table != NULL && !table->isEmpty()) {
        FCIterator itr = table->read(0);
        if (! itr.isEmpty()) {
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
                                    row += VLogUtils::csvString(std::string(buffer));
                                } else {
                                    row += "\t";
                                    row += std::string(buffer);
                                }
                            } else {
                                uint64_t v = iitr->getCurrentValue(m);
                                std::string t = "" + std::to_string(v >> 40) + "_"
                                    + std::to_string((v >> 32) & 0377) + "_"
                                    + std::to_string(v & 0xffffffff);
                                if (csv) {
                                    if (first) {
                                        first = false;
                                    } else {
                                        row += ",";
                                    }
                                    row += VLogUtils::csvString(t);
                                } else {
                                    row += "\t";
                                    row += t;
                                }
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
        }
    }
    streamout.close();
}

static std::string generateFileName(std::string name) {
    std::stringstream stream;

    stream << std::oct << std::setfill('0');

    for(char ch : name) {
        int code = static_cast<unsigned char>(ch);

        if (code != '\\' && code != '/') {
            stream.put(ch);
        } else {
            stream << "\\" << std::setw(3) << code;
        }
    }

    return stream.str();
}

void SemiNaiver::storeOnFiles(std::string path, const bool decompress,
        const int minLevel, const bool csv) {
    char buffer[MAX_TERM_SIZE];

    Utils::create_directories(path);

    //I create a new file for every idb predicate
    for (PredId_t i = 0; i < program->getNPredicates(); ++i) {
        FCTable *table = predicatesTables[i];
        if (table != NULL && !table->isEmpty()) {
            storeOnFile(path + "/" + generateFileName(program->getPredicateName(i)), i, decompress, minLevel, csv);
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

bool SemiNaiver::bodyChangedSince(Rule &rule, size_t iteration) {
    LOG(DEBUGL) << "bodyChangedSince, iteration = " << iteration <<
        " Rule: " << rule.tostring(program, &layer);
    const std::vector<Literal> &body = rule.getBody();
    const int nBodyLiterals = body.size();
    for (int i = 0; i < nBodyLiterals; ++i) {
        if (body[i].getPredicate().getType() == EDB) {
            if (iteration == 0) {
                LOG(DEBUGL) << "Returns true";
                return true;
            }
            continue;
        }

        PredId_t id = body[i].getPredicate().getId();
        FCTable *table = predicatesTables[id];
        if (table == NULL || table->isEmpty()) {
            LOG(DEBUGL) << "Continuing: empty table";
            continue;
        }
        if (table->getMaxIteration() < iteration) {
            LOG(DEBUGL) << "Continuing: old table";
            continue;
        }
        LOG(DEBUGL) << "Returns true";
        return true;
    }
    LOG(DEBUGL) << "Returns false";
    return false;
}

bool SemiNaiver::checkIfAtomsAreEmpty(const RuleExecutionDetails &ruleDetails,
        const RuleExecutionPlan &plan,
        size_t limitView,
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
        if (limitView > 0 && max >= limitView) {
            max = limitView - 1;
        }
        if (min > max) {
            return true;
        }

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
        const size_t iteration,
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

/**
 * SemiNaiver::reorderPlanForNegatedLiterals.
 * @author Larry Gonz\'alez
 * @note: based on SemiNaiver::reorderPlan
 *
 * To support input negation, we need to guarantee that the variables in the
 * negated literal are bounded by other variables in previous literals. This
 * function check if the RuleExecutionPlan satisfy that restriction, and if it
 * doesn't, then it move back the negated literal until their variables are
 * bounded. If it not possible, then it thows an error.
 *
 * @param plan:  &RuleExecutionPlan
 * @param heads: vector < Literal >
 * */
void SemiNaiver::reorderPlanForNegatedLiterals(RuleExecutionPlan &plan, const std::vector<Literal> &heads){
    std::set<uint8_t> bounded_vars;
    std::vector<uint8_t> literal_indexes;
    std::vector<uint8_t> new_order;
    bounded_vars.clear();
    literal_indexes.clear();
    new_order.clear();

    bool c1; // isnegated
    bool c2; // are variables bounded?

    for (uint8_t i=0; i < plan.plan.size(); ++i)
        literal_indexes.push_back(i);

    // literal_indexes[i] is the index --from plan.plan-- of the next literal
    while(!literal_indexes.empty()) {
        int i;
        const Literal *literal_i;
        std::vector<uint8_t> vars_i;

        for (i=0; i < literal_indexes.size(); ++i){
            literal_i = plan.plan[literal_indexes[i]];
            vars_i = literal_i->getAllVars(); // unique variables ordered by appearing order
            std::set<uint8_t> s_vars_i(vars_i.begin(), vars_i.end()); //set

            c1 = literal_i->isNegated();
            c2 = std::includes(std::begin(bounded_vars), std::end(bounded_vars),
                    std::begin(s_vars_i), std::end(s_vars_i));
            if (!c1 || (c1 && c2))
                break;
        }
        if (i >= literal_indexes.size())
            throw std::runtime_error("Input Negation Error. Impossible to bound variables in negated atom.");

        for (auto ele: vars_i)
            bounded_vars.insert(ele);
        new_order.push_back(literal_indexes[i]);
        literal_indexes.erase(literal_indexes.begin() + i);
    }
    bool toReorder = false;

    for (int i=0; i< plan.plan.size(); ++i)
        if (new_order[i] != i){
            toReorder = true;
            break;
        }

    if(toReorder)
        plan = plan.reorder(new_order, heads, checkCyclicTerms);
}

void SemiNaiver::reorderPlan(RuleExecutionPlan &plan,
        const std::vector<size_t> &cards,
        const std::vector<Literal> &heads,
        bool copyAllVars) {
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
        plan = plan.reorder(orderLiterals, heads, copyAllVars);
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
        const size_t iteration, const size_t limitView,
        std::vector<ResultJoinProcessor*> *finalResultContainer) {
    Rule rule = ruleDetails.rule;
    if (! bodyChangedSince(rule, ruleDetails.lastExecution)) {
        LOG(INFOL) << "Rule application: " << iteration << ", rule " << rule.tostring(program, &layer) << " skipped because dependencies did not change since the previous application of this rule";
        return false;
    }

    bool answer = true;
    std::vector<Literal> heads = rule.getHeads();
    answer &= executeRule(ruleDetails, heads, iteration, limitView, finalResultContainer);
    return answer;
}


bool SemiNaiver::executeRule(RuleExecutionDetails &ruleDetails,
        std::vector<Literal> &heads,
        const size_t iteration,
        const size_t limitView,
        std::vector<ResultJoinProcessor*> *finalResultContainer) {
    Rule rule = ruleDetails.rule;

#ifdef WEBINTERFACE
    // Cannot run multithreaded in this case.
    currentRule = rule.tostring(program, &layer);
#endif

    if (ignoreExistentialRules && rule.isExistential()) {
        return false; //Skip the execution of existential rules if the flag is
        //set (should be only during the execution of RMFA or RMFC).
    }

    LOG(DEBUGL) << "Iteration: " << iteration << " Rule: " << rule.tostring(program, &layer);

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
        bool isOneRelEmpty = checkIfAtomsAreEmpty(ruleDetails, plan, limitView, cards);
        if (isOneRelEmpty) {
            LOG(DEBUGL) << "Aborting this combination";
            continue;
        }

        //Reorder the list of atoms depending on the observed cardinalities
        reorderPlan(plan, cards, heads, checkCyclicTerms);
        //Reorder for input negation (can we merge these two?)
        reorderPlanForNegatedLiterals(plan, heads);

#ifdef DEBUG
        std::string listLiterals = "EXEC COMB: ";
        for (const auto literal : plan.plan) {
            listLiterals += literal->tostring(program, &layer);
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
                            chaseMgmt,
                            chaseMgmt->hasRuleToCheck(),
                            ignoreDuplicatesElimination);
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
                                !multithreaded ? -1 : nthreads,
                                ignoreDuplicatesElimination);
                    } else {
                        joinOutput = new FinalRuleProcessor(
                                plan.posFromFirst[optimalOrderIdx],
                                plan.posFromSecond[optimalOrderIdx],
                                listDerivations,
                                heads, &ruleDetails,
                                (uint8_t) orderExecution, iteration,
                                finalResultContainer == NULL,
                                !multithreaded ? -1 : nthreads, this,
                                ignoreDuplicatesElimination);
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
            if (limitView != 0) {
                // For execution of the restricted chase, we must limit the
                // view: we may not include data from the current round.
                // We use a parameter "limitView", which in this case indicates
                // the iteration number after the last round.
                if (max >= limitView) {
                    max = limitView - 1;
                }
            }
            if (min > max) {
                optimalOrderIdx++;
                continue;
            }
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
            if (!lastLiteral && !first) {
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
        LOG(INFOL) << "Rule application: " << iteration << ", derived " << getNLastDerivationsFromList() << " new tuple(s) using rule " << rule.tostring(program, &layer);
    } else {
        LOG(INFOL) << "Rule application: " << iteration << ", derived no new tuples using rule " << rule.tostring(program, &layer);
    }
    LOG(DEBUGL) << "Combinations " << orderExecution
        << ", Processed IDB Tables=" << processedTables
        << ", Total runtime " << stream.str()
        << ", join " << durationJoin.count() * 1000
        << "ms, consolidation " << durationConsolidation.count() * 1000
        << "ms, retrieving first atom " << durationFirstAtom.count() * 1000 << "ms.";

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
    if (literal.isNegated()) {
        return 1;
    }
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
    if (predicatesTables[predid] == NULL) {
        return FCIterator();
    }
    return predicatesTables[predid]->read(0);
}

size_t SemiNaiver::getSizeTable(const PredId_t predid) const {
    return predicatesTables[predid]->getNAllRows();
}

SemiNaiver::~SemiNaiver() {
    // Don't refer to program. It may already have been deallocated.
    for (int i = 0; i < predicatesTables.size(); ++i) {
        if (predicatesTables[i] != NULL) {
            delete predicatesTables[i];
        }
    }

    /*for (EDBCache::iterator itr = edbCache.begin(); itr != edbCache.end(); ++itr) {
      delete itr->second;
      }*/
}

size_t SemiNaiver::countAllIDBs() {
    long c = 0;
    for (PredId_t i = 0; i < program->getNPredicates(); ++i) {
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
    for (PredId_t i = 0; i < program->getNPredicates(); ++i) {
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
                    out.push_back(std::make_pair(program->getPredicateName(i), stats));
                }

                //long count = predicatesTables[i]->getNAllRows();
                //out.push_back(std::make_pair(program->getPredicateName(i), count));
            }
        }
    }
    return out;
}
#endif

void SemiNaiver::printCountAllIDBs(std::string prefix) {
    long c = 0;
    long emptyRel = 0;
    for (PredId_t i = 0; i < program->getNPredicates(); ++i) {
        if (predicatesTables[i] != NULL) {
            if (program->isPredicateIDB(i)) {
                long count = predicatesTables[i]->getNAllRows();
                if (count == 0) {
                    emptyRel++;
                }
                std::string predname = program->getPredicateName(i);
                LOG(DEBUGL) << prefix << "Cardinality of " <<
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
std::string SemiNaiver::getCurrentRule() {
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
