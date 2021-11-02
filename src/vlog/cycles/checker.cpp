#include <vlog/cycles/checker.h>
#include <vlog/reasoner.h>
#include <vlog/seminaiver.h>
#include <vlog/graph.h>

#include <kognac/logs.h>

typedef std::pair<PredId_t, uint8_t> vpos;
typedef std::pair<uint32_t, uint8_t> rpos;

int Checker::checkFromFile(std::string ruleFile, std::string alg,
        std::string sameasAlgo,
        EDBLayer &db, bool rewriteMultihead) {
    //Parse the rules into a program
    Program p(&db);
    std::string s = p.readFromFile(ruleFile, rewriteMultihead);
    if (s != "") {
        LOG(ERRORL) << "Error: " << s;
        throw 10;
    }
    return check(p, alg, sameasAlgo, db);
}

int Checker::checkFromString(std::string rulesString, std::string alg,
        std::string sameasAlgo,
        EDBLayer &db, bool rewriteMultihead) {
    //Parse the rules into a program
    Program p(&db);
    std::string s = p.readFromString(rulesString, rewriteMultihead);
    if (s != "") {
        LOG(ERRORL) << "Error: " << s;
        throw 10;
    }
    return check(p, alg, sameasAlgo, db);
}

int Checker::check(Program &p, std::string alg, std::string sameasAlgo,
        EDBLayer &db) {
    if (! p.areExistentialRules()) {
        LOG(INFOL) << "No existential rules, termination detection not run";
        return 1;
    }

    if (sameasAlgo != "" && sameasAlgo != "NOTHING" && alg != "MFA" && alg != "EMFA") {
        LOG(ERRORL) << "The only acyclicity conditions that support equality"
            "reasoning are MFA and EMFA";
        throw 10;
    }

    if (alg == "MFA") {
        // Model Faithful Acyclic
        return MFA(p, sameasAlgo) ? 1 : 0;
    } else if (alg == "JA") {
        // Joint Acyclic
        return JA(p, false) ? 1 : 0;
    } else if (alg == "RJA") {
        // Restricted Joint Acyclic
        return JA(p, true) ? 1 : 0;
    } else if (alg == "MFC") {
        // Model Faithful Cyclic
        // This one, as the name already suggests, is the other way around: if true, there is a cycle,
        // so we know it won't terminate in some cases.
        return MFC(p) ? 2 : 0;
    } else if (alg == "RMFC") {
        // Restricted Model Faithful Cyclic
        // This one, as the name already suggests, also is the other way around: if true, there is a cycle,
        // so we know it won't terminate in some cases.
        return MFC(p, true) ? 2 : 0;
    } else if (alg == "RMFA") {
        return RMFA(p) ? 1 : 0;
    } else if (alg == "RMSA") {
        return RMSA(p) ? 1 : 0;
    } else if (alg == "MSA") {
        // Model Summarisation Acyclic
        return MSA(p) ? 1 : 0;
    } else if (alg == "EMFA") {
        return EMFA(p) ? 1 : 0;
    } else {
        LOG(ERRORL) << "Unknown algorithm: " << alg;
        throw 10;
        // return 0;
    }
}

// Add entries to the critical instance for each predicate, not just EDB ones.
static void addIDBCritical(Program &p, EDBLayer *db) {
    bool hasNewEDB[256];
    memset(hasNewEDB, 0, 256);
    std::vector<PredId_t> allPredicates = p.getAllPredicateIDs();
    for (PredId_t v : allPredicates) {
        Predicate pred = p.getPredicate(v);
        if (pred.getType() == IDB) {
            std::string name = p.getPredicateName(v);
            LOG(DEBUGL) << "addIDBCritical: " << name;
            if (name.find("__EXCLUDE_DUMMY__") == 0 || name.find("__GENERATED_PRED__") == 0) {
                LOG(DEBUGL) << "Continuing";
                continue;
            }
            // Add a rule with a dummy EDB predicate
            uint8_t cardinality = pred.getCardinality();
            std::string edbName = "__DUMMY__" + std::to_string(cardinality);
            LOG(DEBUGL) << "Checking existence of " << edbName;
            if (! hasNewEDB[cardinality]) {
                PredId_t predId = p.getOrAddPredicate(edbName, cardinality);
                std::vector<std::vector<string>> facts;
                std::vector<string> fact;
                for (int i = 0; i < cardinality; ++i) {
                    fact.push_back("*");
                }
                facts.push_back(fact);
                db->addInmemoryTable(edbName, predId, facts);
                hasNewEDB[cardinality] = true;
            }
            std::string rule = p.getPredicateName(v) + "(";
            std::string paramList = "";
            for (int i = 0; i < cardinality; i++) {
                paramList = paramList + "A" + std::to_string(i);
                if (i < cardinality - 1) {
                    paramList = paramList + ",";
                }
            }
            rule = rule + paramList + ") :- " + edbName + "(" + paramList + ")";
            LOG(DEBUGL) << "Adding rule for IDB critical: " << rule;
            p.parseRule(rule, false);
        }
    }
}

void Checker::createCriticalInstance(Program &newProgram,
        Program &p,
        EDBLayer *db,
        EDBLayer &layer) {

    //Populate the critical instance with new facts
    for(auto p : db->getAllPredicateIDs()) {
        std::vector<std::vector<string>> facts;
        std::vector<string> fact;
        for (int i = 0; i < db->getPredArity(p); ++i) {
            fact.push_back("*");
        }
        facts.push_back(fact);
        layer.addInmemoryTable(db->getPredName(p), facts);
        LOG(DEBUGL) << "Adding inmemorytable for " << db->getPredName(p);
    }

    // Rewrite rules: all constants must be replaced with "*".
    std::vector<Rule> rules = p.getAllRules();
    for (auto rule : rules) {
        std::string ruleString = rule.toprettystring(&p, p.getKB(), true);
        LOG(DEBUGL) << "Adding rule replacing constants: " << ruleString;
        newProgram.parseRule(ruleString, false);
    }

    // The critical instance should have initial values for ALL predicates,
    // not just the EDB ones ... --Ceriel
    addIDBCritical(newProgram, &layer);
}

bool Checker::MFA(Program &p, std::string sameasAlgo) {
    // Create  the critical instance (cdb)
    EDBLayer *db = p.getKB();
    EDBLayer layer(*db, false);

    //I must rewrite the algorithms to support equality reasoning here and not
    //later because dummy rules are added during the computation of the
    //critical instance
    if (sameasAlgo == "AXIOM") {
        //Rewrite the rules to add the equality axioms
        p.axiomatizeEquality();
#if DEBUG
        for(auto &r : p.getAllRules()) {
            LOG(INFOL) << "After AXIOM " << r.tostring(&p, &layer);
        }
#endif
    } else if (sameasAlgo == "SING") {
        p.singulariseEquality();
#if DEBUG
        for(auto &r : p.getAllRules()) {
            LOG(INFOL) << "After SING " << r.tostring(&p, &layer);
        }
#endif
    } else if (sameasAlgo != "" && sameasAlgo != "NOTHING") {
        LOG(ERRORL) << "Type of equality algorithm not recognized";
        throw 10;
    }

    Program newProgram(&layer);
    createCriticalInstance(newProgram, p, db, layer);

    //Launch the skolem chase with the check for cyclic terms
    std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(layer,
            &newProgram, true, true, false, TypeChase::SKOLEM_CHASE, 1, 0, false,
            NULL, "");
    sn->checkAcyclicity();
    //if check succeeds then return 0 (we don't know)
    if (sn->isFoundCyclicTerms()) {
        return false;   // Not MFA
    } else {
        return true;
    }
}

bool Checker::MSA(Program &p) {
    // Create  the critical instance (cdb)
    EDBLayer *db = p.getKB();
    EDBLayer layer(*db, false);

    Program newProgram(&layer);
    createCriticalInstance(newProgram, p, db, layer);

    //Launch a simpler version of the skolem chase with the check for cyclic terms
    std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(layer,
            &newProgram, true, true, false, TypeChase::SUM_CHASE, 1, 0, false);
    sn->checkAcyclicity();
    //if check succeeds then return 0 (we don't know)
    if (sn->isFoundCyclicTerms()) {
        return false;   // Not MSA
    } else {
        return true;
    }
}

bool Checker::EMFA(Program &p) {
    // Create  the critical instance (cdb)
    EDBLayer *db = p.getKB();
    EDBLayer layer(*db, false);

    Program newProgram(&layer);
    createCriticalInstance(newProgram, p, db, layer);

    //Launch a simpler version of the skolem chase with the check for cyclic terms
    std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(layer,
            &newProgram, true, true, false, TypeChase::SKOLEM_CHASE, 1, 0, false, NULL);
    sn->checkAcyclicity();
    //if check succeeds then return 0 (we don't know)
    if (sn->isFoundCyclicTerms()) {
        return false;
    } else {
        return true;
    }
}

// Add special targets that have the head of existential rules as body, and first non-existential variables in the head, followed by the existential variables.
// So, for instance, if we have a rule P(X,Y),Q(Y) :- R(X), then we add a rule Z(X,Y) :- P(X,Y),Q(Y). This is for the implementation
// of the blocked check. This check matches the non-existential variables with a pattern, and checks that the
// existential variables actually have "existential" values.
void Checker::addBlockCheckTargets(Program &p, PredId_t ignorePredId) {
    std::vector<Rule> rules = p.getAllRules();
    for (auto rule : rules) {
        if (rule.isExistential()) {
            auto ruleHeadAtoms = rule.getHeads();
            //Remove from newBody any atom with the special predicate
            std::vector<Literal> newBody;
            for(size_t i = 0; i < ruleHeadAtoms.size(); ++i) {
                auto &atom = ruleHeadAtoms[i];
                if (atom.getPredicate().getId() != ignorePredId) {
                    newBody.push_back(atom);
                }
            }

            std::vector<Var_t> headvars = rule.getFrontierVariables(ignorePredId);
            std::vector<Var_t> existentials = rule.getExistentialVariables();
            std::string newPred = "__GENERATED_PRED__" + std::to_string(rule.getId());
            Predicate newp = p.getPredicate(p.getOrAddPredicate(newPred, headvars.size() + existentials.size()));
            VTuple t(headvars.size() + existentials.size());
            for (int i = 0; i < headvars.size(); i++) {
                t.set(VTerm(headvars[i], 0), i);
            }
            for (int i = 0; i < existentials.size(); i++) {
                t.set(VTerm(existentials[i], 0), headvars.size() + i);
            }
            std::vector<Literal> h;
            h.push_back(Literal(newp, t));
            p.addRule(h, newBody);
        }
    }
}

bool Checker::RMFA(Program &p) {
    // Create  the critical instance (cdb)
    EDBLayer *db = p.getKB();
    EDBLayer layer(*db, false);

    Program newProgram(&layer);
    createCriticalInstance(newProgram, p, db, layer);

    addBlockCheckTargets(newProgram);
    //Launch the (special) restricted chase with the check for cyclic terms
    std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(layer,
            &newProgram, true, true, false, TypeChase::RESTRICTED_CHASE, 1, 0, false);
    sn->checkAcyclicity();
    //if check succeeds then return 0 (we don't know)
    if (sn->isFoundCyclicTerms()) {
        return false;   // Not RMFA
    } else {
        return true;
    }
}

bool Checker::RMSA(Program &originalProgram) {
    // Create  the critical instance (cdb)
    EDBLayer *db = originalProgram.getKB();
    EDBLayer layer(*db, false);


    Program programWithCritical(&layer);
    createCriticalInstance(programWithCritical, originalProgram, db, layer);

    //Add a special predicate to the head of all existential rules to track the
    //dependencies
    std::string nameSpecialPred = "__S__";
    auto specialPredId = programWithCritical.getOrAddPredicate(nameSpecialPred, 2);
    Predicate specialPred(specialPredId, 0, IDB, 2);

    std::vector<Rule> newRules;
    size_t ruleCounter = programWithCritical.getAllRules().size() + 1;
    for(auto &rule : programWithCritical.getAllRules()) {
        if (rule.isExistential()) {
            std::vector<Literal> newHeads;
            auto varsInHeadAndBody = rule.getFrontierVariables();
            if (varsInHeadAndBody.size() > 0) {
                //For each existential var, add a new atom in the head
                auto varsNotInBody = rule.getExistentialVariables();
                for(auto varNotInBody : varsNotInBody) {
                    //Create a special predicate
                    //std::string nameSpecialPredVar = "__SR_" + std::to_string(rule.getId()) + "_" + std::to_string(varNotInBody) + "__";
                    //auto specialPredVarId = programWithCritical.getOrAddPredicate(nameSpecialPredVar, varsInHeadAndBody.size() + 1);
                    //Predicate specialPredVar(specialPredVarId, 0, IDB, varsInHeadAndBody.size() + 1);
                    for(size_t i = 0; i < varsInHeadAndBody.size(); ++i) {
                        VTuple t(2);
                        auto varInHeadAndBody = varsInHeadAndBody[i];
                        t.set(VTerm(varInHeadAndBody, 0), 0);
                        t.set(VTerm(varNotInBody, 0), 1);
                        Literal specialLiteral = Literal(specialPred, t);
                        newHeads.push_back(specialLiteral);
                    }

                    /*//Add also a new rule (necessary to compute the cycles
                      for(size_t i = 0; i < varsInHeadAndBody.size(); ++i) {
                      std::vector<Literal> auxBody;
                      auxBody.push_back(specialLiteral);
                      std::vector<Literal> auxHead;
                      VTuple t(2);
                      t.set(VTerm(varsInHeadAndBody[i], 0), 0);
                      t.set(VTerm(varNotInBody, 0), 1);
                      auxHead.push_back(Literal(specialPred, t));
                      newRules.push_back(Rule(ruleCounter++, auxHead, auxBody));
                      }*/
                }
            }
            auto &heads = rule.getHeads();
            for (auto &head : heads) {
                newHeads.push_back(head);
            }
            newRules.push_back(Rule(rule.getId(), newHeads, rule.getBody(), rule.isEGD()));
        } else {
            newRules.push_back(rule);
        }
    }

    //Add a couple of transitive rules for creating the cycles
    //These rules are
    //S_TRANS(X,Y) :- S(X,Y)
    std::string nameSpecialPredTrans = "__S_TRANS__";
    auto specialPredTransId = programWithCritical.getOrAddPredicate(nameSpecialPredTrans, 2);
    Predicate specialPredTrans(specialPredTransId, 0, IDB, 2);
    VTuple t(2);
    t.set(VTerm(1, 0), 0);
    t.set(VTerm(2, 0), 1);
    std::vector<Literal> auxBody;
    auxBody.push_back(Literal(specialPred, t));
    std::vector<Literal> auxHead;
    auxHead.push_back(Literal(specialPredTrans, t));
    newRules.push_back(Rule(ruleCounter++, auxHead, auxBody, false));
    //S_TRANS(X,Z) :- S_TRANS(X,Y),S(Y,Z)
    auxBody.clear();
    auxBody.push_back(Literal(specialPredTrans, t));
    t.set(VTerm(2, 0), 0);
    t.set(VTerm(3, 0), 1);
    auxBody.push_back(Literal(specialPred, t));
    auxHead.clear();
    t.set(VTerm(1, 0), 0);
    t.set(VTerm(3, 0), 1);
    auxHead.push_back(Literal(specialPredTrans, t));
    newRules.push_back(Rule(ruleCounter++, auxHead, auxBody, false));

    Program rewrittenPrg = programWithCritical.clone();
    rewrittenPrg.cleanAllRules();
    rewrittenPrg.addAllRules(newRules);

    addBlockCheckTargets(rewrittenPrg, specialPredId);
    for(auto &r : rewrittenPrg.getAllRules()) {
        LOG(DEBUGL) << r.toprettystring(&rewrittenPrg, &layer);
    }

    //Launch the (special) restricted chase with the check for cyclic terms
    std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(layer,
            &rewrittenPrg, true, true, false, TypeChase::SUM_RESTRICTED_CHASE, 1, 0, false);
    sn->checkAcyclicity(-1, specialPredId); //run(0, 1, NULL);

    if (sn->isFoundCyclicTerms()) {
	return false;
    }

    //Parse the content of the special relation. If we find a cycle, then we stop
    bool foundCycles = false;
    auto itr = sn->getTable(specialPredTransId);
    while (!itr.isEmpty() && !foundCycles) {
        auto table = itr.getCurrentTable();
        auto tableItr = table->getIterator();
        while (tableItr->hasNext()) {
            tableItr->next();
            Term_t v1 = tableItr->getCurrentValue(0);
            Term_t v2 = tableItr->getCurrentValue(1);
            //std::cout << v1 <<  " " << v2 << std::endl;
            if (v1 == v2) { //Cycle!
                foundCycles = true;
                break;
            }
        }
        table->releaseIterator(tableItr);
        itr.moveNextCount();
    }
    if (foundCycles) {
        return false;
    } else {
        return true;
    }
}

// Check if all body positions of var are included in input.
static bool containsAll(uint8_t var, std::vector<Literal> &body, std::vector<std::pair<PredId_t, uint8_t>> &input) {
    for (auto literal: body) {
        auto tpl = literal.getTuple();
        auto predId = literal.getPredicate().getId();
        for (int i = 0; i < tpl.getSize(); i++) {
            if (tpl.get(i).getId() == var) {
                std::pair<PredId_t, uint8_t> val(predId, i);
                if (std::find(input.begin(), input.end(), val) == input.end()) {
                    return false;
                }
            }
        }
    }
    return true;
}

static void closure(Program &p, std::map<PredId_t, std::vector<uint32_t>> &occurrences,
        std::vector<std::pair<PredId_t, uint8_t>> &input) {
    bool gotNew = true;
    while (gotNew) {
        gotNew = false;
        std::vector<std::pair<PredId_t, uint8_t>> toProcess = input;
        // For all positions in the set, for all rules, if all body positions of a variable in this
        // rule are contained in the set, add all head positions of this variable.
        for (auto pos : toProcess) {
            for (auto ruleId : occurrences[pos.first]) {
                auto &rule = p.getRule(ruleId);
                auto body = rule.getBody();
                auto vars = rule.getFrontierVariables();
                for (auto var : vars) {
                    if (containsAll(var, body, input)) {

                        // Now, we can add all head positions.
                        for (auto head : rule.getHeads()) {
                            auto tpl = head.getTuple();
                            for (int i = 0; i < tpl.getSize(); i++) {
                                if (tpl.get(i).getId() == var) {
                                    std::pair<PredId_t, uint8_t> val(head.getPredicate().getId(), i);
                                    if (std::find(input.begin(), input.end(), val) == input.end()) {
                                        input.push_back(val);
                                        gotNew = true;
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }
    }
}

static void getAllExtPropagatePositions(Program &p, std::map<rpos, std::vector<vpos>> &allExtVarsPos) {
    auto rules = p.getAllRules();
    // Create a map, for each predicate, to find out in which rule bodies it occurs.
    std::map<PredId_t, std::vector<uint32_t>> occurrences;

    for (auto rule : rules) {
        auto body = rule.getBody();
        for (auto lit : body) {
            occurrences[lit.getPredicate().getId()].push_back(rule.getId());
        }
    }

    for (auto rule : rules) {
        if (rule.isExistential()) {
            // First, get the predicate positions of the existential variables in the head(s)
            std::vector<vpos> predPositions[256];
            std::vector<Var_t> extVars = rule.getExistentialVariables();
            for (auto head : rule.getHeads()) {
                VTuple tpl = head.getTuple();
                for (int i = 0; i < tpl.getSize(); i++) {
                    VTerm term = tpl.get(i);
                    Var_t id = term.getId();
                    if (id != 0) {
                        auto pos = std::find(extVars.begin(), extVars.end(), id);
                        if (pos != extVars.end()) {
                            // Found <head.predicate, i> for variable id.
                            predPositions[pos - extVars.begin()].push_back(std::pair<PredId_t, uint8_t>(head.getPredicate().getId(), i));
                        }
                    }
                }
            }
            for (int i = 0; i < extVars.size(); i++) {
                auto positions = predPositions[i];
                // Compute the closures of these position sets: where do they propagate to?
                closure(p, occurrences, positions);
                // We need the positions sorted to be able to call std::set_intersection later on.
                std::sort(positions.begin(), positions.end());
                rpos pos(rule.getId(), i);
                allExtVarsPos[pos] = positions;
                LOG(TRACEL) << "Position set for rule \"" << rule.tostring() << "\", var " << (int) extVars[i] << ": ";
                for (auto pos : positions) {
                    LOG(TRACEL) << "    pred: " << pos.first << ", i = " << (int) pos.second;
                }
            }
        }
    }
}

static void generateConstants(Program &p, std::map<PredId_t, std::vector<std::vector<std::string>>> &edbSet,
        int &constantCount,
        std::map<Var_t, std::string> &varConstantMap,
        const std::vector<Literal> &toAdd) {
    for (auto lit : toAdd) {
        std::vector<std::string> value;
        PredId_t predid = lit.getPredicate().getId();
        VTuple tpl = lit.getTuple();
        for (int i = 0; i < tpl.getSize(); i++) {
            VTerm t = tpl.get(i);
            if (t.isVariable()) {
                Var_t id = t.getId();
                auto pair = varConstantMap.find(id);
                std::string val;
                if (pair == varConstantMap.end()) {
                    val = "_GENC" + std::to_string(constantCount++);
                    varConstantMap[id] = val;
                } else {
                    val = pair->second;
                }
                value.push_back(val);
            } else {
                value.push_back(p.getKB()->getDictText(t.getValue()));
            }
            LOG(TRACEL) << "i = " << i << ", value = " << value[value.size()-1];
        }
        edbSet[predid].push_back(value);
    }
}

static bool rja_check(Program &p, std::vector<std::string> &nonGeneratingRules, const Rule &rulev, const Rule &rulew, Var_t x, Var_t v, Var_t w) {
    std::map<PredId_t, std::vector<std::vector<std::string>>> edbSet;
    int constantCount = 0;
    std::map<Var_t, std::string> varConstantMapw;
    std::map<Var_t, std::string> varConstantMapv;

    generateConstants(p, edbSet, constantCount, varConstantMapw, rulew.getBody());

    // Moving on to a different rule now, so new constant map, but keep the value of x,
    // and assign that to v.
    std::string xval = varConstantMapw[x];
    varConstantMapv[v] = xval;

    generateConstants(p, edbSet, constantCount, varConstantMapv, rulev.getHeads());
    generateConstants(p, edbSet, constantCount, varConstantMapv, rulev.getBody());

    EDBLayer layer(*(p.getKB()), false);

    std::vector<std::string> newRules;

    Program newProgram(&p, &layer);

    for (auto rule : nonGeneratingRules) {
        newProgram.parseRule(rule, false);
    }

    for (auto pair : edbSet) {
        std::string edbName = "__DUMMY__" + std::to_string(pair.first);
        int cardinality = pair.second[0].size();
        PredId_t predId = newProgram.getOrAddPredicate(edbName, cardinality);
        layer.addInmemoryTable(edbName, predId, pair.second);
        /*
        for (int i = 0; i < pair.second.size(); i++) {
            LOG(DEBUGL) << "Adding fact:";
            for (int j = 0; j < cardinality; j++) {
                LOG(DEBUGL) << "    " << pair.second[i][j];
            }
        }
        */
        std::string rule = p.getPredicateName(pair.first) + "(";
        std::string paramList = "";
        for (int i = 0; i < cardinality; i++) {
            paramList = paramList + "A" + std::to_string(i);
            if (i < cardinality - 1) {
                paramList = paramList + ",";
            }
        }
        rule = rule + paramList + ") :- " + edbName + "(" + paramList + ")";
        LOG(DEBUGL) << "Adding rule: \"" << rule << "\"";
        newRules.push_back(rule);
    }

    // Add another rule to the ruleset, to determine if what we are looking for is materialized.
    std::string newRule = "__TARGET__(W) :- ";
    bool first = true;
    for (auto head : rulew.getHeads()) {
        auto tpl = head.getTuple();
        if (! first) {
            newRule = newRule + ",";
        }
        first = false;
        newRule = newRule + p.getPredicateName(head.getPredicate().getId()) + "(";
        for (int i = 0; i < tpl.getSize(); i++) {
            VTerm t = tpl.get(i);
            if (i != 0) {
                newRule = newRule + ",";
            }
            std::string val;
            if (t.isVariable()) {
                Var_t id = t.getId();
                auto pair = varConstantMapw.find(id);
                if (pair == varConstantMapw.end()) {
                    if (id == w) {
                        val = "W";
                    } else {
                        val = "V" + std::to_string(id);
                    }
                } else {
                    val = pair->second;
                }
            } else {
                val = p.getKB()->getDictText(t.getValue());
            }
            newRule = newRule + val;
        }
        newRule = newRule + ")";
    }

    newRules.push_back(newRule);
    LOG(DEBUGL) << "Adding rule: \"" << newRule << "\"";

    for (auto rule: newRules) {
        newProgram.parseRule(rule, false);
    }

    std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(layer,
            &newProgram, true, true, false, TypeChase::SKOLEM_CHASE, 1, 0, false);
    sn->run();
    Reasoner r((uint64_t) 0);
    Dictionary dictVariables;
    Literal query = newProgram.parseLiteral("__TARGET__(W)", dictVariables);

    TupleIterator *iter = r.getIteratorWithMaterialization(sn.get(), query, false, NULL);
    // TupleIterator *iter = r.getTopDownIterator(query, NULL, NULL, layer, newProgram, false, NULL);
    // TODO: does not work now, because of some unimplemented stuff in inmemorytable.
    bool retval = ! iter->hasNext();
    delete iter;
    return retval;
}

bool Checker::JA(Program &p, bool restricted) {
    std::map<rpos, std::vector<vpos>> allExtVarsPos;
    std::string type = restricted ? "RJA" : "JA";

    getAllExtPropagatePositions(p, allExtVarsPos);

    // Create a graph of dependencies
    Graph g(allExtVarsPos.size());
    if (! restricted) {
        int dest = 0;
        for (auto &it : allExtVarsPos) {
            const Rule &rule = p.getRule(it.first.first);
            auto body = rule.getBody();
            auto vars = rule.getFrontierVariables();
            int src = 0;
            for (auto &it2: allExtVarsPos) {
                for (auto var : vars) {
                    if (containsAll(var, body, it2.second)) {
                        g.addEdge(src, dest);
                        break;
                    }
                }
                src++;
            }
            dest++;
        }
        // Now check if the graph is cyclic.
        // If it is, the ruleset is not JA (Joint Acyclic) (which means that the result is inconclusive).
        // If the ruleset is JA, we know that the chase will terminate.
        if (g.isCyclic()) {
            LOG(DEBUGL) << "Ruleset is not " << type << "!";
            return false;
        }
    } else {
        std::vector<std::string> nonGeneratingRules;
        std::vector<Rule> rules = p.getAllRules();
        for (auto rule : rules) {
            if (! rule.isExistential()) {
                std::string ruleString = rule.toprettystring(&p, p.getKB());
                LOG(DEBUGL) << "NonGeneratingRule: \"" << ruleString << "\"";
                nonGeneratingRules.push_back(ruleString);
            }
        }

        int src = 0;
        for (auto &it : allExtVarsPos) {
            const Rule &rulev = p.getRule(it.first.first);
            auto bodyv = rulev.getBody();
            auto extVarsv = rulev.getExistentialVariables();
            Var_t v = extVarsv[it.first.second];
            int dest = 0;
            for (auto &it2: allExtVarsPos) {
                const Rule &rulew = p.getRule(it2.first.first);
                auto bodyw = rulew.getBody();
                auto vars = rulew.getFrontierVariables();
                auto extVarsw = rulew.getExistentialVariables();
                Var_t w = extVarsw[it2.first.second];
                for (auto var : vars) {
                    if (containsAll(var, bodyw, it.second)) {
                        if (rja_check(p, nonGeneratingRules, rulev, rulew, var, v, w)) {
                            g.addEdge(src, dest);
                            // Now check if the graph is cyclic. Cheap test, and may avoid doing more
                            // rja_checks which are expensive.
                            // If the graph is cyclic, the ruleset is not RJA (Restricted Joint Acyclic)
                            // (which means that the result is inconclusive).
                            // If the ruleset is RJA, we know that the chase will terminate.
                            if (g.isCyclic()) {
                                LOG(DEBUGL) << "Ruleset is not " << type << "!";
                                return false;
                            }
                            break;
                        }
                    }
                }
                dest++;
            }
            src++;
        }
    }

    LOG(DEBUGL) << "RuleSet is " << type << "!";
    return true;
}

bool Checker::MFC(Program &prg, bool restricted) {
    // First, create a copy of the rules.
    Program *restrictedProgram = NULL;

    Program p(&prg, prg.getKB());
    for (auto rule: prg.getAllRules()) {
        p.addRule(rule.getHeads(), rule.getBody());
        LOG(DEBUGL) << "MFC: Added rule " << rule.tostring(NULL, NULL);
    }

    if (restricted) {
        addBlockCheckTargets(p);
        restrictedProgram = getProgramForBlockingCheckRMFC(p);
    }

    std::vector<Rule> r = p.getAllRules();

    std::vector<std::string> rules;
    for (auto rule : r) {
        std::string ruleString = rule.toprettystring(&p, p.getKB());
        rules.push_back(ruleString);
    }
    // Then, for each existential rule, do the MFC check.
    int ruleCount = 0;
    for (auto rule : r) {
        if (rule.isExistential()) {
            // Create an EDB set, by taking the body of this rule, and replace each variable with a unique constant.
            std::vector<std::string> newRules = rules;
            auto body = rule.getBody();
            std::map<PredId_t, std::vector<std::vector<std::string>>> edbSet;
            for (auto lit : body) {
                PredId_t predid = lit.getPredicate().getId();
                std::vector<std::string> value;
                VTuple tpl = lit.getTuple();
                for (int i = 0; i < tpl.getSize(); i++) {
                    VTerm t = tpl.get(i);
                    std::string val;
                    if (t.isVariable()) {
                        Var_t id = t.getId();
                        val = "_GENC" + std::to_string(id);
                    } else {
                        val = p.getKB()->getDictText(t.getValue());
                    }
                    value.push_back(val);
                }
                edbSet[predid].push_back(value);
            }

            EDBLayer layer(*(p.getKB()), false);

            for (auto pair : edbSet) {
                std::string edbName = "__DUMMY__" + std::to_string(pair.first);
                int cardinality = pair.second[0].size();
                PredId_t predId = p.getOrAddPredicate(edbName, cardinality);
                layer.addInmemoryTable(edbName, predId, pair.second);
                std::string rule = p.getPredicateName(pair.first) + "(";
                std::string paramList = "";
                for (int i = 0; i < cardinality; i++) {
                    paramList = paramList + "A" + std::to_string(i);
                    if (i < cardinality - 1) {
                        paramList = paramList + ",";
                    }
                }
                rule = rule + paramList + ") :- " + edbName + "(" + paramList + ")";
                newRules.push_back(rule);
            }

            std::vector<PredId_t> predicates = p.getKB()->getAllPredicateIDs();
            for (auto pred : predicates) {
                if (!edbSet.count(pred)) {
                    uint8_t arity = p.getKB()->getPredArity(pred);
                    std::vector<uint64_t> empty;
                    layer.addInmemoryTable(pred, arity, empty);
                }
            }

            // Now create a new program, and materialize. But: don't apply rules to recursive terms.
            Program newProgram(&p, &layer);
            int count = 0;
            for (auto rule : newRules) {
                LOG(DEBUGL) << "Adding rule: \"" << rule << "\"";
                newProgram.parseRule(rule, false);
                count++;
            }
            std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(layer,
                    &newProgram, true, true, false, restricted ? TypeChase::RESTRICTED_CHASE : TypeChase::SKOLEM_CHASE, 1, 0, false, restrictedProgram);
            sn->checkAcyclicity(ruleCount);
            // If we produce a cyclic term FOR THIS RULE, we have MFC.
            if (sn->isFoundCyclicTerms()) {
                LOG(INFOL) << (restricted ? "R" : "") << "MFC: Cyclic rule: " << rule.toprettystring(&p, p.getKB());
                if (restrictedProgram != NULL) {
                    delete restrictedProgram->getKB();
                    delete restrictedProgram;
                }
                return true;    // MFC
            }
        }
        ruleCount++;
    }
    if (restrictedProgram != NULL) {
        delete restrictedProgram->getKB();
        delete restrictedProgram;
    }
    return false;
}

Program *Checker::getProgramForBlockingCheckRMFC(Program &p) {
    // Create  the critical instance (cdb)
    EDBLayer *db = p.getKB();
    // Add "*" to the dictionary of the original program, so that it will not clash with other
    // generated constants.
    uint64_t id = 0;
    db->getOrAddDictNumber("*", 1, id);
    EDBLayer *layer = new EDBLayer(*db, false);

    //Populate the critical instance with new facts
    for(auto p : db->getAllPredicateIDs()) {
        std::vector<std::vector<string>> facts;
        std::vector<string> fact;
        for (int i = 0; i < db->getPredArity(p); ++i) {
            fact.push_back("*");
        }
        facts.push_back(fact);
        layer->addInmemoryTable(db->getPredName(p), facts);
        LOG(DEBUGL) << "Adding inmemory table for " << db->getPredName(p);
    }

    // Rewrite rules: all existential variables must be replaced with "*".
    std::vector<std::string> newRules;
    std::vector<Rule> rules = p.getAllRules();
    size_t count = 0;
    for (auto rule : rules) {
        std::string output = "";
        std::vector<Var_t> existentials = rule.getExistentialVariables();
        bool first = true;
        for(const auto& head : rule.getHeads()) {
            if (! first) {
                output += ",";
            }
            output += p.getPredicateName(head.getPredicate().getId()) + "(";
            VTuple tuple = head.getTuple();
            for (int i = 0; i < tuple.getSize(); ++i) {
                VTerm term = tuple.get(i);
                if (term.isVariable()) {
                    bool present = false;
                    for (int i = 0; i < existentials.size(); i++) {
                        if (term.getId() == existentials[i]) {
                            present = true;
                            break;
                        }
                    }
                    if (present) {
                        output += "*";
                    } else {
                        output += std::string("A") + std::to_string(tuple.get(i).getId());
                    }
                } else {
                    uint64_t id = tuple.get(i).getValue();
                    char text[MAX_TERM_SIZE];
                    if (db->getDictText(id, text)) {
                        std::string v = Program::compressRDFOWLConstants(std::string(text));
                        output += v;
                    } else {
                        std::string t = db->getDictText(id);
                        if (t == std::string("")) {
                            output += std::to_string(id);
                        } else {
                            output += Program::compressRDFOWLConstants(t);
                        }
                    }
                }
                if (i < tuple.getSize() - 1) {
                    output += std::string(",");
                }
            }
            output += ")";
            first = false;
        }
        output += " :- ";
        first = true;
        for(const auto& bodyAtom : rule.getBody()) {
            if (! first) {
                output += ",";
            }
            first = false;
            output += bodyAtom.toprettystring(&p, db, false);
        }
        if (existentials.size() > 0) {
            // Add negated term allowing for exclusion of a specific binding
            output += ", ~";
            output += "__EXCLUDE_DUMMY__" + std::to_string(count) + "(";
            std::vector<Var_t> vars = rule.getFrontierVariables();
            bool f = true;
            for (int i = 0; i < vars.size(); i++) {
                if (! f) {
                    output += ",";
                }
                f = false;
                output += std::string("A") + std::to_string(vars[i]);
            }
            output += ")";
        }

        LOG(DEBUGL) << "Adding rule: " << output;
        newRules.push_back(output);
        count++;
    }

    Program *newProgram = new Program(&p, layer);
    for (auto rule : newRules) {
        newProgram->parseRule(rule, false);
    }
    LOG(DEBUGL) << "New Program: " << newProgram->tostring();

    // The critical instance should have initial values for ALL predicates,
    // not just the EDB ones ... --Ceriel
    addIDBCritical(*newProgram, layer);
    return newProgram;
}
