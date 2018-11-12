#include <vlog/cycles/checker.h>
#include <vlog/reasoner.h>
#include <vlog/seminaiver.h>

#include <kognac/logs.h>

typedef std::pair<PredId_t, uint8_t> vpos;
typedef std::pair<uint32_t, uint8_t> rpos;

int Checker::check(std::string ruleFile, std::string alg, EDBLayer &db) {
    //Parse the rules into a program
    Program p(&db);
    p.readFromFile(ruleFile, false); //we do not rewrite the heads

    if (alg == "MFA") {
	return MFA(p) ? 1 : 0;
    } else if (alg == "JA") {
	return JA(p, false) ? 1 : 0;
    } else if (alg == "RJA") {
	return JA(p, true) ? 1 : 0;
    } else {
	// TODO: RMFA, MSA, RMSA, RMFC
	LOG(ERRORL) << "Unknown algorithm: " << alg;
	return 0;
    }
}

static void addIDBCritical(Program &p, EDBLayer *db) {
    bool hasNewEDB[256];
    memset(hasNewEDB, 0, 256);
    std::vector<PredId_t> allPredicates = p.getAllPredicateIDs();
    for (PredId_t v : allPredicates) {
	Predicate pred = p.getPredicate(v);
	if (pred.getType() == IDB) {
	    // Add a rule with a dummy EDB predicate
	    uint8_t cardinality = pred.getCardinality();
	    std::string edbName = "__DUMMY__" + std::to_string(cardinality);
	    if (! hasNewEDB[cardinality]) {
		PredId_t predId = p.getOrAddPredicate(edbName, cardinality);
		std::vector<std::vector<string>> facts;
		std::vector<string> fact;
		for (int i = 0; i < cardinality; ++i) {
		    fact.push_back("*");
		}
		facts.push_back(fact);
		db->addInmemoryTable(edbName, predId, facts);
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
	    p.parseRule(rule, false);
	    LOG(DEBUGL) << "Adding rule: " << rule;
	}
    }
}

bool Checker::MFA(Program &p) {
    // Create  the critical instance (cdb)
    EDBLayer *db = p.getKB();
    EDBConf conf("", false);
    EDBLayer layer(conf, false);

    //Populate the critical instance with new facts
    for(auto p : db->getAllPredicateIDs()) {
	std::vector<std::vector<string>> facts;
	std::vector<string> fact;
	for (int i = 0; i < db->getPredArity(p); ++i) {
	    fact.push_back("*");
	}
	facts.push_back(fact);
	layer.addInmemoryTable(db->getPredName(p), facts);
    }

    // Rewrite rules: all constants must be replaced with "*".
    std::vector<std::string> newRules;
    std::vector<Rule> rules = p.getAllRules();
    for (auto rule : rules) {
	std::string ruleString = rule.toprettystring(&p, p.getKB(), true);
	LOG(DEBUGL) << "Adding rule: " << ruleString;
	newRules.push_back(ruleString);
    }

    Program newProgram(&layer);
    for (auto rule : newRules) {
	newProgram.parseRule(rule, false);
    }

    // The critical instance should have initial values for ALL predicates, not just the EDB ones ... --Ceriel
    addIDBCritical(newProgram, &layer);

    //Launch the  skolem chase with the check for cyclic terms
    std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(layer,
	    &newProgram, true, true, false, false, 1, 1, false);
    sn->checkAcyclicity();
    //if check succeeds then return 0 (we don't know)
    if (sn->isFoundCyclicTerms()) {
	return false;	// Not MFA
    } else {
	return true;
    }
}

static void closure(Program &p, std::vector<std::pair<PredId_t, uint8_t>> &input) {
    std::vector<std::pair<PredId_t, uint8_t>> toProcess = input;
    while (toProcess.size() > 0) {
	std::vector<std::pair<PredId_t, uint8_t>> newPos;
	// Go through all the rules, checking all bodies. If a <predicate, pos> matches with a variable of the rule,
	// check for this variable in the heads, and add positions.
	auto rules = p.getAllRules();
	for (auto pos : toProcess) {
	    for (auto rule : rules) {
		std::vector<uint8_t> vars = rule.getVarsInBody();
		auto body = rule.getBody();
		for (auto lit : body) {
		    if (lit.getPredicate().getId() == pos.first) {
			VTerm t = lit.getTermAtPos(pos.second);
			if (t.getId() != 0) {
			    // ALL body positions of this variable in the rule must be in input before we may add the head.
			    bool present = true;
			    for (auto lit1 : body) {
				PredId_t predid = lit1.getPredicate().getId();
				for (int i = 0; i < lit1.getTupleSize(); i++) {
				    VTerm r = lit1.getTermAtPos(i);
				    if (r.getId() == t.getId()) {
					bool found = true;
					vpos vp(predid, i);
					auto it = std::find(input.begin(), input.end(), vp);
					if (it == input.end()) {
					    present = false;
					    break;
					}
				    }
				}
				if (! present) break;
			    }
			    if (! present) {
				continue;
			    }
			    // Now, we can add all head positions.
			    for (auto head : rule.getHeads()) {
				auto tpl = head.getTuple();
				for (int i = 0; i < tpl.getSize(); i++) {
				    if (tpl.get(i).getId() == t.getId()) {
					std::pair<PredId_t, uint8_t> val(head.getPredicate().getId(), i);
					if (std::find(input.begin(), input.end(), val) == input.end()) {
					    input.push_back(val);
					    newPos.push_back(val);
					}
				    }
				}
			    }
			}
		    }
		}
	    }
	}
	toProcess = newPos;
    }
}

static void getAllExtPropagatePositions(Program &p, std::map<rpos, std::vector<vpos>> &allExtVarsPos) {
    auto rules = p.getAllRules();
    for (auto rule : rules) {
	if (rule.isExistential()) {
	    // First, get the predicate positions of the existential variables in the head(s)
	    std::vector<vpos> predPositions[256];
	    std::vector<uint8_t> extVars = rule.getVarsNotInBody();
	    for (auto head : rule.getHeads()) {
		VTuple tpl = head.getTuple();
		for (int i = 0; i < tpl.getSize(); i++) {
		    VTerm term = tpl.get(i);
		    uint8_t id = term.getId();
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
		closure(p, positions);
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
	std::map<uint8_t, std::string> &varConstantMap,
	const std::vector<Literal> &toAdd) {
    for (auto lit : toAdd) {
        std::vector<std::string> value;
        PredId_t predid = lit.getPredicate().getId();
        VTuple tpl = lit.getTuple();
        for (int i = 0; i < tpl.getSize(); i++) {
            VTerm t = tpl.get(i);
            if (t.isVariable()) {
                uint8_t id = t.getId();
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
        }
        edbSet[predid].push_back(value);
    }
}

static bool rja_check(Program &p, const Rule &rulev, const Rule &rulew, uint8_t x, uint8_t v, uint8_t w) {
    std::map<PredId_t, std::vector<std::vector<std::string>>> edbSet;
    int constantCount = 0;
    std::map<uint8_t, std::string> varConstantMapw;
    std::map<uint8_t, std::string> varConstantMapv;

    generateConstants(p, edbSet, constantCount, varConstantMapw, rulew.getBody());

    // Moving on to a different rule now, so new constant map, but keep the value of x,
    // and assign that to v.
    std::string xval = varConstantMapw[x];
    varConstantMapv[v] = xval;

    generateConstants(p, edbSet, constantCount, varConstantMapv, rulev.getHeads());
    generateConstants(p, edbSet, constantCount, varConstantMapv, rulev.getBody());

    EDBConf conf("", false);
    EDBLayer layer(conf, false);

    std::vector<std::string> nonGeneratingRules;
    std::vector<Rule> rules = p.getAllRules();
    for (auto rule : rules) {
	if (! rule.isExistential()) {
	    std::string ruleString = rule.toprettystring(&p, p.getKB());
	    LOG(DEBUGL) << "NonGeneratingRule: \"" << ruleString << "\"";
	    nonGeneratingRules.push_back(ruleString);
	}
    }

    for (auto pair : edbSet) {
	std::string edbName = "__DUMMY__" + std::to_string(pair.first);
	layer.addInmemoryTable(edbName, pair.second);
	int cardinality = pair.second[0].size();
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
                uint8_t id = t.getId();
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
    LOG(DEBUGL) << "Adding rule: \"" << newRule << "\"";
    nonGeneratingRules.push_back(newRule);

    Program newProgram(&layer);
    for (auto rule : nonGeneratingRules) {
	newProgram.parseRule(rule, false);
    }
    std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(layer,
            &newProgram, true, true, false, false, 1, 1, false);
    sn->run();
    Reasoner r((uint64_t) 0);
    Dictionary dictVariables;
    Literal query = newProgram.parseLiteral("__TARGET__(W)", dictVariables);

    TupleIterator *iter = r.getIteratorWithMaterialization(sn.get(), query, false, NULL);
    bool retval = ! iter->hasNext();
    delete iter;
    return retval;
}

bool Checker::JA(Program &p, bool restricted) {
    std::map<rpos, std::vector<vpos>> allExtVarsPos;

    getAllExtPropagatePositions(p, allExtVarsPos);

    // Create a graph of dependencies
    Graph g(allExtVarsPos.size());
    int src = 0;
    if (! restricted) {
	for (auto &it : allExtVarsPos) {
	    // For each existential variable, go back to the rule, this time to the body,
	    // and add all <pred, varpos> there to a list.
	    // These are the values used to compute a value for the existential variable.
	    const Rule &rule = p.getRule(it.first.first);
	    LOG(DEBUGL) << "Src = " << src << ", rule " << rule.tostring(&p, p.getKB());
	    auto body = rule.getBody();
	    std::vector<vpos> dependencies;
	    // Collect <PredId_t, tuplepos> values on which the existential variables of this rule depend.
	    for (auto lit: body) {
		for (int i = 0; i < lit.getTupleSize(); i++) {
		    VTerm t = lit.getTermAtPos(i);
		    if (t.getId() != 0) {
			vpos p(lit.getPredicate().getId(), i);
			dependencies.push_back(p);
		    }
		}
	    }
	    // We need the dependencies sorted to be able to call std::set_intersection later on.
	    std::sort(dependencies.begin(), dependencies.end());
	    int dest = 0;
	    for (auto &it2: allExtVarsPos) {
		// For each existential variable, check if a value of it could propagate to a value that is used to compute the
		// current variable.
		std::vector<vpos> intersect(dependencies.size() + it2.second.size());
		auto ipos = std::set_intersection(dependencies.begin(), dependencies.end(), it2.second.begin(), it2.second.end(), intersect.begin());
		if (ipos > intersect.begin()) {
		    g.addEdge(src, dest);
		}
		dest++;
	    }
	    src++;
	}
    } else {
	for (auto &it : allExtVarsPos) {
	    int dest = 0;
	    const Rule &rulev = p.getRule(it.first.first);
	    std::vector<uint8_t> extVars = rulev.getVarsNotInBody();
	    uint8_t v = extVars[it.first.second];
	    LOG(DEBUGL) << "Src = " << src << ", rule " << rulev.tostring(&p, p.getKB());
	    for (auto &it2: allExtVarsPos) {
		const Rule &rulew = p.getRule(it2.first.first);
		auto body = rulew.getBody();
		extVars = rulew.getVarsNotInBody();
		uint8_t w = extVars[it2.first.second];
		LOG(DEBUGL) << "Trying " << dest << ", rule " << rulew.tostring(&p, p.getKB());
		std::map<uint8_t, std::vector<vpos>> positions;
		for (auto lit: body) {
		    for (int i = 0; i < lit.getTupleSize(); i++) {
			VTerm t = lit.getTermAtPos(i);
			if (t.getId() != 0) {
			    vpos p(lit.getPredicate().getId(), i);
			    positions[t.getId()].push_back(p);
			    LOG(TRACEL) << "    pred: " << t.getId() << ", i = " << i;
			}
		    }
		}
		for (auto pair : positions) {
		    std::vector<vpos> intersect(pair.second.size() + it2.second.size());
		    auto ipos = std::set_intersection(it2.second.begin(), it2.second.end(), pair.second.begin(), pair.second.end(), intersect.begin());
		    if (ipos - intersect.begin() == pair.second.size()) {
			// For this variable, all positions in the right-hand-side occur in the propagations of the existential variable.
			uint8_t x = pair.first;
			LOG(DEBUGL) << "We have a match for variable " << (int) x;
			// Now try the materialization and the test.
			// First compute the EDB set to materialize on.
			// See Definition 4 of the paper
			// Restricted Chase (Non) Termination for Existential Rules with Disjunctions
			// by David Carral, Irina Dragoste and Markus Kroetzsch
			if (rja_check(p, rulev, rulew, x, v, w)) {
			    g.addEdge(src,dest);
			    break;
			}
		    }
		}
		dest++;
	    }
	    src++;
	}
    }

    // Now check if the graph is cyclic. If it is, the ruleset is not JA (Joint Acyclic) (which means that the result is inconclusive).
    // If the ruleset is JA, we know that the chase will terminate.
    std::string type = restricted ? "RJA" : "JA";
    if (g.isCyclic()) {
	LOG(DEBUGL) << "Ruleset is not " << type << "!";
	return false;
    }
    LOG(DEBUGL) << "RuleSet is " << type << "!";
    return true;
}


Graph::Graph(int V)
{
	this->V = V;
	adj = new list<int>[V];
}

void Graph::addEdge(int v, int w)
{
	LOG(TRACEL) << "Adding edge from " << v << " to " << w;
	adj[v].push_back(w); // Add w to v’s list.
}

// This function is a variation of DFSUytil() in https://www.geeksforgeeks.org/archives/18212
bool Graph::isCyclicUtil(int v, bool visited[], bool *recStack) {
    if(visited[v] == false) {
	// Mark the current node as visited and part of recursion stack
	visited[v] = true;
	recStack[v] = true;

	// Recur for all the vertices adjacent to this vertex
	list<int>::iterator i;
	for(i = adj[v].begin(); i != adj[v].end(); ++i) {
	    if ( !visited[*i] && isCyclicUtil(*i, visited, recStack) )
		return true;
	    else if (recStack[*i])
		return true;
	}
    }
    recStack[v] = false; // remove the vertex from recursion stack
    return false;
}

// Returns true if the graph contains a cycle, else false.
// This function is a variation of DFS() in https://www.geeksforgeeks.org/archives/18212
bool Graph::isCyclic() {
	// Mark all the vertices as not visited and not part of recursion
	// stack
    bool *visited = new bool[V];
    bool *recStack = new bool[V];
    for(int i = 0; i < V; i++) {
	visited[i] = false;
	recStack[i] = false;
    }

    // Call the recursive helper function to detect cycle in different
    // DFS trees
    for(int i = 0; i < V; i++) {
	if (isCyclicUtil(i, visited, recStack)) {
	    return true;
	}
    }

    return false;
}
