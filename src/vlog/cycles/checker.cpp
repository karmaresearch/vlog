#include <vlog/cycles/checker.h>
#include <vlog/reasoner.h>
#include <vlog/seminaiver.h>

#include <kognac/logs.h>


int Checker::check(std::string ruleFile, std::string alg, EDBLayer &db) {
    //Parse the rules into a program
    Program p(&db);
    p.readFromFile(ruleFile, false); //we do not rewrite the heads

    if (alg == "MFA") {
	//Create  the critical instance (cdb)
	EDBLayer cdb(db);
	//Populate the crit. instance with new facts
	for(auto p : db.getAllPredicateIDs()) {
	    std::vector<std::vector<string>> facts;
	    std::vector<string> fact;
	    for (int i = 0; i < db.getPredArity(p); ++i) {
		fact.push_back("*");
	    }
	    facts.push_back(fact);
	    cdb.addInmemoryTable(db.getPredName(p), facts);
	}

	//Launch the  skolem chase with the check for cyclic terms
	std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(cdb,
		&p, true, true, false, false, 1, 1, false);
	sn->checkAcyclicity();
	//if check succeeds then return 0 (we don't know)
	if (sn->isFoundCyclicTerms()) {
	    return 0; //we don't know
	} else {
	    return 1; //program will always terminate
	}
    } else if (alg == "JA") {
	// If we don't find cyles, the program is "JA", which means the chase will terminate.
	return JA(p) ? 1 : 0;
    } else {
	// TODO: RJA, RMFA, MSA, RMSA, RMFC
	LOG(ERRORL) << "Unknown algorithm: " << alg;
	return 0;
    }
}

typedef std::pair<PredId_t, uint8_t> vpos;
typedef std::pair<uint32_t, uint8_t> rpos;

bool Checker::JA(Program &p) {
    auto rules = p.getAllRules();
    std::map<vpos, std::vector<vpos>> allExtVarsPos;
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
		LOG(TRACEL) << "Position set:";
		for (auto pos : positions) {
		    LOG(TRACEL) << "    pred: " << pos.first << ", i = " << (int) pos.second;
		}
	    }
	}
    }

    // Now, create a graph of dependencies
    Graph g(allExtVarsPos.size());
    int src = 0;
    for (auto &it : allExtVarsPos) {
	// For each existential variable, go back to the rule, this time to the body, and add all <pred, varpos> there to a list.
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
		LOG(TRACEL) << "Adding edge from " << src << " to " << dest;
	    }
	    dest++;
	}
	src++;
    }

    // Now check if the graph is cyclic. If it is, the ruleset is not JA (Joint Acyclic) (which means that the result is inconclusive).
    // If the ruleset is JA, we know that the chase will terminate.
    if (g.isCyclic()) {
	LOG(DEBUGL) << "Graph is cyclic";
	return false;
    }
    LOG(DEBUGL) << "Graph is not cyclic";
    return true;
}

void Checker::closure(Program &p, std::vector<std::pair<PredId_t, uint8_t>> &input) {
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

Graph::Graph(int V)
{
	this->V = V;
	adj = new list<int>[V];
}

void Graph::addEdge(int v, int w)
{
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
