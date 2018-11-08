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
	// TODO: JA, RJA, RMFA, MSA, RMSA, RMFC
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
		rpos pos(rule.getId(), i);
		allExtVarsPos[pos] = positions;
		LOG(TRACEL) << "Position set:";
		for (auto pos : positions) {
		    LOG(TRACEL) << "    pred: " << pos.first << ", i = " << (int) pos.second;
		}
	    }
	}
    }
    
    return false;
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
		auto body = rule.getBody();
		for (auto lit : body) {
		    if (lit.getPredicate().getId() == pos.first) {
			VTerm t = lit.getTermAtPos(pos.second);
			if (t.getId() != 0) {
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
