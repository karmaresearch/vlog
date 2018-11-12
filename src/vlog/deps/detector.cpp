#include <vlog/deps/detector.h>

std::vector<Equivalence> Detector::getDatabaseDependencies(Program &prog, EDBLayer &layer) {
    std::vector<Equivalence> out;
    //Start from every predicate, and walk backwards
    auto preds = prog.getAllPredicateIDs();

    for (const auto &p : preds) {
        //p is an IDB predicate. We want to check in how many possible ways
        //facts in the database can produce "p" facts
        std::vector<Literal> dependentAtoms;

        //Create a canonical literal
        auto pred = prog.getPredicate(p);
        uint8_t arity = pred.getCardinality();
        VTuple tuple(arity);
        for(uint8_t i = 0; i < arity; ++i) {
            tuple.set(VTerm(128 + i, 0), i); //TODO: I should use a var ID that
            //is not used anywhere else in the program...
        }
        Literal lit(pred, tuple);
        std::vector<Literal> cache;

        //Start with an empty subs
        getAllTerminals(dependentAtoms, lit, prog, layer, cache);

        //TODO: Go through all dependencies to identify equal atoms

        //std::cout << "l=" << lit.tostring(&prog, &layer) << " # atoms " << dependentAtoms.size() << std::endl;
        if (dependentAtoms.size() > 1) {
            for(const auto &a : dependentAtoms) {
                std::cout << a.tostring(&prog,&layer) << std::endl;
            }
        }

        std::vector<std::pair<Literal,Literal>> terminals;
    }
    return out;
}


void Detector::getAllTerminals(std::vector<Literal> &out,
        const Literal &l,
        Program &p,
        EDBLayer &layer,
        std::vector<Literal> &cache) {
    std::vector<Rule> rules = p.getAllRulesByPredicate(l.getPredicate().getId());
    for (auto r : rules) {
        //std::cout << "Rule=" << r.tostring(&p, &layer) << std::endl;
        //Rewrite the head using current substitutions
        Mapping m;
        if (m.unify(l, r.getFirstHead())) {
            //Recursively invoke the function to the body
            for (auto const &ba : r.getBody()) {
                auto oldsize = out.size();
                auto rwa = m.rewrite(ba);
                for (const auto &a : rwa) {
                    //Check the cache -> If I've already considered, then skip it
                    bool found = false;
                    for(const auto &cachedLiteral : cache) {
                        std::vector<Substitution> subs;
                        int resp = Literal::subsumes(subs, cachedLiteral, a);
                        if (resp >= 0) {
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        cache.push_back(a);
                        getAllTerminals(out, a, p, layer, cache);
                        if (out.size() == oldsize) {
                            out.push_back(a);
                        }
                    }
                }
            }
        }
    }
}

void Detector::printDatabaseDependencies(Program &prog, EDBLayer &layer) {
    auto eq = getDatabaseDependencies(prog, layer);
    for(const auto &e : eq) {
        std::cout << "Literal " << e.getLiteral1().tostring() << " equals to " << e.getLiteral2().tostring() << std::endl;
    }
}
