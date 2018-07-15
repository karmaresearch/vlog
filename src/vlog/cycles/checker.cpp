#include <vlog/cycles/checker.h>
#include <vlog/reasoner.h>
#include <vlog/seminaiver.h>


int Checker::check(std::string ruleFile, EDBLayer &db) {
    //Parse the rules into a program
    Program p(&db);
    p.readFromFile(ruleFile, false); //we do not rewrite the heads

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
    sn->run();

    //if check succeds then return 0 (we don't know)
    if (sn->isFoundCyclicTerms()) {
        return 0; //we don't know
    } else {
        return 1; //program will always terminate
    }
}
