#include <vlog/incremental/incremental-concepts.h>

#include <unordered_set>

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/reasoner.h>
#include <vlog/seminaiver.h>
#include <vlog/inmemory/inmemorytable.h>


// class IncrementalState;

IncrementalState::IncrementalState(// const
        ProgramArgs &vm,
        const std::shared_ptr<SemiNaiver> from,
        const std::vector<std::string> &eMinus) :
    vm(vm), fromSemiNaiver(from), eMinus(eMinus),
    conf(NULL), layer(NULL), program(NULL) {
        dredDir = vm["dred"].as<string>();
        nthreads = vm["nthreads"].as<int>();
        if (vm["multithreaded"].empty()) {
            nthreads = -1;
        }
        interRuleThreads = vm["interRuleThreads"].as<int>();
        if (vm["multithreaded"].empty()) {
            interRuleThreads = 0;
        }
    }

IncrementalState::~IncrementalState() {
    for (auto r: rm) {
        delete r.second;
    }
    delete program;
    delete layer;
    delete conf;
}

std::string IncrementalState::int2ABC(int x) {
    std::string ABC;
    while (x > 0) {
        ABC.push_back('A' + (x % 26));
        x /= 26;
    }
    return ABC;
}

std::string IncrementalState::printArgs(const Literal &lit, const EDBLayer *kb) {
    std::ostringstream args;
    const VTuple &tuple = lit.getTuple();
    args << "(";
    for (size_t i = 0; i < tuple.getSize(); ++i) {
        const VTerm &term = tuple.get(i);
        uint64_t id = term.getId();
        if (term.isVariable()) {
            args << int2ABC(id);
        } else {
            char rdfowl[MAX_TERM_SIZE];
            uint64_t value = term.getValue();
            if (! kb->getDictText(value, rdfowl)) {
                throw 10;       // Comply with conventions
            }
            // args << Program::rewriteRDFOWLConstants(std::string(rdfowl));
            args << Program::compressRDFOWLConstants(std::string(rdfowl));
        }
        if (i < tuple.getSize() - 1) {
            args << ",";
        } else {
            args << ")";
        }
    }
    return args.str();
}


// class IncrOverdelete : public IncrementalState
IncrOverdelete::IncrOverdelete(// const
        ProgramArgs &vm,
        const std::shared_ptr<SemiNaiver> from,
        const std::vector<std::string> &eMinus) :
    IncrementalState(vm, from, eMinus) {

        // Overdelete
        // Create a Program, create a SemiNaiver, run...

        std::string confString = confContents();

        LOG(INFOL) << "Generated edb.conf:";
        LOG(INFOL) << confString;

        conf = new EDBConf(confString, false);
        NamedSemiNaiver from_map;
        from_map["base"] = from;
        layer = new EDBLayer(*conf, false, from_map);
        layer->setName("overdelete");

        std::string overdelete_rules = convertRules();
        std::cout << "Overdelete rule set:" << std::endl;
        std::cout << overdelete_rules;

        program = new Program(layer);
        program->readFromString(overdelete_rules,
                vm["rewriteMultihead"].as<bool>());

        std::vector<PredId_t> remove_pred;
        for (const auto &n: eMinus) {
            PredId_t p = program->getPredicate(n).getId();
            remove_pred.push_back(p);
            LOG(ERRORL) << "FIXME: the removal predicate is called " << name2eMinus("TE") << " here";
            PredId_t pMinus = program->getPredicate(name2eMinus("TE")).getId();
            rm[p] = new EDBRemoveLiterals(pMinus, layer);
            // rm[p]->dump(std::cerr, *layer);
            LOG(INFOL) << "Add removal predicate " << pMinus << " for predicate " << p;
        }
        layer->addRemoveLiterals(rm);

        //Prepare the materialization
        sn = Reasoner::getSemiNaiver(
                *layer,
                program,
                vm["no-intersect"].empty(),
                vm["no-filtering"].empty(),
                !vm["multithreaded"].empty(),
                vm["restrictedChase"].as<bool>() ? TypeChase::RESTRICTED_CHASE : TypeChase::SKOLEM_CHASE,
                nthreads,
                interRuleThreads,
                ! vm["shufflerules"].empty());
        sn->setName("overdelete");
    }


/**
 * Create an appropriate EDBConf for the Overdelete
 * 1) copy all EDB predicates (incl. names) from the previous EDBLayer
 * 2) for each IDB p predicate in prevSemiNaiver, specify a new
 *    table of type EDBonIDB
 * 3) for each table p of removes, specify an inmemory table with
 *    predicate p@eMinus
 *
 * Note: this is necessary to create an EDBConf, so there is no EDBLayer
 * yet. Therefore, this is a static method.
 */
std::string IncrOverdelete::confContents() const {
    std::ostringstream os;

    size_t nTables = 0;
    const EDBLayer &old_layer = fromSemiNaiver->getEDBLayer();
    const EDBConf &old_conf = old_layer.getConf();
    const std::vector<EDBConf::Table> tables = old_conf.getTables();
    for (const auto &t : tables) {
        std::string predName = "EDB" + std::to_string(nTables);
        os << predName << "_predname=" << t.predname << std::endl;
        os << predName << "_type=EDBimporter" << std::endl;
        os << predName << "_" << "param0=base" << std::endl;
        ++nTables;
    }

    // const
    Program *fromProgram = fromSemiNaiver->getProgram();
    std::vector<std::string> predicates = fromProgram->getAllPredicateStrings();
    for (auto p : predicates) {
        PredId_t pred = fromProgram->getPredicate(p).getId();
        if (fromProgram->isPredicateIDB(pred)) {
            std::string predName = "EDB" + std::to_string(nTables);
            os << predName << "_predname" << "=" << p << std::endl;
            os << predName << "_type=EDBonIDB" << std::endl;
            os << predName << "_param0=base" << std::endl;
            ++nTables;
        }
    }

    for (auto pred : eMinus) {
        std::string predName = "EDB" + std::to_string(nTables);
        // os << predName << "_predname" << "=" << name2eMinus(pred) << std::endl;
        LOG(ERRORL) << "FIXME: the removal predicate is called " << name2eMinus("TE") << " here";
        os << predName << "_predname" << "=" << name2eMinus("TE") << std::endl;
        os << predName << "_type=INMEMORY" << std::endl;
        os << predName << "_param0=" << dredDir << std::endl;
        os << predName << "_param1=" << pred << "_remove" << std::endl;
        ++nTables;
    }

    return os.str();
}

/**
 * Gupta, Mumick, Subrahmanian
 * dMinus(p(x*)) :- s1, ..., dMinus(si), ..., sn
 * dMinus(si): (let q = pred(si))
 *      if q in EDB: Q && Eminus
 *      else: dMinus(q(x*)) so we can iterate
 * si:
 *      if q in EDB: E - Eminus
 *      else: as from I (materialization) of previous Program
 */
std::string IncrOverdelete::convertRules() const {
    std::unordered_map<PredId_t, std::string> dMinus_pred;
    const Program *fromProgram = fromSemiNaiver->getProgram();
    const std::vector<Rule> rs = fromProgram->getAllRules();

    // process the first head of all rules
    for (const auto &r : rs) {
        const std::vector<Literal> &hs = r.getHeads();
        if (hs.size() > 1) {
            LOG(ERRORL) << "No support for rules with multiple heads";
        }
        const Literal &h = r.getFirstHead();
        // create new IDB predicate dq = dMinus(h(...)) with the
        // same arity as h
        PredId_t pred = h.getPredicate().getId();
        std::string name = fromProgram->getPredicateName(pred);
        dMinus_pred[pred] = name2dMinus(name);

        // create EDB predicate which dispatches to the
        // old IDB predicate. Retain name/PredId_t q.
    }

    // process the bodies of all rules
    const EDBLayer *fromKB = fromProgram->getKB();
    std::ostringstream rules;      // assemble textual rules
    for (const auto &r : rs) {
        const Literal h = r.getFirstHead();
        const PredId_t hid = h.getPredicate().getId();
        const std::vector<Literal> &bs = r.getBody();
        for (::size_t i = 0; i < bs.size(); ++i) {
            rules << dMinus_pred[hid];
            rules << printArgs(h, fromKB);
            rules << " :- ";
            // Create rule dMinus(si)
            for (::size_t j = 0; j < bs.size(); ++j) {
                const auto b = bs[j];
                PredId_t pred = b.getPredicate().getId();
                if (i == j) {
                    // case dMinus(si)
                    if (b.getPredicate().getType() == EDB) {
                        // new EDB predicate dp which is initialized
                        // to eMinus
                        rules << name2eMinus(layer->getPredName(pred));
                    } else {
                        // assign dMinus(q)
                        rules << dMinus_pred[pred];
                    }
                } else {
                    // case si
                    if (b.getPredicate().getType() == EDB) {
                        // recycle EDB predicate, but with eMinus
                        // in the removalList
                        LOG(ERRORL) << "Wrong generated predicate name";
                        rules << fromProgram->getPredicateName(pred) << "@E-eMinus";
                    } else {
                        // retain q
                        rules << fromProgram->getPredicateName(pred);
                    }
                }
                rules << printArgs(b, fromKB);

                if (j < bs.size() - 1) {
                    rules << ",";
                } else {
                    rules << "\n";
                }
            }
        }
    }

    return rules.str();
}


/**
 * Gupta, Mumick, Subrahmanian
 *
 * dPlus(p(x*)) :- dMinus(p(x*)), s1, ..., sn
 * si: (let q = pred(si))
 *    if q in EDB: Q - eMinus     -- the same as for OverDelete
 *    else: I(DeltaMinus) + dPlus(p(x*))
 *
 * To express this in Datalog, define the following:
 *
 * E'               E from original problem - eMinus
 * dMinus(q)        EDB generated from the set of OverDeletes
 * v(q)             EDB generated from I(q) - dMinus(q)
 * dPlus(q)         new IDB predicate for each q in IDB
 *
 * So rule transformations in 2 parts:
 * 1) generate rules so dPLus(p) is initialised from v(p) = I(p) - dMinus(p)
 *    dPlus(p()) :- v(p())
 * 2) transform rules as in paper:
 *    dPlus(p()) :- dMinus(p()), s1, ..., sn
 *    with: (let q be predicate of si; let Q be set of q)
 *    q in E': no change
 *    else:    si = dPlus(q())
 *
 * Hence, need to generate an EDBonIDB table for each IDB predicate p in
 * (I - dMinus(p())). Implement EDBonIDB table on v(p) for p in original
 * problem with a removal attribute that contains dMinus(p).
 * Moreover, require an EDBonIDB table for each predicate p in DeltaMinus.
 */

// class IncrRederive : public IncrementalState

IncrRederive::IncrRederive(// const
        ProgramArgs vm,
        const std::shared_ptr<SemiNaiver> from,
        const std::vector<std::string> &eMinus,
        const IncrOverdelete &overdelete) :
    IncrementalState(vm, from, eMinus),
    overdelete(overdelete) {
        std::string confString = confContents();
        LOG(INFOL) << "Generated rederive edb.conf:";
        LOG(INFOL) << confString;

        conf = new EDBConf(confString, false);
        NamedSemiNaiver from_map;
        from_map["base"] = from;
        from_map["overdelete"] = overdelete.getSN();
        layer = new EDBLayer(*conf, false, from_map);
        layer->setName("rederive");

        std::string rules = convertRules();
        std::cout << "Rederive rule set:" << std::endl;
        std::cout << rules;

        program = new Program(layer);
        program->readFromString(rules, vm["rewriteMultihead"].as<bool>());

        // The Removals should contain not only TE@dMinus (= E^-) for TE
        // but also q@dMinus for q (all q)

        // Add the user removals
        for (const auto &r: eMinus) {
            LOG(ERRORL) << "FIXME: the removal predicate is called " << name2eMinus("TE") << " here";
            std::string rm_name = name2eMinus("TE");
            PredId_t e = program->getPredicate(r).getId();
            PredId_t rm_pred = program->getPredicate(rm_name).getId();
            rm[e] = new EDBRemoveLiterals(rm_pred, layer);
        }

        Program *sn_program = fromSemiNaiver->getProgram();
        const std::vector<std::string> idbs = sn_program->getAllPredicateStrings();
        std::vector<std::pair<PredId_t, std::string>> idb_pred;
        for (const std::string &p : idbs) {
            PredId_t pred = sn_program->getPredicate(p).getId();
            if (sn_program->isPredicateIDB(pred)) {
                idb_pred.push_back(std::pair<PredId_t, std::string>(pred, p));
            }
        }

        // const
        Program *op = overdelete.getSN()->getProgram();
        for (const auto &pp : idb_pred) {
            // Read the Q^v values from our own EDB tables
            std::string rm_name = name2dMinus(pp.second);
            PredId_t rm_pred = op->getPredicate(rm_name).getId();
            LOG(INFOL) << "dMinus table for " << pp.second << " is "<< rm_name;
            // Feed that to the Removal
            rm[pp.first] = new EDBRemoveLiterals(rm_pred, layer);
            LOG(INFOL) << "Add removal predicate " << rm_pred << " for predicate " << pp.first;
        }
        layer->addRemoveLiterals(rm);

        //Prepare the materialization
        sn = Reasoner::getSemiNaiver(
                *layer,
                program,
                vm["no-intersect"].empty(),
                vm["no-filtering"].empty(),
                !vm["multithreaded"].empty(),
                vm["restrictedChase"].as<bool>()
                ? TypeChase::RESTRICTED_CHASE : TypeChase::SKOLEM_CHASE,
                nthreads,
                interRuleThreads,
                ! vm["shufflerules"].empty());
        sn->setName("rederive");
    }


/**
 * Create an appropriate EDBConf for the Rederive
 */
std::string IncrRederive::confContents() const {
    std::ostringstream os;

    // Wrap the tables in OverDelete in an EDBimporter

    size_t nTables = 0;
    const EDBLayer &from_layer = fromSemiNaiver->getEDBLayer();
    const EDBConf &from_conf = from_layer.getConf();
    const std::vector<EDBConf::Table> from_tables = from_conf.getTables();
    std::unordered_set<std::string> from_edb;
    for (const auto &t : from_tables) {
        std::string predName = "EDB" + std::to_string(nTables);
        os << predName << "_predname=" << t.predname << std::endl;
        os << predName << "_type=EDBimporter" << std::endl;
        os << predName << "_" << "param0=base" << std::endl;
        from_edb.insert(t.predname);
        ++nTables;
    }

    const EDBLayer &overdelete_layer = overdelete.getSN()->getEDBLayer();
    const EDBConf &overdelete_conf = overdelete_layer.getConf();
    const std::vector<EDBConf::Table> overdelete_tables =
        overdelete_conf.getTables();
    for (const auto &t : overdelete_tables) {
        if (from_edb.find(t.predname) == from_edb.end()) {
            std::string predName = "EDB" + std::to_string(nTables);
            os << predName << "_predname=" << t.predname << std::endl;
            os << predName << "_type=EDBimporter" << std::endl;
            os << predName << "_" << "param0=overdelete" << std::endl;
            ++nTables;
        }
    }

    LOG(INFOL) << "Inherit conf from overdelete:";
    LOG(INFOL) << "\n" << os.str();

    // Add the pred@dMinus tables, one for each IDB predicate
    // const
    Program *od_program = overdelete.getSN()->getProgram();

    std::vector<std::string> idb_names;
    for (const std::string &p : od_program->getAllPredicateStrings()) {
        PredId_t pred = od_program->getPredicate(p).getId();
        if (od_program->isPredicateIDB(pred)) {
            std::string predName = "EDB" + std::to_string(nTables);
            os << predName << "_predname=" << p << "\n";
            os << predName << "_type=EDBonIDB\n";
            os << predName << "_param0=overdelete\n";
            ++nTables;
        }
    }

    return os.str();
}

/**
 * Create a Rederive rule set as described above
 *
 * Rule transformations in 2 parts, based on the original rule set:
 * 1) generate rules so dPLus(p) is initialised from v(p) = I(p) - dMinus(p)
 *    dPlus(p()) :- v(p())
 * 2) transform rules as in paper:
 *    dPlus(p()) :- dMinus(p()), s1, ..., sn
 *    with: (let q be predicate of si; let Q be set of q)
 *    q in E': no change
 *    else:    si = dPlus(q())
 */
std::string IncrRederive::convertRules() const {
    const Program *fromProgram = fromSemiNaiver->getProgram();
    const std::vector<Rule> rs = fromProgram->getAllRules();
    const EDBLayer *fromKB = fromProgram->getKB();

    std::unordered_map<PredId_t, std::string> dMinus_pred;
    std::unordered_map<PredId_t, std::string> actual_pred;
    std::unordered_map<PredId_t, std::string> dPlus_pred;
    std::unordered_map<PredId_t, std::string> v_pred;
    std::unordered_map<PredId_t, std::string> pred_args;

    // process the first head of all rules
    for (const auto &r : rs) {
        const std::vector<Literal> &hs = r.getHeads();
        if (hs.size() > 1) {
            LOG(ERRORL) << "No support for rules with multiple heads";
        }
        const Literal &h = r.getFirstHead();
        // create new IDB predicate dq = dPlus(h(...)) with the
        // same arity as h
        PredId_t pred = h.getPredicate().getId();
        std::string name = fromProgram->getPredicateName(pred);
        actual_pred[pred] = name;
        dMinus_pred[pred] = name2dMinus(name);
        v_pred[pred] = name2v(name);
        dPlus_pred[pred] = name2dPlus(name);
        pred_args[pred] = printArgs(h, fromKB);
    }

    std::ostringstream rules;      // assemble textual rules

    for (const auto &pn : pred_args) {
        PredId_t hid = pn.first;

        // create the union rule 1)
        rules << v_pred[hid];
        rules << pn.second;
        rules << " :- ";
        rules << dPlus_pred[hid];
        rules << pn.second;
        rules << "\n";

        // create the union rule 2)
        rules << v_pred[hid];
        rules << pn.second;
        rules << " :- ";
        rules << actual_pred[hid];
        rules << pn.second;
        rules << "\n";
    }

    for (const auto &r : rs) {
        const Literal h = r.getFirstHead();
        const PredId_t hid = h.getPredicate().getId();
        const std::vector<Literal> &bs = r.getBody();

        // create the transformed rule
        rules << dPlus_pred[hid];
        rules << printArgs(h, fromKB);
        rules << " :- ";
        rules << dMinus_pred[hid];
        rules << printArgs(h, fromKB);
        // Create rule dPlus(si)
        for (const auto &b: bs) {
            rules << ",";
            PredId_t pred = b.getPredicate().getId();
            if (b.getPredicate().getType() == EDB) {
                // recycle EDB predicate, but with eMinus
                // in the removalList
                rules << fromProgram->getPredicateName(pred);
            } else {
                // retain q
                rules << v_pred[pred];
            }
            rules << printArgs(b, fromKB);
        }
        rules << "\n";
    }

    return rules.str();
}


/**
 * Class IncrAdd: additions for the DRed algorithm
 *
 * Gupta, Mumick, Subrahmanian
 * dAdd(p(x*)) :- s1, ..., dAdd(si), ..., sn
 * dAdd(si): (let q = pred(si))
 *      if q in EDB(eAdd): q@eAdd       the additions)
 *      else: dAdd(q(x*))               so we can iterate
 * si:
 *      union of dAdd(q) and q from Q^v, the result of the Rederive:
 *              Q^rederive = Q^I - Q@dMinus + Q@dPlus
 *      like with rederive, define an intermediate predicate u(q):
 *      u(q) :- dAdd(q)
 *      u(q) :- Q^I - q@dMinus          this is an "EDB" relation
 *      u(q) :- q@dPlus                 this is an "EDB" relation
 */
IncrAdd::IncrAdd(// const
        ProgramArgs vm,
        const std::shared_ptr<SemiNaiver> from,
        const std::vector<std::string> &eMinus,
        const std::vector<std::string> &eAdd,
        const IncrOverdelete &overdelete,
        const IncrRederive &rederive) :
    IncrementalState(vm, from, eMinus), eAdd(eAdd), overdelete(overdelete),
    rederive(rederive) {
        std::string confString = confContents();
        LOG(INFOL) << "Generated additions edb.conf:";
        LOG(INFOL) << confString;

        // LOG(ERRORL) << "For now, grab an additions EDBConf from file";
        // conf = new EDBConf(vm["dred"].as<string>() + "/edb.conf-additions");
        conf = new EDBConf(confString, false);
        NamedSemiNaiver from_map;
        from_map["base"] = from;
        from_map["overdelete"] = overdelete.getSN();
        from_map["rederive"] = rederive.getSN();
        layer = new EDBLayer(*conf, false, from_map);
        layer->setName("additions");

        std::string rules = convertRules();
        std::cout << "Addition rule set:" << std::endl;
        std::cout << rules;

        program = new Program(layer);
        program->readFromString(rules, vm["rewriteMultihead"].as<bool>());

        // The Removals should contain not only TE@dMinus (= E^-) for TE
        // but also q@dMinus for q (all q)

        // Add the user removals
        for (const auto &r: eMinus) {
            std::string rm_name = name2eMinus(r);
            PredId_t e = program->getPredicate(r).getId();
            PredId_t rm_pred = program->getPredicate(rm_name).getId();
            rm[e] = new EDBRemoveLiterals(rm_pred, layer);
        }

        Program *sn_program = fromSemiNaiver->getProgram();
        const std::vector<std::string> idbs = sn_program->getAllPredicateStrings();
        std::vector<std::pair<PredId_t, std::string>> idb_pred;
        for (const std::string &p : idbs) {
            PredId_t pred = sn_program->getPredicate(p).getId();
            if (sn_program->isPredicateIDB(pred)) {
                idb_pred.push_back(std::pair<PredId_t, std::string>(pred, p));
            }
        }

        // const
        Program *op = overdelete.getSN()->getProgram();
        for (const auto &pp : idb_pred) {
            // Read the Q^v values from our own EDB tables
            std::string rm_name = name2dMinus(pp.second);
            PredId_t rm_pred = op->getPredicate(rm_name).getId();
            LOG(DEBUGL) << "dMinus table for " << pp.second << " is "<< rm_name;
            // Feed that to the Removal
            rm[pp.first] = new EDBRemoveLiterals(rm_pred, layer);
        }
        for (const auto &r : rm) {
            LOG(INFOL) << "Add removal predicate for predicate " << r.first;
        }
        layer->addRemoveLiterals(rm);

        //Prepare the materialization
        sn = Reasoner::getSemiNaiver(
                *layer,
                program,
                vm["no-intersect"].empty(),
                vm["no-filtering"].empty(),
                !vm["multithreaded"].empty(),
                vm["restrictedChase"].as<bool>()
                ? TypeChase::RESTRICTED_CHASE : TypeChase::SKOLEM_CHASE,
                nthreads,
                interRuleThreads,
                ! vm["shufflerules"].empty());
        sn->setName("additions");
    }

IncrAdd::~IncrAdd() {
}


std::string IncrAdd::convertRules() const {
    const Program *fromProgram = fromSemiNaiver->getProgram();
    const std::vector<Rule> rs = fromProgram->getAllRules();
    const EDBLayer *fromKB = fromProgram->getKB();

    std::unordered_map<PredId_t, std::string> predicates;
    std::unordered_map<PredId_t, std::string> dAdd_pred;
    std::unordered_map<PredId_t, std::string> u_pred;
    std::unordered_map<PredId_t, std::string> pred_args;

    // process the first head of all rules
    for (const auto &r : rs) {
        const std::vector<Literal> &hs = r.getHeads();
        if (hs.size() > 1) {
            LOG(ERRORL) << "No support for rules with multiple heads";
        }
        const Literal &h = r.getFirstHead();
        PredId_t pred = h.getPredicate().getId();
        std::string name = fromProgram->getPredicateName(pred);
        predicates[pred] = name;
        dAdd_pred[pred] = name2dAdd(name);
        u_pred[pred] = name2u(name);
        pred_args[pred] = printArgs(h, fromKB);
    }

    std::ostringstream rules;

    // Create rules for q@u
    for (const auto &p: predicates) {
        // q@u :- q@dAdd
        rules << name2u(p.second) << pred_args[p.first] << " :- ";
        rules << name2dAdd(p.second) << pred_args[p.first] << "\n";

        // q@u :- q     implementation knows this is Q - q@dMinus
        rules << name2u(p.second) << pred_args[p.first] << " :- ";
        rules << fromProgram->getPredicateName(p.first) << pred_args[p.first];
        rules << "\n";

        // q@u :- q@dPlus
        rules << name2u(p.second) << pred_args[p.first] << " :- ";
        rules << IncrRederive::name2dPlus(p.second) << pred_args[p.first] << "\n";
    }

    // process the bodies of all rules
    for (const auto &r : rs) {
        const Literal h = r.getFirstHead();
        const PredId_t hid = h.getPredicate().getId();
        const std::vector<Literal> &bs = r.getBody();
        for (::size_t i = 0; i < bs.size(); ++i) {
            rules << dAdd_pred[hid];
            rules << printArgs(h, fromKB);
            rules << " :- ";
            // Create rule dAdd(q)
            for (::size_t j = 0; j < bs.size(); ++j) {
                const auto b = bs[j];
                PredId_t pred = b.getPredicate().getId();
                if (i == j) {
                    // case dAdd(si)
                    if (b.getPredicate().getType() == EDB) {
                        // new EDB predicate dp which is initialized
                        // to eAdd
                        rules << name2eAdd(layer->getPredName(pred));
                    } else {
                        // assign dAdd(q)
                        rules << dAdd_pred[pred];
                    }
                } else {
                    // case si
                    if (b.getPredicate().getType() == EDB) {
                        // recycle EDB predicate, but with eMinus
                        // in the removalList
                        LOG(ERRORL) << "Wrong generated predicate name";
                        rules << fromProgram->getPredicateName(pred) << "@E-eMinus";
                    } else {
                        // retain q
                        rules << name2u(fromProgram->getPredicateName(pred));
                    }
                }
                rules << printArgs(b, fromKB);

                if (j < bs.size() - 1) {
                    rules << ",";
                } else {
                    rules << "\n";
                }
            }
        }
    }

    return rules.str();
}

std::string IncrAdd::confContents() const {
    std::ostringstream os;

    // Wrap the tables in OverDelete in an EDBimporter

    size_t nTables = 0;
    const EDBLayer &old_layer = rederive.getSN()->getEDBLayer();
    const EDBConf &old_conf = old_layer.getConf();
    const std::vector<EDBConf::Table> tables = old_conf.getTables();
    for (const auto &t : tables) {
        std::string predName = "EDB" + std::to_string(nTables);
        os << predName << "_predname=" << t.predname << std::endl;
        os << predName << "_type=EDBimporter" << std::endl;
        os << predName << "_" << "param0=rederive" << std::endl;
        ++nTables;
    }

    LOG(INFOL) << "Inherit conf from rederive:";
    LOG(INFOL) << "\n" << os.str();

    // Add the dpred@dPlus tables, one for each IDB predicate
    // const
    Program *rd_program = rederive.getSN()->getProgram();

    std::vector<std::string> idb_names;
    for (const std::string &p : rd_program->getAllPredicateStrings()) {
        PredId_t pred = rd_program->getPredicate(p).getId();
        if (rd_program->isPredicateIDB(pred)) {
            std::string predName = "EDB" + std::to_string(nTables);
            os << predName << "_predname=" << p << "\n";
            os << predName << "_type=EDBonIDB\n";
            os << predName << "_param0=rederive\n";
            ++nTables;
        }
    }

    for (auto pred : eAdd) {
        std::string predName = "EDB" + std::to_string(nTables);
        os << predName << "_predname" << "=" << name2eAdd(pred) << std::endl;
        os << predName << "_type=INMEMORY" << std::endl;
        os << predName << "_param0=" << dredDir << std::endl;
        os << predName << "_param1=" << pred << "_add" << std::endl;
        ++nTables;
    }

    return os.str();

}
