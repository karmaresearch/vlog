#ifndef INCREMENTAL__CONCEPTS_H__
#define INCREMENTAL__CONCEPTS_H__

#include <unordered_set>

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/seminaiver.h>
#include <vlog/inmemory/inmemorytable.h>


class IncrementalState {

protected:
    const std::shared_ptr<SemiNaiver> fromSemiNaiver;
    const std::vector<PredId_t> &eMinus;

    const EDBLayer *layer;

    std::shared_ptr<SemiNaiver> sn;

    // cache vm[*]
    int nthreads;
    int interRuleThreads;

    IncrementalState(const std::shared_ptr<SemiNaiver> from,
                     const std::vector<PredId_t> &eMinus,
                     const EDBLayer *layer) :
            fromSemiNaiver(from), eMinus(eMinus), layer(layer) {
    }

    virtual ~IncrementalState() {
    }

    static std::string int2ABC(int x) {
        std::string ABC;
        while (x > 0) {
            ABC.push_back('A' + (x % 26));
            x /= 26;
        }
        return ABC;
    }

    static std::string printArgs(const Literal &lit, const EDBLayer *kb) {
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
                if (! kb->getDictText(id, rdfowl)) {
                    throw 10;       // Comply with conventions
                }
                args << Program::rewriteRDFOWLConstants(std::string(rdfowl));
            }
            if (i < tuple.getSize() - 1) {
                args << ",";
            } else {
                args << ")";
            }
        }
        return args.str();
    }

    virtual std::string convertRules() = 0;

public:
    std::shared_ptr<SemiNaiver> getPrevSemiNaiver() const {
        return fromSemiNaiver;
    }

    static std::string name2dMinus(const std::string &name) {
        return name + "@dMinus";
    }

    static std::string name2eMinus(const std::string &name) {
        return name + "@eMinus";
    }

    void run() {
        sn->run();
    }

    const std::shared_ptr<SemiNaiver> getSN() {
        return sn;
    }
};


class IncrOverdelete : public IncrementalState {

public:
    IncrOverdelete(const std::shared_ptr<SemiNaiver> from,
                   const std::vector<PredId_t> &eMinus_preds,
                   const EDBLayer *layer) :
            IncrementalState(from, eMinus_preds, layer) {
    }

    virtual ~IncrOverdelete() {
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
    static std::string confContents(const std::shared_ptr<SemiNaiver> fromSemiNaiver,
                                    const std::string &dred_dir,
                                    const std::vector<std::string> &eMinus) {
        std::ostringstream os;

        size_t nTables = 0;
        const EDBLayer &layer = fromSemiNaiver->getEDBLayer();
        const EDBConf &old_conf = layer.getConf();
        const std::vector<EDBConf::Table> tables = old_conf.getTables();
        for (const auto &t : tables) {
            std::string predName = "EDB" + std::to_string(nTables);
            os << predName << "_predname=" << t.predname << std::endl;
            os << predName << "_type=" << t.type << std::endl;
            for (size_t j = 0; j < t.params.size(); ++j) {
                os << predName << "_" << "param" << std::to_string(j) << "=" <<
                    t.params[j] << std::endl;
            }
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
                ++nTables;
            }
        }

        for (auto rm : eMinus) {
            std::string predName = "EDB" + std::to_string(nTables);
            os << predName << "_predname" << "=" << name2eMinus(rm) << std::endl;
            os << predName << "_type=INMEMORY" << std::endl;
            os << predName << "_param0=" << dred_dir << std::endl;
            os << predName << "_param1=" << rm << "_remove" << std::endl;
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
    virtual std::string convertRules() {
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

#if 0
    std::vector<PredId_t> getIDBPredicates() const {
        std::vector<PredId_t> res;
        for (auto p_s : dMinus_pred) {
            res.push_back(p_s.first);
        }

        return res;
    }
#endif
};

// #if STILL_TYPING
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
class IncrRederive : public IncrementalState {

protected:
    const std::shared_ptr<SemiNaiver> overdelete_SN;

    static std::string name2dPlus(const std::string &pred) {
        return pred + "@dPlus";
    }

    static std::string name2v(const std::string &pred) {
        return pred + "@v";
    }

public:
    IncrRederive(const std::shared_ptr<SemiNaiver> from,
                 const std::shared_ptr<SemiNaiver> overdelete_SN,
                 const std::vector<PredId_t> &eMinus,
                 const EDBLayer *layer) :
            IncrementalState(from, eMinus, layer), overdelete_SN(overdelete_SN) {
    }

    virtual ~IncrRederive() {
    }

    /**
     * Create an appropriate EDBConf for the Rederive
     *
     * Note: this is necessary to create an EDBConf, so there is no EDBLayer
     * yet. Therefore, this is a static method.
     */
    static std::string confContents(const std::shared_ptr<SemiNaiver> fromSN,
                                    const std::shared_ptr<SemiNaiver> overdeleteSN,
                                    const std::string &dred_dir,
                                    const std::vector<std::string> &eMinus) {
        std::ostringstream os;

        // Start out with the tables as in OverDelete
        std::string fromContents = IncrOverdelete::confContents(fromSN, dred_dir, eMinus);
        os << fromContents;

        // Add the dpred@dMinus tables, one for each IDB predicate
        // const
        Program *sn_program = fromSN->getProgram();

        size_t nTables = sn_program->getNEDBPredicates() +
                             sn_program->getNIDBPredicates() + eMinus.size();
        std::vector<std::string> idb_names;
        for (const std::string &p : sn_program->getAllPredicateStrings()) {
            PredId_t pred = sn_program->getPredicate(p).getId();
            if (sn_program->isPredicateIDB(pred)) {
                std::string predName = "EDB" + std::to_string(nTables);
                os << predName << "_predname=" << p << "@dMinus\n";
                os << predName << "_type=EDBonIDB\n";
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
    virtual std::string convertRules() {
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
};
// #endif

/*
class IncrAdd : public IncrementalState {
};
*/

class DRed {
};

#endif  // def INCREMENTAL__CONCEPTS_H__
