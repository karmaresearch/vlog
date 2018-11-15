#ifndef INCREMENTAL__CONCEPTS_H__
#define INCREMENTAL__CONCEPTS_H__

#include <unordered_set>

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/seminaiver.h>
#include <vlog/inmemory/inmemorytable.h>


class IncrementalState {

protected:
    // const
    std::shared_ptr<SemiNaiver> fromSemiNaiver;
    // const
    EDBLayer *layer;

    std::unordered_set<PredId_t> inPreviousSemiNaiver;
    std::string rules;

    IncrementalState(// const
                     std::shared_ptr<SemiNaiver> from,
                     // const
                     EDBLayer *layer) :
        fromSemiNaiver(from), layer(layer) {
    }

    static std::string int2ABC(int x) {
        std::string ABC;
        while (x > 0) {
            ABC.push_back('A' + (x % 26));
            x /= 26;
        }
        return ABC;
    }

    static std::string printArgs(const Literal &lit, // const
                                 EDBLayer *kb) {
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

public:
    std::shared_ptr<SemiNaiver> getPrevSemiNaiver() const {
        return fromSemiNaiver;
    }

    virtual std::string convertRules() = 0;

    bool predInPreviousSemiNaiver(const Literal &literal) const {
        return inPreviousSemiNaiver.find(literal.getPredicate().getId()) !=
            inPreviousSemiNaiver.end();
    }

    const std::string &getRules() const {
        return rules;
    }

    bool hasLiteral(const Literal &literal) const {
        LOG(ERRORL) << "FIXME: implement hasLiteral";
        return false;
    }
};


class IncrOverdelete : public IncrementalState {

private:
    std::unordered_map<PredId_t, std::string> dMinus_pred;
    std::unordered_map<PredId_t, std::string> eMinus_pred;
    const std::vector<InmemoryTable> &eMinus;      // the deletes

    static std::string name2dMinus(const std::string &name) {
        return name + "@dMinus";
    }

public:
    IncrOverdelete(// const
                   std::shared_ptr<SemiNaiver> from,
                   const std::vector<PredId_t> &eMinus_preds,
                   // const
                   EDBLayer *layer) :
            IncrementalState(from, layer), eMinus(eMinus) {
        LOG(ERRORL) << "FIXME: implement " << __func__;

        Program *fromProgram = fromSemiNaiver->getProgram();
        for (auto p : eMinus_preds) {
            eMinus_pred[p] = layer->getPredName(p);
        }

        LOG(ERRORL) << "FIXME: add the IDBonEDB stuff";
    }

    /**
     * Gupta, Mumick, Subrahmanian
     * dMinus(p(x*)) :- s1, ..., dMinus(si), ..., sn
     * dMinus(si): q = pred(si).
     *      if q in EDB: Q && Eminus
     *      else: dMinus(q(x*)) so we can iterate
     * si:
     *      if q in EDB: E - Eminus
     *      else: as from I (materialization) of previous Program
     */
    virtual std::string convertRules() {
        // const
        Program *fromProgram = fromSemiNaiver->getProgram();
        const std::vector<Rule> rs = fromProgram->getAllRules();
        // const
        EDBLayer *fromKB = fromProgram->getKB();

        // process the first head of all rules
        for (const auto &r : rs) {
            const std::vector<Literal> &hs = r.getHeads();
            if (hs.size() > 1) {
                LOG(WARNL) << "No support for rules with multiple heads";
            }
            const Literal &h = r.getFirstHead();
            // create new IDB predicate dq = dMinus(h(...)) with the
            // same arity as h
            PredId_t pred = h.getPredicate().getId();
            std::string name = fromProgram->getPredicateName(pred);
            dMinus_pred[pred] = name2dMinus(name);

            // create EDB predicate which dispatches to the
            // old IDB predicate. Retain name/PredId_t q.
            inPreviousSemiNaiver.insert(pred);
        }

        // process the bodies of all rules
        for (const auto &r : rs) {
            const Literal h = r.getFirstHead();
            const PredId_t hid = h.getPredicate().getId();
            const std::vector<Literal> &bs = r.getBody();
            for (::size_t i = 0; i < bs.size(); ++i) {
                std::ostringstream rule;      // assemble textual rule
                rule << dMinus_pred[hid];
                rule << printArgs(h, fromKB);
                rule << " :- ";
                // Create rule dMinus(si)
                for (::size_t j = 0; j < bs.size(); ++j) {
                    const auto b = bs[j];
                    PredId_t pred = b.getPredicate().getId();
                    if (i == j) {
                        // case dMinus(si)
                        if (b.getPredicate().getType() == EDB) {
                            // new EDB predicate dp which is initialized
                            // to eMinus
                            rule << layer->getPredName(pred) << "@eMinus";
                        } else {
                            // assign dMinus(q)
                            rule << dMinus_pred[pred];
                        }
                    } else {
                        // case si
                        if (b.getPredicate().getType() == EDB) {
                            // recycle EDB predicate, but with eMinus
                            // in the removalList
                            rule << fromProgram->getPredicateName(pred) << "@E-eMinus";
                        } else {
                            // retain q
                            rule << fromProgram->getPredicateName(pred);
                        }
                    }
                    rule << printArgs(b, fromKB);

                    if (j < bs.size() - 1) {
                        rule << ",";
                    } else {
                        rule << "\n";
                    }
                }
                rules += rule.str();
            }
        }

        return rules;
    }
};

/*
class IncrRederive : public IncrementalState {
};

class IncrAdd : public IncrementalState {
};
*/

#endif  // def INCREMENTAL__CONCEPTS_H__
