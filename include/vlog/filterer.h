#ifndef _FILTERER_H
#define _FILTERER_H

#include <vlog/seminaiver.h>
#include <vlog/fctable.h>
#include <vlog/edb.h>
#include <vlog/concepts.h>

#include <vector>
#include <map>
#include <set>

using namespace std;

class FCInternalTable;
class TableFilterer {

private:

    static bool opt_intersection;

    SemiNaiver *naiver;

    std::unique_ptr<Literal> getRecursiveLiteral(const Rule &rule, const Literal &lit) {
        for (const auto &bodyEl : rule.getBody()) {
            if (bodyEl.getPredicate().getId() == lit.getPredicate().getId()) {
                return std::unique_ptr<Literal>(new Literal(bodyEl));
            }
        }
        return std::unique_ptr<Literal>();
    }

    std::unique_ptr<Literal> getNonRecursiveLiteral(
        const Rule &rule,
        const Literal &lit) {
        for (const auto &bodyEl : rule.getBody()) {
            if (bodyEl.getPredicate().getId() != lit.getPredicate().getId()) {
                return std::unique_ptr<Literal>(new Literal(bodyEl));
            }
        }
        return std::unique_ptr<Literal>();
    }

    bool producedDerivationInPreviousStepsWithSubs_rec(
        const FCBlock *block,
        const map<Term_t, std::vector<Term_t>> &mapSubstitutions,
        const Literal &outputQuery,
        const Literal &currentQuery,
        const size_t posHead_first,
        const size_t posLit_second
    );

public:
    TableFilterer(SemiNaiver *naiver);

    static bool intersection(const Literal &currentQuery,
                             const FCBlock & block);

    static void setOptIntersect(bool v) {
        opt_intersection = v;
    }

    static bool getOptIntersect() {
        return opt_intersection;
    }

    bool producedDerivationInPreviousSteps(const Literal &outputQuery,
                                           const Literal &currentQuery,
                                           const FCBlock *block);

    bool isEligibleForPartialSubs(
        const FCBlock *block,
        const std::vector<Literal> &heads,
        const FCInternalTable *currentResults,
        const int nPosFromFirst,
        const int nPosFromSecond);

    bool producedDerivationInPreviousStepsWithSubs(
        const FCBlock *block,
        const std::vector<Literal> &outputQueries,
        const Literal &currentQuery,
        const FCInternalTable *currentResults,
        const int nPosForHead,
        const std::pair<uint8_t, uint8_t> *posHead,
        const int nPosForLit,
        const std::pair<uint8_t, uint8_t> *posLiteral);

};

#endif
