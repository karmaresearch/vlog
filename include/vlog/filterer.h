/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Vlog.

   Vlog is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Vlog is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Vlog.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef _FILTERER_H
#define _FILTERER_H

#include <vlog/seminaiver.h>
#include <vlog/fctable.h>
#include <vlog/edb.h>
#include <vlog/concepts.h>

#include <vector>

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
        const Literal &headRule,
        const FCInternalTable *currentResults,
        const int nPosFromFirst,
        const int nPosFromSecond);

    bool producedDerivationInPreviousStepsWithSubs(
        const FCBlock *block,
        const Literal &outputQuery,
        const Literal &currentQuery,
        const FCInternalTable *currentResults,
        const int nPosForHead,
        const std::pair<uint8_t, uint8_t> *posHead,
        const int nPosForLit,
        const std::pair<uint8_t, uint8_t> *posLiteral);

};

#endif
