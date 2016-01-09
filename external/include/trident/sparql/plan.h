/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Trident.

   Trident is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Trident is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Trident.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef _PLAN_H
#define _PLAN_H

#include <trident/sparql/sparqloperators.h>
#include <trident/sparql/query.h>
#include <trident/sparql/tupleiterators.h>
#include <trident/reasoning/edb.h>
#include <trident/reasoning/concepts.h>

#include <rdf3x/PlanGen.hpp>

#define SIMPLE 0
#define BOTTOMUP 1
#define NONE 2

class Querier;
class TridentQueryPlan {
private:

    KB *kb;
    Querier *q;
    const bool reasoning;
    const uint64_t reasoningThreshold;
    const uint64_t matThreshold;
    EDBLayer *layer;
    Program *program;

    std::map<string, uint64_t> mapVars1;
    std::map<uint64_t, string> mapVars2;

    std::shared_ptr<SPARQLOperator> root;

    std::shared_ptr<SPARQLOperator> translateJoin(Plan *plan);

    std::shared_ptr<SPARQLOperator> translateIndexScan(Plan *plan);

    std::shared_ptr<SPARQLOperator> translateProjection(Plan *plan);

    std::shared_ptr<SPARQLOperator> translateOperator(Plan *plan);

public:

    TridentQueryPlan(KB * kb, Querier *q, const bool reasoning,
                     const uint64_t reasoningThreshold, const uint64_t matThreshold,
                     EDBLayer *layer, Program *program) : kb(kb), q(q), reasoning(reasoning),
        reasoningThreshold(reasoningThreshold), matThreshold(matThreshold),
        layer(layer), program(program) {
    }

    void create(Query & query, int typePlanning);

    TupleIterator *getIterator();

    void releaseIterator(TupleIterator * itr);

    void print();
};

#endif
