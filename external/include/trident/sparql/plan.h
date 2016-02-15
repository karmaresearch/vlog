#ifndef _PLAN_H
#define _PLAN_H

#include <trident/sparql/sparqloperators.h>
#include <trident/sparql/query.h>
#include <trident/iterators/tupleiterators.h>

//#include <cts/plangen/PlanGen.hpp>

#define SIMPLE 0
#define BOTTOMUP 1
#define NONE 2

class Querier;
//class Plan;
class TridentQueryPlan {
private:

    Querier *q;

    std::map<string, uint64_t> mapVars1;
    std::map<uint64_t, string> mapVars2;

    std::shared_ptr<SPARQLOperator> root;

/*    std::shared_ptr<SPARQLOperator> translateJoin(Plan *plan);

    std::shared_ptr<SPARQLOperator> translateIndexScan(Plan *plan);

    std::shared_ptr<SPARQLOperator> translateProjection(Plan *plan);

    std::shared_ptr<SPARQLOperator> translateOperator(Plan *plan);*/

public:

    TridentQueryPlan(Querier *q) : q(q) {
    }

    void create(Query & query, int typePlanning);

    TupleIterator *getIterator();

    void releaseIterator(TupleIterator * itr);

    void print();
};

#endif
