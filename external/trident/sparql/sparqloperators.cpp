#include <trident/sparql/tuplekbiterator.h>
#include <trident/sparql/sparqloperators.h>
#include <trident/iterators/tupleiterators.h>
#include <trident/sparql/query.h>
#include <trident/sparql/joins.h>

Join::Join(std::vector<std::shared_ptr<SPARQLOperator>> children) {
    this->children = children;
    for (int i = 0; i < children.size(); ++i) {
        this->fields = JoinPlan::mergeFields(
                           this->fields, children[i]->getTupleFieldsIDs());
    }

    //Construct join plan
    plan = shared_ptr<JoinPlan>(new JoinPlan());
    plan->prepare(children);
}

Join::Join(std::vector<std::shared_ptr<SPARQLOperator>> children,
           std::vector<string> &projections) {
    this->children = children;
    for (int i = 0; i < children.size(); ++i) {
        this->fields = JoinPlan::mergeFields(
                           this->fields, children[i]->getTupleFieldsIDs());
    }

    //Construct join plan
    plan = shared_ptr<JoinPlan>(new JoinPlan());
    plan->prepare(children, projections);
}


NestedMergeJoin::NestedMergeJoin(Querier *q,
                                 std::vector<std::shared_ptr<SPARQLOperator>> children) : Join(children) {
    this->q = q;
    //The original plan is not considered in this case. We use an optimized version
    std::vector<Filter *> filters;
    filters.resize(children.size());
    std::vector<Pattern *> listPatterns;
    for (int i = 0; i < children.size(); ++i) {
        listPatterns.push_back(
            std::static_pointer_cast<Scan>(children[i])->getPattern());
    }

    nestedPlan = std::shared_ptr<NestedJoinPlan>(new NestedJoinPlan());
    nestedPlan->prepare(q, listPatterns, filters, getTupleFieldsIDs());

}
NestedMergeJoin::NestedMergeJoin(Querier *q,
                                 std::vector<std::shared_ptr<SPARQLOperator>> children,
                                 std::vector<string> &projections)
    : Join(children) {
    this->q = q;
    //The original plan is not considered in this case. We use an optimized version
    std::vector<Filter *> filters;
    filters.resize(children.size());
    std::vector<Pattern *> listPatterns;
    for (int i = 0; i < children.size(); ++i) {
        listPatterns.push_back(
            std::static_pointer_cast<Scan>(children[i])->getPattern());
    }

    nestedPlan = std::shared_ptr<NestedJoinPlan>(new NestedJoinPlan());
    nestedPlan->prepare(q, listPatterns, filters, projections);
}

NestedMergeJoin::NestedMergeJoin(NestedMergeJoin &existing,
                                 std::vector<string> &projections) : Join(existing.getChildren(),
                                             projections) {
    this->q = existing.q;
    //This procedure can be optimized by reusing the existing plan. However, the cost of this op
    //is quite small
    std::vector<Filter *> filters;
    filters.resize(children.size());
    std::vector<Pattern *> listPatterns;
    for (int i = 0; i < children.size(); ++i) {
        listPatterns.push_back(
            std::static_pointer_cast<Scan>(children[i])->getPattern());
    }

    nestedPlan = std::shared_ptr<NestedJoinPlan>(new NestedJoinPlan());
    nestedPlan->prepare(q, listPatterns, filters, projections);
}

void NestedMergeJoin::print(int indent) {
    for (int i = 0; i < indent; ++i)
        cerr << ' ';

    BOOST_LOG_TRIVIAL(debug) << "MERGEJOIN";

    for (int i = 0; i < children.size(); ++i) {
        children[i]->print(indent + 1);
    }
}

TupleIterator *NestedMergeJoin::getIterator() {
    return new NestedMergeJoinItr(q, nestedPlan);
}

void NestedMergeJoin::releaseIterator(TupleIterator *itr) {
    delete itr;
}

TridentHashJoin::TridentHashJoin(
    std::vector<std::shared_ptr<SPARQLOperator>> children) : Join(children) {
}

TridentHashJoin::TridentHashJoin(std::vector<std::shared_ptr<SPARQLOperator>> children,
                   std::vector<string> &projections) : Join(children, projections) {
}

TupleIterator *TridentHashJoin::getIterator() {
    return new HashJoinItr(children, plan);
}

void TridentHashJoin::releaseIterator(TupleIterator *itr) {
    delete itr;
}

void TridentHashJoin::print(int indent) {
    for (int i = 0; i < indent; ++i)
        cerr << ' ';

    BOOST_LOG_TRIVIAL(debug) << "HASHJOIN";

    for (int i = 0; i < children.size(); ++i) {
        children[i]->print(indent + 1);
    }
}

Scan::Scan(Pattern *p) {
    for (int i = 0; i < p->getNVars(); ++i)
        fields.push_back(p->getVar(i));
    this->pattern = p;
}

void Scan::print(int indent) {
    for (int i = 0; i < indent; ++i)
        cerr << ' ';

    BOOST_LOG_TRIVIAL(debug) << "SCAN " << pattern->toString();
}

KBScan::KBScan(Querier *q, Pattern *p) : Scan(p), t(3) {
    this->q = q;
    if (p->subject() < 0) {
        t.set(Term((uint8_t) p->subject(), 0), 0);
    } else {
        t.set(Term(0, p->subject()), 0);
    }
    if (p->predicate() < 0) {
        t.set(Term((uint8_t) p->predicate(), 0), 1);
    } else {
        t.set(Term(0, p->predicate()), 1);
    }
    if (p->object() < 0) {
        t.set(Term((uint8_t) p->object(), 0), 2);
    } else {
        t.set(Term(0, p->object()), 2);
    }

    //Add the variables. This should work also with repeated variables because
    //only one variable can be repeated
    std::vector<int> *pv = p->getPosVars();
    uint8_t idxVar = 1;
    std::string nameVar;
    for (int i = 0; i < pv->size(); ++i) {
        t.set(Term(idxVar, 0), pv->at(i));
        if (p->getVar(i) != nameVar) {
            idxVar++;
            nameVar = p->getVar(i);
        }
    }
}

TupleIterator *KBScan::getIterator() {
    TupleKBItr *itr = new TupleKBItr();
    itr->init(q, &t, NULL, true);
    return itr;
}

TupleIterator *KBScan::getSampleIterator() {
    Querier &sampleQuerier = q->getSampler();
    TupleKBItr *itr = new TupleKBItr();
    itr->init(&sampleQuerier, &t, NULL, true);
    return itr;

}

long KBScan::estimateCost() {
    long s, p, o;
    if (t.get(0).isVariable()) {
        s = -1;
    } else {
        s = t.get(0).getValue();
    }
    if (t.get(1).isVariable()) {
        p = -1;
    } else {
        p = t.get(1).getValue();
    }
    if (t.get(2).isVariable()) {
        o = -1;
    } else {
        o = t.get(2).getValue();
    }
    return q->getCard(s, p, o);
}

void KBScan::releaseIterator(TupleIterator *itr) {
    delete itr;
}

/*ReasoningScan::ReasoningScan(Pattern *pattern, EDBLayer *layer,
                             Program *program, DictMgmt *dict,
                             const uint64_t threshold) : Scan(pattern),
    reasoner(threshold) {
    this->pattern = pattern;
    this->program = program;
    this->layer = layer;
    this->dict = dict;
    optimize(NULL, NULL);
}

void ReasoningScan::optimize(std::vector<uint8_t> *posBindings,
                             std::vector<uint64_t> *valueBindings) {
    if (posBindings == NULL || posBindings->size() < pattern->getNVars()) {
        mode = reasoner.chooseMostEfficientAlgo(pattern, *layer, *program,
                                                dict, posBindings, valueBindings);
    }
}

TupleIterator *ReasoningScan::getIterator() {
    if (mode == TOPDOWN) {
        return reasoner.getTopDownIterator(pattern, NULL, NULL, *layer,
                                           *program, dict, true);
    } else {
        return reasoner.getMagicIterator(pattern, NULL, NULL, *layer,
                                         *program, dict, true);
    }
}

TupleIterator *ReasoningScan::getIterator(std::vector<uint8_t> &positions,
        std::vector<uint64_t> &values) {

    //Can I first check whether I can remove some bindings?
    if (positions.size() == pattern->getNVars()) {
        return reasoner.getIncrReasoningIterator(pattern, &positions, &values, *layer,
                *program, dict, true);
    } else {
        if (mode == TOPDOWN)
            return reasoner.getTopDownIterator(pattern, &positions, &values, *layer,
                                               *program, dict, true);
        else
            return reasoner.getMagicIterator(pattern, &positions, &values, *layer,
                                             *program, dict, true);
    }
}

long ReasoningScan::estimateCost() {
    return reasoner.estimate(pattern, NULL, NULL, *layer, *program, dict);
}

void ReasoningScan::releaseIterator(TupleIterator *itr) {
    delete itr;
}

MaterializedScan::MaterializedScan(std::shared_ptr<SemiNaiver> sn,
                                   Pattern *p, Program *program) : Scan(p) {
    Tuple t(3);
    bool isSConst = false;
    bool isPConst = false;
    bool isOConst = false;
    long s, pr, o;
    // BOOST_LOG_TRIVIAL(debug) << "MaterializedScan: [" << p->subject() << ", "
    //                          << p->predicate() << ", " << p->object() << "]; " << p->toString();
    // Here, if there are variables in the pattern, the corresponding
    // value is -1!

    vector<int> repeated = p->getRepeatedVars();

    uint8_t nVars = 0;
    int nRepeated = 0;

    if (p->subject() < 0) {
        nVars++;
        t.set(Term(nVars, 0), 0);
    } else {
        t.set(Term(0, p->subject()), 0);
        isSConst = true;
        s = p->subject();
    }
    if (p->predicate() < 0) {
        if (repeated.size() > 0 && repeated[0] == 0 && repeated[1] == 1) {
            nRepeated++;
        } else {
            nVars++;
        }
        t.set(Term(nVars, 0), 1);
    } else {
        t.set(Term(0, p->predicate()), 1);
        isPConst = true;
        pr = p->predicate();
    }
    if (p->object() < 0) {
        if (repeated.size() > 0) {
            if (repeated[0] == 0 && repeated[1] == 2) {
                t.set(Term(1, 0), 2);
                nRepeated++;
            } else if (repeated[0] == 1 && repeated[1] == 2) {
                t.set(Term(2, 0), 2);
                nRepeated++;
            } else if (repeated.size() > 2) {
                t.set(Term(1, 0), 2);
                nRepeated++;
            } else {
                nVars++;
                t.set(Term(nVars, 0), 2);
            }
        } else {
            nVars++;
            t.set(Term(nVars, 0), 2);
        }
    } else {
        t.set(Term(0, p->object()), 2);
        isOConst = true;
        o = p->object();
    }
    //Literal TI
    string ti = string("TI");
    Literal l = Literal(program->getPredicate(ti), t);
    // BOOST_LOG_TRIVIAL(debug) << "MaterializedScan: literal = " << l.tostring();
    FCIterator itr = sn->getTable(l, 0, (size_t) - 1);
    table = std::unique_ptr<TupleTable>(new TupleTable(nVars + nRepeated));
    while (!itr.isEmpty()) {
        std::shared_ptr<const FCInternalTable> t = itr.getCurrentTable();
        FCInternalTableItr *tableItr = t->getIterator();
        while (tableItr->hasNext()) {
            tableItr->next();
            uint8_t idxVar = 0;
            if (!isSConst)
                table->addValue(tableItr->getCurrentValue(idxVar++));
            // else
            //     table->addValue(s);
            if (!isPConst)
                table->addValue(tableItr->getCurrentValue(idxVar++));
            // else
            //     table->addValue(pr);
            if (!isOConst)
                table->addValue(tableItr->getCurrentValue(idxVar++));
            // else
            //     table->addValue(o);
        }
        t->releaseIterator(tableItr);
        itr.moveNextCount();
    }
}

TupleIterator *MaterializedScan::getIterator() {
    return new TupleTableItr(table);
}

TupleIterator *MaterializedScan::getSampleIterator() {
    //not supported
    throw 10;
}

long MaterializedScan::estimateCost() {
    return table->getNRows();
}

void MaterializedScan::releaseIterator(TupleIterator *itr) {
    delete itr;
}*/
