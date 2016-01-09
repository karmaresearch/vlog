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

#ifndef SPARQL_OPERATOR_H
#define SPARQL_OPERATOR_H

#include <trident/sparql/joins.h>
#include <trident/model/table.h>
#include <trident/kb/kb.h>

#include <cctype>
#include <inttypes.h>
#include <vector>

typedef enum { SCAN, NESTEDMERGEJOIN, HASHJOIN } Op;
class SPARQLOperator {
public:

    virtual Op getType() = 0;

    virtual TupleIterator *getIterator() = 0;

    virtual void releaseIterator(TupleIterator *itr) = 0;

    virtual size_t getOutputTupleSize() = 0;

    virtual std::vector<string> getTupleFieldsIDs() = 0;

    virtual void print(int indent) = 0;

    virtual bool doesSupportsSideways() {
        return false;
    }

    virtual void optimize(std::vector<uint8_t> *posBindings,
                          std::vector<uint64_t> *valueBindings) {
    }

    virtual TupleIterator *getIterator(
        std::vector<uint8_t> &positions, std::vector<uint64_t> &values) {
        BOOST_LOG_TRIVIAL(error) << "Not supported";
        throw 10;
    }

    virtual ~SPARQLOperator() {}
};

class Join : public SPARQLOperator {
private:
    std::vector<string> fields;

protected:
    std::vector<std::shared_ptr<SPARQLOperator>> children;
    std::shared_ptr<JoinPlan> plan;

public:
    Join(std::vector<std::shared_ptr<SPARQLOperator>> children);

    Join(std::vector<std::shared_ptr<SPARQLOperator>> children,
         std::vector<string> &projections);

    size_t getOutputTupleSize() {
        return fields.size();
    }

    std::vector<string> getTupleFieldsIDs() {
        return fields;
    }

    std::vector<std::shared_ptr<SPARQLOperator>> getChildren() {
        return children;
    }
};

class NestedMergeJoin : public Join {
private:
    Querier *q;
    std::shared_ptr<NestedJoinPlan> nestedPlan;

public:
    NestedMergeJoin(Querier *q,
                    std::vector<std::shared_ptr<SPARQLOperator>> children);

    NestedMergeJoin(Querier *q,
                    std::vector<std::shared_ptr<SPARQLOperator>> children,
                    std::vector<string> &projections);

    NestedMergeJoin(NestedMergeJoin &exiting,
                    std::vector<string> &projections);

    Op getType() {
        return NESTEDMERGEJOIN;
    }

    TupleIterator *getIterator();

    void releaseIterator(TupleIterator *itr);

    void print(int indent);
};

class HashJoin : public Join {
public:
    HashJoin(std::vector<std::shared_ptr<SPARQLOperator>> children);

    HashJoin(std::vector<std::shared_ptr<SPARQLOperator>> children,
             std::vector<string> &projections);

    Op getType() {
        return HASHJOIN;
    }

    TupleIterator *getIterator();

    void releaseIterator(TupleIterator *itr);

    void print(int indent);
};

class Scan : public SPARQLOperator {
private:
    std::vector<string> fields;
    Pattern *pattern;

public:
    virtual TupleIterator *getIterator() = 0;

    virtual long estimateCost() = 0;

    Scan(Pattern *pattern);

    Pattern *getPattern() {
        return pattern;
    }

    Op getType() {
        return SCAN;
    }

    size_t getOutputTupleSize() {
        return fields.size();
    }

    std::vector<string> getTupleFieldsIDs() {
        return fields;
    }

    void print(int indent);
};

class KBScan : public Scan {
private:
    Querier *q;

public:
    KBScan(Querier *q, Pattern *p);

    TupleIterator *getIterator();

    long estimateCost();

    TupleIterator *getSampleIterator();

    void releaseIterator(TupleIterator *itr);
};
#endif
