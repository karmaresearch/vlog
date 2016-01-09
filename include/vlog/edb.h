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

#ifndef EDB_LAYER_H
#define EDB_LAYER_H

#include <vlog/concepts.h>
#include <vlog/qsqquery.h>
#include <vlog/idxtupletable.h>
#include <vlog/tuplekbitr.h>

#include <tridentcompr/utils/factory.h>

#include <vector>

#define EDB_KB 0
#define EDB_MEM 1
class EDBIterator {
public:
    virtual bool hasNext() = 0;

    virtual void next() = 0;

    virtual Term_t getElementAt(const uint8_t p) = 0;

    virtual uint8_t getType() = 0;

    virtual void skipDuplicatedFirstColumn() = 0;

    virtual ~EDBIterator() {}
};

class EDBKBIterator : public EDBIterator {
private:
    uint8_t nfields;

    TupleKBItr kbItr;

    //long nconcepts;

public:
    EDBKBIterator() {
        //nconcepts = 0;
    }

    void init(Querier *q, const Literal &literal);

    void init(Querier *q, const Literal &literal, const std::vector<uint8_t> &fields);

    bool hasNext();

    void next();

    void clear();

    void skipDuplicatedFirstColumn();

    uint8_t getType() {
        return EDB_KB;
    }

    Term_t getElementAt(const uint8_t p);

    ~EDBKBIterator() {
        //BOOST_LOG_TRIVIAL(debug) << "Iterated over " << nconcepts;
    }
};

class Column;
class EDBMemIterator : public EDBIterator {
private:
    uint8_t nfields = 0;
    bool isFirst = false, hasFirst = false;
    bool equalFields = false, isNextCheck = false, isNext = false;
    bool ignoreSecondColumn = false;
    bool isIgnoreAllowed = true;

    std::vector<Term_t>::iterator oneColumn;
    std::vector<Term_t>::iterator endOneColumn;

    std::vector<std::pair<Term_t, Term_t>>::iterator pointerEqualFieldsNext;
    std::vector<std::pair<Term_t, Term_t>>::iterator twoColumns;
    std::vector<std::pair<Term_t, Term_t>>::iterator endTwoColumns;

public:
    EDBMemIterator() {}

    void init1(std::vector<Term_t>*, const bool c1, const Term_t vc1);

    void init2(const bool defaultSorting,
               std::vector<std::pair<Term_t, Term_t>>*, const bool c1,
               const Term_t vc1, const bool c2, const Term_t vc2,
               const bool equalFields);

    void skipDuplicatedFirstColumn();

    bool hasNext();

    void next();

    uint8_t getType() {
        return EDB_MEM;
    }

    Term_t getElementAt(const uint8_t p);

    ~EDBMemIterator() {}
};

class EDBLayer {
private:
    Querier *q;
    const PredId_t kbId;
    const Predicate kbPredicate;

    Factory<EDBKBIterator> kbItrFactory;
    Factory<EDBMemIterator> memItrFactory;

    IndexedTupleTable *tmpRelations[MAX_NPREDS];

    std::vector<std::shared_ptr<Column>> performAntiJoin(const Literal &l1,
                                      std::vector<uint8_t> &pos1, const Literal &l2,
                                      std::vector<uint8_t> &pos2);


    std::vector<std::shared_ptr<Column>> performAntiJoin(
                                          std::vector<std::shared_ptr<Column>>
                                          &valuesToCheck,
                                          const Literal &l,
                                          std::vector<uint8_t> &pos);

    void getQueryFromEDBRelation0(QSQQuery *query, TupleTable *outputTable/*,
                                  Timeout *timeout*/);

    void getQueryFromEDBRelation12(QSQQuery *query, TupleTable *outputTable,
                                   std::vector<uint8_t> *posToFilter,
                                   std::vector<Term_t> *valuesToFilter/*,
                                   Timeout *timeout*/);

    void getQueryFromEDBRelation12(Term s, Term p, Term o, TupleTable *outputTable,
                                   std::vector<uint8_t> *posToFilter,
                                   std::vector<std::pair<uint64_t, uint64_t>> *pairs,
                                   std::vector<int> &posVarsToReturn,
                                   std::vector<std::pair<int, int>> &joins,
                                   std::vector<std::vector<int>> &posToCopy/*,
                                   Timeout *timeout*/);

    void getQueryFromEDBRelation3(QSQQuery *query, TupleTable *outputTable,
                                  std::vector<Term_t> *valuesToFilter/*,
                                  Timeout *timeout*/);

public:
    EDBLayer(Querier *q, Predicate kbPredicate) : q(q), kbId(kbPredicate.getId()), kbPredicate(kbPredicate) {
        for (int i = 0; i < MAX_NPREDS; ++i) {
            tmpRelations[i] = NULL;
        }
    }

    ~EDBLayer() {
        for (int i = 0; i < MAX_NPREDS; ++i) {
            if (tmpRelations[i] != NULL) {
                delete tmpRelations[i];
            }
        }
    }

    void addTmpRelation(Predicate &pred, IndexedTupleTable *table);

    bool isTmpRelationEmpty(Predicate &pred) {
        return tmpRelations[pred.getId()] == NULL ||
               tmpRelations[pred.getId()]->getNTuples() == 0;
    }

    Predicate getKBPredicate() {
        return kbPredicate;
    }

    size_t getSizeTmpRelation(Predicate &pred) {
        return tmpRelations[pred.getId()]->getNTuples();
    }

    bool checkValueInTmpRelation(const uint8_t relId,
                                 const uint8_t posInRelation,
                                 const Term_t value) const;

    std::vector<std::shared_ptr<Column>> checkNewIn(const Literal &l1,
                                      std::vector<uint8_t> &posInL1,
                                      const Literal &l2,
                                      std::vector<uint8_t> posInL2);

    std::vector<std::shared_ptr<Column>> checkNewIn(
                                          std::vector <
                                          std::shared_ptr<Column >> &checkValues,
                                          const Literal &l2,
                                          std::vector<uint8_t> posInL2);

    std::shared_ptr<Column> checkIn(
        std::vector<Term_t> &values,
        const Literal &l2,
        uint8_t posInL2,
        size_t &sizeOutput);

    //execute the query on the knowledge base
    void query(QSQQuery *query, TupleTable *outputTable,
               std::vector<uint8_t> *posToFilter,
               std::vector<Term_t> *valuesToFilter/*,
               Timeout *timeout*/);

    /*
    //Works only for EDB
    long getSizeOutput(QSQQuery *query,
                       std::vector<uint8_t> *posToFilter,
                       std::vector<Term_t> *valuesToFilter);
    */

    size_t estimateCardinality(const QSQQuery *query);

    size_t getCardinality(const QSQQuery *query);

    size_t getCardinalityColumn(const QSQQuery *query, uint8_t posColumn);

    bool isEmpty(const QSQQuery *query, std::vector<uint8_t> *posToFilter,
                 std::vector<Term_t> *valuesToFilter);

    EDBIterator *getIterator(const QSQQuery *query);

    EDBIterator *getSortedIterator(const QSQQuery *query,
                                   const std::vector<uint8_t> &fields);

    void releaseIterator(EDBIterator *itr);
};

#endif
