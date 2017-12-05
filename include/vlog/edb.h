#ifndef EDB_LAYER_H
#define EDB_LAYER_H

#include <vlog/consts.h>
#include <vlog/concepts.h>
#include <vlog/qsqquery.h>
#include <vlog/support.h>
#include <vlog/idxtupletable.h>

#include <vlog/edbtable.h>
#include <vlog/edbiterator.h>
#include <vlog/edbconf.h>

#include <kognac/factory.h>

#include <vector>
#include <map>

class Column;
class EDBMemIterator : public EDBIterator {
    private:
        uint8_t nfields = 0;
        bool isFirst = false, hasFirst = false;
        bool equalFields = false, isNextCheck = false, isNext = false;
        bool ignoreSecondColumn = false;
        bool isIgnoreAllowed = true;
        PredId_t predid;

        std::vector<Term_t>::iterator oneColumn;
        std::vector<Term_t>::iterator endOneColumn;

        std::vector<std::pair<Term_t, Term_t>>::iterator pointerEqualFieldsNext;
        std::vector<std::pair<Term_t, Term_t>>::iterator twoColumns;
        std::vector<std::pair<Term_t, Term_t>>::iterator endTwoColumns;

    public:
        EDBMemIterator() {}

        void init1(PredId_t id, std::vector<Term_t>*, const bool c1, const Term_t vc1);

        void init2(PredId_t id, const bool defaultSorting,
                std::vector<std::pair<Term_t, Term_t>>*, const bool c1,
                const Term_t vc1, const bool c2, const Term_t vc2,
                const bool equalFields);

        void skipDuplicatedFirstColumn();

        bool hasNext();

        void next();

        PredId_t getPredicateID() {
            return predid;
        }

        void moveTo(const uint8_t fieldId, const Term_t t) {}

        Term_t getElementAt(const uint8_t p);

        void clear() {}

        ~EDBMemIterator() {}
};

class EDBLayer {
    private:

        struct EDBInfoTable {
            PredId_t id;
            uint8_t arity;
            string type;
            std::shared_ptr<EDBTable> manager;
        };

        Dictionary predDictionary;
        std::map<PredId_t, EDBInfoTable> dbPredicates;

        Factory<EDBMemIterator> memItrFactory;
        IndexedTupleTable *tmpRelations[MAX_NPREDS];

        void addTridentTable(const EDBConf::Table &tableConf, bool multithreaded);

#ifdef MYSQL
        void addMySQLTable(const EDBConf::Table &tableConf);
#endif
#ifdef ODBC
        void addODBCTable(const EDBConf::Table &tableConf);
#endif
#ifdef MAPI
        void addMAPITable(const EDBConf::Table &tableConf);
#endif
#ifdef MDLITE
        void addMDLiteTable(const EDBConf::Table &tableConf);
#endif
        void addInmemoryTable(const EDBConf::Table &tableConf);

    public:
        EDBLayer(EDBConf &conf, bool multithreaded) {
            const std::vector<EDBConf::Table> tables = conf.getTables();
            for (const auto &table : tables) {
                if (table.type == "Trident") {
                    addTridentTable(table, multithreaded);
#ifdef MYSQL
                } else if (table.type == "MySQL") {
                    addMySQLTable(table);
#endif
#ifdef ODBC
                } else if (table.type == "ODBC") {
                    addODBCTable(table);
#endif
#ifdef MAPI
                } else if (table.type == "MAPI") {
                    addMAPITable(table);
#endif
#ifdef MDLITE
                } else if (table.type == "MDLITE") {
                    addMDLiteTable(table);
#endif
                } else if (table.type == "INMEMORY") {
                    addInmemoryTable(table);
                } else {
                    LOG(ERRORL) << "Type of table is not supported";
                    throw 10;
                }
            }

            for (int i = 0; i < MAX_NPREDS; ++i) {
                tmpRelations[i] = NULL;
            }
        }

        std::vector<PredId_t> getAllPredicateIDs();

        uint64_t getPredSize(PredId_t id);

        string getPredType(PredId_t id);

        string getPredName(PredId_t id);

        uint8_t getPredArity(PredId_t id);

        void addTmpRelation(Predicate &pred, IndexedTupleTable *table);

        bool isTmpRelationEmpty(Predicate &pred) {
            return tmpRelations[pred.getId()] == NULL ||
                tmpRelations[pred.getId()]->getNTuples() == 0;
        }

        const Dictionary &getPredDictionary() {
            return predDictionary;
        }

        bool doesPredExists(PredId_t id) const;

        PredId_t getFirstEDBPredicate() {
            if (!dbPredicates.empty()) {
                auto p = dbPredicates.begin();
                return p->first;
            } else {
                LOG(ERRORL) << "There is no EDB Predicate!";
                throw 10;
            }
        }

        bool checkValueInTmpRelation(const uint8_t relId,
                const uint8_t posInRelation,
                const Term_t value) const;

        size_t getSizeTmpRelation(Predicate &pred) {
            return tmpRelations[pred.getId()]->getNTuples();
        }

        bool supportsCheckIn(const Literal &l);

        std::vector<std::shared_ptr<Column>> checkNewIn(const Literal &l1,
                std::vector<uint8_t> &posInL1,
                const Literal &l2,
                std::vector<uint8_t> &posInL2);

        std::vector<std::shared_ptr<Column>> checkNewIn(
                std::vector <
                std::shared_ptr<Column >> &checkValues,
                const Literal &l2,
                std::vector<uint8_t> &posInL2);

        std::shared_ptr<Column> checkIn(
                std::vector<Term_t> &values,
                const Literal &l2,
                uint8_t posInL2,
                size_t &sizeOutput);

        void query(QSQQuery *query, TupleTable *outputTable,
                std::vector<uint8_t> *posToFilter,
                std::vector<Term_t> *valuesToFilter);

        EDBIterator *getIterator(const Literal &query);

        EDBIterator *getSortedIterator(const Literal &query,
                const std::vector<uint8_t> &fields);

        bool isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
                std::vector<Term_t> *valuesToFilter);

        size_t estimateCardinality(const Literal &query);

        size_t getCardinality(const Literal &query);

        size_t getCardinalityColumn(const Literal &query,
                uint8_t posColumn);

        bool getDictNumber(const char *text, const size_t sizeText, uint64_t &id);

        bool getDictText(const uint64_t id, char *text);

        Predicate getDBPredicate(int idx);

        std::shared_ptr<EDBTable> getEDBTable(PredId_t id) {
            if (dbPredicates.count(id)) {
                return dbPredicates.find(id)->second.manager;
            } else {
                return std::shared_ptr<EDBTable>();
            }
        }

        string getTypeEDBPredicate(PredId_t id) {
            if (dbPredicates.count(id)) {
                return dbPredicates.find(id)->second.type;
            } else {
                return "";
            }
        }

        uint64_t getNTerms();

        void releaseIterator(EDBIterator *itr);

        ~EDBLayer() {
            for (int i = 0; i < MAX_NPREDS; ++i) {
                if (tmpRelations[i] != NULL) {
                    delete tmpRelations[i];
                }
            }
        }
};

#endif
