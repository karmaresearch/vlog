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

#include <vlog/incremental/removal.h>

#include <kognac/factory.h>

#include <vector>
#include <map>

class Column;
class SemiNaiver;       // Why cannot I break the software hierarchy? RFHH
class EDBFCInternalTable;

using RemoveLiteralOf = std::unordered_map<PredId_t, const EDBRemoveLiterals *>;

using NamedSemiNaiver = std::unordered_map<std::string,
                                           std::shared_ptr<SemiNaiver>>;

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

        const EDBConf &conf;

        std::shared_ptr<Dictionary> predDictionary;
        std::map<PredId_t, EDBInfoTable> dbPredicates;

        Factory<EDBMemIterator> memItrFactory;
        std::vector<IndexedTupleTable *>tmpRelations;

        std::unique_ptr<Dictionary> termsDictionary;

        VLIBEXP void addTridentTable(const EDBConf::Table &tableConf, bool multithreaded);

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
        VLIBEXP void addInmemoryTable(const EDBConf::Table &tableConf);
        VLIBEXP void addSparqlTable(const EDBConf::Table &tableConf);

        VLIBEXP void addEDBonIDBTable(const EDBConf::Table &tableConf);
        VLIBEXP void addEDBimporter(const EDBConf::Table &tableConf);

        // literals to be removed during iteration
        RemoveLiteralOf removals;

        // For incremental reasoning: EDBonIDB must know which SemiNaiver(s) to
        // query
        NamedSemiNaiver prevSemiNaiver;

        // need to import the mapping predid -> Predicate from prevSemiNaiver
        void handlePrevSemiNaiver();

        std::string name;

    public:
        EDBLayer(EDBLayer &db, bool copyTables = false);

        EDBLayer(const EDBConf &conf, bool multithreaded,
                 const NamedSemiNaiver &prevSemiNaiver) :
                conf(conf), prevSemiNaiver(prevSemiNaiver) {

            const std::vector<EDBConf::Table> tables = conf.getTables();

            predDictionary = std::shared_ptr<Dictionary>(new Dictionary());

            if (prevSemiNaiver.size() != 0) {
                handlePrevSemiNaiver();
            }

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
#ifdef SPARQL
                } else if (table.type == "SPARQL") {
                    addSparqlTable(table);
#endif
                } else if (table.type == "EDBonIDB") {
                    addEDBonIDBTable(table);
                } else if (table.type == "EDBimporter") {
                    addEDBimporter(table);
                } else {
                    LOG(ERRORL) << "Type of table is not supported";
                    throw 10;
                }
            }
        }

        EDBLayer(const EDBConf &conf, bool multithreaded) :
            EDBLayer(conf, multithreaded, NamedSemiNaiver()) {
        }

        std::vector<PredId_t> getAllPredicateIDs() const;

        uint64_t getPredSize(PredId_t id) const;

        string getPredType(PredId_t id) const;

        string getPredName(PredId_t id) const;

        uint8_t getPredArity(PredId_t id) const;

        PredId_t getPredID(const std::string &name) const;

        void addTmpRelation(Predicate &pred, IndexedTupleTable *table);

        bool isTmpRelationEmpty(Predicate &pred) {
            if (pred.getId() >= tmpRelations.size()) {
                return false;
            }
            return tmpRelations[pred.getId()] == NULL ||
                tmpRelations[pred.getId()]->getNTuples() == 0;
        }

        const Dictionary &getPredDictionary() {
            assert(predDictionary.get() != NULL);
            return *(predDictionary.get());
        }

        std::unordered_map< PredId_t, uint8_t> getPredicateCardUnorderedMap () {
            std::unordered_map< PredId_t, uint8_t> ret;
            for (auto item : dbPredicates)
                ret.insert(make_pair(item.first, item.second.arity));
            return ret;
        }

        VLIBEXP bool doesPredExists(PredId_t id) const;

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
            if (pred.getId() >= tmpRelations.size()) {
                return 0;
            }
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

        VLIBEXP void query(QSQQuery *query, TupleTable *outputTable,
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

        VLIBEXP bool getDictNumber(const char *text,
                const size_t sizeText, uint64_t &id) const;

        VLIBEXP bool getOrAddDictNumber(const char *text,
                const size_t sizeText, uint64_t &id);

        VLIBEXP bool getDictText(const uint64_t id, char *text) const;

        VLIBEXP std::string getDictText(const uint64_t id) const;

        Predicate getDBPredicate(int idx) const;

        std::shared_ptr<EDBTable> getEDBTable(PredId_t id) const {
            if (dbPredicates.count(id)) {
                return dbPredicates.find(id)->second.manager;
            } else {
                return std::shared_ptr<EDBTable>();
            }
        }

        string getTypeEDBPredicate(PredId_t id) const {
            if (dbPredicates.count(id)) {
                return dbPredicates.find(id)->second.type;
            } else {
                return "";
            }
        }

        VLIBEXP uint64_t getNTerms() const;

        bool expensiveEDBPredicate(PredId_t id) {
            if (dbPredicates.count(id)) {
                return dbPredicates.find(id)->second.manager->expensiveLayer();
            }
            return false;
        }

        void releaseIterator(EDBIterator *itr);

        VLIBEXP void addRemoveLiterals(const RemoveLiteralOf &rm) {
            removals.insert(rm.begin(), rm.end());
        }

        VLIBEXP bool hasRemoveLiterals(PredId_t pred) const {
            if (removals.empty()) {
                return false;
            }
            if (removals.find(pred) == removals.end()) {
                return false;
            }
            if (removals.at(pred)->size() == 0) {
                return false;
            }

            return true;
        }

        // For JNI interface ...
        VLIBEXP void addInmemoryTable(std::string predicate,
                std::vector<std::vector<std::string>> &rows);

        VLIBEXP void addInmemoryTable(std::string predicate,
                PredId_t id, std::vector<std::vector<std::string>> &rows);

        //For RMFA check
        VLIBEXP void addInmemoryTable(PredId_t predicate,
                uint8_t arity,
                std::vector<uint64_t> &rows);

        const EDBConf &getConf() const {
            return conf;
        }

        void setName(const std::string &name) {
            this->name = name;
        }

        const std::string &getName() const {
            return name;
        }

        ~EDBLayer() {
            for (int i = 0; i < tmpRelations.size(); ++i) {
                if (tmpRelations[i] != NULL) {
                    delete tmpRelations[i];
                }
            }
        }
};

#endif
