#ifndef _ODBCITERATOR_H
#define _ODBCITERATOR_H

#include <vlog/edbiterator.h>
#include <vlog/concepts.h>
#include <vlog/consts.h>

#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>

class ODBCIterator : public EDBIterator {
private:
    PredId_t predid;

    bool hasNextChecked;
    bool hasNextValue;
    bool isFirst;
    bool skipDuplicatedFirst;
    int posFirstVar;
    SQLSMALLINT columns;
    SQLLEN *indicator;
    SQLUBIGINT *values;

    SQLHANDLE stmt;

public:
    ODBCIterator(SQLHANDLE con,
                  string tableName,
                  const Literal &query,
                  const std::vector<string> &fieldsTable,
                  const std::vector<uint8_t> *sortingFieldsIdx);

    ODBCIterator(SQLHANDLE con,
	    string sqlquery,
	    const Literal &query);

    bool hasNext();

    void next();

    void clear();

    void skipDuplicatedFirstColumn();

    void moveTo(const uint8_t fieldId, const Term_t t);

    PredId_t getPredicateID() {
        return predid;
    }

    Term_t getElementAt(const uint8_t p);

    ~ODBCIterator();
};

#endif
