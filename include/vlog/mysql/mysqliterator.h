#ifndef _MYSQLITERATOR_H
#define _MYSQLITERATOR_H

#include <vlog/edbiterator.h>
#include <vlog/concepts.h>
#include <vlog/consts.h>

//Mysql connectors header
#include <mysql_connection.h>
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>

class MySQLIterator : public EDBIterator {
private:
    PredId_t predid;

    bool hasNextChecked;
    bool hasNextValue;
    bool isFirst;
    bool skipDuplicatedFirst;
    int posFirstVar;

    sql::Statement *stmt;
    sql::ResultSet *res;

public:
    MySQLIterator(sql::Connection *con,
                  string tableName,
                  const Literal &query,
                  const std::vector<string> &fieldsTable,
                  const std::vector<uint8_t> *sortingFieldsIdx);

    MySQLIterator(sql::Connection *con,
                  string sqlQuery,
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

    ~MySQLIterator();
};

#endif
