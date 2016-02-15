#ifndef _MAPIITERATOR_H
#define _MAPIITERATOR_H

#include <vlog/edbiterator.h>
#include <vlog/concepts.h>
#include <vlog/consts.h>

#include <mapi.h>

class MAPIIterator : public EDBIterator {
private:
    PredId_t predid;

    bool hasNextChecked;
    bool hasNextValue;
    bool isFirst;
    bool skipDuplicatedFirst;
    int posFirstVar;
    int columns;
    uint64_t *values;
    MapiHdl handle = NULL;

public:
    MAPIIterator(Mapi con,
                  string tableName,
                  const Literal &query,
                  const std::vector<string> &fieldsTable,
                  const std::vector<uint8_t> *sortingFieldsIdx);

    MAPIIterator(Mapi con,
	    string sqlquery,
	    const std::vector<string> &fieldsTable,
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

    ~MAPIIterator();
};

#endif
