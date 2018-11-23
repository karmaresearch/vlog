#ifndef QUERY_H
#define QUERY_H

#include <vlog/concepts.h>
#include <vlog/consts.h>

class QSQQuery {
private:
    const Literal literal;
    uint8_t nPosToCopy;
    uint8_t posToCopy[256];
    uint8_t nRepeatedVars;
    std::pair<uint8_t, uint8_t> repeatedVars[256];

public:
    VLIBEXP QSQQuery(const Literal literal);

    const Literal *getLiteral() const {
        return &literal;
    }

    uint8_t getNPosToCopy() const {
        return nPosToCopy;
    }

    uint8_t getNRepeatedVars() const {
        return nRepeatedVars;
    }

    std::pair<uint8_t, uint8_t> getRepeatedVar(const uint8_t idx) const {
        return repeatedVars[idx];
    }

    uint8_t *getPosToCopy() {
        return posToCopy;
    }

    std::string tostring();
};

#endif
