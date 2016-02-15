#ifndef _TERM_H
#define _TERM_H

#include <inttypes.h>

class Term {
private:
    uint8_t id; //ID != 0 => variable. ID==0 => const value
    uint64_t value;
public:
    Term() : id(0), value(0) {}
    Term(const uint8_t id, const uint64_t value) : id(id), value(value) {}
    uint8_t getId() const {
        return id;
    }
    uint64_t getValue() const {
        return value;
    }
    void setId(const uint8_t i) {
        id = i;
    }
    void setValue(const uint64_t v) {
        value = v;
    }
    bool isVariable() const {
        return id > 0;
    }
    bool operator==(const Term& rhs) const {
        return id == rhs.getId() && value == rhs.getValue();
    }
    bool operator!=(const Term& rhs) const {
        return id != rhs.getId() || value != rhs.getValue();
    }
};

#endif
