#ifndef _MODEL_H
#define _MODEL_H

#include <vector>

/*** TERMS ***/
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

/*** TUPLES ***/
#define SIZETUPLE 3
class Tuple {
private:
    const uint8_t sizetuple;
    Term terms[SIZETUPLE];
public:
    Tuple(const uint8_t sizetuple) : sizetuple(sizetuple) {}
    size_t getSize() const {
        return sizetuple;
    }
    Term get(const int pos) const {
        return terms[pos];
    }
    void set(const Term term, const int pos) {
        terms[pos] = term;
    }

    std::vector<std::pair<uint8_t, uint8_t>> getRepeatedVars() const {
        std::vector<std::pair<uint8_t, uint8_t>> output;
        for (uint8_t i = 0; i < sizetuple; ++i) {
            Term t1 = get(i);
            if (t1.isVariable()) {
                for (uint8_t j = i + 1; j < sizetuple; ++j) {
                    Term t2 = get(j);
                    if (t2.getId() == t1.getId()) {
                        output.push_back(std::make_pair(i, j));
                    }
                }
            }
        }
        return output;
    }
};

#endif

