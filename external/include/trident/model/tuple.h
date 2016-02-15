#ifndef _TUPLE_H
#define _TUPLE_H

#include <trident/model/term.h>
#include <vector>

#define MAXTUPLESIZE 3
class Tuple {
private:
    const uint8_t sizetuple;
    Term terms[MAXTUPLESIZE];
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
