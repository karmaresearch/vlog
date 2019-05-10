#ifndef _EQUIVALENCE_H
#define _EQUIVALENCE_H

#include <vlog/concepts.h>

class Equivalence {
    private:
        Literal l1, l2;

    public:
        Equivalence(Literal l1, Literal l2) : l1(l1), l2(l2) {}

        const Literal &getLiteral1() const {
            return l1;
        }

        const Literal &getLiteral2() const {
            return l2;
        }
};

#endif 
