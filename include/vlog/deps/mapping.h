#ifndef _MAPPING_H
#define _MAPPING_H

#include <vlog/concepts.h>

#include <vector>
#include <map>

class Mapping {
    private:
        std::map<int, std::vector<VTerm>> mappings;
        std::map<int, std::vector<VTerm>> inv_mappings;

        void rewriteVar(Predicate &pred,
                VTuple &t, int pos,
                std::vector<Literal> &out) const;

    public:
        Mapping() {
        }

        bool unify(const Literal from, const Literal to);

        std::vector<Literal> rewrite(const Literal &in) const;
};

#endif
