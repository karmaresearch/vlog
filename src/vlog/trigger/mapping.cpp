#include <vlog/trigger/mapping.h>

bool Mapping::unify(const Literal from, const Literal to) {
    //returns true if the literal in "front" can be mapped to "to"
    if (from.getPredicate().getCardinality() != to.getPredicate().getCardinality()) {
        return false;
    }
    const uint8_t card = from.getPredicate().getCardinality();
    for(int i = 0; i < card; ++i) {
        auto tf = from.getTermAtPos(i);
        auto tt = to.getTermAtPos(i);
        if (!tf.isVariable()) {
            if (!tt.isVariable())  {
                if (tt.getValue() != tf.getValue()) {
                    return false;
                }
            } else {
                //Add it to the inverse mapping
                inv_mappings[tt.getId()].push_back(VTerm(0, tf.getValue()));
            }
        } else {
            if (tt.isVariable()) {
                mappings[tf.getId()].push_back(VTerm(tt.getId(), 0));
                inv_mappings[tt.getId()].push_back(VTerm(tf.getId(), 0));
            } else {
                mappings[tf.getId()].push_back(VTerm(0, tt.getValue()));
            }
        }
    }
    return true;
}

void Mapping::rewriteVar(Predicate &pred, VTuple &tuple, int pos,
        std::vector<Literal> &out) const {
    //Get term at pos "pos"
    VTerm t = tuple.get(pos);
    if (t.isVariable() && inv_mappings.count(t.getId())) {
        const auto &replacements = inv_mappings.at(t.getId());
        for(const auto &r : replacements) {
            tuple.set(r, pos);
            if (pos == tuple.getSize() - 1) {
                out.push_back(Literal(pred, tuple));
            } else {
                rewriteVar(pred, tuple, pos + 1, out);
            }

        }
    } else {
        //nothing I should do
        if (pos == tuple.getSize() - 1) {
            out.push_back(Literal(pred, tuple));
        } else {
            rewriteVar(pred, tuple, pos + 1, out);
        }
    }
}

std::vector<Literal> Mapping::rewrite(const Literal &in) const {
    std::vector<Literal> out;
    VTuple t = in.getTuple();
    auto pred = in.getPredicate();
    rewriteVar(pred, t, 0, out);
    return out;
}
