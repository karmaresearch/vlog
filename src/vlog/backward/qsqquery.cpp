#include <vlog/qsqquery.h>

QSQQuery::QSQQuery(const Literal literal) : literal(literal) {
    //Calculate the positions to copy
    nPosToCopy = 0;
    nRepeatedVars = 0;

    uint8_t nExistingVars = 0;
    std::pair<uint8_t, uint8_t> existingVars[256]; //value,pos

    for (int i = 0; i < (uint8_t) literal.getTupleSize(); ++i) {
        if (literal.getTermAtPos(i).isVariable()) {
            posToCopy[nPosToCopy++] = i;

            int8_t posRepeated = -1;
            if (nExistingVars > 0) {
                for (int j = 0; j < nExistingVars; ++j) {
                    if (literal.getTermAtPos(i).getId() == existingVars[j].first) {
                        posRepeated = j;
                        break;
                    }
                }
            }

            if (posRepeated == -1) {
                existingVars[nExistingVars++] = std::make_pair(literal.getTermAtPos(i).getId(), i);
            } else {
                repeatedVars[nRepeatedVars++] = std::make_pair(existingVars[posRepeated].second, i);
            }
        }
    }
}

std::string QSQQuery::tostring() {
    std::string out = std::string("[") + literal.tostring();
    out += std::string("nPosToCopy=") + std::to_string(nPosToCopy) + std::string("]");
    return out;
}
