#include <vlog/trident/tridentiterator.h>
#include <trident/iterators/pairitr.h>
#include <trident/binarytables/newcolumntable.h>

void TridentIterator::init(PredId_t id, Querier * q, const Literal & literal, std::mutex *mutex) {
    predid = id;
    VTuple tuple = literal.getTuple();
    kbItr.init(q, &tuple, NULL
               , mutex
              );
    duplicatedFirstColumn = false;
}

void TridentIterator::init(PredId_t id, Querier * q, const Literal & literal, const std::vector<uint8_t> &fields, std::mutex *mutex) {
    predid = id;
    this->nfields = literal.getNVars();
    VTuple tuple = literal.getTuple();
    if (fields.size() > 0) {
        std::vector<uint8_t> sortedFields;
        for (uint8_t i = 0; i < fields.size(); ++i) {
            uint8_t pos = fields[i];
            uint8_t nvars = 0;
            for (uint8_t j = 0; j < literal.getTupleSize(); ++j) {
                if (literal.getTermAtPos(j).isVariable()) {
                    if (pos == nvars)
                        sortedFields.push_back(j);
                    nvars++;
                }
            }
        }
        kbItr.init(q, &tuple, &sortedFields, mutex);
    } else {
        kbItr.init(q, &tuple, NULL, mutex);
    }
    duplicatedFirstColumn = false;
}

void TridentIterator::skipDuplicatedFirstColumn() {
    kbItr.ignoreSecondColumn();
    duplicatedFirstColumn = true;
}

void TridentIterator::moveTo(const uint8_t fieldId, const Term_t t) {
    if (fieldId == 1) {
        kbItr.move(t, 0);
    } else if (fieldId == 2) {
        throw 10;
    }
}

const char* TridentIterator::getUnderlyingArray(uint8_t column) {
    return kbItr.getUnderlyingArray(column);
}

std::pair<uint8_t, std::pair<uint8_t, uint8_t>> TridentIterator::getSizeElemUnderlyingArray(uint8_t column) {
     return kbItr.getSizeElemUnderlyingArray(column);
}

bool TridentIterator::hasNext() {
    return kbItr.hasNext();
}

void TridentIterator::next() {
    kbItr.next();
}

void TridentIterator::clear() {
    kbItr.clear();
}

Term_t TridentIterator::getElementAt(const uint8_t p) {
    return kbItr.getElementAt(p);
}
