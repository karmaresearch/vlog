#ifndef _TOPK_ITERATOR_H
#define _TOPK_ITERATOR_H

#include <vlog/column.h>
#include <vlog/edbtable.h>
#include <vlog/edbiterator.h>
#include <vlog/segment.h>


class TopKIterator : public EDBIterator {
    private:
        const PredId_t predid;
        const size_t topk;
        const Term_t ent;
        const Term_t rel;
        const std::vector<std::pair<double, size_t>> &scores;
        uint64_t nextTerm;

    public:
        TopKIterator(PredId_t predid,
                size_t topk,
                Term_t ent,
                Term_t rel,
                std::vector<std::pair<double, size_t>> &scores) :
            predid(predid), topk(topk),ent(ent),rel(rel),scores(scores),
            nextTerm(0) {
                assert(scores.size() > 0);
            }

        bool hasNext() {
            return nextTerm < topk && nextTerm < scores.size() - 1;
        }

        void next() {
            nextTerm++;
        }

        Term_t getElementAt(const uint8_t p) {
            if (p == 0) {
                return ent;
            } else if (p == 1) {
                return rel;
            } else {
                assert(p == 2);
                return scores[nextTerm-1].second;
            }
        }

        PredId_t getPredicateID() {
            return predid;
        }

        void skipDuplicatedFirstColumn() {
        }

        void clear() {
            nextTerm = 0;
        }
};

#endif
