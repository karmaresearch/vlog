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
        std::vector<std::pair<double, size_t>> scores;
        uint64_t nextTerm;

    public:
        static bool ent_sorter(const std::pair<double, int> &a, const std::pair<double, int> &b) {
            return a.second < b.second;
        }


        TopKIterator(PredId_t predid,
                size_t topk,
                Term_t ent,
                Term_t rel,
                std::vector<std::pair<double, size_t>> &scores,
                bool sorted) :
            predid(predid), topk(topk), ent(ent), rel(rel),
            nextTerm(0) {
                assert(scores.size() > 0);
                auto nelements = std::min(scores.size(), topk);
                std::copy(scores.begin(), scores.begin() + nelements,
                        std::back_inserter(this->scores));
                if (sorted) {
                    std::sort(this->scores.begin(), this->scores.end(),
                            TopKIterator::ent_sorter);
                }
            }

        bool hasNext() {
            return nextTerm < scores.size() - 1;
        }

        void next() {
            nextTerm++;
        }

        Term_t getElementAt(const uint8_t p) {
            if (p == 0) {
                return ent;
            } else if (p == 1) {
                return rel;
            } else if (p == 2) {
                return scores[nextTerm-1].second;
            } else if (p == 3) {
                float value = scores[nextTerm-1].first;
                return FLOAT32_MASK(value);
            } else {
                LOG(ERRORL) << "This should not have happened";
                throw 10;
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
