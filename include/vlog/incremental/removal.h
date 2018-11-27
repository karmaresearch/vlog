#ifndef VLOG__INCREMENTAL__REMOVAL_H__
#define VLOG__INCREMENTAL__REMOVAL_H__

#include <string>
#include <vector>
#include <unordered_map>

#include <vlog/concepts.h>
#include <vlog/edbtable.h>
#include <vlog/edbiterator.h>

class EDBLayer;

class EDBRemoveItem {
    public:
        std::unordered_map<Term_t, EDBRemoveItem *> has;

        EDBRemoveItem() { }

        ~EDBRemoveItem() {
            for (auto & c : has) {
                delete c.second;
            }
        }
};


class EDBRemoveLiterals {
    private:
        EDBRemoveItem removed;
        EDBLayer *layer;
        size_t num_rows;

        void insert(const std::vector<Term_t> &terms);

        std::ostream &dump_recursive(std::ostream &of,
                                     const EDBRemoveItem *item,
                                     std::string prefix) const;
        std::ostream &dump_recursive_name(std::ostream &of,
                                          const EDBLayer &layer,
                                          const EDBRemoveItem *item,
                                          std::string prefix) const;

    public:
        EDBRemoveLiterals(const std::string &file,
                          EDBLayer *layer);
        // Looks up the table in layer
        EDBRemoveLiterals(PredId_t predid, EDBLayer *layer);

        bool present(const std::vector<Term_t> &terms) const;

        size_t size() const {
            return num_rows;
        }

        std::ostream &dump(std::ostream &of,
                           const EDBLayer &layer) const;

        ~EDBRemoveLiterals() { }
};


class EDBRemovalIterator : public EDBIterator {
    private:
        // uint8_t arity;
        const std::vector<uint8_t> &fields;
        uint8_t adornment;
        const Literal &query;
        const EDBRemoveLiterals &removeTuples;
        EDBIterator *itr;

        std::vector<Term_t> current_term;
        std::vector<Term_t> term_ahead;
        bool expectNext;
        bool hasNext_ahead;

    public:
        // Around a non-sorted Iterator
        EDBRemovalIterator(const Literal &query,
                           const EDBRemoveLiterals &removeTuples,
                           EDBIterator *itr) :
                query(query), fields(std::vector<uint8_t>()),
                removeTuples(removeTuples), itr(itr),
                expectNext(false) {
            // arity = query.getTuple().getSize();
            adornment = 0;
            current_term.resize(query.getTuple().getSize());
            term_ahead.resize(query.getTuple().getSize());
        }

        // Around a SortedIterator
        EDBRemovalIterator(const Literal &query,
                           const std::vector<uint8_t> &fields,
                           const EDBRemoveLiterals &removeTuples,
                           EDBIterator *itr) :
                query(query), fields(fields), removeTuples(removeTuples),
                itr(itr), expectNext(false) {
            Predicate pred = query.getPredicate();
            VTuple tuple = query.getTuple();
            adornment = pred.calculateAdornment(tuple);
            // arity = tuple.getSize() - pred.getNFields(adornment);
            // arity = fields.size();
            current_term.resize(tuple.getSize());
            term_ahead.resize(tuple.getSize());
            for (int i = 0; i < tuple.getSize(); ++i) {
                if (adornment & (0x1 << i)) {
                    term_ahead[i] = tuple.get(i).getValue();
                }
            }
        }

        virtual bool hasNext() {
            if (expectNext) {
                return hasNext_ahead;
            }

            hasNext_ahead = false;
            while (true) {
                if (! itr->hasNext()) {
                    return false;
                }
                itr->next();
                if (adornment == 0) {
                    // fast path
                    for (int i = 0; i < term_ahead.size(); ++i) {
                        term_ahead[i] = itr->getElementAt(i);
                    }
                } else {
                    int it = 0;
                    for (int i = 0; i < term_ahead.size(); ++i) {
                        if (! (adornment & (0x1 << i))) {
                            term_ahead[i] = itr->getElementAt(it);
                            it++;
                        }
                    }
                }
                if (! removeTuples.present(term_ahead)) {
                    break;
                }
                LOG(DEBUGL) << "***** OK: skip one row";
            }

            hasNext_ahead = true;
            expectNext = true;
            return hasNext_ahead;
        }

        virtual void next() {
            if (! expectNext) {
                (void)hasNext();
            }
            expectNext = false;
            hasNext_ahead = false;
            current_term.swap(term_ahead);
            // done in hasNext()
        }

        virtual Term_t getElementAt(const uint8_t p) {
            return current_term[p];
        }

        virtual PredId_t getPredicateID() {
            return itr->getPredicateID();
        }

        virtual void moveTo(const uint8_t field, const Term_t t) {
            LOG(ERRORL) << "FIXME: what should I do in " << __func__ << "?";
            itr->moveTo(field, t);
        }

        virtual void skipDuplicatedFirstColumn() {
            LOG(ERRORL) << "FIXME: what should I do in " << __func__ << "?";
            itr->skipDuplicatedFirstColumn();
        }

        virtual void clear() {
            itr->clear();
        }

        // Until we find a way to handle the removals here, forbid.
        virtual const char *getUnderlyingArray(uint8_t column) {
            return NULL;
        }

        virtual std::pair<uint8_t, std::pair<uint8_t, uint8_t>> getSizeElemUnderlyingArray(uint8_t column) {
            return itr->getSizeElemUnderlyingArray(column);
        }

        EDBIterator *getUnderlyingIterator() const {
            return itr;
        }

        virtual ~EDBRemovalIterator() {
        }
};


#endif
