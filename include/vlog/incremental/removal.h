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
            // std::cerr << "For now, don't delete the lower RemoveItems" << std::endl;
            // return;
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

        EDBRemoveItem *insert_recursive(const std::vector<Term_t> &terms, size_t offset);

        std::ostream &dump_recursive(std::ostream &of,
                                     EDBLayer *layer,
                                     const EDBRemoveItem *item) const;
        std::ostream &dump_recursive_name(std::ostream &of,
                                          EDBLayer *layer,
                                          const EDBRemoveItem *item) const;

    public:
        EDBRemoveLiterals(const std::string &file,
                          EDBLayer *layer);
        // Looks up the table in layer
        EDBRemoveLiterals(PredId_t predid, EDBLayer *layer);

        void insert(const std::vector<Term_t> &terms);

        bool present(const std::vector<Term_t> &terms) const;

        size_t size() const {
            return num_rows;
        }

        std::ostream &dump(std::ostream &of,
                           /* const */ EDBLayer *layer) const;

        ~EDBRemoveLiterals() { }
};


class EDBRemovalIterator : public EDBIterator {
    private:
        const uint8_t arity;
        const EDBRemoveLiterals &removeTuples;
        EDBIterator *itr;

        std::vector<Term_t> current_term;
        std::vector<Term_t> term_ahead;
        bool expectNext;
        bool hasNext_ahead;

    public:
        EDBRemovalIterator(const uint8_t arity,
                           const EDBRemoveLiterals &removeTuples,
                           EDBIterator *itr) :
            arity(arity), removeTuples(removeTuples), itr(itr), expectNext(false) {
            current_term.resize(arity);
            term_ahead.resize(arity);
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
                for (int i = 0; i < arity; ++i) {
                    term_ahead[i] = itr->getElementAt(i);
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
