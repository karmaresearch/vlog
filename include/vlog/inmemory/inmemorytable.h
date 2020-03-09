#ifndef _INMEMORY_H
#define _INMEMORY_H

#include <vlog/column.h>
#include <vlog/edbtable.h>
#include <vlog/edbiterator.h>
#include <vlog/segment.h>

class InmemoryIterator : public EDBIterator {
    private:
        std::shared_ptr<const Segment> segment;
        std::unique_ptr<SegmentIterator> iterator;
        std::vector<uint8_t> sortFields;
        PredId_t predid;
        bool skipDuplicatedFirst;
        bool hasNextChecked;
        bool hasNextValue;
        bool isFirst;

    public:
        InmemoryIterator(std::shared_ptr<const Segment> segment, PredId_t predid, std::vector<uint8_t> sortFields) :
            segment(segment), predid(predid), sortFields(sortFields), skipDuplicatedFirst(false),
            hasNextChecked(false), hasNextValue(false), isFirst(true) {
                if (! segment) {
                    iterator = NULL;
                } else {
                    iterator = segment->iterator();
                }
            }

        bool hasNext();

        void next();

        Term_t getElementAt(const uint8_t p);

        PredId_t getPredicateID();

        void skipDuplicatedFirstColumn();

        void clear();
};

class InmemoryTable : public EDBTable {
    private:
        struct Coordinates {
            uint64_t offset;
            uint64_t len;
            Coordinates(uint64_t offset, uint64_t len) :
                offset(offset), len(len) {
                }
        };
        typedef std::unordered_map<uint64_t, Coordinates> HashMap;
        struct HashMapEntry {
            HashMap map;
            std::shared_ptr<const Segment> segment;
            HashMapEntry(std::shared_ptr<const Segment> segment) :
                segment(segment) {
                }
        };

        std::vector<std::string> varnames; // TODO is InmemoryTable.varnames ever used?
        PredId_t predid;
        uint8_t arity;
        EDBLayer *layer;

        std::shared_ptr<const Segment> segment;
        std::map<uint64_t, std::shared_ptr<const Segment>> cachedSortedSegments;
        std::map<uint64_t, std::shared_ptr<HashMapEntry>> cacheHashes;

        std::shared_ptr<const Segment> getSortedCachedSegment(
                std::shared_ptr<const Segment> segment,
                const std::vector<uint8_t> &filterBy);

        // This version has fields corresponding to the query.
        EDBIterator *getSortedIterator2(const Literal &query,
                const std::vector<uint8_t> &fields);

    public:
        InmemoryTable(std::string repository, std::string tablename, PredId_t predid,
                EDBLayer *layer, char sep=',', bool loadData = true);

        InmemoryTable(PredId_t predid, std::vector<std::vector<std::string>> &entries, EDBLayer *layer);

        InmemoryTable(PredId_t predid, uint8_t arity, std::vector<uint64_t> &entries, EDBLayer *layer);

        InmemoryTable(PredId_t predid,
                      const Literal &query,
                      // const
                      EDBIterator *iter,
                      EDBLayer *layer);

        virtual uint8_t getArity() const;

        void query(QSQQuery *query, TupleTable *outputTable,
                std::vector<uint8_t> *posToFilter,
                std::vector<Term_t> *valuesToFilter);

        size_t estimateCardinality(const Literal &query);

        size_t getCardinality(const Literal &query);

        size_t getCardinalityColumn(const Literal &query, uint8_t posColumn);

        bool isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
                std::vector<Term_t> *valuesToFilter);

        EDBIterator *getIterator(const Literal &query);

        bool useSegments() {
            return true;
        }

        std::shared_ptr<const Segment> getSegment() {
            return segment;
        }

        // This version has fields corresponding to the VARIABLES IN THE QUERY!!!!!
        EDBIterator *getSortedIterator(const Literal &query,
                const std::vector<uint8_t> &fields);

        bool getDictNumber(const char *text, const size_t sizeText, uint64_t &id);

        bool getDictText(const uint64_t id, char *text);

        bool getDictText(const uint64_t id, std::string &text);

        uint64_t getNTerms();

        void releaseIterator(EDBIterator *itr);

        uint64_t getSize();

        std::ostream &dump(std::ostream &os) {
            std::string name = layer->getPredName(predid);
            os << "Table " << name << std::endl;
            Predicate pred(predid, 0, EDB, arity);
            VTuple t = VTuple(arity);
            for (uint8_t i = 0; i < t.getSize(); ++i) {
                t.set(VTerm(i + 1, 0), i);
            }
            Literal lit(pred, t);
            EDBIterator *itr = getIterator(lit);
            const uint8_t sizeRow = getArity();
            while (itr->hasNext()) {
                itr->next();
                os << "Get another row from the InmemoryTable!" << std::endl;
                for (uint8_t m = 0; m < sizeRow; ++m) {
                    os << "\t";
                    std::string buffer;
                    if (getDictText(itr->getElementAt(m), buffer)) {
                        os << buffer;
                    } else {
                        uint64_t v = itr->getElementAt(m);
                        std::string t = "" + std::to_string(v >> 40) + "_"
                            + std::to_string((v >> 32) & 0377) + "_"
                            + std::to_string(v & 0xffffffff);
                        os << t;
                    }
                }
                os << std::endl;
            }

            releaseIterator(itr);

            return os;
        }

        ~InmemoryTable();
};

#endif
