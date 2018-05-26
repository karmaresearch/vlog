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

        std::vector<string> varnames;
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
        InmemoryTable(string repository, string tablename, PredId_t predid, EDBLayer *layer);

        InmemoryTable(PredId_t predid, std::vector<std::vector<std::string>> &entries, EDBLayer *layer);

        uint8_t getArity() const;

        void query(QSQQuery *query, TupleTable *outputTable,
                std::vector<uint8_t> *posToFilter,
                std::vector<Term_t> *valuesToFilter);

        size_t estimateCardinality(const Literal &query);

        size_t getCardinality(const Literal &query);

        size_t getCardinalityColumn(const Literal &query, uint8_t posColumn);

        bool isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
                std::vector<Term_t> *valuesToFilter);

        EDBIterator *getIterator(const Literal &query);

	// This version has fields corresponding to the VARIABLES IN THE QUERY!!!!!
        EDBIterator *getSortedIterator(const Literal &query,
                const std::vector<uint8_t> &fields);

        bool getDictNumber(const char *text, const size_t sizeText,
                uint64_t &id);

        bool getDictText(const uint64_t id, char *text);

        bool getDictText(const uint64_t id, std::string &text);

        uint64_t getNTerms();

        void releaseIterator(EDBIterator *itr);

        uint64_t getSize();

        ~InmemoryTable();
};

#endif
