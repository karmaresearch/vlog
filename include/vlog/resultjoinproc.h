#ifndef _RESULTJOINPROC_H
#define _RESULTJOINPROC_H

#include <vlog/segment.h>
#include <vlog/fctable.h>
#include <vlog/chasemgmt.h>

#include <vector>

#define MAX_MAPPINGS 8

#define TMPT_THRESHOLD  (32*1024*1024)

// Set USE_DUPLICATE_DETECTION to 1 to enable the duplicate detection code.
// Set to 0 for the AAAI-16 version.
#define USE_DUPLICATE_DETECTION 0

#if USE_DUPLICATE_DETECTION
struct MyHash {
    size_t operator() (const std::vector<Term_t> &p) const {
        uint64_t result = 0;
        for (int i = 0; i < p.size(); i++) {
            uint64_t tmp = p[i];
            while (tmp != 0) {
                result = result * 576460752303422839 + (tmp & 0xFFFF);
                tmp = (tmp >> 16);
            }
        }
        return (size_t) result;
    }
};
struct MyEq {
    bool operator() (const std::vector<Term_t> &p, const std::vector<Term_t> &q) const {
        for (int i = 0; i < p.size(); i++) {
            if (p[i] != q[i]) {
                return false;
            }
        }
        return true;
    }
};

typedef google::dense_hash_set<std::vector<Term_t>, MyHash, MyEq> HashSet;
#endif

class ResultJoinProcessor {
    protected:
        const uint8_t rowsize;
        Term_t *row;
        bool deleteRow;
        const uint8_t nCopyFromFirst;
        const uint8_t nCopyFromSecond;
        std::pair<uint8_t, uint8_t> posFromFirst[MAX_MAPPINGS];
        std::pair<uint8_t, uint8_t> posFromSecond[MAX_MAPPINGS];
        const int nthreads;
#if USE_DUPLICATE_DETECTION
        HashSet *rowsHash;
        size_t rowCount;
#endif
    protected:
        virtual void processResults(const int blockid,
                const bool unique, std::mutex *m) = 0;

        void copyRawRow(const Term_t *first, const Term_t* second);

        void copyRawRow(const Term_t *first, FCInternalTableItr* second);

    public:
        ResultJoinProcessor(const uint8_t rowsize,
                const uint8_t nCopyFromFirst,
                const uint8_t nCopyFromSecond,
                const std::pair<uint8_t, uint8_t> *posFromFirst,
                const std::pair<uint8_t, uint8_t> *posFromSecond,
                const int nthreads) :
            ResultJoinProcessor(rowsize, new Term_t[rowsize], true, nCopyFromFirst,
                    nCopyFromSecond, posFromFirst, posFromSecond, nthreads) {
            }

        ResultJoinProcessor(const uint8_t rowsize, Term_t *row,
                bool deleteRow,
                const uint8_t nCopyFromFirst,
                const uint8_t nCopyFromSecond,
                const std::pair<uint8_t, uint8_t> *posFromFirst,
                const std::pair<uint8_t, uint8_t> *posFromSecond,
                const int nthreads) :
            rowsize(rowsize), row(row), deleteRow(deleteRow),
            nCopyFromFirst(nCopyFromFirst),
            nCopyFromSecond(nCopyFromSecond),
            nthreads(nthreads) {
                for (uint8_t i = 0; i < nCopyFromFirst; ++i) {
                    this->posFromFirst[i] = posFromFirst[i];
                }
                for (uint8_t i = 0; i < nCopyFromSecond; ++i) {
                    this->posFromSecond[i] = posFromSecond[i];
                }
#if USE_DUPLICATE_DETECTION
                rowCount = 0;
                rowsHash = NULL;
#endif
            }

#if DEBUG
        virtual void checkSizes() const {
        }
#endif

        virtual void processResults(const int blockid, const Term_t *first,
                FCInternalTableItr* second, const bool unique) = 0;

        virtual void processResults(const int blockid,
                const std::vector<const std::vector<Term_t> *> &vectors1, size_t i1,
                const std::vector<const std::vector<Term_t> *> &vectors2, size_t i2,
                const bool unique) = 0;

        virtual void processResults(std::vector<int> &blockid, Term_t *p, std::vector<bool> &unique, std::mutex *m) = 0;

        virtual void processResults(const int blockid, FCInternalTableItr *first,
                FCInternalTableItr* second, const bool unique) = 0;

        void processResults(const int blockid, const bool unique) {
            processResults(blockid, unique, NULL);
        }

        virtual void processResultsAtPos(const int blockid, const uint8_t pos,
                const Term_t v, const bool unique) = 0;

        virtual bool isBlockEmpty(const int blockId, const bool unique) const = 0;

        //virtual uint32_t getRowsInBlock(const int blockId, const bool unique) const = 0;

        virtual void addColumns(const int blockid,
                std::vector<std::shared_ptr<Column>> &columns,
                const bool unique, const bool sorted) = 0;

        virtual void addColumns(const int blockid, FCInternalTableItr *itr,
                const bool unique, const bool sorted,
                const bool lastInsert) = 0;

        virtual void addColumn(const int blockid, const uint8_t pos,
                std::shared_ptr<Column> column,
                const bool unique, const bool sorted) = 0;

        virtual bool isEmpty() const = 0;

        virtual uint8_t getRowSize() {
            return rowsize;
        }

        Term_t *getRawRow() {
            return row;
        }

        uint8_t getNCopyFromSecond() const {
            return nCopyFromSecond;
        }

        std::pair<uint8_t, uint8_t> *getPosFromSecond() {
            return posFromSecond;
        }

        uint8_t getNCopyFromFirst() const {
            return nCopyFromFirst;
        }

        const std::pair<uint8_t, uint8_t> *getPosFromFirst() const {
            return posFromFirst;
        }

        virtual void consolidate(const bool isFinished) {}

        virtual ~ResultJoinProcessor() {
            if (deleteRow)
                delete[] row;
#if USE_DUPLICATE_DETECTION
            if (rowsHash != NULL) {
                delete rowsHash;
            }
#endif
        }
};

#define SIZE_HASHCOUNT 100000
#define MAX_NSEGMENTS 3
class InterTableJoinProcessor: public ResultJoinProcessor {
    private:
        uint32_t currentSegmentSize;
        std::shared_ptr<SegmentInserter> *segments;

        std::shared_ptr<const FCInternalTable> table;

        void enlargeArray(const uint32_t blockid) {
            if (blockid >= currentSegmentSize) {
                std::shared_ptr<SegmentInserter> *newsegments =
                    new std::shared_ptr<SegmentInserter>[blockid + 1];
                for (uint32_t i = 0; i < currentSegmentSize; ++i) {
                    newsegments[i] = segments[i];
                }
                for (int i = currentSegmentSize; i < blockid + 1; ++i)
                    newsegments[i] = shared_ptr<SegmentInserter>
                        (new SegmentInserter(rowsize));
                currentSegmentSize = blockid + 1;
                delete[] segments;
                segments = newsegments;
            }
        }

#if USE_DUPLICATE_DETECTION
        void processResults(const int blockid, const bool unique, std::mutex *m);
#else
        void processResults(const int blockid, const bool unique, std::mutex *m) {
            enlargeArray(blockid);
            if (rowsize == 0) {
                LOG(DEBUGL) << "Added empty row!";
            }
            segments[blockid]->addRow(row, rowsize);
        }
#endif

    public:

#if DEBUG
        void checkSizes() const {
            for (int i = 0; i < currentSegmentSize; i++) {
                segments[i]->checkSizes();
            }
        }
#endif

        InterTableJoinProcessor(const uint8_t rowsize,
                std::vector<std::pair<uint8_t, uint8_t>> &posFromFirst,
                std::vector<std::pair<uint8_t, uint8_t>> &posFromSecond,
                const int nthreads);

        void processResults(std::vector<int> &blockid, Term_t *p, std::vector<bool> &unique, std::mutex *m);

        void processResults(const int blockid, const Term_t *first,
                FCInternalTableItr* second, const bool unique);

        void processResults(const int blockid, FCInternalTableItr *first,
                FCInternalTableItr* second, const bool unique);

        void processResults(const int blockid,
                const std::vector<const std::vector<Term_t> *> &vectors1, size_t i1,
                const std::vector<const std::vector<Term_t> *> &vectors2, size_t i2,
                const bool unique);

        void processResultsAtPos(const int blockid, const uint8_t pos,
                const Term_t v, const bool unique);

        void addColumns(const int blockid,
                std::vector<std::shared_ptr<Column>> &columns,
                const bool unique, const bool sorted);

        void addColumn(const int blockid, const uint8_t pos,
                std::shared_ptr<Column> column, const bool unique,
                const bool sorted);

        void addColumns(const int blockid, FCInternalTableItr *itr,
                const bool unique, const bool sorted,
                const bool lastInsert) {
            //Not supported
            throw 10;
        }

        void consolidate(const bool isFinished);

        bool isBlockEmpty(const int blockId, const bool unique) const;

        bool isEmpty() const;

        //uint32_t getRowsInBlock(const int blockId, const bool unique) const;

        std::shared_ptr<const FCInternalTable> getTable();

        ~InterTableJoinProcessor();
};

#endif
