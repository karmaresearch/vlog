#ifndef BINDINGS_TABLE_H
#define BINDINGS_TABLE_H

#include <vlog/concepts.h>
#include <trident/model/table.h>

#include <unordered_set>
#include <functional>

struct BindingsRow {
    uint8_t size;
    Term_t const * row;

    BindingsRow() : size(0), row(NULL) {}

    BindingsRow(const uint8_t s, Term_t const * const r) : size(s), row(r) {}

    bool operator==(const BindingsRow &other) const;
};

struct hash_BindingsRow {
    size_t operator()(const BindingsRow &x) const {
        size_t hash = 0;
        for (uint8_t i = 0; i < x.size; ++i) {
            hash = (hash + (324723947 + x.row[i])) ^93485734985;
        }
        return hash;
     }
};

class RawBindings {
    private:
        const uint8_t sizeArray;
        const size_t sizeFrag;
        std::vector<Term_t*> arrays;
        int arraysIdx;
        Term_t *array;
        size_t currentOffset;
    public:
        RawBindings(uint8_t sizeArray) : sizeArray(sizeArray), sizeFrag(sizeArray * 1000) {
            array = new Term_t[sizeFrag];
            arrays.push_back(array);
            currentOffset = 0;
            arraysIdx = 0;
        }

        Term_t *newRow() {
            if (currentOffset + sizeArray > sizeFrag) {
                arraysIdx++;
                Term_t *newArray = NULL;
                if (arraysIdx == arrays.size()) {
                    newArray = new Term_t[sizeFrag];
                    arrays.push_back(newArray);
                } else {
                    newArray = arrays[arraysIdx];
                }
                currentOffset = 0;
                array = newArray;
            }
            currentOffset += sizeArray;
            return array + currentOffset - sizeArray;
        }

        Term_t *getOffset(size_t idx) {
            int seg = idx / sizeFrag;
            int mod = idx % sizeFrag;
            return arrays[seg] + mod;
        }

        void clear() {
            currentOffset = 0;
            arraysIdx = 0;
            array = arrays[0];
        }

        ~RawBindings() {
            for (std::vector<Term_t*>::iterator itr = arrays.begin(); itr != arrays.end();
                    ++itr)
                delete[] *itr;
        }
};

class BindingsTable {
    private:
        std::unordered_set<BindingsRow, hash_BindingsRow> uniqueElements;
        RawBindings *rawBindings;
        Term_t *currentRow;

        size_t nPosToCopy;
        size_t *posToCopy;

        struct FieldsSorter {

            uint8_t fields[SIZETUPLE];
            const uint8_t nfields;

            FieldsSorter(std::vector<uint8_t> &f) : nfields((uint8_t) f.size()) {
                int i = 0;
                for (std::vector<uint8_t>::iterator itr = f.begin(); itr != f.end();
                        ++itr) {
                    this->fields[i++] = *itr;
                }
            }
            bool operator ()(const BindingsRow &i1, const BindingsRow &i2) const {
                for (uint8_t i = 0; i < nfields; ++i) {
                    if (i1.row[fields[i]] != i2.row[fields[i]])
                        return i1.row[fields[i]] < i2.row[fields[i]];
                }
                return false;
            }
        };

        void insertIfNotExists(Term_t const * const cr);
    public:
        BindingsTable(uint8_t sizeAdornment, uint8_t adornment);

        BindingsTable(size_t sizeTuple);

        BindingsTable(uint8_t sizeTuple, std::vector<int> posToCopy);

        void addTuple(const Literal *t);

#if ! TERM_IS_UINT64
        void addTuple(const uint64_t *t);
#endif

        void addTuple(const Term_t *t);

        void addTuple(const uint64_t *t1, const uint8_t sizeT1,
                const uint64_t *t2, const uint8_t sizeT2);

        void addTuple(const uint64_t *t, const uint8_t *posToCopy);

        void addRawTuple(Term_t *row);

        std::vector<Term_t> getProjection(std::vector<uint8_t> pos);

        std::vector<Term_t> getUniqueSortedProjection(std::vector<uint8_t> pos);

        size_t getNTuples();

        TupleTable *sortBy(std::vector<uint8_t> &fields);

        TupleTable *projectAndFilter(const Literal &l, const std::vector<uint8_t> *posToFilter,
                const std::vector<Term_t> *valuesToFilter);

        TupleTable *filter(const Literal &l, const std::vector<uint8_t> *posToFilter,
                const std::vector<Term_t> *valuesToFilter);

        size_t getSizeTuples() {
            return nPosToCopy;
        }
        size_t *getPosFromAdornment() {
            return posToCopy;
        }

        void print();

        Term_t const *getTuple(size_t idx);

        void clear();
#ifdef DEBUG
        void statistics();
#endif

        ~BindingsTable();
};

#endif
