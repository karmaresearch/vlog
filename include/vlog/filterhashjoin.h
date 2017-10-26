#ifndef _FILTERHASHJOIN_H
#define _FILTERHASHJOIN_H

#include <vlog/resultjoinproc.h>
#include <vlog/joinprocessor.h>

#include <vector>

struct FilterHashJoinBlock {
    const FCInternalTable *table;
    uint32_t iteration;
};

struct FilterHashJoinSorter {
    const uint8_t nfields;
    uint8_t fields[3];

    FilterHashJoinSorter(const uint8_t s, const std::pair<uint8_t, uint8_t> *positions);

    bool operator() (const Term_t *r1, const Term_t *r2) const {
        for (uint8_t i = 0; i < nfields; ++i) {
            long diff = r1[fields[i]] - r2[fields[i]];
            if (diff < 0)
                return true;
            else if (diff > 0)
                return false;
        }
        return false;
    }
};

class FilterHashJoin {
private:
    ResultJoinProcessor *output;

    const JoinHashMap *map1;
    const DoubleJoinHashMap *map2;
    const std::vector<Term_t> *mapValues;
    const uint8_t mapRowSize;
    const uint8_t njoinfields;
    const uint8_t joinField1;
    const uint8_t joinField2;

    const uint8_t nValuesHead;
    const std::pair<uint8_t, uint8_t> *posValuesHead;
    //If I process the records row by row, then I need to variables ordered by head occurence, since I compare myself against that order. Therefore, I must reorder the original posValuesHead pairs
    std::vector<std::pair<uint8_t, uint8_t>> posValuesHeadRowEdition;
    const uint8_t nValuesHashHead;
    const std::pair<uint8_t, uint8_t> *posValuesHashHead;

    const Literal *literal;
    const bool isDerivationUnique;
    const bool literalSubsumesHead;

    const FilterHashJoinSorter sorter;
    std::vector<const Term_t*> matches;

    std::vector<DuplicateContainers> *existingTuples;

    const uint8_t nLastLiteralPosConstsInHead;
    const Term_t *lastLiteralValueConstsInHead;
    const uint8_t *lastLiteralPosConstsInHead;

    size_t processedElements;

    inline void doJoin_join(const Term_t* constantValues, const std::vector<Term_t> &joins1,
                            const std::vector<std::pair<Term_t, Term_t>> &joins2,
                            std::vector<Term_t> &otherVariablesContainer);

    inline void doJoin_cartprod(const Term_t *constantValues,
                                size_t start, const size_t end,
                                std::vector<Term_t> &otherVariablesContainer);

    void run_processitr_rowversion(FCInternalTableItr *itr, const bool cartprod,
                                   const size_t startCarprod, const size_t endCartprod,
                                   const uint8_t *posOtherVariables,
                                   const std::vector<std::pair<uint8_t, Term_t>> *valueColumnsToFilter,
                                   const std::vector<std::pair<uint8_t, uint8_t>> *columnsToFilterOut);

    void run_processitr_columnversion(FCInternalTableItr *itr,
                                      const size_t startCarprod, const size_t endCartprod,
                                      const uint8_t *posOtherVariables,
                                      const std::vector<std::pair<uint8_t, Term_t>> *valueColumnsToFilter,
                                      const std::vector<std::pair<uint8_t, uint8_t>> *columnsToFilterOut);

public:
    FilterHashJoin(ResultJoinProcessor *output, const JoinHashMap *map, const DoubleJoinHashMap *map2,
                   std::vector<Term_t> *mapValues,
                   const uint8_t mapRowSize, const uint8_t njoinfields,
                   const uint8_t joinField1, const uint8_t joinField2,
                   const Literal *literal, const bool isDerivationUnique, const bool literalSubsumesHead,
                   std::vector<DuplicateContainers> *existingTuples, const uint8_t nLastLiteralPosConstsInHead,
                   const Term_t *lastLiteralValueConstsInHead, const uint8_t *lastLiteralPosConstsInHead);

    void run(const std::vector<FilterHashJoinBlock> &tables, const bool cartprod,
             const size_t startCarprod, const size_t endCartprod,
             const std::vector<uint8_t> posToSort, int &processedTables,
             const std::vector<std::pair<uint8_t, Term_t>> *valueColumnsToFilter,
             const std::vector<std::pair<uint8_t, uint8_t>> *columnsToFilterOut);

    size_t getProcessedElements() {
        return processedElements;
    }
};

#endif
