#ifndef _FORWARDCHAININGTABLE_H
#define _FORWARDCHAININGTABLE_H

#include <trident/model/table.h>
#include <vlog/concepts.h>
#include <vlog/fcinttable.h>

#include <inttypes.h>
#include <string>
#include <unordered_map>

struct RuleExecutionDetails;
class FCTable;
class TableFilterer;

struct FCRow {
    const Term_t *row;
    const size_t iteration;
    FCRow(const Term_t *row, const size_t iteration) : row(row), iteration(iteration) {}
};

struct FCBlock {
    size_t iteration;
    std::shared_ptr<const FCInternalTable> table;

    Literal query;
    const RuleExecutionDetails *rule;
    const uint8_t ruleExecOrder;

    bool isCompleted;

    FCBlock(size_t iteration, std::shared_ptr<const FCInternalTable> table, Literal query, const RuleExecutionDetails *rule,
            const uint8_t ruleExecOrder, bool isCompleted) : iteration(iteration),
        table(table), query(query), rule(rule), ruleExecOrder(ruleExecOrder), isCompleted(isCompleted) {
    }

    FCBlock operator=(const FCBlock &other) {
        return FCBlock(other.iteration, other.table, other.query, other.rule, other.ruleExecOrder, other.isCompleted);
    }
};

struct FCCacheBlock {
    std::shared_ptr<FCTable> table;
    size_t begin, end;
};

class FCIterator {
private:
    const size_t ntables;

    std::vector<FCBlock>::const_iterator itr, end;
public:
    FCIterator() : ntables(0) {
        itr = end;
    }

    FCIterator(std::vector<FCBlock>::const_iterator itr,
               std::vector<FCBlock>::const_iterator end);

    size_t getNTables();

    bool isEmpty() const;

    std::shared_ptr<const FCInternalTable> getCurrentTable() const;

    size_t getCurrentIteration() const;

    const FCBlock *getCurrentBlock() const;

    const RuleExecutionDetails *getRule() const;

    void moveNextCount();
};

typedef std::unordered_map<std::string, FCCacheBlock, std::hash<std::string>, std::equal_to<std::string>> FCCache;

class FCTable {
private:
    const uint8_t sizeRow;

    std::vector<FCBlock> blocks;

    FCCache cache;
    std::string getSignature(const Literal &literal);

    void removeBlock(const size_t iteration);
public:
    FCTable(const uint8_t sizeRow);

    std::shared_ptr<const FCTable> filter(const Literal &literal) {
        return filter(literal, 0, NULL);
    }

    size_t getMaxIteration() const {
        if (blocks.size() == 0) {
            return 0;
        } else {
            return blocks.back().iteration;
        }
    }

    size_t getMinIteration() const {
        if (blocks.size() == 0) {
            return 0;
        } else {
            return blocks.front().iteration;
        }
    }


    std::shared_ptr<const FCTable> filter(const Literal &literal,
                                          const size_t minIteration, TableFilterer *filterer);

    FCIterator read(const size_t mincount) const;

    FCIterator read(const size_t mincount, const size_t maxcount) const;

    size_t estimateCardInRange(const size_t mincount,
                               const size_t maxcount) const;

    size_t getNAllRows() const;

    size_t getNRows(const size_t iteration) const;

    bool isEmpty() const;

    bool isEmpty(size_t count) const;

    FCBlock &getLastBlock();

    //size_t getNRows(size_t count) const;

    size_t estimateCardinality(const Literal &literal, const size_t min, const size_t max) const;

    uint8_t getSizeRow() const {
        return sizeRow;
    }

    std::shared_ptr<const Segment> retainFrom(
        std::shared_ptr<const Segment> t,
        const bool dupl) const;

    void addBlock(FCBlock block);

    bool add(std::shared_ptr<const FCInternalTable> t, const Literal &literal, const RuleExecutionDetails *detailsRule,
             const uint8_t ruleExecOrder,
             const size_t iteration, const bool isCompleted);

    ~FCTable();
};

#endif
