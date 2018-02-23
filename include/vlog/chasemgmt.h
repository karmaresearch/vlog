#ifndef _CHASE_MGMT_H
#define _CHASE_MGMT_H

#include <vlog/column.h>
#include <vlog/ruleexecdetails.h>

#include <vector>
#include <map>
#include <unordered_map>

#define SIZE_BLOCK 1000

struct ChaseRow {
    uint8_t sz;
    uint64_t *row;

    ChaseRow() : sz(0), row(NULL) {}

    ChaseRow(const uint8_t s, uint64_t *r) : sz(s), row(r) {}

    bool operator == (const ChaseRow &other) const {
        for (int i = 0; i < sz; i++) {
            if (row[i] != other.row[i]) {
                return false;
            }
        }
        return true;
    }
};

struct hash_ChaseRow {
    size_t operator() (const ChaseRow &x) const {
        uint64_t result = 0;
        for (int i = 0; i < x.sz; i++) {
	    result = (result + (324723947 + x.row[i])) ^93485734985;
        }
        return (size_t) result;
    }
};

class ChaseMgmt {
    private:
        class Rows {
            private:
                const uint64_t startCounter;
                const uint8_t sizerow;
		const bool restricted;
                uint64_t currentcounter;
                std::vector<std::unique_ptr<uint64_t>> blocks;
                uint32_t blockCounter;
                uint64_t *currentblock;
		std::unordered_map<ChaseRow, uint64_t, hash_ChaseRow> rows;

            public:
                Rows(uint64_t startCounter, uint8_t sizerow, bool restricted) :
                    startCounter(startCounter), sizerow(sizerow), restricted(restricted) {
                        blockCounter = 0;
                        currentblock = NULL;
                        currentcounter = startCounter;
                    }

                uint8_t getSizeRow() {
                    return sizerow;
                }

                uint64_t addRow(uint64_t* row);

                bool existingRow(uint64_t *row, uint64_t &value);
        };

        class RuleContainer {
            private:
                std::map<uint8_t, std::vector<uint8_t>> dependencies;
                std::map<uint8_t, ChaseMgmt::Rows> vars2rows;
                uint64_t ruleBaseCounter;
            public:
                RuleContainer(uint64_t ruleBaseCounter,
                        std::map<uint8_t, std::vector<uint8_t>> dep) {
                    this->ruleBaseCounter = ruleBaseCounter;
                    dependencies = dep;
                }

                ChaseMgmt::Rows *getRows(uint8_t var, bool restricted);
        };

        std::vector<std::unique_ptr<ChaseMgmt::RuleContainer>> rules;
        const bool restricted;


    public:
        ChaseMgmt(std::vector<RuleExecutionDetails> &rules,
                const bool restricted);

        std::shared_ptr<Column> getNewOrExistingIDs(
                uint32_t ruleid,
                uint8_t var,
                std::vector<std::shared_ptr<Column>> &columns,
                uint64_t size);

        bool isRestricted() {
            return restricted;
        }
};

#endif
