#ifndef _CHASE_MGMT_H
#define _CHASE_MGMT_H

#include <vlog/column.h>
#include <vlog/ruleexecdetails.h>

#include <vector>
#include <map>

#define SIZE_BLOCK 1000

class ChaseMgmt {
    private:
        class Rows {
            private:
                uint64_t startCounter;
                const uint8_t sizerow;
                std::vector<std::unique_ptr<uint64_t>> blocks;
                uint32_t currentcounter;
                uint64_t *currentblock;

            public:
                Rows(uint32_t startCounter, uint8_t sizerow) :
                    startCounter(startCounter), sizerow(sizerow) {
                    currentcounter = 0;
                    currentblock = NULL;
                }

                uint8_t getSizeRow() {
                    return sizerow;
                }

                uint64_t addRow(uint64_t* row);
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

                ChaseMgmt::Rows *getRows(uint8_t var);
        };

        std::vector<std::unique_ptr<ChaseMgmt::RuleContainer>> rules;
        const bool restricted;

        bool existingRow(uint64_t *row, uint64_t &value);

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
