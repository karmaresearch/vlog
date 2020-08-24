#ifndef _CHASE_MGMT_H
#define _CHASE_MGMT_H

#include <vlog/column.h>
#include <vlog/ruleexecdetails.h>

#include <vector>
#include <map>
#include <set>
#include <unordered_map>

#define SIZE_BLOCK 1000

#define RULE_MASK INT64_C(0xffffff0000000000)
#define RULE_SHIFT(x) (((uint64_t) ((x) + 1)) << 40)
#define GET_RULE(x) (((x) >> 40) - 1)
#define VAR_MASK INT64_C(0x0ff00000000)
#define VAR_SHIFT(v) ((uint64_t) (v) << 32)
#define GET_VAR(v) (((v) & VAR_MASK) >> 32)
#define RULEVARMASK (RULE_MASK|VAR_MASK)
#define COUNTER(v) (v & 0xFFFFFFFF)

typedef enum TypeChase {RESTRICTED_CHASE, SKOLEM_CHASE, SUM_CHASE, SUM_RESTRICTED_CHASE } TypeChase;

struct ChaseRow {
    uint8_t sz;
    uint64_t *row;

    ChaseRow() : sz(0), row(NULL) {}

    ChaseRow(const uint8_t s, uint64_t *r) : sz(s), row(r) {
    }

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
                std::vector<Var_t> nameArgVars;
                uint64_t currentcounter;
                std::vector<std::unique_ptr<uint64_t[]>> blocks;
                uint32_t blockCounter;
                uint64_t *currentblock;
                std::unordered_map<ChaseRow, uint64_t, hash_ChaseRow> rows;
                TypeChase typeChase;
                std::set<uint64_t> deps;    // For SUM chases.

            public:
                Rows(uint64_t startCounter, uint8_t sizerow,
                        std::vector<Var_t> nameArgVars,
                        TypeChase typeChase) :
                    startCounter(startCounter), sizerow(sizerow),
                    nameArgVars(nameArgVars) {
                        blockCounter = 0;
                        currentblock = NULL;
                        currentcounter = startCounter;
                        this->typeChase = typeChase;
                    }

                uint8_t getSizeRow() {
                    return sizerow;
                }

                uint64_t *getRow(size_t id);

                const std::vector<Var_t> &getNameArgVars() {
                    return nameArgVars;
                }

                uint64_t addRow(uint64_t* row);

                bool existingRow(uint64_t *row, uint64_t &value);

                bool checkRecursive(uint64_t target, uint64_t value,
                        std::set<uint64_t> &toCheck);
        };

        class RuleContainer {
            private:
                std::map<Var_t, std::vector<Var_t>> dependencies;
                std::map<Var_t, ChaseMgmt::Rows> vars2rows;
                uint64_t ruleBaseCounter;
                Rule const * rule;
                TypeChase chase;

            public:
                RuleContainer(uint64_t ruleBaseCounter,
                        std::map<Var_t, std::vector<Var_t>> dep,
                        Rule const * rule,
                        TypeChase chase) {
                    this->ruleBaseCounter = ruleBaseCounter;
                    dependencies = dep;
                    this->rule = rule;
                    this->chase = chase;
                }

                Rule const *getRule() {
                    return rule;
                }

                ChaseMgmt::Rows *getRows(Var_t var);
        };

        std::vector<std::unique_ptr<ChaseMgmt::RuleContainer>> rules;
        const TypeChase typeChase;
        const bool checkCyclic;

        const int ruleToCheck;
        bool cyclic;
        PredId_t predIgnoreBlock;

        bool checkSingle(uint64_t target, uint64_t rv, std::set<uint64_t> &toCheck);

        bool checkRecursive(uint64_t target, uint64_t rv);

    public:
        ChaseMgmt(std::vector<RuleExecutionDetails> &rules,
                const TypeChase typeChase, const bool checkCyclic,
                const int ruleToCheck = -1,
                const PredId_t predIgnoreBlocking = -1);

        std::shared_ptr<Column> getNewOrExistingIDs(
                uint32_t ruleid,
                Var_t var,
                std::vector<std::shared_ptr<Column>> &columns,
                uint64_t size);

        bool checkCyclicTerms(uint32_t ruleid);

        bool checkRecursive(uint64_t rv);

        uint64_t countDepth(uint64_t id, uint64_t depth = 0);

        RuleContainer *getRuleContainer(size_t id) const {
            if (id < rules.size()) {
                return rules[id].get();
            } else {
                return NULL;
            }
        }

        PredId_t getPredicateIgnoreBlocking() {
            return predIgnoreBlock;
        }

        bool isRestricted() {
            return typeChase == TypeChase::RESTRICTED_CHASE ||
                typeChase == TypeChase::SUM_RESTRICTED_CHASE;
        }

        TypeChase getChaseType() {
            return typeChase;
        }

        bool hasRuleToCheck() {
            return ruleToCheck >= 0;
        }

        int getRuleToCheck() {
            return ruleToCheck;
        }

        bool isCheckCyclicMode() {
            return checkCyclic;
        }
};

#endif
