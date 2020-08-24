#include <vlog/chasemgmt.h>

//************** ROWS ***************
uint64_t ChaseMgmt::Rows::addRow(uint64_t* row) {
    // LOG(TRACEL) << "Addrow: " << row[0];
    if (!currentblock || blockCounter >= SIZE_BLOCK) {
        //Create a new block
        std::unique_ptr<uint64_t[]> n =
            std::unique_ptr<uint64_t[]>(
                    new uint64_t[sizerow * SIZE_BLOCK]);
        currentblock = n.get();
        blocks.push_back(std::move(n));
        blockCounter = 0;
    }
    for(uint8_t i = 0; i < sizerow; ++i) {
        currentblock[i] = row[i];
    }
    ChaseRow r(sizerow, currentblock);
    rows[r] = currentcounter;
    currentblock += sizerow;
    blockCounter++;
    if (((uint32_t)currentcounter) == UINT32_MAX) {
        LOG(ERRORL) << "I can assign at most 2^32 new IDs to an ext. variable... Stop!";
        throw 10;
    }
    auto out = currentcounter;
    if (typeChase != TypeChase::SUM_CHASE && typeChase != TypeChase::SUM_RESTRICTED_CHASE) {
        currentcounter++;
    } else {
        for(uint8_t i = 0; i < sizerow; ++i) {
            if (row[i] & RULEVARMASK) {
                // LOG(DEBUGL) << "Adding dependency " << row[i] << ", current = " << out;
                deps.insert(row[i]);
            }
        }
    }
    // LOG(DEBUGL) << "addRow returns " << out;
    return out;
}

bool ChaseMgmt::Rows::existingRow(uint64_t *row, uint64_t &value) {
    ChaseRow r(sizerow, row);
    auto search = rows.find(r);
    if (search != rows.end()) {
        value = search->second;
        return true;
    }
    return false;
}

uint64_t *ChaseMgmt::Rows::getRow(size_t id) {
    uint64_t blocknr = id / SIZE_BLOCK;
    uint64_t offset = id % SIZE_BLOCK;
    auto &block_content = blocks[blocknr];
    return block_content.get() + offset * sizerow;
}

static bool checkValue(uint64_t target, uint64_t v, std::set<uint64_t> &toCheck) {
    // LOG(TRACEL) << "checkValue: target = " << target << ", v = " << v;
    if ((v & RULEVARMASK) == target) {
        // LOG(TRACEL) << "TRUE";
        return true;
    }
    if (v != 0) {
        toCheck.insert(v);
    }
    // LOG(TRACEL) << "FALSE";
    return false;
}

//************** END ROWS *************

//************** RULE CONTAINER ***************
ChaseMgmt::Rows *ChaseMgmt::RuleContainer::getRows(Var_t var) {
    if (!vars2rows.count(var)) {
        uint8_t sizerow = dependencies[var].size();
        uint64_t startCounter = ruleBaseCounter;
        // The variable is encoded in the space between the rule ID and the
        // counter ID.
        startCounter += VAR_SHIFT(var);
        vars2rows.insert(std::make_pair(var, Rows(startCounter, sizerow,
                        dependencies[var], chase)));
    }
    return &vars2rows.find(var)->second;
}

//************** END RULE CONTAINER *************

//************** CHASE MGMT ***************
ChaseMgmt::ChaseMgmt(std::vector<RuleExecutionDetails> &rules,
        const TypeChase typeChase, const bool checkCyclic,
        const int ruleToCheck, const PredId_t predIgnoreBlock) :
    typeChase(typeChase), checkCyclic(checkCyclic),
    ruleToCheck(ruleToCheck), cyclic(false),
    predIgnoreBlock(predIgnoreBlock) {
        this->rules.resize(rules.size());
        for(const auto &r : rules) {
            if (r.rule.getId() >= rules.size()) {
                LOG(ERRORL) << "Should not happen...";
                throw 10;
            }
            uint64_t ruleBaseCounter = RULE_SHIFT(r.rule.getId());
            this->rules[r.rule.getId()] = std::unique_ptr<ChaseMgmt::RuleContainer>(
                    new ChaseMgmt::RuleContainer(ruleBaseCounter,
                        r.orderExecutions[0].dependenciesExtVars,
                        &r.rule,
                        typeChase));
        }
    }

bool ChaseMgmt::Rows::checkRecursive(uint64_t target, uint64_t value, std::set<uint64_t> &toCheck) {
    if (typeChase == TypeChase::SUM_CHASE || typeChase == TypeChase::SUM_RESTRICTED_CHASE) {
        for (auto v : deps) {
            if (checkValue(target, v, toCheck)) {
                return true;
            }
        }
    }
    else {
        // Find the right block to check
        size_t blockNo = value / SIZE_BLOCK;
        size_t offset = (value % SIZE_BLOCK) * sizerow;
        auto block = blocks[blockNo].get();
        for (size_t i = offset; i < offset + sizerow; i++) {
            if (checkValue(target, block[i], toCheck)) {
                return true;
            }
        }
    }
    return false;
}

bool ChaseMgmt::checkSingle(uint64_t target, uint64_t rv, std::set<uint64_t> &toCheck) {
    auto &ruleContainer = rules[GET_RULE(rv)];
    uint8_t var = GET_VAR(rv);
    uint64_t value = rv & ~RULEVARMASK;

    // Enter values
    auto rows = ruleContainer->getRows(var);

    return rows->checkRecursive(target, value, toCheck);
}

bool ChaseMgmt::checkRecursive(uint64_t target, uint64_t rv) {

    std::set<uint64_t> toCheck;

    if (checkSingle(target, rv, toCheck)) {
        return true;
    }

    std::set<uint64_t> moreChecks;

    while (toCheck.size() > 0) {
        moreChecks.clear();
        // Investigate nested terms.
        for (uint64_t v : toCheck) {
            uint64_t mask = v & RULEVARMASK;
            if (mask == 0) {
                continue;
            }
            if (checkSingle(target, v, moreChecks)) {
                return true;
            }
        }
        for (uint64_t v : toCheck) {
            moreChecks.erase(v);
        }
        toCheck.swap(moreChecks);
    }
    return false;
}

// Check if rv is recursive.
bool ChaseMgmt::checkRecursive(uint64_t rv) {
    uint64_t mask = rv & RULEVARMASK;
    if (mask == 0) {
        return false;
    }
    return checkRecursive(mask, rv);
}

std::shared_ptr<Column> ChaseMgmt::getNewOrExistingIDs(
        uint32_t ruleid,
        Var_t var,
        std::vector<std::shared_ptr<Column>> &columns,
        uint64_t sizecolumns) {
    assert(sizecolumns > 0);
    auto &ruleContainer = rules[ruleid];
    auto rows = ruleContainer->getRows(var);
    const uint8_t sizerow = rows->getSizeRow();
    assert(sizerow == columns.size());
    std::vector<Term_t> functerms;
    uint64_t row[256];

    std::vector<std::unique_ptr<ColumnReader>> readers;
    for(uint8_t j = 0; j < sizerow; ++j) {
        readers.push_back(columns[j]->getReader());
    }
    uint64_t rulevar = RULE_SHIFT(ruleid) + VAR_SHIFT(var);
    for(uint64_t i = 0; i < sizecolumns; ++i) {
        for(uint8_t j = 0; j < sizerow; ++j) {
            if (!readers[j]->hasNext()) {
                LOG(ERRORL) << "Should not happen ...";
                throw 10;
            }
            row[j] = readers[j]->next();
            if (checkCyclic) {
                if ((ruleToCheck < 0 || ruleToCheck == ruleid) && ! cyclic) {
                    // Check if we are about to introduce a cyclic term ...
                    if ((row[j] & RULEVARMASK) != 0) {
                        LOG(TRACEL) << "to check: " << rulevar << ", read value " << row[j];
                        if ((row[j] & RULEVARMASK) == rulevar) {
                            cyclic = true;
                        } else {
                            cyclic = checkRecursive(rulevar, row[j]);
                        }
                        // LOG(TRACEL) << "cyclic = " << cyclic;
                    }
                }
            }
        }
        LOG(TRACEL) << "check: ruleid = " << ruleid << ", row[0] = " << row[0];
        uint64_t value = 0;
        if (!rows->existingRow(row, value)) {
            value = rows->addRow(row);
        }
        functerms.push_back(value);
    }
    return ColumnWriter::getColumn(functerms, false);
}

bool ChaseMgmt::checkCyclicTerms(uint32_t ruleid) {
    return cyclic;
}

uint64_t ChaseMgmt::countDepth(uint64_t id, uint64_t depth) {
    if ((id & RULEVARMASK) != 0) {
        auto &ruleContainer = rules[GET_RULE(id)];
        uint8_t var = GET_VAR(id);
        auto rows = ruleContainer->getRows(var);
        uint64_t value = id & ~RULEVARMASK;
        uint64_t *row = rows->getRow(value);
        uint64_t depthChildren = 0;
        for(uint64_t i = 0; i < rows->getSizeRow(); ++i) {
            uint64_t depthChild = countDepth(row[i]);
            if (depthChild > depthChildren) {
                depthChildren = depthChild;
            }
        }
        return 1 + depthChildren;
    }
    return depth;
}

//************** END CHASE MGMT ************
