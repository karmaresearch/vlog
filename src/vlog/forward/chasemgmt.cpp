#include <vlog/chasemgmt.h>

//************** ROWS ***************
uint64_t ChaseMgmt::Rows::addRow(uint64_t* row) {
    if (!currentblock || currentcounter >= SIZE_BLOCK) {
        //Create a new block
        std::unique_ptr<uint64_t> n =
            std::unique_ptr<uint64_t>(
                    new uint64_t[sizerow * SIZE_BLOCK]);
        currentblock = n.get();
        blocks.push_back(std::move(n));
        currentcounter = 0;
    }
    for(uint8_t i = 0; i < sizerow; ++i) {
        currentblock[i] = row[i];
    }
    currentblock += sizerow;
    currentcounter++;
    return startCounter++;
}
//************** END ROWS *************

//************** RULE CONTAINER ***************
ChaseMgmt::Rows *ChaseMgmt::RuleContainer::getRows(uint8_t var) {
    if (!vars2rows.count(var)) {
        uint8_t sizerow = dependencies[var].size();
        uint64_t startCounter = ruleBaseCounter;
        startCounter += (uint64_t) var << 32; // The variable is encoded in the space between the rule ID and the counter ID.
        vars2rows.insert(std::make_pair(var, Rows(startCounter, sizerow)));
    }
    return &vars2rows.find(var)->second;
}
//************** END RULE CONTAINER *************

//************** CHASE MGMT ***************
ChaseMgmt::ChaseMgmt(std::vector<RuleExecutionDetails> &rules,
        const bool restricted) : restricted(restricted) {
    this->rules.resize(rules.size());
    for(const auto &r : rules) {
        if (r.rule.getId() >= rules.size()) {
            LOG(ERRORL) << "Should not happen...";
            throw 10;
        }
        uint64_t ruleBaseCounter = (uint64_t) r.rule.getId() << 40;
        this->rules[r.rule.getId()] = std::unique_ptr<ChaseMgmt::RuleContainer>(
                new ChaseMgmt::RuleContainer(ruleBaseCounter,
                    r.orderExecutions[0].dependenciesExtVars));
    }
}

std::shared_ptr<Column> ChaseMgmt::getNewOrExistingIDs(
        uint32_t ruleid,
        uint8_t var,
        std::vector<std::shared_ptr<Column>> &columns,
        uint64_t sizecolumns) {
    assert(sizecolumns > 0);
    auto &ruleContainer = rules[ruleid];
    auto rows = ruleContainer->getRows(var);
    const uint8_t sizerow = rows->getSizeRow();
    assert(sizerow == columns.size());
    std::vector<Term_t> functerms;
    uint64_t row[128];
    assert(sizerow <= 128);

    std::vector<std::unique_ptr<ColumnReader>> readers;
    for(uint8_t j = 0; j < sizerow; ++j) {
        readers.push_back(columns[j]->getReader());
    }
    for(uint64_t i = 0; i < sizecolumns; ++i) {
        for(uint8_t j = 0; j < sizerow; ++j) {
            if (!readers[j]->hasNext()) {
                LOG(ERRORL) << "Should not happen ...";
                throw 10;
            }
            row[j] = readers[j]->next();
        }
        uint64_t value;
        if (restricted || !existingRow(row, value))
            value = rows->addRow(row);
        functerms.push_back(value);
    }
    return ColumnWriter::getColumn(functerms, false);
}

bool ChaseMgmt::existingRow(uint64_t *row, uint64_t &value) {
    return false; //TODO
}
//************** END CHASE MGMT ************
