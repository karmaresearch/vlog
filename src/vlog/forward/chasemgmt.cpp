#include <vlog/chasemgmt.h>

//************** ROWS ***************
uint64_t ChaseMgmt::Rows::addRow(uint64_t* row) {
    if (! restricted) {
	if (!currentblock || blockCounter >= SIZE_BLOCK) {
	    //Create a new block
	    std::unique_ptr<uint64_t> n =
		std::unique_ptr<uint64_t>(
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
    }
    if (startCounter == UINT32_MAX) {
        LOG(ERRORL) << "I can assign at most 2^32 new IDs to an ext. variable... Stop!";
        throw 10;
    }
    return currentcounter++;
}

bool ChaseMgmt::Rows::existingRow(uint64_t *row, uint64_t &value) {
    //Linear search among the blocks. If I find one equivalent to row, then I add
    //its ID and return true
    //    uint64_t i = startCounter;
    //    for(auto &b : blocks) {
    //        uint64_t *start = b.get();
    //        uint64_t *end = start + sizerow * SIZE_BLOCK;
    //        while (i < currentcounter && start < end) {
    //            //Compare a row
    //            bool found = true;
    //            for(uint32_t j = 0; j < sizerow; ++j) {
    //                if (start[j] != row[j]) {
    //                    found = false;
    //                    break;
    //                }
    //            }
    //            if (found) {
    //                value = i;
    //                return true;
    //            }
    //            start += sizerow;
    //            i++;
    //        }
    //    }
    //    return false;
    ChaseRow r(sizerow, row);
    auto search = rows.find(r);
    if (search != rows.end()) {
        value = search->second;
        return true;
    }
    return false;
}
//************** END ROWS *************

//************** RULE CONTAINER ***************
ChaseMgmt::Rows *ChaseMgmt::RuleContainer::getRows(uint8_t var, bool restricted) {
    if (!vars2rows.count(var)) {
        uint8_t sizerow = dependencies[var].size();
        uint64_t startCounter = ruleBaseCounter;
        // The variable is encoded in the space between the rule ID and the
        // counter ID.
        startCounter += (uint64_t) var << 32;
        vars2rows.insert(std::make_pair(var, Rows(startCounter, sizerow, restricted)));
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
        uint64_t ruleBaseCounter = (uint64_t) (r.rule.getId()+1) << 40;
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
    auto rows = ruleContainer->getRows(var, restricted);
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
        if (restricted || !rows->existingRow(row, value))
            value = rows->addRow(row);
        functerms.push_back(value);
    }
    return ColumnWriter::getColumn(functerms, false);
}
//************** END CHASE MGMT ************
