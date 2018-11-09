#include <vlog/chasemgmt.h>

#define RULE_MASK INT64_C(0xffffff0000000000)
#define RULE_SHIFT(x) (((uint64_t) ((x) + 1)) << 40)
#define GET_RULE(x) (((x) >> 40) - 1)
#define VAR_MASK INT64_C(0x0ff00000000)
#define VAR_SHIFT(v) ((uint64_t) (v) << 32)
#define GET_VAR(v) (((v) & VAR_MASK) >> 32)
#define RULEVARMASK (RULE_MASK|VAR_MASK)

//************** ROWS ***************
uint64_t ChaseMgmt::Rows::addRow(uint64_t* row) {
    LOG(TRACEL) << "Addrow: " << row[0];
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
    if (startCounter == UINT32_MAX) {
        LOG(ERRORL) << "I can assign at most 2^32 new IDs to an ext. variable... Stop!";
        throw 10;
    }
    return currentcounter++;
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
//************** END ROWS *************

//************** RULE CONTAINER ***************
ChaseMgmt::Rows *ChaseMgmt::RuleContainer::getRows(uint8_t var) {
    if (!vars2rows.count(var)) {
        uint8_t sizerow = dependencies[var].size();
        uint64_t startCounter = ruleBaseCounter;
        // The variable is encoded in the space between the rule ID and the
        // counter ID.
        startCounter += VAR_SHIFT(var);
        vars2rows.insert(std::make_pair(var, Rows(startCounter, sizerow)));
    }
    return &vars2rows.find(var)->second;
}

bool ChaseMgmt::RuleContainer::containsCyclicTerms() {
    for (auto &p : vars2rows) {
        if (p.second.containsCyclicTerms())
            return true;
    }
    return false;
}
//************** END RULE CONTAINER *************

//************** CHASE MGMT ***************
ChaseMgmt::ChaseMgmt(std::vector<RuleExecutionDetails> &rules,
        const bool restricted, const bool checkCyclic) : restricted(restricted), checkCyclic(checkCyclic), cyclic(false) {
    this->rules.resize(rules.size());
    for(const auto &r : rules) {
        if (r.rule.getId() >= rules.size()) {
            LOG(ERRORL) << "Should not happen...";
            throw 10;
        }
        uint64_t ruleBaseCounter = RULE_SHIFT(r.rule.getId());
        this->rules[r.rule.getId()] = std::unique_ptr<ChaseMgmt::RuleContainer>(
                new ChaseMgmt::RuleContainer(ruleBaseCounter,
                    r.orderExecutions[0].dependenciesExtVars));
    }
}

static bool checkValue(uint64_t target, uint64_t v, std::vector<uint64_t> &toCheck) {
    LOG(TRACEL) << "checkValue: target = " << target << ", v = " << v;
    if (v == target) {
	return true;
    }
    if (v != 0) {
	bool found = false;
	for (auto const& value: toCheck) {
	    if (value == v) {
		found = true;
		break;
	    }
	}
	if (! found) {
	    toCheck.push_back(v);
	}
    }
    return false;
}

bool ChaseMgmt::Rows::checkRecursive(uint64_t target, std::vector<uint64_t> &toCheck) {

    const uint8_t sizerow = getSizeRow();

    // Potentially expensive operation: we need to check all rows of this container.
    // Let's do this breadth-first
    // First check the complete blocks
    // But keep track of nested terms to investigate.
    
    for (int i = 0; i < blocks.size() - 1; i++) {
	auto block = blocks[i].get();
	for (size_t j = 0; j < SIZE_BLOCK * sizerow; j++) {
	    if (checkValue(target, block[j] & RULEVARMASK, toCheck)) {
		return true;
	    }
	}
    }
    // Now check the last block
    auto block = blocks[blocks.size() - 1].get();
    while (block < currentblock) {
	if (checkValue(target, *block & RULEVARMASK, toCheck)) {
	    return true;
	}
	block++;
    }
    return false;
    
}

bool ChaseMgmt::checkRecursive(uint64_t target, uint64_t rv, std::vector<uint64_t> &checked) {
    if (rv == target) {
	LOG(DEBUGL) << "Found an immediate cycle!";
	return true;
    }
    uint64_t mask = rv & RULEVARMASK;
    for (auto const& value: checked) {
	if (value == mask) {
	    // already checked or being checked
	    return false;
	}
    }
    checked.push_back(rv);

    //Recursive check required.
    auto &ruleContainer = rules[GET_RULE(rv)];
    uint8_t var = GET_VAR(rv);
    // Enter values 
    auto rows = ruleContainer->getRows(var);
    std::vector<uint64_t> toCheck;

    if (rows->checkRecursive(target, toCheck)) {
	LOG(DEBUGL) << "Found a direct cycle!";
	return true;
    }

    // Investigate nested terms.
    for (uint64_t v : toCheck) {
	if (checkRecursive(target, v, checked)) {
	    LOG(DEBUGL) << "Found an indirect cycle!";
	    return true;
	}
    }
    return false;

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
    std::vector<uint64_t> checked;

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
	    if (checkCyclic && ! cyclic) {
		uint64_t rv = (row[j] & RULEVARMASK);
		LOG(TRACEL) << "to check: " << rulevar << ", read value " << rv;
		if (rv != 0) {
		    cyclic = checkRecursive(rulevar, rv, checked);
		}
	    }
	}
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
//************** END CHASE MGMT ************
