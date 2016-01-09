/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Vlog.

   Vlog is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Vlog is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Vlog.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef _SEMINAIVER_H
#define _SEMINAIVER_H

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/fctable.h>
#include <trident/model/table.h>
#include <trident/kb/dictmgmt.h>

#include <vector>
#include <unordered_map>

struct RuleExecutionPlan {

    std::vector<const Literal*> plan;
    std::vector<std::pair<size_t, size_t>> ranges;

    //This variable tells whether the last literal shares some values with the
    //head. This allows us to group the input to avoid duplicates.
    bool lastLiteralSharesWithHead;
    std::vector<uint8_t> lastSorting; //If the previous var is set, this
    //container tells which variables we should sort on.

    //These three variables record whether we can use the values of the last
    //IDB literal to collect tuples to filter out duplicates. If the last literal
    //is more generic than the head, then the position of the constants allow us to
    //filter it
    bool lastLiteralSubsumesHead;
    std::vector<Term_t> lastLiteralValueConstsInHead;
    std::vector<uint8_t> lastLiteralPosConstsInHead;

    //This variable is used to check whether we can filter out entries in the hashmap.
    //It is used if the rule might trigger derivation that is equal to the last literal
    bool filterLastHashMap;

    //Check if we can apply filtering HashMap. See comment above
    void checkIfFilteringHashMapIsPossible(const Literal &head);

    struct MatchVariables {
        uint8_t posLiteralInOrder;
        std::vector<std::pair<uint8_t, uint8_t>> matches;
    };
    std::vector<MatchVariables> matches;

    //When I execute the joins, the following variables contain the size of the
    //intermediate tuples,
    //and all the positions to join and copy the results
    std::vector<uint8_t> sizeOutputRelation;
    std::vector<std::vector<std::pair<uint8_t, uint8_t>>> joinCoordinates;
    std::vector<std::vector<std::pair<uint8_t, uint8_t>>> posFromFirst;
    std::vector<std::vector<std::pair<uint8_t, uint8_t>>> posFromSecond;

    void calculateJoinsCoordinates(const Literal &headLiteral);

    RuleExecutionPlan reorder(std::vector<uint8_t> &order, const Literal &headLiteral);

};

struct RuleExecutionDetails {
    const Rule rule;
    std::vector<Literal> bodyLiterals;
    uint32_t lastExecution = 0;

    bool failedBecauseEmpty = false;
    const Literal *atomFailure = NULL;

    uint8_t nIDBs = 0;
    std::vector<RuleExecutionPlan> orderExecutions;

    std::vector<uint8_t> posEDBVarsInHead;
    std::vector<std::vector<std::pair<uint8_t, uint8_t>>> occEDBVarsInHead;
    std::vector<std::pair<uint8_t,
        std::vector<std::pair<uint8_t, uint8_t>>>> edbLiteralPerHeadVars;

    RuleExecutionDetails(Rule rule) : rule(rule) {}

    void createExecutionPlans();

    void calculateNVarsInHeadFromEDB();

    static void checkWhetherEDBsRedundantHead(RuleExecutionPlan &plan, const Literal &head);

    static void checkFilteringStrategy(RuleExecutionPlan &outputPlan, const Literal &lastLiteral, const Literal &head);

private:

    void rearrangeLiterals(std::vector<const Literal*> &vector, const size_t idx);

    void groupLiteralsBySharedVariables(std::vector<uint8_t> &startVars,
                                        std::vector<const Literal *> &set, std::vector<const Literal*> &leftelements);

    void extractAllEDBPatterns(std::vector<const Literal*> &output, const std::vector<Literal> &input);

    RuleExecutionDetails operator=(const RuleExecutionDetails &other) {
        return RuleExecutionDetails(other.rule);
    }
};

typedef std::unordered_map<std::string, FCTable*> EDBCache;

//#define MAX_IDBS 8

class SemiNaiver {
private:
    std::vector<RuleExecutionDetails> ruleset;
    std::vector<RuleExecutionDetails> edbRuleset;
    std::vector<FCBlock> listDerivations;
    EDBLayer &layer;
    DictMgmt *dict;
    Program *program;
    bool opt_intersect;
    bool opt_filtering;

    FCTable *predicatesTables[MAX_NPREDS];

    bool executeRule(RuleExecutionDetails &ruleDetails,
                     const uint32_t iteration);

    FCIterator getTableFromEDBLayer(const Literal & literal);

    size_t estimateCardTable(const Literal &literal, const size_t minIteration,
                               const size_t maxIteration);

    FCIterator getTableFromIDBLayer(const Literal & literal, const size_t minIteration, TableFilterer *filter);

    FCIterator getTableFromIDBLayer(const Literal & literal, const size_t minIteration,
                                    const size_t maxIteration, TableFilterer *filter);

    size_t countAllIDBs();

public:
    SemiNaiver(std::vector<Rule> ruleset, EDBLayer &layer, DictMgmt *dict,
               Program *program, bool opt_intersect, bool opt_filtering);

    void run() {
        run(0, 1);
    }

    bool opt_filter() {
	return opt_filtering;
    }

    bool opt_inter() {
	return opt_intersect;
    }

    void run(size_t lastIteration, size_t iteration);

    void storeOnFiles(std::string path, const bool decompress,
		    const int minLevel);

    FCIterator getTable(const Literal &literal, const size_t minIteration,
                        const size_t maxIteration) {
        return getTable(literal, minIteration, maxIteration, NULL);
    }

    std::vector<FCBlock> &getDerivationsSoFar() {
        return listDerivations;
    }

    Program *getProgram() {
        return program;
    }

    DictMgmt *getDict() {
        return dict;
    }

    void addDataToIDBRelation(const Predicate pred, FCBlock block);

    FCIterator getTable(const Literal &literal, const size_t minIteration,
                        const size_t maxIteration, TableFilterer *filter);

    EDBLayer &getEDBLayer() {
        return layer;
    }

    size_t estimateCardinality(const Literal &literal, const size_t min,
                                 const size_t max);

    void printCountAllIDBs();

    ~SemiNaiver();

    static std::pair<uint8_t, uint8_t> removePosConstants(
        std::pair<uint8_t, uint8_t> columns,
        const Literal &literal);
};

#endif
