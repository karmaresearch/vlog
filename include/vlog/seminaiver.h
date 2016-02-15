#ifndef _SEMINAIVER_H
#define _SEMINAIVER_H

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/fctable.h>
#include <trident/model/table.h>

#include <boost/chrono.hpp>
#include <vector>
#include <unordered_map>

namespace timens = boost::chrono;

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

    //The two functions above were written for a full materialization. As TODO
    //I need to remove them and replace them with the datastructurs below. They
    //do the roughly the same thing
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

    RuleExecutionPlan reorder(std::vector<uint8_t> &order,
                              const Literal &headLiteral) const;

    bool hasCartesian();
};

struct RuleExecutionDetails {
    const Rule rule;
    const size_t ruleid;
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

    RuleExecutionDetails(Rule rule, size_t ruleid) : rule(rule), ruleid(ruleid) {}

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
        return RuleExecutionDetails(other.rule, other.ruleid);
    }
};

struct StatsRule {
    size_t iteration;
    long derivation;
    int idRule;
    long timems;
    long totaltimems;
    StatsRule() : idRule(-1) {}
};

struct StatsSizeIDB {
    size_t iteration;
    int idRule;
    long derivation;
};

typedef std::unordered_map<std::string, FCTable*> EDBCache;
class ResultJoinProcessor;
class SemiNaiver {
private:
    std::vector<RuleExecutionDetails> ruleset;
    std::vector<RuleExecutionDetails> edbRuleset;
    std::vector<FCBlock> listDerivations;

    std::vector<StatsRule> statsRuleExecution;

    EDBLayer &layer;
    Program *program;
    bool opt_intersect;
    bool opt_filtering;
    boost::chrono::system_clock::time_point startTime;

    FCTable *predicatesTables[MAX_NPREDS];

    size_t iteration;
    long statsLastIteration;
    string currentRule;
    PredId_t currentPredicate;
    bool running;

    bool executeRule(RuleExecutionDetails &ruleDetails,
                     const uint32_t iteration);

    FCIterator getTableFromEDBLayer(const Literal & literal);

    size_t estimateCardTable(const Literal &literal, const size_t minIteration,
                             const size_t maxIteration);

    FCIterator getTableFromIDBLayer(const Literal & literal, const size_t minIteration, TableFilterer *filter);

    FCIterator getTableFromIDBLayer(const Literal & literal, const size_t minIteration,
                                    const size_t maxIteration, TableFilterer *filter);

    size_t countAllIDBs();

    bool checkIfAtomsAreEmpty(const RuleExecutionDetails &ruleDetails,
                              const RuleExecutionPlan &plan,
                              const Literal &headLiteral,
                              std::vector<size_t> &cards);

    void processRuleFirstAtom(const uint8_t nBodyLiterals,
                              const Literal *bodyLiteral,
                              const Literal &headLiteral,
                              const size_t min,
                              const size_t max,
                              int &processedTables,
                              const bool lastLiteral,
                              FCTable *endTable,
                              const uint32_t iteration,
                              const RuleExecutionDetails &ruleDetails,
                              const uint8_t orderExecution,
                              std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
                              ResultJoinProcessor *joinOutput);

    void reorderPlan(RuleExecutionPlan &plan,
                     const std::vector<size_t> &cards,
                     const Literal &headLiteral);

    //int getRuleID(const RuleExecutionDetails *rule);

public:
    SemiNaiver(std::vector<Rule> ruleset, EDBLayer &layer,
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

    void addDataToIDBRelation(const Predicate pred, FCBlock block);

    FCIterator getTable(const Literal &literal, const size_t minIteration,
                        const size_t maxIteration, TableFilterer *filter);

    EDBLayer &getEDBLayer() {
        return layer;
    }

    size_t estimateCardinality(const Literal &literal, const size_t min,
                               const size_t max);

    ~SemiNaiver();

    static std::pair<uint8_t, uint8_t> removePosConstants(
        std::pair<uint8_t, uint8_t> columns,
        const Literal &literal);

    //Statistics methods

    void printCountAllIDBs();

    size_t getCurrentIteration();

    string getCurrentRule();

    bool isRunning();

    std::vector<std::pair<string, std::vector<StatsSizeIDB>>> getSizeIDBs();

    std::vector<StatsRule> getOutputNewIterations();

    string getListAllRulesForJSONSerialization();

    boost::chrono::system_clock::time_point getStartingTimeMs() {
        return startTime;
    }

};

#endif
