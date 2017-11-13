#ifndef _RULE_EXECUTION_DETAILS_H
#define _RULE_EXECUTION_DETAILS_H

#include <vlog/concepts.h>
#include <vlog/ruleexecplan.h>

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

    //static void checkWhetherEDBsRedundantHead(RuleExecutionPlan &plan,
    //        const Literal &head);

    static void checkFilteringStrategy(const Literal &lastLiteral,
            const Literal &head, RuleExecutionPlan &hv);

    private:

    void rearrangeLiterals(std::vector<const Literal*> &vector, const size_t idx);

    void groupLiteralsBySharedVariables(std::vector<uint8_t> &startVars,
            std::vector<const Literal *> &set,
            std::vector<const Literal*> &leftelements);

    void extractAllEDBPatterns(std::vector<const Literal*> &output,
            const std::vector<Literal> &input);
};


#endif
