#ifndef QSQEVALUATION_H
#define QSQEVALUATION_H

// #define RECURSIVE_QSQR

#include <vlog/qsqquery.h>
#include <vlog/concepts.h>
#include <vlog/bindingstable.h>
#include <vlog/reasoner.h>
//#include <vlog/ruleexecutor.h>
#include <vlog/edb.h>

#include <kognac/utils.h>

#include <trident/model/table.h>

#include <vector>

class TupleTable;
class RuleExecutor;

#define QSQR_EVAL 0
#define QSQR_EST 1

#ifndef RECURSIVE_QSQR

class QSQR;

enum QSQR_TaskType { RULE, QUERY, RULE_QUERY}; //Execute rule or query
struct QSQR_Task {
    const QSQR_TaskType type;

    //To execute a query I need to know:
    Predicate pred;
    BindingsTable *inputTable;
    size_t offsetInput;
    bool repeat;
    int currentRuleIndex;
    size_t totalAnswers;

    //To execute a rule I need to know:
    BindingsTable **supplRelations;
    //currentRuleIndex stores the atom that should be executed
    QSQR *qsqr;
    EDBLayer *layer;
    RuleExecutor *executor;


    QSQR_Task(QSQR_TaskType type) : type(type), pred(0, 0, 0, 0), repeat(false) {}

    QSQR_Task(QSQR_TaskType type, Predicate &p) : type(type), pred(p), repeat(false) {}
};
#endif

class QSQR {
private:
    EDBLayer &layer;
    Program *program;

    //Store all the inputs used during the computation
    std::vector<uint8_t> sizePreds;
    std::vector<BindingsTable **>inputs;
    std::vector<BindingsTable **>answers;
    std::vector<RuleExecutor ***>rules;

    //const Timeout * timeout;

#ifndef RECURSIVE_QSQR
    std::vector<QSQR_Task> tasks;
    void processTask(QSQR_Task &task);
#endif


    size_t calculateAllAnswers();

    void createRules(Predicate &pred);

public:
    QSQR(EDBLayer &layer, Program *program) : layer(layer), program(program) {
        int nPreds = program->getNPredicates();
        sizePreds.resize(nPreds);
        inputs.resize(nPreds);
        answers.resize(nPreds);
        rules.resize(nPreds);
    }

    /*void raiseIfExpired() {
        if (timeout != NULL)
            timeout->raiseIfExpired();
    }*/

#ifndef RECURSIVE_QSQR
    void pushTask(QSQR_Task &task) {
        tasks.push_back(task);
    }
#endif

    void setProgram(Program *program) {
        this->program = program;
    }

    void deallocateAllRules();

    void cleanAllInputs();

    //Get input for specific query
    BindingsTable *getInputTable(const Predicate pred);

    //Get answers for specific query
    BindingsTable *getAnswerTable(const Predicate pred, uint8_t adornment);

    BindingsTable *getAnswerTable(const Literal *literal) {
        return getAnswerTable(literal->getPredicate(), literal->getPredicate().getAdornment());
    }

    void estimateQuery(Metrics &metrics, int depth, Literal &l, std::vector<uint32_t> &execRules, vector<uint32_t> & idbs);

    void estimateRule(Metrics &metrics, int depth, Rule &rule, vector<Substitution>& subs, int nsubs, std::vector<uint32_t> &execRules, vector<uint32_t> & idbs);

    size_t estimate(int depth, Predicate &pred, BindingsTable *inputTable/*, size_t offsetInput*/);

#ifdef LINEAGE
    TupleTable *evaluateQuery(QSQQuery *query,
                              std::vector<uint8_t> *posJoins,
                              std::vector<Term_t> *possibleValuesJoins,
                              bool returnOnlyVars) {
        std::vector<LineageInfo> info;
        return evaluateQuery(QSQR_EVAL, query, posJoins, possibleValuesJoins, returnOnlyVars, info);
    }
#endif

    TupleTable *evaluateQuery(int evaluateOrEstimate, QSQQuery *query,
                              std::vector<uint8_t> *posJoins,
                              std::vector<Term_t> *possibleValuesJoins,
                              bool returnOnlyVars);

    void evaluate(Predicate &pred, BindingsTable *inputTable,
                  size_t offsetInput) {
        evaluate(pred, inputTable, offsetInput, true);
    }

    void evaluate(Predicate &pred, BindingsTable *inputTable,
                  size_t offsetInput, bool repeat);

    ~QSQR();
};

#endif
