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

#ifndef QSQEVALUATION_H
#define QSQEVALUATION_H

#include <vlog/qsqquery.h>
#include <vlog/concepts.h>
#include <vlog/bindingstable.h>
#include <vlog/ruleexecutor.h>
#include <vlog/edb.h>

#include <tridentcompr/utils/utils.h>

#include <trident/kb/querier.h>
#include <trident/kb/kb.h>
#include <trident/model/table.h>

#include <vector>

class TupleTable;
class DictMgmt;

#define QSQR_EVAL 0
#define QSQR_EST 1

enum QSQR_TaskType { RULE, QUERY, RULE_QUERY}; //Execute rule or query
struct QSQR_Task {
    const QSQR_TaskType type;

    //To execute a query I need to know:
    Predicate pred;
    BindingsTable *inputTable;
    size_t offsetInput;
    bool repeat;
    int currentRuleIndex;
    //bool shouldRepeat;
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

class QSQR {
private:
    EDBLayer &layer;
    Program *program;
    DictMgmt *dict;

    //Store all the inputs used during the computation
    uint8_t sizePreds[MAX_NPREDS];
    BindingsTable **inputs[MAX_NPREDS];
    BindingsTable **answers[MAX_NPREDS];
    RuleExecutor ***rules[MAX_NPREDS];

    //const Timeout * timeout;

    std::vector<QSQR_Task> tasks;


    size_t calculateAllAnswers();

    void createRules(Predicate &pred);

    void processTask(QSQR_Task &task);

public:
    QSQR(EDBLayer &layer, Program *program, DictMgmt *dict) : layer(layer),
        program(program), dict(dict) {
        for (int i = 0; i < MAX_NPREDS; ++i) {
            inputs[i] = NULL;
            answers[i] = NULL;
            rules[i] = NULL;
        }
        //timeout = NULL;
    }

    /*void raiseIfExpired() {
        if (timeout != NULL)
            timeout->raiseIfExpired();
    }*/

    void pushTask(QSQR_Task &task) {
        tasks.push_back(task);
    }

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
        return getAnswerTable(literal->getPredicate(), literal->getPredicate().getAdorment());
    }

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
