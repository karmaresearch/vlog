#include <vlog/qsqr.h>
#include <vlog/concepts.h>
#include <vlog/bindingstable.h>
#include <vlog/ruleexecutor.h>
#include <trident/model/table.h>
#include <trident/iterators/arrayitr.h>

#include <cstring>
#include <cmath>
#include <unordered_map>

BindingsTable *QSQR::getInputTable(const Predicate pred) {
    //raiseIfExpired();
    BindingsTable **table = inputs[pred.getId()];
    if (table == NULL) {
        const uint8_t maxAdornments = (uint8_t)pow(2, pred.getCardinality());
        table = new BindingsTable*[maxAdornments];
        memset(table, 0, sizeof(BindingsTable*)*maxAdornments);
        inputs[pred.getId()] = table;
        sizePreds[pred.getId()] = maxAdornments;
    }
    if (table[pred.getAdorment()] == NULL) {
        table[pred.getAdorment()] = new BindingsTable(pred.getCardinality(), pred.getAdorment());
    }
    return table[pred.getAdorment()];
}

BindingsTable *QSQR::getAnswerTable(const Predicate pred, uint8_t adornment) {
    //raiseIfExpired();
    BindingsTable **table = answers[pred.getId()];
    if (table == NULL) {
        const uint8_t maxAdornments = (uint8_t)pow(2, pred.getCardinality());
        table = new BindingsTable*[maxAdornments];
        memset(table, 0, sizeof(BindingsTable*)*maxAdornments);
        answers[pred.getId()] = table;
        sizePreds[pred.getId()] = maxAdornments;
    }
    if (table[adornment] == NULL) {
        table[adornment] = new BindingsTable(pred.getCardinality());
    }
    return table[adornment];
}

QSQR::~QSQR() {
    for (PredId_t i = 0; i < MAX_NPREDS; ++i) {
        if (inputs[i] != NULL) {
            for (uint32_t j = 0; j < sizePreds[i]; ++j)
                delete inputs[i][j];
            delete[] inputs[i];
        }
        if (answers[i] != NULL) {
            for (uint32_t j = 0; j < sizePreds[i]; ++j)
                delete answers[i][j];
            delete[] answers[i];
        }
        if (rules[i] != NULL) {
            for (int j = 0; j < sizePreds[i]; ++j) {
                if (rules[i][j] != NULL) {
                    for (int m = 0; m < program->getNRulesByPredicate(i); ++m) {
                        delete rules[i][j][m];
                    }
                    delete[] rules[i][j];
                }
            }
            delete[] rules[i];
        }
    }
}

void QSQR::deallocateAllRules() {
    for (PredId_t i = 0; i < MAX_NPREDS; ++i) {
        if (rules[i] != NULL) {
            for (int j = 0; j < sizePreds[i]; ++j) {
                if (rules[i][j] != NULL) {
                    for (int m = 0; m < program->getNRulesByPredicate(i); ++m) {
                        delete rules[i][j][m];
                    }
                    delete[] rules[i][j];
                }
            }
            delete[] rules[i];
            rules[i] = NULL;
        }
    }
}

size_t QSQR::calculateAllAnswers() {
    size_t total = 0;
    for (int i = 0; i < MAX_NPREDS; ++i) {
        if (answers[i] != NULL) {
            for (uint32_t j = 0; j < sizePreds[i]; ++j) {
                if (answers[i][j] != NULL) {
                    total += answers[i][j]->getNTuples();
                }
            }
        }
    }
    return total;
}

void QSQR::cleanAllInputs() {
    for (int i = 0; i < MAX_NPREDS; ++i) {
        if (inputs[i] != NULL) {
            for (uint32_t j = 0; j < sizePreds[i]; ++j) {
                if (inputs[i][j] != NULL) {
                    inputs[i][j]->clear();
                }
            }
        }
    }
}

void QSQR::createRules(Predicate &pred) {
    //check if the adorned rules are created. If not, then create them.
    if (rules[pred.getId()] == NULL) {
        const uint16_t maxAdornments = (uint16_t)pow(2, pred.getCardinality());
        rules[pred.getId()] = new RuleExecutor**[maxAdornments];
        memset(rules[pred.getId()], 0, sizeof(RuleExecutor**)*maxAdornments);
    }

    if (rules[pred.getId()][pred.getAdorment()] == NULL) {
        const auto rulesIds = program->getRulesIDsByPredicate(pred.getId());
        // LOG(DEBUGL) << "createRules for predicate " << pred.getId() << ", adornment = " << pred.getAdorment() << ", r->size = " << r->size();
        rules[pred.getId()][pred.getAdorment()] =
            new RuleExecutor*[rulesIds.size()];
        int m = 0;
        for (auto ruleId : rulesIds) {
            rules[pred.getId()][pred.getAdorment()][m] =
                new RuleExecutor(program->getRule(ruleId), pred.getAdorment(), program, layer);
            m++;
        }
    }
}

size_t QSQR::estimate(int depth, Predicate &pred, BindingsTable *inputTable/*, size_t offsetInput*/) {

    if (depth > 2) {
        return 0;
    }

    createRules(pred);

    std::vector<size_t> outputs;
    size_t output = 0;
    for (int i = 0; i < program->getNRulesByPredicate(pred.getId()); ++i) {
        RuleExecutor *exec = rules[pred.getId()][pred.getAdorment()][i];
        size_t r = exec->estimate(depth + 1, inputTable/*, offsetInput*/, this, layer);
	if (r != 0) {
	    // if (depth > 0 || r <= 10) {
		output += r;
	    /*
	    } else {
		// Somewhat silly duplicate detection heuristic ...
		bool found = false;
		for (int i = 0; i < outputs.size(); i++) {
		    if (outputs[i] == r) {
			LOG(DEBUGL) << "Ignoring " << r << " results";
			// Assume it is the same answer ...
			found = true;
			break;
		    }
		}
		if (! found) {
		    outputs.push_back(r);
		    output += r;
		}
	    }
	    */
	}
    }
    return output;
}

void QSQR::evaluate(Predicate &pred, BindingsTable *inputTable,
                    size_t offsetInput, bool repeat) {
#ifdef RECURSIVE_QSQR
    size_t totalAnswers;
    bool shouldRepeat = false;

    //Calculate all answers produced so far.
    totalAnswers = calculateAllAnswers();
    do {
        //Create rules
        createRules(pred);
        for (int i = 0; i < program->getAllRulesByPredicate(pred.getId())->size(); ++i) {
            RuleExecutor *exec = rules[pred.getId()][pred.getAdorment()][i];
            exec->evaluate(inputTable, offsetInput, this, layer);
        }

        //Repeat the execution if new answers were being produced
        //Clean up all the inputs relations to ensure completeness.
        size_t newTotalAnswers = calculateAllAnswers();

        shouldRepeat = newTotalAnswers > totalAnswers;
        totalAnswers = newTotalAnswers;
    } while (repeat && shouldRepeat);
    // LOG(DEBUGL) << "QSQR: finished execution of query";
#else
    createRules(pred);
    size_t sz = program->getNRulesByPredicate(pred.getId());
    if (sz > 0) {
	QSQR_Task task(QSQR_TaskType::QUERY, pred);
	task.currentRuleIndex = 1;
	task.inputTable = inputTable;
	task.offsetInput = offsetInput;
	task.repeat = repeat;
	task.totalAnswers = calculateAllAnswers();
	pushTask(task);
        RuleExecutor *exec = rules[pred.getId()][pred.getAdorment()][0];
        exec->evaluate(inputTable, offsetInput, this, layer);
    }
#endif
}

#ifndef RECURSIVE_QSQR
void QSQR::processTask(QSQR_Task &task) {
    switch (task.type) {
    case QUERY: {
	size_t sz = program->getNRulesByPredicate(task.pred.getId());
	if (task.currentRuleIndex < sz) {
            //Execute the next rule
	    QSQR_Task newTask(QSQR_TaskType::QUERY, task.pred);
	    newTask.currentRuleIndex = task.currentRuleIndex + 1;
	    newTask.inputTable = task.inputTable;
	    newTask.offsetInput = task.offsetInput;
	    newTask.repeat = task.repeat;
	    newTask.totalAnswers = task.totalAnswers;
	    pushTask(newTask);
            // LOG(DEBUGL) << "pushed new task QUERY, totalAnswers = " << newTask.totalAnswers;
            RuleExecutor *exec = rules[task.pred.getId()]
                                 [task.pred.getAdorment()][task.currentRuleIndex];
            exec->evaluate(task.inputTable, task.offsetInput, this, layer);
        } else {
            size_t newAnswers = calculateAllAnswers();
            if (task.repeat && newAnswers > task.totalAnswers) {
                createRules(task.pred);
		QSQR_Task newTask(QSQR_TaskType::QUERY, task.pred);
		newTask.currentRuleIndex = 1;
		newTask.inputTable = task.inputTable;
		newTask.offsetInput = task.offsetInput;
		newTask.repeat = task.repeat;
		//newTask.shouldRepeat = false;
		newTask.totalAnswers = newAnswers;
		pushTask(newTask);
                // LOG(DEBUGL) << "pushed new task QUERY(0), totalAnswers = " << newTask.totalAnswers;
                RuleExecutor *exec = rules[task.pred.getId()]
                                     [task.pred.getAdorment()][0];
                exec->evaluate(task.inputTable, task.offsetInput, this, layer);
            }
        }
        break;
    }
    case RULE:
    case RULE_QUERY:
        RuleExecutor *exec = task.executor;
        exec->processTask(&task);
        break;
    }
}
#endif

TupleTable *QSQR::evaluateQuery(int evaluateOrEstimate, QSQQuery *query,
                                std::vector<uint8_t> *posJoins,
                                std::vector<Term_t> *possibleValuesJoins,
                                bool returnOnlyVars/*,
                                const Timeout * const timeout*/) {

    Predicate pred = query->getLiteral()->getPredicate();
    if (pred.getType() == EDB) {
        if (evaluateOrEstimate == QSQR_EVAL) {
            uint8_t nvars = query->getLiteral()->getNVars();
            TupleTable *outputTable = new TupleTable(nvars);
            layer.query(query, outputTable, posJoins, possibleValuesJoins);
            return outputTable;
        } else {
            TupleTable *output = new TupleTable(1);
            uint64_t card = layer.estimateCardinality(*query->getLiteral());
            output->addRow(&card);
            return output;
        }
    } else {
        cleanAllInputs();
        size_t totalAnswers, newTotalAnswers;
        bool shouldRepeat = false;
        uint8_t adornment = pred.getAdorment();
        do {
            BindingsTable *inputTable;
            if (posJoins != NULL) {
                //Modify the adornment of the pred. Set constant values that were
                //set as variables
                for (uint8_t i = 0; i < posJoins->size(); ++i) {
                    adornment = Predicate::changeVarToConstInAdornment(adornment,
                                posJoins->at(i));
                }
                Predicate pred2 = Predicate(pred.getId(), adornment, pred.getType(),
                                            pred.getCardinality());
                inputTable = getInputTable(pred2);

                assert(possibleValuesJoins != NULL);
                assert(query->getLiteral()->getTupleSize() <= 3);
                Term_t tuple[3];
                //Fill the tuple with the content of the query
                VTuple t = query->getLiteral()->getTuple();
                for (int i = 0; i < t.getSize(); ++i) {
                    tuple[i] = t.get(i).getValue();
                }

                //Add all possible literals
                std::vector<Term_t>::iterator itr = possibleValuesJoins->begin();
                while (itr != possibleValuesJoins->end()) {
                    //raiseIfExpired();
                    for (uint8_t j = 0; j < posJoins->size(); ++j) {
                        tuple[posJoins->at(j)] = *itr;
                        itr++;
                    }
                    inputTable->addTuple(tuple);
                }

                //raiseIfExpired();
                totalAnswers = calculateAllAnswers();

                if (evaluateOrEstimate == QSQR_EVAL) {
                    evaluate(pred2, inputTable, 0, false);
#ifndef RECURSIVE_QSQR
                    //evaluate in this case is not recursive. Process the tasks
                    //until the queue is empty
                    while (tasks.size() > 0) {
                        // LOG(DEBUGL) << "Task size=" << tasks.size();
                        QSQR_Task task = tasks.back();
                        tasks.pop_back();
                        processTask(task);
                    }
#endif

                } else {
                    TupleTable *output = new TupleTable(1);
                    uint64_t est = estimate(0, pred2, inputTable/*, 0*/);
		    // Incorporate size of possible join values?
		    // Useless, I think, because in the planning phase, we don't actually have more than
		    // one possiblevaluesjoin. --Ceriel
		    est = est + (est * (possibleValuesJoins->size() / posJoins->size() - 1)) / 10; 
                    output->addRow(&est);
                    return output;
                }
            } else {
                inputTable = getInputTable(pred);
                inputTable->addTuple(query->getLiteral());
                if (evaluateOrEstimate == QSQR_EVAL) {
                    totalAnswers = calculateAllAnswers();
                    evaluate(pred, inputTable, 0, false);
#ifndef RECURSIVE_QSQR
                    //evaluate in this case is not recursive. Process the tasks
                    //until the queue is empty
                    while (tasks.size() > 0) {
                        //LOG(DEBUGL) << "Task size=" << tasks.size();
                        QSQR_Task task = tasks.back();
                        tasks.pop_back();
                        processTask(task);
                    }
#endif

                } else { //ESTIMATE
                    TupleTable *output = new TupleTable(1);
                    uint64_t est = estimate(0, pred, inputTable/*, 0*/);
                    output->addRow(&est);
                    return output;
                }
            }

            newTotalAnswers = calculateAllAnswers();
            shouldRepeat = newTotalAnswers > totalAnswers;
            if (shouldRepeat) {
                cleanAllInputs();
            }
        } while (shouldRepeat);

        const Literal *l = query->getLiteral();
        BindingsTable *answer = getAnswerTable(l->getPredicate(), adornment);

        if (returnOnlyVars) {
            //raiseIfExpired();
            return answer->projectAndFilter(*l, posJoins, possibleValuesJoins);
        } else {
            //raiseIfExpired();
            return answer->filter(*l, posJoins, possibleValuesJoins);
        }
    }
}
