#ifndef _SEMI_NAIVER_THREADED_H
#define _SEMI_NAIVER_THREADED_H

#include <vlog/seminaiver.h>

#include <mutex>
#include <thread>

struct SemiNaiver_Threadlocal {
    std::vector<FCBlock> listDerivations;
    std::vector<StatsRule> statsRuleExecution;
    size_t iteration;
};

class StatusRuleExecution_ThreadSafe {
    private:
        //Internal data structures

        //Hold all rules to execution
        std::mutex mutexRules;
        int rulecount;
        const int nrules;

        std::vector<ResultJoinProcessor*> tmpderivations;

    public:

        StatusRuleExecution_ThreadSafe(const int nrules);

        int getRuleIDToExecute();

        void registerDerivations(ResultJoinProcessor *res);

        std::vector<ResultJoinProcessor*> &getTmpDerivations() {
            return  tmpderivations;
        }
};

class SemiNaiverThreaded: public SemiNaiver {

    private:
        //const int nthreads;
        //TODO This variable is only visible to the thread
        std::shared_ptr<SemiNaiver_Threadlocal> thread_data;

        bool marked[MAX_NPREDS];
        bool newMarked[MAX_NPREDS];

        /*** VARIOUS MUTEXES */
        std::mutex mutexInsert;
        std::mutex mutexIteration;
        std::mutex mutexGetTable;
        std::mutex mutexStatistics;
        std::mutex mutexListDer;
        const int interRuleThreads;

        //Create one mutex per table
        std::mutex mutexes[MAX_NPREDS];

        size_t getAtomicIteration() {
            std::lock_guard<std::mutex> lock(mutexIteration);
            return iteration++;
        }

        bool doGlobalConsolidation(StatusRuleExecution_ThreadSafe &data);

        bool tryLock(std::vector<PredId_t> &predicates, PredId_t idHeadPredicate);

        void lock(std::vector<PredId_t> &predicates, PredId_t idHeadPredicate);

        void unlock(std::vector<PredId_t> &predicates, PredId_t idHeadPredicate);

    public:
        SemiNaiverThreaded(std::vector<Rule> ruleset,
                EDBLayer &layer,
                Program *program,
                bool opt_intersect,
                bool opt_filtering,
                bool shuffleRules,
                const int nthreads,
                const int interRuleThreads) : SemiNaiver(ruleset, layer,
                    program, opt_intersect, opt_filtering, true,
                    nthreads, shuffleRules),
                interRuleThreads(interRuleThreads) {

                    // Marks for parallel version
                    for (int i = 0; i < MAX_NPREDS; i++) {
                        marked[i] = true;
                        newMarked[i] = false;
                    }
                }

    protected:
        long getNLastDerivationsFromList();

        void saveDerivationIntoDerivationList(FCTable *endTable);

        void saveStatistics(StatsRule &stats);

        FCTable *getTable(const PredId_t pred, const uint8_t card);

        FCIterator getTableFromEDBLayer(const Literal & literal);

        void runThread(
                std::vector<RuleExecutionDetails> &ruleset,
                StatusRuleExecution_ThreadSafe *status,
                std::vector<StatIteration> *costRules,
                size_t lastExec);

        bool executeUntilSaturation(
                std::vector<RuleExecutionDetails> &ruleset,
                std::vector<StatIteration> &costRules,
                bool fixpoint);
};

#endif
