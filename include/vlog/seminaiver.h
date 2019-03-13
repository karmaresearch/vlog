#ifndef _SEMINAIVER_H
#define _SEMINAIVER_H

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/fctable.h>
#include <vlog/ruleexecplan.h>
#include <vlog/ruleexecdetails.h>
#include <vlog/chasemgmt.h>
#include <vlog/consts.h>

#include <trident/model/table.h>

#include <vector>
#include <unordered_map>

struct StatIteration {
    size_t iteration;
    const Rule *rule;
    double time;
    bool derived;

    bool operator <(const StatIteration &it) const {
        return time > it.time;
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
        std::vector<RuleExecutionDetails> allEDBRules;
        bool opt_intersect;
        bool opt_filtering;
        bool multithreaded;
        bool restrictedChase;
        bool checkCyclicTerms;
        bool foundCyclicTerms;
        bool ignoreExistentialRules;
        std::shared_ptr<ChaseMgmt> chaseMgmt;

        std::chrono::system_clock::time_point startTime;
        bool running;

        std::vector<FCBlock> listDerivations;
        std::vector<StatsRule> statsRuleExecution;

        bool ignoreDuplicatesElimination;


#ifdef WEBINTERFACE
        long statsLastIteration;
        string currentRule;
        PredId_t currentPredicate;
#endif

    private:
        FCIterator getTableFromIDBLayer(const Literal & literal,
                const size_t minIteration,
                TableFilterer *filter);

        FCIterator getTableFromIDBLayer(const Literal & literal,
                const size_t minIteration,
                const size_t maxIteration,
                TableFilterer *filter);

        size_t countAllIDBs();

        bool bodyChangedSince(Rule &rule, uint32_t iteration);

        bool checkIfAtomsAreEmpty(const RuleExecutionDetails &ruleDetails,
                const RuleExecutionPlan &plan,
                uint32_t limitView,
                std::vector<size_t> &cards);

        void processRuleFirstAtom(const uint8_t nBodyLiterals,
                const Literal *bodyLiteral,
                std::vector<Literal> &heads,
                const size_t min,
                const size_t max,
                int &processedTables,
                const bool lastLiteral,
                const uint32_t iteration,
                const RuleExecutionDetails &ruleDetails,
                const uint8_t orderExecution,
                std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
                ResultJoinProcessor *joinOutput);

        void reorderPlan(RuleExecutionPlan &plan,
                const std::vector<size_t> &cards,
                const std::vector<Literal> &headLiteral,
                bool copyAllVars);

        bool executeRules(std::vector<RuleExecutionDetails> &allEDBRules,
                std::vector<RuleExecutionDetails> &allIDBRules,
                std::vector<StatIteration> &costRules,
                const uint32_t limitView,
                bool fixpoint, unsigned long *timeout = NULL);

        bool executeRule(RuleExecutionDetails &ruleDetails,
                std::vector<Literal> &heads,
                const uint32_t iteration,
                const uint32_t limitView,
                std::vector<ResultJoinProcessor*> *finalResultContainer);

        size_t estimateCardTable(const Literal &literal,
                const size_t minIteration,
                const size_t maxIteration);

    protected:
        std::vector<FCTable *>predicatesTables;
        EDBLayer &layer;
        Program *program;
        std::vector<RuleExecutionDetails> allIDBRules;
        size_t iteration;
        int nthreads;

        bool executeRule(RuleExecutionDetails &ruleDetails,
                const uint32_t iteration,
                const uint32_t limitView,
                std::vector<ResultJoinProcessor*> *finalResultContainer);

        virtual FCIterator getTableFromEDBLayer(const Literal & literal);

        virtual long getNLastDerivationsFromList();

        virtual void saveDerivationIntoDerivationList(FCTable *endTable);

        virtual void saveStatistics(StatsRule &stats);

        virtual bool executeUntilSaturation(
                std::vector<RuleExecutionDetails> &ruleset,
                std::vector<StatIteration> &costRules,
                uint32_t limitView,
                bool fixpoint, unsigned long *timeout = NULL);

        void prepare(std::vector<RuleExecutionDetails> &allrules,
                size_t lastExecution,
                int singleRuleToCheck);

        void setIgnoreDuplicatesElimination() {
            ignoreDuplicatesElimination = true;
        }

    public:
        VLIBEXP SemiNaiver(std::vector<Rule> ruleset, EDBLayer &layer,
                Program *program, bool opt_intersect,
                bool opt_filtering, bool multithreaded,
                bool restrictedChase, int nthreads, bool shuffleRules,
                bool ignoreExistentialRules);

        //disable restricted chase
        VLIBEXP SemiNaiver(std::vector<Rule> ruleset, EDBLayer &layer,
                Program *program, bool opt_intersect,
                bool opt_filtering, bool multithreaded,
                int nthreads, bool shuffleRules,
                bool ignoreExistentialRules) :
            SemiNaiver(ruleset, layer, program, opt_intersect, opt_filtering,
                    multithreaded, false, nthreads, shuffleRules,
                    ignoreExistentialRules) {
            }

        VLIBEXP void run(unsigned long *timeout = NULL,
                bool checkCyclicTerms = false) {
            run(0, 1, timeout, checkCyclicTerms, -1);
        }

        bool opt_filter() {
            return opt_filtering;
        }

        bool opt_inter() {
            return opt_intersect;
        }

        virtual FCTable *getTable(const PredId_t pred, const uint8_t card);

        VLIBEXP void run(size_t lastIteration,
                size_t iteration,
                unsigned long *timeout = NULL,
                bool checkCyclicTerms = false,
                int singleRule = -1);

        VLIBEXP void storeOnFile(std::string path, const PredId_t pred, const bool decompress,
                const int minLevel, const bool csv);

        VLIBEXP void storeOnFiles(std::string path, const bool decompress,
                const int minLevel, const bool csv);

        FCIterator getTable(const Literal &literal, const size_t minIteration,
                const size_t maxIteration) {
            return getTable(literal, minIteration, maxIteration, NULL);
        }

        FCIterator getTable(const PredId_t predid);

        size_t getSizeTable(const PredId_t predid) const;

        std::vector<FCBlock> &getDerivationsSoFar() {
            return listDerivations;
        }

        VLIBEXP void createGraphRuleDependency(std::vector<int> &nodes,
                std::vector<std::pair<int, int>> &edges);

        Program *getProgram() {
            return program;
        }

        bool isFoundCyclicTerms() {
            return foundCyclicTerms;
        }

        void addDataToIDBRelation(const Predicate pred, FCBlock block);

        EDBLayer &getEDBLayer() {
            return layer;
        }

        size_t estimateCardinality(const Literal &literal, const size_t min,
                const size_t max);

        virtual ~SemiNaiver();

        static std::pair<uint8_t, uint8_t> removePosConstants(
                std::pair<uint8_t, uint8_t> columns,
                const Literal &literal);

        virtual FCIterator getTable(const Literal &literal, const size_t minIteration,
                const size_t maxIteration, TableFilterer *filter);

        void checkAcyclicity(int singleRule = -1) {
            run(0, 1, NULL, true, singleRule);
        }

        //Statistics methods

        VLIBEXP void printCountAllIDBs(string prefix);

        size_t getCurrentIteration();

#ifdef WEBINTERFACE
        string getCurrentRule();

        bool isRunning();

        std::vector<std::pair<string, std::vector<StatsSizeIDB>>> getSizeIDBs();

        std::vector<StatsRule> getOutputNewIterations();
#endif

        std::chrono::system_clock::time_point getStartingTimeMs() {
            return startTime;
        }

};

#endif
