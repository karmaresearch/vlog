#ifndef _SEMINAIVER_H
#define _SEMINAIVER_H

#include <vlog/concepts.h>
#include <vlog/edb.h>
#include <vlog/fctable.h>
#include <vlog/ruleexecplan.h>
#include <vlog/ruleexecdetails.h>
#include <vlog/chasemgmt.h>

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
        std::vector<RuleExecutionDetails> edbRuleset;
        bool opt_intersect;
        bool opt_filtering;
        bool multithreaded;
        bool restrictedChase;
        std::shared_ptr<ChaseMgmt> chaseMgmt;

        std::chrono::system_clock::time_point startTime;
        bool running;

        std::vector<FCBlock> listDerivations;
        std::vector<StatsRule> statsRuleExecution;


#ifdef WEBINTERFACE
        long statsLastIteration;
        string currentRule;
        PredId_t currentPredicate;
        string allRules;
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

        bool checkIfAtomsAreEmpty(const RuleExecutionDetails &ruleDetails,
                const RuleExecutionPlan &plan,
                const Literal &headLiteral,
                std::vector<size_t> &cards);

        void processRuleFirstAtom(const uint8_t nBodyLiterals,
                const Literal *bodyLiteral,
                const Literal &headLiteral,
                const uint8_t posHeadInRule,
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
                const Literal &headLiteral,
                int posHead);

        //int getRuleID(const RuleExecutionDetails *rule);

        bool executeRule(RuleExecutionDetails &ruleDetails,
                Literal &headLiteral,
                int posHeadLiteral,
                const uint32_t iteration,
                std::vector<ResultJoinProcessor*> *finalResultContainer);

        size_t estimateCardTable(const Literal &literal,
                const size_t minIteration,
                const size_t maxIteration);

    protected:
        FCTable *predicatesTables[MAX_NPREDS];
        EDBLayer &layer;
        Program *program;
        std::vector<RuleExecutionDetails> ruleset;
        size_t iteration;
        int nthreads;

        bool executeRule(RuleExecutionDetails &ruleDetails,
                const uint32_t iteration,
                std::vector<ResultJoinProcessor*> *finalResultContainer);

        virtual FCIterator getTableFromEDBLayer(const Literal & literal);

        virtual long getNLastDerivationsFromList();

        virtual void saveDerivationIntoDerivationList(FCTable *endTable);

        virtual void saveStatistics(StatsRule &stats);

        virtual FCTable *getTable(const PredId_t pred, const uint8_t card);

        virtual void executeUntilSaturation(std::vector<StatIteration> &costRules);

    public:
        SemiNaiver(std::vector<Rule> ruleset, EDBLayer &layer,
                Program *program, bool opt_intersect,
                bool opt_filtering, bool multithreaded,
                bool restrictedChase, int nthreads, bool shuffleRules);

        //disable restricted chase
        SemiNaiver(std::vector<Rule> ruleset, EDBLayer &layer,
                Program *program, bool opt_intersect,
                bool opt_filtering, bool multithreaded,
                int nthreads, bool shuffleRules) :
            SemiNaiver(ruleset, layer, program, opt_intersect, opt_filtering,
                    multithreaded, false, nthreads, shuffleRules) {
            }

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

        FCIterator getTable(const PredId_t predid);

        size_t getSizeTable(const PredId_t predid) const;

        std::vector<FCBlock> &getDerivationsSoFar() {
            return listDerivations;
        }

        void createGraphRuleDependency(std::vector<int> &nodes,
                std::vector<std::pair<int, int>> &edges);

        Program *getProgram() {
            return program;
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

        //Statistics methods

        void printCountAllIDBs();

        size_t getCurrentIteration();

#ifdef WEBINTERFACE
        string getCurrentRule();

        bool isRunning();

        std::vector<std::pair<string, std::vector<StatsSizeIDB>>> getSizeIDBs();

        std::vector<StatsRule> getOutputNewIterations();

        string getListAllRulesForJSONSerialization();
#endif

        std::chrono::system_clock::time_point getStartingTimeMs() {
            return startTime;
        }

};

#endif
