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
        std::vector<RuleExecutionDetails> allEDBRules;
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
                const std::vector<Literal> &headLiteral);

        bool executeRules(std::vector<RuleExecutionDetails> &allEDBRules,
                std::vector<RuleExecutionDetails> &allIDBRules,
                std::vector<StatIteration> &costRules,
                bool fixpoint);

        bool executeRule(RuleExecutionDetails &ruleDetails,
                std::vector<Literal> &heads,
                const uint32_t iteration,
                std::vector<ResultJoinProcessor*> *finalResultContainer);

        size_t estimateCardTable(const Literal &literal,
                const size_t minIteration,
                const size_t maxIteration);

    protected:
        FCTable *predicatesTables[MAX_NPREDS];
        EDBLayer &layer;
        Program *program;
        std::vector<RuleExecutionDetails> allIDBRules;
        size_t iteration;
        int nthreads;

        bool executeRule(RuleExecutionDetails &ruleDetails,
                const uint32_t iteration,
                std::vector<ResultJoinProcessor*> *finalResultContainer);

        virtual FCIterator getTableFromEDBLayer(const Literal & literal);

        virtual long getNLastDerivationsFromList();

        virtual void saveDerivationIntoDerivationList(FCTable *endTable);

        virtual void saveStatistics(StatsRule &stats);

        virtual bool executeUntilSaturation(
                std::vector<RuleExecutionDetails> &ruleset,
                std::vector<StatIteration> &costRules,
                bool fixpoint);

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

        virtual FCTable *getTable(const PredId_t pred, const uint8_t card);

        void run(size_t lastIteration, size_t iteration);

        void storeOnFiles(std::string path, const bool decompress,
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

        void printCountAllIDBs(string prefix);

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
