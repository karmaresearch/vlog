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

        std::chrono::system_clock::time_point startTime;
        bool running;

        std::vector<FCBlock> listDerivations;
        std::vector<StatsRule> statsRuleExecution;

        bool ignoreDuplicatesElimination;
        std::vector<int> stratification;
        int nStratificationClasses;
        Program *RMFC_program;
        std::string sameasAlgo;
        const bool UNA;

#ifdef WEBINTERFACE
        long statsLastIteration;
        std::string currentRule;
        PredId_t currentPredicate;
#endif

        std::string name;

    private:
        FCIterator getTableFromIDBLayer(const Literal & literal,
                const size_t minIteration,
                TableFilterer *filter);

        FCIterator getTableFromIDBLayer(const Literal & literal,
                const size_t minIteration,
                const size_t maxIteration,
                TableFilterer *filter);

        size_t countAllIDBs();

        bool bodyChangedSince(Rule &rule, size_t iteration);

        bool checkIfAtomsAreEmpty(const RuleExecutionDetails &ruleDetails,
                const RuleExecutionPlan &plan,
                size_t limitView,
                std::vector<size_t> &cards);

        void processRuleFirstAtom(const int nBodyLiterals,
                const Literal *bodyLiteral,
                std::vector<Literal> &heads,
                const size_t min,
                const size_t max,
                int &processedTables,
                const bool lastLiteral,
                const size_t iteration,
                const RuleExecutionDetails &ruleDetails,
                const int orderExecution,
                std::vector<std::pair<uint8_t, uint8_t>> *filterValueVars,
                ResultJoinProcessor *joinOutput);

        void reorderPlan(RuleExecutionPlan &plan,
                const std::vector<size_t> &cards,
                const std::vector<Literal> &headLiteral,
                bool copyAllVars);

        void reorderPlanForNegatedLiterals(RuleExecutionPlan &plan,
                const std::vector<Literal> &heads);

        void executeRules(
                std::vector<RuleExecutionDetails> &EDBRules,
                std::vector<RuleExecutionDetails> &ExtEDBRules,
                std::vector<std::vector<RuleExecutionDetails>> &IDBRules,    // one entry for each stratification class
                std::vector<std::vector<RuleExecutionDetails>> &ExtIDBRules,    // one entry for each stratification class
                std::vector<StatIteration> &costRules,
                unsigned long *timeout = NULL);

        bool executeRule(RuleExecutionDetails &ruleDetails,
                std::vector<Literal> &heads,
                const size_t iteration,
                const size_t limitView,
                std::vector<ResultJoinProcessor*> *finalResultContainer);

        size_t estimateCardTable(const Literal &literal,
                const size_t minIteration,
                const size_t maxIteration);

	bool checkEmpty(const Literal *lit);

    protected:
        TypeChase typeChase;
        bool checkCyclicTerms;
        bool foundCyclicTerms;
        PredId_t predIgnoreBlock; //RMSA
        bool ignoreExistentialRules;
        std::shared_ptr<ChaseMgmt> chaseMgmt;

        std::vector<FCTable *>predicatesTables;
        EDBLayer &layer;
        Program *program;
        std::vector<std::vector<RuleExecutionDetails>> allIDBRules;
        // one entry for each stratification class
        size_t iteration;
        int nthreads;
        uint64_t triggers;

        bool executeRule(RuleExecutionDetails &ruleDetails,
                const size_t iteration,
                const size_t limitView,
                std::vector<ResultJoinProcessor*> *finalResultContainer);

        virtual FCIterator getTableFromEDBLayer(const Literal & literal);

        virtual size_t getNLastDerivationsFromList();

        virtual void saveDerivationIntoDerivationList(FCTable *endTable);

        virtual void saveStatistics(StatsRule &stats);

        virtual bool executeUntilSaturation(
                std::vector<RuleExecutionDetails> &ruleset,
                std::vector<StatIteration> &costRules,
                size_t limitView,
                bool fixpoint, unsigned long *timeout = NULL);

        void prepare(size_t lastExecution, int singleRuleToCheck, std::vector<RuleExecutionDetails> &allrules);

        void setIgnoreDuplicatesElimination() {
            ignoreDuplicatesElimination = true;
        }

    public:
        VLIBEXP SemiNaiver(EDBLayer &layer,
                Program *program, bool opt_intersect,
                bool opt_filtering, bool multithreaded,
                TypeChase chase, int nthreads, bool shuffleRules,
                bool ignoreExistentialRule, Program *RMFC_check = NULL,
                std::string sameasAlgo = "",
                bool UNA = false);

        //disable restricted chase
        VLIBEXP SemiNaiver(EDBLayer &layer,
                Program *program, bool opt_intersect,
                bool opt_filtering, bool multithreaded,
                int nthreads, bool shuffleRules,
                bool ignoreExistentialRules,
                std::string sameasAlgo = "") :
            SemiNaiver(layer, program, opt_intersect, opt_filtering,
                    multithreaded, TypeChase::SKOLEM_CHASE, nthreads, shuffleRules,
                    ignoreExistentialRules, NULL, sameasAlgo, false) {
            }

        VLIBEXP void run(unsigned long *timeout = NULL,
                bool checkCyclicTerms = false) {
            run(0, 1, timeout, checkCyclicTerms, -1, -1);
        }

        Program *get_RMFC_program() {
            return RMFC_program;
        }

        bool opt_filter() {
            return opt_filtering;
        }

        bool opt_inter() {
            return opt_intersect;
        }

        std::shared_ptr<ChaseMgmt> getChaseManager() {
            return chaseMgmt;
        }

        virtual FCTable *getTable(const PredId_t pred, const int card);

        VLIBEXP void run(size_t lastIteration,
                size_t iteration,
                unsigned long *timeout = NULL,
                bool checkCyclicTerms = false,
                int singleRule = -1,
                PredId_t predIgnoreBlock = -1);

        VLIBEXP void storeOnFile(std::string path, const PredId_t pred, const bool decompress,
                const int minLevel, const bool csv);

        VLIBEXP void storeOnFiles(std::string path, const bool decompress,
                const int minLevel, const bool csv);

        std::ostream& dumpTables(std::ostream &os) {
            for (PredId_t i = 0; i < MAX_NPREDS; ++i) {
                FCTable *table = predicatesTables[i];
                if (table != NULL && !table->isEmpty()) {
                    char buffer[MAX_TERM_SIZE];

                    os << "Table " << getProgram()->getPredicateName(i) << std::endl;
                    FCIterator itr = table->read(0);
                    const uint8_t sizeRow = table->getSizeRow();
                    while (!itr.isEmpty()) {
                        std::shared_ptr<const FCInternalTable> t = itr.getCurrentTable();
                        FCInternalTableItr *iitr = t->getIterator();
                        while (iitr->hasNext()) {
                            iitr->next();
                            std::string row = "    ";
                            row += std::to_string(iitr->getCurrentIteration());
                            for (uint8_t m = 0; m < sizeRow; ++m) {
                                row += "\t" + std::to_string(iitr->getCurrentValue(m));
                            }
                            os << row << std::endl;
                        }
                        t->releaseIterator(iitr);
                        itr.moveNextCount();
                    }
                }
            }

            return os;
        }

        FCIterator getTable(const Literal &literal, const size_t minIteration,
                const size_t maxIteration) {
            return getTable(literal, minIteration, maxIteration, NULL);
        }

        VLIBEXP FCIterator getTable(const PredId_t predid);

        VLIBEXP size_t getSizeTable(const PredId_t predid) const;

        bool isEmpty(const PredId_t predid) const;

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

        void checkAcyclicity(int singleRule = -1, PredId_t predIgnoreBlock = -1) {
            run(0, 1, NULL, true, singleRule, predIgnoreBlock);
        }

        //Statistics methods

        VLIBEXP void printCountAllIDBs(std::string prefix);

        size_t getCurrentIteration();

#ifdef WEBINTERFACE
        std::string getCurrentRule();

        bool isRunning();

        std::vector<std::pair<std::string, std::vector<StatsSizeIDB>>> getSizeIDBs();

        std::vector<StatsRule> getOutputNewIterations();
#endif

        std::chrono::system_clock::time_point getStartingTimeMs() {
            return startTime;
        }

        void setName(const std::string &name) {
            this->name = name;
        }

        const std::string &getName() const {
            return name;
        }
};

#endif
