#include <vlog/consts.h>
#include <trident/utils/json.h>
#include <dblayer.hpp>
#include <cts/infra/QueryGraph.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <rts/runtime/QueryDict.hpp>

#include <string>

#ifndef __VLOG_UTILS_INCLUDED
#define __VLOG_UTILS_INCLUDED
class VLogUtils {
    private:
        static void parseQuery(bool &success,
                SPARQLParser &parser,
                std::shared_ptr<QueryGraph> &queryGraph,
                QueryDict &queryDict,
                DBLayer &db);
    public:
        VLIBEXP static std::string csvString(std::string);
        VLIBEXP static void execSPARQLQuery(std::string sparqlquery,
                bool explain,
                long nterms,
                DBLayer &db,
                bool printstdout,
                bool jsonoutput,
                JSON *jsonvars,
                JSON *jsonresults,
                JSON *jsonstats);

};
#endif
// This function generates m random numbers from the range (0, n) and stores them in indexes vector
void getRandomTupleIndexes(uint64_t m, uint64_t n, std::vector<int>& indexes);
std::vector<std::string> split( std::string str, char sep = ' ' );
std::string stringJoin(std::vector<std::string>& vec, char delimiter=',');
std::vector<std::string> rsplit(std::string logLine, char sep = ' ', int maxSplits = 4);
