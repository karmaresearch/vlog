#ifndef __VLOG_UTILS_INCLUDED
#define __VLOG_UTILS_INCLUDED

#include <vlog/consts.h>
#include <trident/utils/json.h>
#include <dblayer.hpp>
#include <cts/infra/QueryGraph.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <rts/runtime/QueryDict.hpp>

#include <string>

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
