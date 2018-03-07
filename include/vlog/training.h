#ifndef _TRAINING_H
#define _TRAINING_H

#include <vlog/seminaiver.h>
#include <vlog/trident/tridenttable.h>
#include <vlog/reasoner.h>
#include <vlog/LogisticRegression.h>
#include <layers/TridentLayer.hpp>

#include <trident/kb/kb.h>
#include <trident/utils/json.h>
#include <trident/server/server.h>

#include <kognac/utils.h>
#include <kognac/progargs.h>

typedef std::pair<PredId_t, vector<Substitution>> EndpointWithEdge;
typedef std::unordered_map<uint16_t, std::vector<EndpointWithEdge>> Graph;

typedef enum QueryType{
    QUERY_TYPE_MIXED = 0,
    QUERY_TYPE_GENERIC = 100,
    QUERY_TYPE_BOOLEAN = 1000
}QueryType;

class Training {

    private:
        Graph graph;
    public:

    static std::vector<pair<string, int>> generateTrainingQueries(EDBConf& conf,
        EDBLayer& layer,
        Program& p,
        int depth,
        uint64_t maxTuples,
        std::vector<uint8_t>& vt);

    static double runAlgo (string& algo,
        Reasoner& reasoner,
        EDBLayer& edb,
        Program& p,
        Literal& literal,
        stringstream& ss,
        uint64_t timeoutMillis,
        uint8_t repeatQuery);

    static void execLiteralQuery(string& literalquery,
        EDBLayer& edb,
        Program& p,
        JSON* jsonResults,
        JSON* jsonFeatures,
        JSON* jsonQsqrTime,
        JSON* jsonMagicTime,
        uint64_t timeout,
        uint8_t repeatQuery);

    static void execLiteralQueries(vector<string>& literalquery,
        EDBLayer& edb,
        Program& p,
        JSON* jsonResults,
        JSON* jsonFeatures,
        JSON* jsonQsqrTime,
        JSON* jsonMagicTime,
        uint64_t timeout,
        uint8_t repeatQuery);

    static void trainModel();
};

#endif
