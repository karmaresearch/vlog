#ifndef _TRAINING_H
#define _TRAINING_H

#include <vlog/seminaiver.h>
#include <vlog/trident/tridenttable.h>
#include <vlog/reasoner.h>
#include <vlog/ml/LogisticRegression.h>
#include <layers/TridentLayer.hpp>
#include <vlog/ml/ml.h>

#include <trident/kb/kb.h>
#include <trident/utils/json.h>
#include <trident/server/server.h>

#include <kognac/utils.h>
#include <kognac/progargs.h>

typedef std::pair<PredId_t, vector<Substitution>> EndpointWithEdge;
typedef struct Edge {
    EndpointWithEdge endpoint;
    vector<Substitution> backEdge;
} Edge;

typedef std::unordered_map<uint16_t, vector<Edge>> DepGraph;

class Training {

    private:
        DepGraph graph;
    public:

    static std::vector<pair<string, int>> generateTrainingQueries(EDBConf& conf,
        EDBLayer& layer,
        Program& p,
        int depth,
        uint64_t maxTuples,
        std::vector<uint8_t>& vt);

    static std::vector<pair<string, int>> generateNewTrainingQueries(EDBConf& conf,
        EDBLayer& layer,
        Program& p,
        int depth,
        uint64_t maxTuples,
        std::vector<uint8_t>& vt);

    static std::vector<pair<string, int>> generateTrainingQueriesAllPaths(EDBConf& conf,
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
        uint64_t timeoutMillis);

    static void execLiteralQuery(string& literalquery,
        EDBLayer& edb,
        Program& p,
        string& strResults,
        string& strFeatures,
        string& strQsqrTime,
        string& strMagicTime,
        uint64_t timeout,
        uint8_t repeatQuery,
        int featureDepth,
        vector<Metrics>& featuresVector,
        vector<int>& decisionVector,
        vector<double>& featuresTimesVector);

    static void execLiteralQueries(vector<string>& literalquery,
        EDBLayer& edb,
        Program& p,
        JSON* jsonResults,
        JSON* jsonFeatures,
        JSON* jsonQsqrTime,
        JSON* jsonMagicTime,
        uint64_t timeout,
        uint8_t repeatQuery);

    static void runQueries(vector<string>& trainingQueriesVector,
        EDBLayer& edb,
        Program& p,
        uint64_t timeout,
        uint8_t repeatQuery,
        vector<Metrics>& featuresVector,
        vector<int>& decisionVector,
        vector<double>& featuresTimesVector,
        int& nMagicQueries,
        string& logFileName,
        int featureDepth);

    static void trainAndTestModel(vector<string>& trainingQueriesVector,
        vector<string>& testQueriesLog,
        EDBLayer& edb,
        Program& p,
        double& accuracy,
        uint64_t timeout,
        uint8_t repeatQuery,
        string& logFileName,
        int featureDepth);
};

#endif
