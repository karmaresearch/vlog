#ifndef _WEB_INTERFACE_H
#define _WEB_INTERFACE_H

/* Code inspired by the tutorial available at http://pastebin.com/1KLsjJLZ */

#ifdef WEBINTERFACE

#include <vlog/seminaiver.h>
#include <vlog/trident/tridenttable.h>
#include <vlog/reasoner.h>
#include <layers/TridentLayer.hpp>

#include <trident/kb/kb.h>
#include <trident/utils/json.h>
#include <trident/server/server.h>

#include <kognac/progargs.h>

#include <dblayer.hpp>
#include <cts/infra/QueryGraph.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <rts/runtime/QueryDict.hpp>

#include <map>
#include <condition_variable>
#include <mutex>

class VLogLayer;
class WebInterface {
    protected:
        ProgramArgs &vm;
        std::unique_ptr<Program> program;
        std::unique_ptr<EDBLayer> edb;
        std::unique_ptr<VLogLayer> vloglayer;
        std::unique_ptr<TridentLayer> tridentlayer;

        void setupTridentLayer();

    private:
        std::shared_ptr<SemiNaiver> sn;
        std::thread t;
        std::thread matRunner;
        std::mutex mtxMatRunner;
        std::condition_variable cvMatRunner;
        std::string dirhtmlfiles;
        std::string cmdArgs;

        std::shared_ptr<HttpServer> server;

        bool isActive;
        std::string edbFile;
        int webport;
        int nthreads;

        map<std::string, std::string> cachehtml;

        void startThread(int port);

        void processMaterialization();

        void processRequest(std::string req, std::string &resp);

        void getResultsQueryLiteral(std::string predicate, long limit, JSON &out);

    public:
        WebInterface(ProgramArgs &vm, std::shared_ptr<SemiNaiver> sn, std::string htmlfiles,
                std::string cmdArgs, std::string edbfile);

        void start(int port);

        void connect();

        void stop();

        std::string getDefaultPage();

        std::string getPage(std::string page);

        long getDurationExecMs();

        void setActive() {
            isActive = true;
        }

        void setInactive() {
            isActive = false;
        }

        void join() {
            t.join();
        }

        std::shared_ptr<SemiNaiver> getSemiNaiver() {
            return sn;
        }

        std::string getCommandLineArgs() {
            return cmdArgs;
        }

        static std::string lookup(std::string sId, DBLayer &db);

        static void execSPARQLQuery(std::string sparqlquery,
                bool explain,
                long nterms,
                DBLayer &db,
                bool printstdout,
                bool jsonoutput,
                JSON *jsonvars,
                JSON *jsonresults,
                JSON *jsonstats);

        static void execLiteralQuery(std::string& literalquery,
                EDBLayer &edb,
                Program &p,
                bool jsonoutput,
                JSON *jsonresults,
                JSON *jsonFeatures,
                JSON *jsonQsqrTime,
                JSON *jsonMagicTime);
        static double runAlgo(std::string& algo,
                Reasoner& reasoner,
                EDBLayer& edb,
                Program& p,
                Literal& literal,
                std::stringstream& ss,
                uint64_t timeoutMillis);
};
#endif
#endif
