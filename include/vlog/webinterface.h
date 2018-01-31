#ifndef _WEB_INTERFACE_H
#define _WEB_INTERFACE_H

/* Code inspired by the tutorial available at http://pastebin.com/1KLsjJLZ */

#ifdef WEBINTERFACE

#include <vlog/seminaiver.h>
#include <vlog/trident/tridenttable.h>

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
        string dirhtmlfiles;
        string cmdArgs;

        std::shared_ptr<HttpServer> server;

        bool isActive;
        string edbFile;
        int webport;
        int nthreads;

        map<string, string> cachehtml;

        void startThread(int port);

        void processMaterialization();

        static void parseQuery(bool &success,
                SPARQLParser &parser,
                std::shared_ptr<QueryGraph> &queryGraph,
                QueryDict &queryDict,
                DBLayer &db);

        void processRequest(std::string req, std::string &resp);

    public:
        WebInterface(ProgramArgs &vm, std::shared_ptr<SemiNaiver> sn, string htmlfiles,
                string cmdArgs, string edbfile);

        void start(int port);

        void connect();

        void stop();

        string getDefaultPage();

        string getPage(string page);

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

        string getCommandLineArgs() {
            return cmdArgs;
        }

        static string lookup(string sId, DBLayer &db);

        static void execSPARQLQuery(string sparqlquery,
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
#endif
