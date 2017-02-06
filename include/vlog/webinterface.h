#ifndef _WEB_INTERFACE_H
#define _WEB_INTERFACE_H

/* Code inspired by the tutorial available at http://pastebin.com/1KLsjJLZ */

#ifdef WEBINTERFACE

#include <vlog/seminaiver.h>
#include <vlog/trident/tridenttable.h>

#include <layers/TridentLayer.hpp>

#include <trident/kb/kb.h>

#include <dblayer.hpp>
#include <cts/infra/QueryGraph.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <rts/runtime/QueryDict.hpp>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <map>

class VLogLayer;
class TridentLayer;
class WebInterface {
protected:
    //program details
    std::unique_ptr<Program> program;
    std::unique_ptr<EDBLayer> edb;
    std::unique_ptr<VLogLayer> vloglayer;
    std::unique_ptr<TridentLayer> tridentlayer;

    void setupTridentLayer();

private:
    std::shared_ptr<SemiNaiver> sn;
    string dirhtmlfiles;
    boost::thread t;
    string cmdArgs;

    boost::asio::io_service io;
    boost::asio::ip::tcp::acceptor acceptor;
    boost::asio::ip::tcp::resolver resolver;

    bool isActive;
    string edbFile;
    string webport;

    map<string, string> cachehtml;

    class Server: public boost::enable_shared_from_this<Server> {
    private:
        std::string res, req;
        WebInterface *inter;

        std::ostringstream ss;
        std::unique_ptr<char[]> data_;

    public:
        boost::asio::ip::tcp::socket socket;
        Server(boost::asio::io_service &io, WebInterface *inter):
            inter(inter), socket(io) {
            data_ = std::unique_ptr<char[]>(new char[4096]);
        }
        void writeHandler(const boost::system::error_code &err, std::size_t bytes);
        void readHeader(boost::system::error_code const &err, size_t bytes);
        void acceptHandler(const boost::system::error_code &err);
    };

    void startThread(string address, string port);

    static void parseQuery(bool &success,
                           SPARQLParser &parser,
                           QueryGraph &queryGraph,
                           QueryDict &queryDict,
                           DBLayer &db);
public:
    WebInterface(std::shared_ptr<SemiNaiver> sn, string htmlfiles,
                 string cmdArgs, string edbfile);

    void start(string address, string port);

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
                                boost::property_tree::ptree *jsonvars,
                                boost::property_tree::ptree *jsonresults,
                                boost::property_tree::ptree *jsonstats);
};
#endif
#endif
