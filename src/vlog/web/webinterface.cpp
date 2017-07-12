#ifdef WEBINTERFACE

#include <vlog/webinterface.h>
#include <vlog/materialization.h>

#include <launcher/vloglayer.h>
#include <cts/parser/SPARQLLexer.hpp>
#include <cts/semana/SemanticAnalysis.hpp>
#include <cts/plangen/PlanGen.hpp>
#include <cts/codegen/CodeGen.hpp>
#include <rts/runtime/Runtime.hpp>
#include <rts/operator/Operator.hpp>
#include <rts/operator/PlanPrinter.hpp>
#include <rts/operator/ResultsPrinter.hpp>

#include <kognac/utils.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <curl/curl.h>

#include <string>
#include <fstream>
#include <chrono>
#include <thread>

using boost::property_tree::ptree;
using boost::property_tree::read_json;
using boost::property_tree::write_json;

WebInterface::WebInterface(std::shared_ptr<SemiNaiver> sn, string htmlfiles,
        string cmdArgs, string edbfile) : sn(sn),
    dirhtmlfiles(htmlfiles), cmdArgs(cmdArgs),
    acceptor(io), resolver(io), isActive(false),
    edbFile(edbfile) {
        //Setup the EDB layer
        EDBConf conf(edbFile);
        edb = std::unique_ptr<EDBLayer>(new EDBLayer(conf, false));
        setupTridentLayer();
    }

void WebInterface::setupTridentLayer() {
    tridentlayer = std::unique_ptr<TridentLayer>();
    if (edb) {
        //Setup a TridentLayer (for queries without datalog)
        PredId_t p = edb->getFirstEDBPredicate();
        string typedb = edb->getTypeEDBPredicate(p);
        tridentlayer = NULL;
        if (typedb == "Trident") {
            auto edbTable = edb->getEDBTable(p);
            KB *kb = ((TridentTable*)edbTable.get())->getKB();
            tridentlayer = std::unique_ptr<TridentLayer>(new TridentLayer(*kb));
            tridentlayer->disableBifocalSampling();
        }
    }
}

void WebInterface::startThread(string address, string port) {
    this->webport = port;
    boost::asio::ip::tcp::resolver::query query(address, port);
    boost::asio::ip::tcp::endpoint endpoint = *resolver.resolve(query);
    acceptor.open(endpoint.protocol());
    acceptor.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
    acceptor.bind(endpoint);
    acceptor.listen();
    connect();
    io.run();
}

void WebInterface::start(string address, string port) {
    t = boost::thread(&WebInterface::startThread, this, address, port);
}

void WebInterface::stop() {
    BOOST_LOG_TRIVIAL(info) << "Stopping server ...";
    while (isActive) {
        std::this_thread::sleep_for(chrono::milliseconds(100));
    }
    acceptor.cancel();
    acceptor.close();
    io.stop();
    BOOST_LOG_TRIVIAL(info) << "Done";
}

long WebInterface::getDurationExecMs() {
    boost::chrono::system_clock::time_point start = sn->getStartingTimeMs();
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    return boost::chrono::duration_cast<boost::chrono::milliseconds>(sec).count();
}

void WebInterface::connect() {
    boost::shared_ptr<Server> conn(new Server(io, this));
    acceptor.async_accept(conn->socket,
            boost::bind(&Server::acceptHandler,
                conn, boost::asio::placeholders::error));
};

void WebInterface::Server::acceptHandler(const boost::system::error_code &err) {
    if (err == boost::system::errc::success) {
        socket.async_read_some(boost::asio::buffer(data_.get(), 4096),
                boost::bind(&Server::readHeader, shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred));
    }
}

static string _getValueParam(string req, string param) {
    int pos = req.find(param);
    if (pos == string::npos) {
        return "";
    } else {
        int postart = req.find("=", pos);
        int posend = req.find("&", pos);
        if (posend == string::npos) {
            return req.substr(postart + 1);
        } else {
            return req.substr(postart + 1, posend - postart - 1);
        }
    }
}

void WebInterface::parseQuery(bool &success,
        SPARQLParser &parser,
        QueryGraph &queryGraph,
        QueryDict &queryDict,
        DBLayer &db) {

    //Sometimes the query introduces new constants which need an ID
    try {
        parser.parse();
    } catch (const SPARQLParser::ParserException& e) {
        cerr << "parse error: " << e.message << endl;
        success = false;
        return;
    }

    // And perform the semantic anaylsis
    try {
        SemanticAnalysis semana(db, queryDict);
        semana.transform(parser, queryGraph);
    } catch (const SemanticAnalysis::SemanticException& e) {
        cerr << "semantic error: " << e.message << endl;
        success = false;
        return;
    }
    if (queryGraph.knownEmpty()) {
        cout << "<empty result -- known empty>" << endl;
        success = false;
        return;
    }

    success = true;
    return;
}

string WebInterface::lookup(string sId, DBLayer &db) {
    const char *start;
    const char *end;
    unsigned id = stoi(sId);
    ::Type::ID type;
    unsigned st;
    db.lookupById(id, start, end, type, st);
    return string(start, end - start);
}

void WebInterface::execSPARQLQuery(string sparqlquery,
        bool explain,
        long nterms,
        DBLayer &db,
        bool printstdout,
        bool jsonoutput,
        boost::property_tree::ptree *jsonvars,
        boost::property_tree::ptree *jsonresults,
        boost::property_tree::ptree *jsonstats) {
    std::unique_ptr<QueryDict> queryDict = std::unique_ptr<QueryDict>(new QueryDict(nterms));
    std::unique_ptr<QueryGraph> queryGraph = std::unique_ptr<QueryGraph>(new QueryGraph());
    bool parsingOk;

    std::unique_ptr<SPARQLLexer> lexer =
        std::unique_ptr<SPARQLLexer>(new SPARQLLexer(sparqlquery));
    std::unique_ptr<SPARQLParser> parser = std::unique_ptr<SPARQLParser>(
            new SPARQLParser(*lexer.get()));
    boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
    parseQuery(parsingOk, *parser.get(), *queryGraph.get(), *queryDict.get(), db);
    if (!parsingOk) {
        boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Runtime query: 0ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime total: " << duration.count() * 1000 << "ms.";
        BOOST_LOG_TRIVIAL(info) << "# rows = 0";
        return;
    }

    if (jsonvars) {
        //Copy the output of the query in the json vars
        for (QueryGraph::projection_iterator itr = queryGraph->projectionBegin();
                itr != queryGraph->projectionEnd(); ++itr) {
            string namevar = parser->getVariableName(*itr);
            boost::property_tree::ptree var;
            var.put("", namevar);
            jsonvars->push_back(std::make_pair("", var));
        }
    }

    // Run the optimizer
    PlanGen *plangen = new PlanGen();
    Plan* plan = plangen->translate(db, *queryGraph.get(), false);
    // delete plangen;  Commented out, because this also deletes all plans!
    // In particular, it corrupts the current plan.
    // --Ceriel
    if (!plan) {
        cerr << "internal error plan generation failed" << endl;
        delete plangen;
        return;
    }
    if (explain)
        plan->print(0);

    // Build a physical plan
    Runtime runtime(db, NULL, queryDict.get());
    Operator* operatorTree = CodeGen().translate(runtime, *queryGraph.get(), plan, false);

    // Execute it
    if (explain) {
        DebugPlanPrinter out(runtime, false);
        operatorTree->print(out);
        delete operatorTree;
    } else {
#if DEBUG
        DebugPlanPrinter out(runtime, false);
        operatorTree->print(out);
#endif
        //set up output options for the last operators
        ResultsPrinter *p = (ResultsPrinter*) operatorTree;
        p->setSilent(!printstdout);
        if (jsonoutput) {
	    std::vector<std::string> jsonvars;
            p->setJSONOutput(jsonresults, jsonvars);
        }

        boost::chrono::system_clock::time_point startQ = boost::chrono::system_clock::now();
        if (operatorTree->first()) {
            while (operatorTree->next());
        }
        boost::chrono::duration<double> durationQ = boost::chrono::system_clock::now() - startQ;
        boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Runtime query: " << durationQ.count() * 1000 << "ms.";
        BOOST_LOG_TRIVIAL(info) << "Runtime total: " << duration.count() * 1000 << "ms.";
        if (jsonstats) {
            jsonstats->put("runtime", to_string(durationQ.count()));
            jsonstats->put("nresults", to_string(p->getPrintedRows()));

        }
        if (printstdout) {
            long nElements = p->getPrintedRows();
            BOOST_LOG_TRIVIAL(info) << "# rows = " << nElements;
        }
        delete operatorTree;
    }
    delete plangen;
}

void WebInterface::Server::readHeader(boost::system::error_code const &err,
        size_t bytes) {
    inter->setActive();

    ss << string(data_.get(), bytes);
    string tmpstring = ss.str();
    int pos = tmpstring.find("Content-Length:");
    if (pos != string::npos) {
        int endpos = tmpstring.find("\r\n\r\n", pos);
        string slenparams = tmpstring.substr(pos + 16, endpos - pos - 16);
        int lenparams = std::stoi(slenparams);
        int remsize = tmpstring.size() - endpos - 4;
        if (remsize < lenparams) {
            //I must keep reading ...
            acceptHandler(err);
            return;
        }
    }

    req = ss.str();
    //Get the page
    string page;
    string message = "";
    bool isjson = false;

    if (boost::starts_with(req, "POST")) {
        int pos = req.find("HTTP");
        string path = req.substr(5, pos - 6);
        if (path == "/sparql") {
            //Get the SPARQL query
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string printresults = _getValueParam(form, "print");
            string sparqlquery = _getValueParam(form, "query");
            //Decode the query
            CURL *curl;
            curl = curl_easy_init();
            if (curl) {
                int newlen = 0;
                char *un2 = curl_easy_unescape(curl, sparqlquery.c_str(),
                        sparqlquery.size(), &newlen);
                sparqlquery = string(un2, newlen);
                curl_free(un2);
                boost::algorithm::replace_all(sparqlquery, "+", " ");
                boost::algorithm::replace_all(sparqlquery, "\r\n", "\n");
            } else {
                throw 10;
            }
            curl_easy_cleanup(curl);

            //Execute the SPARQL query
            ptree pt;
            ptree vars;
            ptree bindings;
            ptree stats;
            bool jsonoutput = printresults == string("true");
            if (inter->program) {
                BOOST_LOG_TRIVIAL(info) << "Answering the SPARQL query with VLog ...";
                WebInterface::execSPARQLQuery(sparqlquery,
                        false,
                        inter->edb->getNTerms(),
                        *(inter->vloglayer.get()),
                        false,
                        jsonoutput,
                        &vars,
                        &bindings,
                        &stats);
            } else {
                BOOST_LOG_TRIVIAL(info) << "Answering the SPARQL query with Trident ...";
                WebInterface::execSPARQLQuery(sparqlquery,
                        false,
                        inter->edb->getNTerms(),
                        *(inter->tridentlayer.get()),
                        false,
                        jsonoutput,
                        &vars,
                        &bindings,
                        &stats);
            }
            pt.add_child("head.vars", vars);
            pt.add_child("results.bindings", bindings);
            pt.add_child("stats", stats);

            std::ostringstream buf;
            write_json(buf, pt, false);
            page = buf.str();
            isjson = true;
        } else if (path == "/lookup") {
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string id = _getValueParam(form, "id");
            //Lookup the value
            string value = lookup(id, *(inter->tridentlayer.get()));
            ptree pt;
            pt.put("value", value);
            std::ostringstream buf;
            write_json(buf, pt, false);
            page = buf.str();
            isjson = true;
        } else if (path == "/setup") {
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string srules = _getValueParam(form, "rules");
            string spremat = _getValueParam(form, "queries");
            string sauto = _getValueParam(form, "automat");
            int reasoningThreshold = 1000;
            int automatThreshold = 1000000; // microsecond timeout

            //Decode the data in the forms
            CURL *curl;
            curl = curl_easy_init();
            if (curl) {
                int newlen = 0;
                char *un2 = curl_easy_unescape(curl, srules.c_str(), srules.size(), &newlen);
                srules = string(un2, newlen);
                curl_free(un2);
                boost::algorithm::replace_all(srules, "+", " ");
                boost::algorithm::replace_all(srules, "\r\n", "\n");
                char *un3 = curl_easy_unescape(curl, spremat.c_str(), spremat.size(), &newlen);
                spremat = string(un3, newlen);
                curl_free(un3);
                boost::algorithm::replace_all(spremat, "+", " ");
                boost::algorithm::replace_all(spremat, "\r\n", "\n");

            } else {
                throw 10;
            }
            curl_easy_cleanup(curl);

            BOOST_LOG_TRIVIAL(info) << "Setting up the KB with the given rules ...";

            //Cleanup and install the EDB layer
            EDBConf conf(inter->edbFile);
            inter->edb = std::unique_ptr<EDBLayer>(new EDBLayer(conf, false));
            inter->setupTridentLayer();

            //Setup the program
            inter->program = std::unique_ptr<Program>(new Program(
                        inter->edb->getNTerms(), inter->edb.get()));
            inter->program->readFromString(srules);
            inter->program->sortRulesByIDBPredicates();
            //Set up the ruleset and perform the pre-materialization if necessary
            if (sauto != "") {
                //Automatic prematerialization
                timens::system_clock::time_point start = timens::system_clock::now();
                Materialization *mat = new Materialization();
                mat->guessLiteralsFromRules(*inter->program, *inter->edb.get());
                mat->getAndStorePrematerialization(*inter->edb.get(),
                        *inter->program,
                        true, automatThreshold);
                delete mat;
                boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                    - start;
                BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                    sec.count() * 1000 << " milliseconds";
            } else if (spremat != "") {
                timens::system_clock::time_point start = timens::system_clock::now();
                Materialization *mat = new Materialization();
                mat->loadLiteralsFromString(*inter->program, spremat);
                mat->getAndStorePrematerialization(*inter->edb.get(), *inter->program, false, ~0l);
                inter->program->sortRulesByIDBPredicates();
                delete mat;
                boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                    - start;
                BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                    sec.count() * 1000 << " milliseconds";
            }

            //Setup the VLogLayer
            inter->vloglayer = std::unique_ptr<VLogLayer>(new VLogLayer(*(inter->edb.get()),
                        *(inter->program),
                        reasoningThreshold, "TI", "TE"));
            page = "OK!";
        } else {
            page = "Error!";
        }
    } else if (boost::starts_with(req, "GET")) {
        //Get the page
        int pos = req.find("HTTP");
        string path = req.substr(4, pos - 5);
        if (path == "/refresh") {
            //Create JSON object
            ptree pt;
            long usedmem = (long)Utils::get_max_mem(); //Already in MB
            long totmem = Utils::getSystemMemory() / 1024 / 1024;
            long ramperc = (((double)usedmem / totmem) * 100);
            pt.put("ramperc", to_string(ramperc));
            pt.put("usedmem", to_string(usedmem));
            long time = inter->getDurationExecMs();
            pt.put("runtime", to_string(time));
            //Semi naiver details
            if (inter->getSemiNaiver()->isRunning())
                pt.put("finished", "false");
            else
                pt.put("finished", "true");
            size_t currentIteration = inter->getSemiNaiver()->getCurrentIteration();
            pt.put("iteration", currentIteration);
            pt.put("rule", inter->getSemiNaiver()->getCurrentRule());

            std::vector<StatsRule> outputrules =
                inter->getSemiNaiver()->
                getOutputNewIterations();
            string outrules = "";
            for (const auto &el : outputrules) {
                outrules += to_string(el.iteration) + "," +
                    to_string(el.derivation) + "," +
                    to_string(el.idRule) + "," +
                    to_string(el.timems) + ";";
            }
            outrules = outrules.substr(0, outrules.size() - 1);
            pt.put("outputrules", outrules);

            std::ostringstream buf;
            write_json(buf, pt, false);
            page = buf.str();
            isjson = true;

        } else if (path == "/refreshmem") {
            ptree pt;
            long usedmem = (long)Utils::get_max_mem(); //Already in MB
            long totmem = Utils::getSystemMemory() / 1024 / 1024;
            long ramperc = (((double)usedmem / totmem) * 100);
            pt.put("ramperc", to_string(ramperc));
            pt.put("usedmem", to_string(usedmem));

            std::ostringstream buf;
            write_json(buf, pt, false);
            page = buf.str();
            isjson = true;

        } else if (path == "/genopts") {
            ptree pt;
            long totmem = Utils::getSystemMemory() / 1024 / 1024;
            pt.put("totmem", to_string(totmem));
            pt.put("commandline", inter->getCommandLineArgs());
            pt.put("nrules", inter->getSemiNaiver()->getProgram()->getNRules());
            pt.put("rules", inter->getSemiNaiver()->getListAllRulesForJSONSerialization());
            pt.put("nedbs", inter->getSemiNaiver()->getProgram()->getNEDBPredicates());
            pt.put("nidbs", inter->getSemiNaiver()->getProgram()->getNIDBPredicates());
            std::ostringstream buf;
            write_json(buf, pt, false);
            page = buf.str();
            isjson = true;

        } else if (path == "/getmemcmd") {
            ptree pt;
            long totmem = Utils::getSystemMemory() / 1024 / 1024;
            pt.put("totmem", to_string(totmem));
            pt.put("commandline", inter->getCommandLineArgs());
            if (inter->tridentlayer.get()) {
                pt.put("tripleskb", to_string(inter->tridentlayer->getKB()->getSize()));
                pt.put("termskb", to_string(inter->tridentlayer->getKB()->getNTerms()));
            } else {
                pt.put("tripleskb", -1);
                pt.put("termskb", -1);
            }

            std::ostringstream buf;
            write_json(buf, pt, false);
            page = buf.str();
            isjson = true;

        } else if (path == "/getprograminfo") {
            ptree pt;
            if (inter->program) {
                pt.put("nrules", inter->program->getNRules());
                pt.put("nedb", inter->program->getNEDBPredicates());
                pt.put("nidb", inter->program->getNIDBPredicates());
            } else {
                pt.put("nrules", 0);
                pt.put("nedb", 0);
                pt.put("nidb", 0);
            }
            std::ostringstream buf;
            write_json(buf, pt, false);
            page = buf.str();
            isjson = true;

        } else if (path == "/sizeidbs") {
            ptree pt;
            std::vector<std::pair<string, std::vector<StatsSizeIDB>>> sizeIDBs = inter->getSemiNaiver()->getSizeIDBs();
            //Construct the string
            string flat = "";
            for (auto el : sizeIDBs) {

                string listderivations = "";
                for (const auto &stats : el.second) {
                    listderivations += to_string(stats.iteration) + "," +
                        to_string(stats.idRule) + "," +
                        to_string(stats.derivation) + ",";
                }
                listderivations = listderivations.substr(0, listderivations.size() - 1);

                flat += el.first + ";" + listderivations + ";";
            }
            flat = flat.substr(0, flat.size() - 1);
            pt.put("sizeidbs", flat);
            std::ostringstream buf;
            write_json(buf, pt, false);
            page = buf.str();
            isjson = true;

        } else if (path.size() > 1) {
            page = inter->getPage(path);
        }
    }

    if (page == "") {
        //return the main page
        page = inter->getDefaultPage();
    }
    if (isjson) {
        res = "HTTP/1.1 200 OK\r\nContent-Type: application/json\nContent-Length: " + to_string(page.size()) + "\r\n\r\n" + page;
    } else {    res = "HTTP/1.1 200 OK\r\nContent-Length: " + to_string(page.size()) + "\r\n\r\n" + page;
    }

    boost::asio::async_write(socket, boost::asio::buffer(res),
            boost::asio::transfer_all(),
            boost::bind(&Server::writeHandler,
                shared_from_this(),
                boost::asio::placeholders::error,
                boost::asio::placeholders::bytes_transferred));

    inter->setInactive();
}

void WebInterface::Server::writeHandler(const boost::system::error_code &err,
        std::size_t bytes) {
    socket.close();
    inter->connect();
};

string WebInterface::getDefaultPage() {
    return getPage("/index.html");
}

string WebInterface::getPage(string f) {
    if (cachehtml.count(f)) {
        return cachehtml.find(f)->second;
    }

    //Read the file (if any) and return it to the user
    string pathfile = dirhtmlfiles + "/" + f;
    if (boost::filesystem::exists(boost::filesystem::path(pathfile))) {
        //Read the content of the file
        BOOST_LOG_TRIVIAL(debug) << "Reading the content of " << pathfile;
        ifstream ifs(pathfile);
        stringstream sstr;
        sstr << ifs.rdbuf();
        string contentFile = sstr.str();
        //Replace WEB_PORT with the right port
        size_t index = 0;
        index = contentFile.find("WEB_PORT", index);
        if (index != std::string::npos)
            contentFile.replace(index, 8, webport);

        cachehtml.insert(make_pair(f, contentFile));
        return contentFile;
    }

    return "Error! I cannot find the page to show.";
}
#endif
