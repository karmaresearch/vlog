#ifdef WEBINTERFACE

#include <vlog/webinterface.h>
#include <vlog/materialization.h>
#include <vlog/seminaiver.h>

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
#include <trident/utils/json.h>

#include <string>
#include <fstream>
#include <chrono>
#include <thread>
#include <regex>

WebInterface::WebInterface(
        ProgramArgs &vm, std::shared_ptr<SemiNaiver> sn, string htmlfiles,
        string cmdArgs, string edbfile) : vm(vm), sn(sn),
    dirhtmlfiles(htmlfiles), cmdArgs(cmdArgs),
    isActive(false),
    edbFile(edbfile),
    nthreads(1) {
        //Setup the EDB layer
        EDBConf conf(edbFile);
        edb = std::unique_ptr<EDBLayer>(new EDBLayer(conf, false));
        //If the database is a single RDF Graph, then we can query it without launching any program
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

void WebInterface::processMaterialization() {
    std::unique_lock<std::mutex> lck(mtxMatRunner);
    while (true) {
        cvMatRunner.wait(lck);
        if (!sn)
            break;
        sn->run();
    }
}

void WebInterface::startThread(int port) {
    this->webport = port;
    server->start();
}

void WebInterface::start(int port) {
    matRunner = std::thread(&WebInterface::processMaterialization, this);
    auto f = std::bind(&WebInterface::processRequest, this,
            std::placeholders::_1,
            std::placeholders::_2);
    server = std::shared_ptr<HttpServer>(new HttpServer(port,
                f, nthreads));
    this->webport = port;
    t = std::thread(&WebInterface::startThread, this, port);
}

void WebInterface::stop() {
    LOG(INFOL) << "Stopping server ...";
    while (isActive) {
        std::this_thread::sleep_for(chrono::milliseconds(100));
    }
    LOG(INFOL) << "Done";
}

long WebInterface::getDurationExecMs() {
    std::chrono::system_clock::time_point start = sn->getStartingTimeMs();
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    return std::chrono::duration_cast<std::chrono::milliseconds>(sec).count();
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
        std::shared_ptr<QueryGraph> &queryGraph,
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

    queryGraph = std::shared_ptr<QueryGraph>(new QueryGraph(parser.getVarCount()));

    // And perform the semantic anaylsis
    try {
        SemanticAnalysis semana(db, queryDict);
        semana.transform(parser, *queryGraph.get());
    } catch (const SemanticAnalysis::SemanticException& e) {
        cerr << "semantic error: " << e.message << endl;
        success = false;
        return;
    }
    if (queryGraph->knownEmpty()) {
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
        JSON *jsonvars,
        JSON *jsonresults,
        JSON *jsonstats) {
    std::unique_ptr<QueryDict> queryDict = std::unique_ptr<QueryDict>(new QueryDict(nterms));
    bool parsingOk;

    std::unique_ptr<SPARQLLexer> lexer =
        std::unique_ptr<SPARQLLexer>(new SPARQLLexer(sparqlquery));
    std::unique_ptr<SPARQLParser> parser = std::unique_ptr<SPARQLParser>(
            new SPARQLParser(*lexer.get()));
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
    std::shared_ptr<QueryGraph> queryGraph;
    parseQuery(parsingOk, *parser.get(), queryGraph, *queryDict.get(), db);
    if (!parsingOk) {
        std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
        LOG(INFOL) << "Runtime query: 0ms.";
        LOG(INFOL) << "Runtime total: " << duration.count() * 1000 << "ms.";
        LOG(INFOL) << "# rows = 0";
        return;
    }

    if (jsonvars) {
        //Copy the output of the query in the json vars
        for (QueryGraph::projection_iterator itr = queryGraph->projectionBegin();
                itr != queryGraph->projectionEnd(); ++itr) {
            string namevar = parser->getVariableName(*itr);
            JSON var;
            var.put("", namevar);
            jsonvars->push_back(var);
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

        std::chrono::system_clock::time_point startQ = std::chrono::system_clock::now();
        if (operatorTree->first()) {
            while (operatorTree->next());
        }
        std::chrono::duration<double> durationQ = std::chrono::system_clock::now() - startQ;
        std::chrono::duration<double> duration = std::chrono::system_clock::now() - start;
        LOG(INFOL) << "Runtime query: " << durationQ.count() * 1000 << "ms.";
        LOG(INFOL) << "Runtime total: " << duration.count() * 1000 << "ms.";
        if (jsonstats) {
            jsonstats->put("runtime", to_string(durationQ.count()));
            jsonstats->put("nresults", to_string(p->getPrintedRows()));

        }
        if (printstdout) {
            long nElements = p->getPrintedRows();
            LOG(INFOL) << "# rows = " << nElements;
        }
        delete operatorTree;
    }
    delete plangen;
}

void WebInterface::processRequest(std::string req, std::string &resp) {
    setActive();
    //Get the page
    string page;
    bool isjson = false;
    int error = 0;

    if (Utils::starts_with(req, "POST")) {
        int pos = req.find("HTTP");
        string path = req.substr(5, pos - 6);
        if (path == "/sparql") {
            //Get the SPARQL query
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string printresults = _getValueParam(form, "print");
            string sparqlquery = _getValueParam(form, "query");
            //Decode the query
            sparqlquery = HttpServer::unescape(sparqlquery);
            std::regex e1("\\+");
            std::string replacedString;
            std::regex_replace(std::back_inserter(replacedString),
                    sparqlquery.begin(), sparqlquery.end(),
                    e1, "$1 ");
            sparqlquery = replacedString;
            std::regex e2("\\r\\n");
            replacedString = "";
            std::regex_replace(std::back_inserter(replacedString),
                    sparqlquery.begin(), sparqlquery.end(), e2, "$1\n");
            sparqlquery = replacedString;

            //Execute the SPARQL query
            JSON pt;
            JSON vars;
            JSON bindings;
            JSON stats;
            bool jsonoutput = printresults == string("true");
            if (program) {
                LOG(INFOL) << "Answering the SPARQL query with VLog ...";
                WebInterface::execSPARQLQuery(sparqlquery,
                        false,
                        edb->getNTerms(),
                        *(vloglayer.get()),
                        false,
                        jsonoutput,
                        &vars,
                        &bindings,
                        &stats);
            } else {
                LOG(INFOL) << "Answering the SPARQL query with Trident ...";
                WebInterface::execSPARQLQuery(sparqlquery,
                        false,
                        edb->getNTerms(),
                        *(tridentlayer.get()),
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
            JSON::write(buf, pt);
            //write_json(buf, pt, false);
            page = buf.str();
            isjson = true;
        } else if (path == "/lookup") {
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string id = _getValueParam(form, "id");
            //Lookup the value
            string value = lookup(id, *(tridentlayer.get()));
            JSON pt;
            pt.put("value", value);
            std::ostringstream buf;
            JSON::write(buf, pt);
            //write_json(buf, pt, false);
            page = buf.str();
            isjson = true;
        } else if (path == "/setup") {
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string srules = _getValueParam(form, "rules");
            string spremat = _getValueParam(form, "queries");
            string sauto = _getValueParam(form, "automat");
            int automatThreshold = 1000000; // microsecond timeout

            srules = HttpServer::unescape(srules);
            std::regex e1("\\+");
            std::string replacedString;
            std::regex_replace(std::back_inserter(replacedString),
                    srules.begin(), srules.end(),
                    e1, "$1 ");
            srules = replacedString;
            std::regex e2("\\r\\n");
            replacedString = "";
            std::regex_replace(std::back_inserter(replacedString),
                    srules.begin(), srules.end(), e2, "$1\n");
            srules = replacedString;

            spremat = HttpServer::unescape(spremat);
            replacedString = "";
            std::regex_replace(std::back_inserter(replacedString),
                    spremat.begin(), spremat.end(),
                    e1, "$1 ");
            spremat = replacedString;
            replacedString = "";
            std::regex_replace(std::back_inserter(replacedString),
                    spremat.begin(), spremat.end(), e2, "$1\n");
            spremat = replacedString;

            LOG(INFOL) << "Setting up the KB with the given rules ...";

            //Cleanup and install the EDB layer
            EDBConf conf(edbFile);
            edb = std::unique_ptr<EDBLayer>(new EDBLayer(conf, false));
            setupTridentLayer();

            //Setup the program
            program = std::unique_ptr<Program>(new Program(
                        edb->getNTerms(), edb.get()));
            program->readFromString(srules, vm["rewriteMultihead"].as<bool>());
            program->sortRulesByIDBPredicates();
            //Set up the ruleset and perform the pre-materialization if necessary
            if (sauto != "") {
                //Automatic prematerialization
                std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
                Materialization *mat = new Materialization();
                mat->guessLiteralsFromRules(*program, *edb.get());
                mat->getAndStorePrematerialization(*edb.get(),
                        *program,
                        true, automatThreshold);
                delete mat;
                std::chrono::duration<double> sec = std::chrono::system_clock::now()
                    - start;
                LOG(INFOL) << "Runtime pre-materialization = " <<
                    sec.count() * 1000 << " milliseconds";
            } else if (spremat != "") {
                std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
                Materialization *mat = new Materialization();
                mat->loadLiteralsFromString(*program, spremat);
                mat->getAndStorePrematerialization(*edb.get(), *program, false, ~0l);
                program->sortRulesByIDBPredicates();
                delete mat;
                std::chrono::duration<double> sec = std::chrono::system_clock::now()
                    - start;
                LOG(INFOL) << "Runtime pre-materialization = " <<
                    sec.count() * 1000 << " milliseconds";
            }
            page = "OK!";
        } else {
            page = "Error!";
        }
    } else if (Utils::starts_with(req, "GET")) {
        //Get the page
        int pos = req.find("HTTP");
        string path = req.substr(4, pos - 5);
        if (path == "/refresh") {
            //Create JSON object
            JSON pt;
            long usedmem = (long)Utils::get_max_mem(); //Already in MB
            long totmem = Utils::getSystemMemory() / 1024 / 1024;
            long ramperc = (((double)usedmem / totmem) * 100);
            pt.put("ramperc", to_string(ramperc));
            pt.put("usedmem", to_string(usedmem));
            long time = getDurationExecMs();
            pt.put("runtime", to_string(time));
            //Semi naiver details
            if (getSemiNaiver()->isRunning())
                pt.put("finished", "false");
            else
                pt.put("finished", "true");
            size_t currentIteration = getSemiNaiver()->getCurrentIteration();
            pt.put("iteration", currentIteration);
            pt.put("rule", getSemiNaiver()->getCurrentRule());

            std::vector<StatsRule> outputrules =
                getSemiNaiver()->
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
            JSON::write(buf, pt);
            //write_json(buf, pt, false);
            page = buf.str();
            isjson = true;

        } else if (path == "/refreshmem") {
            JSON pt;
            long usedmem = (long)Utils::get_max_mem(); //Already in MB
            long totmem = Utils::getSystemMemory() / 1024 / 1024;
            long ramperc = (((double)usedmem / totmem) * 100);
            pt.put("ramperc", to_string(ramperc));
            pt.put("usedmem", to_string(usedmem));

            std::ostringstream buf;
            JSON::write(buf, pt);
            //write_json(buf, pt, false);
            page = buf.str();
            isjson = true;

        } else if (path == "/genopts") {
            JSON pt;
            long totmem = Utils::getSystemMemory() / 1024 / 1024;
            pt.put("totmem", to_string(totmem));
            pt.put("commandline", getCommandLineArgs());
            pt.put("nrules", (unsigned int) getSemiNaiver()->getProgram()->getNRules());
            ////obsolete
            //pt.put("rules", getSemiNaiver()->getListAllRulesForJSONSerialization());
            pt.put("nedbs", (unsigned int) getSemiNaiver()->getProgram()->getNEDBPredicates());
            pt.put("nidbs", (unsigned int) getSemiNaiver()->getProgram()->getNIDBPredicates());
            std::ostringstream buf;
            JSON::write(buf, pt);
            //write_json(buf, pt, false);
            page = buf.str();
            isjson = true;

        } else if (path == "/getmemcmd") {
            JSON pt;
            long totmem = Utils::getSystemMemory() / 1024 / 1024;
            pt.put("totmem", to_string(totmem));
            pt.put("commandline", getCommandLineArgs());
            /*if (tridentlayer.get()) {
              pt.put("tripleskb", to_string(tridentlayer->getKB()->getSize()));
              pt.put("termskb", to_string(tridentlayer->getKB()->getNTerms()));
              } else {
              pt.put("tripleskb", -1);
              pt.put("termskb", -1);
              }*/

            std::ostringstream buf;
            JSON::write(buf, pt);
            page = buf.str();
            isjson = true;

        } else if (path == "/getprograminfo") {
            JSON pt;
            JSON rules;
            if (program) {
                pt.put("nrules", (unsigned int) program->getNRules());
                pt.put("nedb", (unsigned int) program->getNEDBPredicates());
                pt.put("nidb", (unsigned int) program->getNIDBPredicates());
                int i = 0;
                for(auto &r : program->getAllRules()) {
                    if (r.getId() != i) {
                        throw 10;
                    }
                    rules.push_back(r.toprettystring(program.get(), edb.get()));
                    i++;
                }
            } else {
                pt.put("nrules", 0u);
                pt.put("nedb", 0u);
                pt.put("nidb", 0u);
            }
            pt.add_child("rules", rules);
            std::ostringstream buf;
            JSON::write(buf, pt);
            //write_json(buf, pt, false);
            page = buf.str();
            isjson = true;

        } else if (path == "/getedbinfo") {
            JSON pt;
            auto predicates = edb->getAllPredicateIDs();
            for(auto predid : predicates) {
                JSON entry;
                entry.put("name", edb->getPredName(predid));
                entry.put("size", (unsigned long) edb->getPredSize(predid));
                entry.put("arity", (unsigned int) edb->getPredArity(predid));
                entry.put("type", edb->getPredType(predid));
                pt.push_back(entry);
            }
            std::ostringstream buf;
            JSON::write(buf, pt);
            page = buf.str();
            isjson = true;

        } else if (path == "/launchMat") {
            //Start a materialization
            if (program) {
                if (!sn || !sn->isRunning()) {
                    bool multithreaded = !vm["multithreaded"].empty();
                    sn = Reasoner::getSemiNaiver(*edb.get(),
                            program.get(), vm["no-intersect"].empty(),
                            vm["no-filtering"].empty(),
                            multithreaded,
                            vm["restrictedChase"].as<bool>(),
                            multithreaded ? vm["nthreads"].as<int>() : -1,
                            multithreaded ? vm["interRuleThreads"].as<int>() : 0,
                            !vm["shufflerules"].empty());
                    cvMatRunner.notify_one(); //start the computation
                    page = getPage("/newmat.html");
                } else {
                    error = 1;
                    page = "Materialization is already running!";
                }
            } else {
                error = 1;
                page = "You first need to load the rules!";
            }

        } else if (path == "/sizeidbs") {
            JSON pt;
            std::vector<std::pair<string, std::vector<StatsSizeIDB>>> sizeIDBs = getSemiNaiver()->getSizeIDBs();
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
            JSON::write(buf, pt);
            //write_json(buf, pt, false);
            page = buf.str();
            isjson = true;

        } else if (path.size() > 1) {
            page = getPage(path);
        }
    }

    if (page == "") {
        //return the main page
        page = getDefaultPage();
    }

    string code = "200 OK ";
    if (error) {
        code = "500 ERROR ";
    }

    if (isjson) {
        resp = "HTTP/1.1 " + code + "\r\nContent-Type: application/json\nContent-Length: " + to_string(page.size()) + "\r\n\r\n" + page;
    } else {
        resp = "HTTP/1.1 " + code + "\r\nContent-Length: " + to_string(page.size()) + "\r\n\r\n" + page;
    }
    setInactive();
}

string WebInterface::getDefaultPage() {
    return getPage("/index.html");
}

string WebInterface::getPage(string f) {
    if (cachehtml.count(f)) {
        return cachehtml.find(f)->second;
    }

    //Read the file (if any) and return it to the user
    string pathfile = dirhtmlfiles + "/" + f;
    if (Utils::exists(pathfile)) {
        //Read the content of the file
        LOG(DEBUGL) << "Reading the content of " << pathfile;
        ifstream ifs(pathfile);
        stringstream sstr;
        sstr << ifs.rdbuf();
        string contentFile = sstr.str();
        //Replace WEB_PORT with the right port
        size_t index = 0;
        index = contentFile.find("WEB_PORT", index);
        if (index != std::string::npos)
            contentFile.replace(index, 8, to_string(webport));

        cachehtml.insert(make_pair(f, contentFile));
        return contentFile;
    }

    return "Error! I cannot find the page to show.";
}
#endif
