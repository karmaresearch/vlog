#ifdef WEBINTERFACE

#include <vlog/webinterface.h>
#include <vlog/materialization.h>
#include <vlog/seminaiver.h>
#include <vlog/ml/training.h>
#include <vlog/utils.h>
#include <vlog/ml/helper.h>

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
#include <trident/utils/httpclient.h>

#include <string>
#include <fstream>
#include <chrono>
#include <thread>
#include <regex>
#include <csignal>

WebInterface::WebInterface(
        ProgramArgs &vm, std::shared_ptr<SemiNaiver> sn, std::string htmlfiles,
        std::string cmdArgs, std::string edbfile) : vm(vm), sn(sn),
    dirhtmlfiles(htmlfiles), cmdArgs(cmdArgs),
    isActive(false),
    edbFile(edbfile),
    nthreads(1) {
        //Setup the EDB layer
        EDBConf conf(edbFile, true);
        edb = std::unique_ptr<EDBLayer>(new EDBLayer(conf, false));
        //If the database is a single RDF Graph, then we can query it without launching any program
        setupTridentLayer();
    }

void WebInterface::setupTridentLayer() {
    tridentlayer = std::unique_ptr<TridentLayer>();
    if (edb) {
        //Setup a TridentLayer (for queries without datalog)
        PredId_t p = edb->getFirstEDBPredicate();
        std::string typedb = edb->getTypeEDBPredicate(p);
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

static std::string _getValueParam(std::string req, std::string param) {
    int pos = req.find(param);
    if (pos == std::string::npos) {
        return "";
    } else {
        int postart = req.find("=", pos);
        int posend = req.find("&", pos);
        if (posend == std::string::npos) {
            return req.substr(postart + 1);
        } else {
            return req.substr(postart + 1, posend - postart - 1);
        }
    }
}

std::string WebInterface::lookup(std::string sId, DBLayer &db) {
    const char *start;
    const char *end;
    unsigned id = stoi(sId);
    ::Type::ID type;
    unsigned st;
    db.lookupById(id, start, end, type, st);
    return std::string(start, end - start);
}


void WebInterface::getResultsQueryLiteral(std::string predicate, long limit, JSON &out) {
    long nresults = 0;
    long nshownresults = 0;
    JSON data;
    if (program != NULL && sn != NULL) {
        Predicate pred = program->getPredicate(predicate);
        nresults = sn->getSizeTable(pred.getId());
        auto itr = sn->getTable(pred.getId());
        while (!itr.isEmpty() && (limit == -1 || nshownresults < limit)) {
            auto table = itr.getCurrentTable();
            auto tableItr = table->getIterator();
            auto card = table->getRowSize();
            while (tableItr->hasNext() && (limit == -1 || nshownresults < limit)) {
                tableItr->next();
                JSON row;
                for(int j = 0; j < card; ++j) {
                    auto termId = tableItr->getCurrentValue(j);
                    auto txtTerm = edb->getDictText(termId);
                    row.push_back(txtTerm);
                }
                data.push_back(row);
                nshownresults++;
            }
            table->releaseIterator(tableItr);
            itr.moveNextCount();
        }
    }
    out.add_child("rows", data);
    out.put("nresults", nresults);
    out.put("nshownresults", nshownresults);
}



void WebInterface::processRequest(std::string req, std::string &resp) {
    setActive();
    //Get the page
    std::string page;
    bool isjson = false;
    int error = 0;
    if (Utils::starts_with(req, "POST")) {
        int pos = req.find("HTTP");
        std::string path = req.substr(5, pos - 6);
        if (path == "/sparql") {
            //Get the SPARQL query
            std::string form = req.substr(req.find("application/x-www-form-urlencoded"));
            std::string printresults = _getValueParam(form, "print");
            std::string sparqlquery = _getValueParam(form, "query");
            //Decode the query
            sparqlquery = HttpClient::unescape(sparqlquery);
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
            bool jsonoutput = printresults == std::string("true");
            if (program) {
                LOG(INFOL) << "Answering the SPARQL query with VLog ...";
                VLogUtils::execSPARQLQuery(sparqlquery,
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
                VLogUtils::execSPARQLQuery(sparqlquery,
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
            page = buf.str();
            isjson = true;
        } else if (path == "/gentq") {
            //Get all query
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string queries = _getValueParam(form, "query");
            string timeoutStr = _getValueParam(form, "timeout");
            string repeatQueryStr = _getValueParam(form, "repeatQuery");
            string maxTrainingStr = _getValueParam(form, "maxTraining");
            // 1. generate queries and run them and train model
            EDBConf conf(edbFile);
            int depth = 5;
            uint64_t maxTuples = 50;
            uint8_t vt1 = 1;
            uint8_t vt2 = 2;
            uint8_t vt3 = 3;
            uint8_t vt4 = 4;
            std::vector<uint8_t> vt;
            vt.push_back(vt1);
            vt.push_back(vt2);
            vt.push_back(vt3);
            vt.push_back(vt4);
            LOG(INFOL) << "Generating training queries: ";
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            std::vector<std::pair<std::string,int>> trainingQueries = Training::generateTrainingQueriesAllPaths(conf,
                    *edb.get(),
                    *program.get(),
                    depth,
                    maxTuples,
                    vt);
            std::chrono::duration<double> sec = std::chrono::system_clock::now()- start;
            int nQueries = trainingQueries.size();
            LOG(INFOL) << nQueries << " queries generated in " << sec.count() << " seconds";
            vector<string> trainingQueriesVector;
            int nMaxTrainingQueries = stoi(maxTrainingStr);//nQueries;
            if (nMaxTrainingQueries < 0) {
                nMaxTrainingQueries = nQueries;
            }
            LOG(INFOL) << "Max training queries : " << nMaxTrainingQueries;
            int i = 0;
            vector<int> queryIndexes(nMaxTrainingQueries);
            getRandomTupleIndexes(nMaxTrainingQueries, nQueries, queryIndexes);
            LOG(INFOL) << "query indexes received : " << queryIndexes.size();
            for (auto qi: queryIndexes) {
                trainingQueriesVector.push_back(trainingQueries[qi].first);
            }
            // Add a few generic queries in training
            int nIDBpredicates = program->getAllIDBPredicateIds().size();
            LOG(INFOL) << "# of IDB predicates = " << nIDBpredicates;
            int nGenericQueries = nIDBpredicates/4;
            if (nGenericQueries > 100) {
                nGenericQueries = 100;
            }
            //LOG(INFOL) << "# of generic queries that will be added = "<< nGenericQueries;
            // We know that generic queries are all at the end of trainingQueries vector
            // because generateNewTrainingQueries() adds them to the end
            //int cntAdded = 0;
            //for (int i = trainingQueries.size()-1; i >= 0; --i) {
            //    // add the generic query only if it is not already present
            //    if (std::find(trainingQueriesVector.begin(), trainingQueriesVector.end(), trainingQueries[i].first) ==
            //        trainingQueriesVector.end()) {
            //        trainingQueriesVector.push_back(trainingQueries[i].first);
            //    }
            //    if(++cntAdded > nGenericQueries) {
            //        break;
            //    }
            //}
            LOG(INFOL) << "training queries ready";
            // 2. test the model against test queries

            uint64_t timeout = stoull(timeoutStr);
            uint8_t repeatQuery = stoul(repeatQueryStr);
            //Decode the query
            queries = HttpClient::unescape(queries);
            std::regex e1("\\+");
            std::string replacedString;
            std::regex_replace(std::back_inserter(replacedString),
                    queries.begin(), queries.end(),
                    e1, "$1 ");
            queries = replacedString;
            std::regex e2("\\r\\n");
            replacedString = "";
            std::regex_replace(std::back_inserter(replacedString),
                    queries.begin(), queries.end(), e2, "$1\n");
            queries = replacedString;
            vector<string> testQueriesLog;
            stringstream ss(queries);
            string logLine;

            while (std::getline(ss, logLine, '\n')) {
                testQueriesLog.push_back(logLine);
            }
            LOG(INFOL) << "test Queries at web interface = " << testQueriesLog.size();
            double accuracy = 0.0;
            string logFileName = "training-queries.datalog";
            if (program) {
                Training::trainAndTestModel(trainingQueriesVector,
                        testQueriesLog,
                        *edb.get(),
                        *program.get(),
                        accuracy,
                        timeout,
                        repeatQuery,
                        logFileName,
                        5);
            }
            // 3. Set the output in json
            JSON node;
            node.put("accuracy", accuracy);
            std::ostringstream buf;
            JSON::write(buf, node);
            page = buf.str();
            isjson = true;
        } else if (path == "/query") {

            //Get all query
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string queries = _getValueParam(form, "query");
            string timeoutStr = _getValueParam(form, "timeout");
            string repeatQueryStr = _getValueParam(form, "repeatQuery");

            uint64_t timeout = stoull(timeoutStr);
            uint8_t repeatQuery = stoul(repeatQueryStr);
            //Decode the query
            queries = HttpClient::unescape(queries);
            std::regex e1("\\+");
            std::string replacedString;
            std::regex_replace(std::back_inserter(replacedString),
                    queries.begin(), queries.end(),
                    e1, "$1 ");
            queries = replacedString;
            std::regex e2("\\r\\n");
            replacedString = "";
            std::regex_replace(std::back_inserter(replacedString),
                    queries.begin(), queries.end(), e2, "$1\n");
            queries = replacedString;
            vector<string> queryVector;
            stringstream ss(queries);
            string query;

            while (std::getline(ss, query, '\n')) {
                queryVector.push_back(query);
            }

            JSON node;
            JSON queryResults;
            JSON queryFeatures;
            JSON queryQsqrTimes;
            JSON queryMagicTimes;
            if (program) {
                Training::execLiteralQueries(queryVector,
                        *edb.get(),
                        *program.get(),
                        &queryResults,
                        &queryFeatures,
                        &queryQsqrTimes,
                        &queryMagicTimes,
                        timeout,
                        repeatQuery);
            }
            node.add_child("results", queryResults);
            node.add_child("features", queryFeatures);
            node.add_child("qsqrtimes", queryQsqrTimes);
            node.add_child("magictimes", queryMagicTimes);
            std::ostringstream buf;
            JSON::write(buf, node);
            page = buf.str();
            isjson = true;

        } else if (path == "/lookup") {
            std::string form = req.substr(req.find("application/x-www-form-urlencoded"));
            std::string id = _getValueParam(form, "id");
            //Lookup the value
            std::string value = lookup(id, *(tridentlayer.get()));
            JSON pt;
            pt.put("value", value);
            std::ostringstream buf;
            JSON::write(buf, pt);
            //write_json(buf, pt, false);
            page = buf.str();
            isjson = true;
        } else if (path == "/queryliteral") {
            string form = req.substr(req.find("application/x-www-form-urlencoded"));
            string predicate = _getValueParam(form, "predicate");
            string slimit = _getValueParam(form, "limit");
            JSON pt;
            pt.put("predicate", predicate);
            long limit = -1;
            if (slimit != "") {
                limit = stoi(slimit);
            }
            getResultsQueryLiteral(predicate, limit, pt);
            std::ostringstream buf;
            JSON::write(buf, pt);
            page = buf.str();
            isjson = true;

        } else if (path == "/setup") {
            std::string form = req.substr(req.find("application/x-www-form-urlencoded"));
            std::string srules = _getValueParam(form, "rules");
            std::string spremat = _getValueParam(form, "queries");
            std::string sauto = _getValueParam(form, "automat");
            int automatThreshold = 1000000; // microsecond timeout

            srules = HttpClient::unescape(srules);
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

            spremat = HttpClient::unescape(spremat);
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
            EDBConf conf(edbFile, true);
            edb = std::unique_ptr<EDBLayer>(new EDBLayer(conf, false));
            setupTridentLayer();

            //Setup the program
            program = std::unique_ptr<Program>(new Program(edb.get()));
            std::string s = program->readFromString(srules, vm["rewriteMultihead"].as<bool>());
            if (s != "") {
                error = 1;
                page = s;
            } else {
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
            }
        } else {
            page = "Error!";
        }
    } else if (Utils::starts_with(req, "GET")) {
        //Get the page
        int pos = req.find("HTTP");
        std::string path = req.substr(4, pos - 5);
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
            std::string outrules = "";
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
                    bool multithreaded = vm["multithreaded"].as<bool>();
                    sn = Reasoner::getSemiNaiver(*edb.get(),
                            program.get(), ! vm["no-intersect"].as<bool>(),
                            ! vm["no-filtering"].as<bool>(),
                            multithreaded,
                            vm["restrictedChase"].as<bool>()
                            ? TypeChase::RESTRICTED_CHASE : TypeChase::SKOLEM_CHASE,
                            multithreaded ? vm["nthreads"].as<int>() : -1,
                            multithreaded ? vm["interRuleThreads"].as<int>() : 0,
                            vm["shufflerules"].as<bool>());
                    cvMatRunner.notify_one(); //start the computation
                    page = getPage("/mat/infobox.html");
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
            std::string flat = "";
            for (auto el : sizeIDBs) {

                std::string listderivations = "";
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

    std::string code = "200 OK ";
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

std::string WebInterface::getDefaultPage() {
    return getPage("/index.html");
}

std::string WebInterface::getPage(std::string f) {
    if (cachehtml.count(f)) {
        return cachehtml.find(f)->second;
    }

    //Read the file (if any) and return it to the user
    std::string pathfile = dirhtmlfiles + "/" + f;
    if (Utils::exists(pathfile)) {
        //Read the content of the file
        LOG(DEBUGL) << "Reading the content of " << pathfile;
        ifstream ifs(pathfile);
        std::stringstream sstr;
        sstr << ifs.rdbuf();
        std::string contentFile = sstr.str();
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
