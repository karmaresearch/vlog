//VLog
#include <vlog/reasoner.h>
#include <vlog/materialization.h>
#include <vlog/seminaiver.h>
#include <vlog/edbconf.h>
#include <vlog/edb.h>
#include <vlog/webinterface.h>
#include <vlog/fcinttable.h>
#include <vlog/exporter.h>
#include <vlog/utils.h>
#include <vlog/ml/ml.h>

//Incremental, will probably move away
#include <vlog/inmemory/inmemorytable.h>
#include <vlog/incremental/edb-table-from-idb.h>
#include <vlog/incremental/incremental-concepts.h>

//Used to load a Trident KB
#include <vlog/trident/tridenttable.h>
#include <launcher/vloglayer.h>
#include <trident/loader.h>
#include <kognac/utils.h>
#include <kognac/progargs.h>
#include <layers/TridentLayer.hpp>
#include <trident/utils/tridentutils.h>

//RDF3X
#include <cts/parser/SPARQLLexer.hpp>
#include <cts/parser/SPARQLParser.hpp>
#include <cts/infra/QueryGraph.hpp>
#include <cts/semana/SemanticAnalysis.hpp>
#include <cts/plangen/PlanGen.hpp>
#include <cts/codegen/CodeGen.hpp>
#include <rts/runtime/Runtime.hpp>
#include <rts/runtime/QueryDict.hpp>
#include <rts/operator/Operator.hpp>
#include <rts/operator/PlanPrinter.hpp>
#include <rts/operator/ResultsPrinter.hpp>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <chrono>
#include <thread>
#include <cmath>

// #include <valgrind/callgrind.h>

using namespace std;

void printHelp(const char *programName, ProgramArgs &desc) {
    cout << "Usage: " << programName << " <command> [options]" << endl << endl;
    cout << "Possible commands:" << endl;
    cout << "help\t\t produce help message." << endl;
    cout << "mat\t\t perform a full materialization." << endl;
    cout << "query\t\t execute a SPARQL query." << endl;
    cout << "queryLiteral\t\t execute a Literal query." << endl;
    cout << "server\t\t starts in server mode." << endl;
    cout << "load\t\t load a Trident KB." << endl;
    cout << "gentq\t\t generate training queries from rules file." << endl;
    cout << "lookup\t\t lookup for values in the dictionary." << endl << endl;

    cout << desc.tostring() << endl;
}

inline void printErrorMsg(const char *msg) {
    cout << endl << "*** ERROR: " << msg << "***" << endl << endl
        << "Please type the subcommand \"help\" for instructions (e.g. Vlog help)."
        << endl;
}

bool checkParams(ProgramArgs &vm, int argc, const char** argv) {

    string cmd;
    if (argc < 2) {
        printErrorMsg("Command is missing!");
        return false;
    } else {
        cmd = argv[1];
    }

    if (cmd != "help" && cmd != "query" && cmd != "lookup" && cmd != "load" && cmd != "queryLiteral"
            && cmd != "mat" && cmd != "rulesgraph" && cmd != "server" && cmd != "gentq") {
        printErrorMsg(
                (string("The command \"") + cmd + string("\" is unknown.")).c_str());
        return false;
    }

    if (cmd == "help") {
        printHelp(argv[0], vm);
        return false;
    } else {
        /*** Check specific parameters ***/
        if (cmd == "query" || cmd == "queryLiteral") {
            string queryFile = vm["query"].as<string>();
            if (cmd == "query" && (queryFile == ""  || !Utils::exists(queryFile))) {
                printErrorMsg(
                        (string("The file ") + queryFile
                         + string(" doesn't exist.")).c_str());
                return false;
            }

            if (vm["rules"].as<string>().compare("") != 0) {
                string path = vm["rules"].as<string>();
                if (!Utils::exists(path)) {
                    printErrorMsg((string("The rule file ") + path + string(" doe not exists")).c_str());
                    return false;
                }
            }
        } else if (cmd == "lookup") {
            if (!vm.count("text") && !vm.count("number")) {
                printErrorMsg(
                        "Neither the -t nor -n parameters are set. At least one of them must be set.");
                return false;
            }

            if (vm.count("text") && vm.count("number")) {
                printErrorMsg(
                        "Both the -t and -n parameters are set, and this is ambiguous. Please choose either one or the other.");
                return false;
            }
        } else if (cmd == "load") {
            if (!vm.count("input") && !vm.count("comprinput")) {
                printErrorMsg(
                        "The parameter -i (path to the triple files) is not set. Also --comprinput (file with the compressed triples) is not set.");
                return false;
            }

            if (vm.count("comprinput")) {
                string tripleDir = vm["comprinput"].as<string>();
                if (!Utils::exists(tripleDir)) {
                    printErrorMsg(
                            (string("The file ") + tripleDir
                             + string(" does not exist.")).c_str());
                    return false;
                }
                if (!vm.count("comprdict")) {
                    printErrorMsg(
                            "The parameter -comprdict (path to the compressed dict) is not set.");
                    return false;
                }
            } else {
                string tripleDir = vm["input"].as<string>();
                if (!Utils::exists(tripleDir)) {
                    printErrorMsg(
                            (string("The path ") + tripleDir
                             + string(" does not exist.")).c_str());
                    return false;
                }
            }

            if (!vm.count("output")) {
                printErrorMsg(
                        "The parameter -o (path to the kb is not set.");
                return false;
            }
            string kbdir = vm["output"].as<string>();
            if (Utils::exists(kbdir)) {
                printErrorMsg(
                        (string("The path ") + kbdir
                         + string(" already exist. Please remove it or choose another path.")).c_str());
                return false;
            }
            if (vm["maxThreads"].as<int>() < 1) {
                printErrorMsg(
                        "The number of threads to use must be at least 1");
                return false;
            }

            if (vm["readThreads"].as<int>() < 1) {
                printErrorMsg(
                        "The number of threads to use to read the input must be at least 1");
                return false;
            }

        } else if (cmd == "gentq") {
            if (vm["rules"].as<string>().compare("") != 0) {
                string path = vm["rules"].as<string>();
                if (!Utils::exists(path)) {
                    printErrorMsg((string("The rule file ") + path + string(" doe not exists")).c_str());
                    return false;
                }
            }
        } else if (cmd == "mat") {
            string path = vm["rules"].as<string>();
            if (path == "") {
                printErrorMsg(string("You must set up the 'rules' parameter to launch the materialization").c_str());
            }
            if (path != "" && !Utils::exists(path)) {
                printErrorMsg((string("The rule file '") +
                            path + string("' does not exists")).c_str());
                return false;
            }
            if (! vm["rm"].empty()) {
                std::string filename = vm["rm"].as<string>();
                if (!Utils::exists(filename)) {
                    printErrorMsg((string("The remove-literals file ") +
                                   filename +
                                   string(" does not exist")).c_str());
                    return false;
                }
            }
        }
    }

    return true;
}

bool initParams(int argc, const char** argv, ProgramArgs &vm) {

    ProgramArgs::GroupArgs& query_options = *vm.newGroup("Options for <query>, <queryLiteral> or <mat>");
    query_options.add<string>("q", "query", "",
            "The path of the file with a query. It is REQUIRED with <query> or <queryLiteral>", false);
    query_options.add<string>("","rules", "",
            "Activate reasoning during query answering using the rules defined at this path. It is REQUIRED in case the command is <mat>. Default is '' (disabled).", false);
    query_options.add<bool>("", "rewriteMultihead", false,
            "try to split up rules with multiple heads.", false);
    query_options.add<int64_t>("", "reasoningThreshold", 1000000,
            "This parameter sets a threshold to estimate the reasoning cost of a pattern. This cost can be broadly associated to the cardinality of the pattern. It is used to choose either TopDown or Magic evalution. Default is 1000000 (1M).", false);
    query_options.add<string>("", "reasoningAlgo", "",
            "Determines the reasoning algo (only for <queryLiteral>). Possible values are \"qsqr\", \"magic\", \"auto\".", false);
    query_options.add<string>("", "selectionStrategy", "",
            "Determines the selection strategy (only for <queryLiteral>, when \"auto\" is specified for the reasoningAlgorithm). Possible values are \"cardEst\", ... (to be extended) .", false);
    query_options.add<int64_t>("", "matThreshold", 10000000,
            "In case reasoning is activated, this parameter sets a threshold above which a full materialization is performed before we execute the query. Default is 10000000 (10M).", false);
    query_options.add<bool>("", "printResults", true,
            "Print the answers of a literal query.", false);
    query_options.add<bool>("", "automat", false,
            "Automatically premateralialize some atoms.", false);
    query_options.add<bool>("", "printRepresentationSize", false,
            "Print the representation size of the materialization.", false);
    query_options.add<int>("", "timeoutPremat", 1000000,
            "Timeout used during automatic prematerialization (in microseconds). Default is 1000000 (i.e. one second per query)", false);
    query_options.add<string>("", "premat", "",
            "Pre-materialize the atoms in the file passed as argument. Default is '' (disabled).", false);
    query_options.add<bool>("","multithreaded", false,
            "Run multithreaded (currently only supported for <mat>).", false);
    query_options.add<bool>("","restrictedChase", true,
            "Use the restricted chase if there are existential rules.", false);
    query_options.add<int>("", "nthreads", std::max((unsigned int)1, std::thread::hardware_concurrency() / 2),
            string("Set maximum number of threads to use when run in multithreaded mode. Default is " + to_string(std::max((unsigned int)1, std::thread::hardware_concurrency() / 2))).c_str(), false);
    query_options.add<int>("", "interRuleThreads", 0,
            "Set maximum number of threads to use for inter-rule parallelism. Default is 0", false);
    query_options.add<string>("", "rm", "",
            "File with facts to suppress (remove) from the EDB", false);
    query_options.add<string>("", "dred", "",
            "Directory with files with facts to add/remove from the EDB", false);
    query_options.add<string>("", "dred-rm", "",
            "file with facts to remove from the EDB", false);
    query_options.add<string>("", "dred-add", "",
            "file with facts to add to the EDB", false);

    query_options.add<bool>("", "shufflerules", false,
            "shuffle rules randomly instead of using heuristics (only for <mat>, and only when running multithreaded).", false);
    query_options.add<int>("r", "repeatQuery", 0,
            "Repeat the query <arg> times. If the argument is not specified, then the query will not be repeated.", false);
    query_options.add<string>("","storemat_path", "",
            "Directory where to store all results of the materialization. Default is '' (disable).",false);
    query_options.add<string>("","storemat_format", "files",
            "Format in which to dump the materialization. 'files' simply dumps the IDBs in files. 'csv' creates comma-separated files. 'db' creates a new RDF database. Default is 'files'.",false);
    query_options.add<bool>("","explain", false,
            "Explain the query instead of executing it. Default is false.",false);
    query_options.add<bool>("","decompressmat", false,
            "Decompress the results of the materialization when we write it to a file. Default is false.",false);
#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
    query_options.add<bool>("","monitorThread", false,
            "Launch an additional thread which prints statistics about resource usage on the console. Uses the DEBUL level so logging must be properly instructed. Default is false.",false);
#endif

#ifdef WEBINTERFACE
    query_options.add<bool>("","webinterface", false,
            "Start a web interface to monitor the execution. Default is false.",false);
    query_options.add<int>("","port", 8080, "Port to use for the web interface. Default is 8080",false);
#endif

    query_options.add<bool>("","no-filtering", false, "Disable filter optimization.",false);
    query_options.add<bool>("","no-intersect", false, "Disable intersection optimization.",false);
    query_options.add<string>("","graphfile", "", "Path to store the rule dependency graph",false);

    ProgramArgs::GroupArgs& load_options = *vm.newGroup("Options for <load>");
    load_options.add<string>("i","input", "",
            "Path to the files that contain the compressed triples. This parameter is REQUIRED if already compressed triples/dict are not provided.", false);
    load_options.add<string>("o","output", "",
            "Path to the KB that should be created. This parameter is REQUIRED.", false);
    load_options.add<int>("","maxThreads",
            Utils::getNumberPhysicalCores(),
            "Sets the maximum number of threads to use during the compression. Default is the number of physical cores",false);
    load_options.add<int>("","readThreads", 2,
            "Sets the number of concurrent threads that reads the raw input. Default is '2'",false);
    load_options.add<string>("","comprinput", "",
            "Path to a file that contains a list of compressed triples.",false);
    load_options.add<string>("","comprdict", "",
            "Path to a file that contains the dictionary for the compressed triples.",false);

    ProgramArgs::GroupArgs& lookup_options = *vm.newGroup("Options for <lookup>");
    lookup_options.add<string>("t","text", "",
            "Textual term to search", false);
    lookup_options.add<int64_t>("n","number", 0, "Numeric term to search",false);

    ProgramArgs::GroupArgs& server_options = *vm.newGroup("Options for <server>");
    server_options.add<string>("","webpages", "../webinterface",
            "Path to the webpages relative to where the executable is. Default is ../webinterface", false);

    ProgramArgs::GroupArgs& generateTraining_options = *vm.newGroup("Options for command <gentq>");
    generateTraining_options.add<int>("", "maxTuples", 500, "Number of EDB tuples to consider for training", false);
    generateTraining_options.add<int>("", "depth", 5, "Recursion level of training generation procedure", false);

    ProgramArgs::GroupArgs& cmdline_options = *vm.newGroup("Parameters");
    cmdline_options.add<string>("l","logLevel", "info",
            "Set the log level (accepted values: trace, debug, info, warning, error, fatal). Default is info.", false);

    cmdline_options.add<string>("e", "edb", "default",
            "Path to the edb conf file. Default is 'edb.conf' in the same directory as the exec file.",false);
    cmdline_options.add<int>("","sleep", 0, "sleep <arg> seconds before starting the run. Useful for attaching profiler.",false);

    vm.parse(argc, argv);
    return checkParams(vm, argc, argv);
}

void lookup(EDBLayer &layer, ProgramArgs &vm) {
    if (vm.count("text")) {
        uint64_t value;
        string textTerm = vm["text"].as<string>();
        if (!layer.getDictNumber((char*) textTerm.c_str(), textTerm.size(), value)) {
            cout << "Term " << textTerm << " not found" << endl;
        } else {
            cout << value << endl;
        }
    } else {
        int64_t key = vm["number"].as<int64_t>();
        char supportText[MAX_TERM_SIZE];
        if (!layer.getDictText(key, supportText)) {
            cout << "Term " << key << " not found" << endl;
        } else {
            cout << supportText << endl;
        }
    }
}

string flattenAllArgs(int argc,
        const char** argv) {
    string args = "";
    for (int i = 1; i < argc; ++i) {
        args += " " + string(argv[i]);
    }
    return args;
}

void writeRuleDependencyGraph(EDBLayer &db, string pathRules, string filegraph) {
    LOG(INFOL) << " Write the graph file to " << filegraph;
    Program p(&db);
    std::string s = p.readFromFile(pathRules, false);
    if (s != "") {
        LOG(ERRORL) << s;
        return;
    }
    std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(db,
            &p, true, true, false, false, 1, 1, false);

    std::vector<int> nodes;
    std::vector<std::pair<int, int>> edges;
    sn->createGraphRuleDependency(nodes, edges);

    //Write down the details of the graph on a file
    ofstream fout(filegraph);
    fout << "#nodes" << endl;
    for (auto el : nodes)
        fout << to_string(el) << endl;
    fout << "#edges" << endl;
    for (auto el : edges)
        fout << el.first << "\t" << el.second << endl;
    fout.close();
}

#ifdef WEBINTERFACE
void startServer(int argc,
        const char** argv,
        string pathExec,
        ProgramArgs &vm) {
    std::unique_ptr<WebInterface> webint;
    int port = vm["port"].as<int>();
    std::string webinterface = vm["webpages"].as<string>();
    webint = std::unique_ptr<WebInterface>(
            new WebInterface(vm, NULL, pathExec + "/" + webinterface,
                flattenAllArgs(argc, argv),
                vm["edb"].as<string>()));
    webint->start(port);
    LOG(INFOL) << "Server is launched at 0.0.0.0:" << to_string(port);
    webint->join();
}
#endif

static void store_mat(const std::string &path, ProgramArgs &vm,
                      const std::shared_ptr<SemiNaiver> sn) {
    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    Exporter exp(sn);

    string storemat_format = vm["storemat_format"].as<string>();

    if (storemat_format == "files" || storemat_format == "csv") {
        sn->storeOnFiles(path,
                vm["decompressmat"].as<bool>(), 0, storemat_format == "csv");
    } else if (storemat_format == "db") {
        //I will store the details on a Trident index
        exp.generateTridentDiffIndex(path);
    } else if (storemat_format == "nt") {
        exp.generateNTTriples(path, vm["decompressmat"].as<bool>());
    } else {
        LOG(ERRORL) << "Option 'storemat_format' not recognized";
        throw 10;
    }

    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Time to index and store the materialization on disk = " << sec.count() << " seconds";
}

void printRepresentationSize(std::shared_ptr<SemiNaiver> sn) {
    size_t size = 0;
    std::set<uint64_t> columnsIDs;
    for(size_t i = 0; i < MAX_NPREDS; ++i) {
        if (!sn->getProgram()->doesPredicateExist(i)) {
            continue;
        }
        FCIterator itr = sn->getTable(i);
        while (!itr.isEmpty()) {
            auto table = itr.getCurrentTable();
            //Get predicate name
            std::string predName = sn->getProgram()->getPredicateName(i);
            LOG(DEBUGL) << "Adding the representation size for " << i << " " << predName << " current size: " << size;
            size += table->getRepresentationSize(columnsIDs);
            itr.moveNextCount();
        }
    }
    LOG(INFOL) << "Representation size: " << size;
}

void launchFullMat(int argc,
        const char** argv,
        string pathExec,
        EDBLayer &db,
        ProgramArgs &vm,
        std::string pathRules) {
    //Load a program with all the rules
    Program p(&db);
    std::string s = p.readFromFile(pathRules,vm["rewriteMultihead"].as<bool>());
    if (s != "") {
        LOG(ERRORL) << s;
        return;
    }

    //Existential check
    if (p.areExistentialRules()) {
        LOG(INFOL) << "The program might not terminate due to existential rules ...";
    }

    //Set up the ruleset and perform the pre-materialization if necessary
    {
        if (!vm["automat"].empty()) {
            //Automatic prematerialization
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            Materialization mat;
            mat.guessLiteralsFromRules(p, db);
            mat.getAndStorePrematerialization(db, p, true,
                    vm["timeoutPremat"].as<int>());
            std::chrono::duration<double> sec = std::chrono::system_clock::now()
                - start;
            LOG(INFOL) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        } else if (vm["premat"].as<string>() != "") {
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            Materialization mat;
            mat.loadLiteralsFromFile(p, vm["premat"].as<string>());
            mat.getAndStorePrematerialization(db, p, false, ~0l);
            std::chrono::duration<double> sec = std::chrono::system_clock::now()
                - start;
            LOG(INFOL) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        }

        int nthreads = vm["nthreads"].as<int>();
        if (vm["multithreaded"].empty()) {
            nthreads = -1;
        }
        int interRuleThreads = vm["interRuleThreads"].as<int>();
        if (vm["multithreaded"].empty()) {
            interRuleThreads = 0;
        }

        //Prepare the materialization
        std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(db,
                &p, vm["no-intersect"].empty(),
                vm["no-filtering"].empty(),
                !vm["multithreaded"].empty(),
                vm["restrictedChase"].as<bool>(),
                nthreads,
                interRuleThreads,
                ! vm["shufflerules"].empty());

#ifdef WEBINTERFACE
        //Start the web interface if requested
        std::unique_ptr<WebInterface> webint;
        if (vm["webinterface"].as<bool>()) {
            webint = std::unique_ptr<WebInterface>(
                    new WebInterface(vm, sn, pathExec + "/webinterface",
                        flattenAllArgs(argc, argv),
                        vm["edb"].as<string>()));
            int port = vm["port"].as<int>();
            webint->start(port);
        }
#endif
        //Starting monitoring thread
#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
        std::thread monitor;
        std::mutex mtx;
        std::condition_variable cv;
        bool isFinished = false;
        if (vm["monitorThread"].as<bool>()) {
            //Activate it only for Linux systems
            monitor = std::thread(
                    std::bind(TridentUtils::monitorPerformance,
                        1, &cv, &mtx, &isFinished));
        }
#endif

        if (vm["dred"].empty()) {
            // CALLGRIND_START_INSTRUMENTATION;
        }

        LOG(INFOL) << "Starting full materialization";
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        sn->run();
        std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
        LOG(INFOL) << "Runtime materialization = " << sec.count() * 1000 << " milliseconds";
        sn->printCountAllIDBs("");

        if (! vm["dred"].empty()) {
            std::chrono::system_clock::time_point start;
            std::chrono::duration<double> sec;

            std::vector<std::string> remove_pred_names;
            if (vm["dred-rm"].empty()) {
                LOG(INFOL) << "use default remove table TE_remove";
                remove_pred_names.push_back("TE");
            } else {
                remove_pred_names.push_back(vm["dred-rm"].as<string>());
            }
            std::vector<std::string> add_pred_names;
            if (vm["dred-add"].empty()) {
                LOG(INFOL) << "use default add table TE_add";
                add_pred_names.push_back("TE");
            } else {
                add_pred_names.push_back(vm["dred-add"].as<string>());
            }

            LOG(INFOL) << "***************** Create Overdelete";

            IncrOverdelete overdelete(vm, sn, remove_pred_names);

            // CALLGRIND_START_INSTRUMENTATION;

            LOG(INFOL) << "Starting overdeletion materialization";
            start = std::chrono::system_clock::now();
            overdelete.run();
            sec = std::chrono::system_clock::now() - start;
            LOG(INFOL) << "Runtime overdelete = " << sec.count() * 1000 << " milliseconds";
            overdelete.getSN()->printCountAllIDBs("");

            if (vm["storemat_path"].as<string>() != "") {
                store_mat(vm["storemat_path"].as<string>() + ".overdelete", vm, overdelete.getSN());
            }

            if (false) {
                // Continue same with Rederive
                // Create a Program, create a SemiNaiver, run...
                LOG(INFOL) << "***************** Create Rederive";

                IncrRederive rederive(vm, sn, remove_pred_names, overdelete);

                LOG(INFOL) << "Starting rederive materialization";
                start = std::chrono::system_clock::now();
                rederive.run();
                sec = std::chrono::system_clock::now() - start;
                LOG(INFOL) << "Runtime rederive = " << sec.count() * 1000 << " milliseconds";
                rederive.getSN()->printCountAllIDBs("");

                if (vm["storemat_path"].as<string>() != "") {
                    store_mat(vm["storemat_path"].as<string>() + ".rederive", vm, rederive.getSN());
                }

                if (false) {
                    // Continue same with Addition
                    // Create a Program, create a SemiNaiver, run...
                    LOG(INFOL) << "***************** Create Addition";

                    IncrAdd addition(vm, sn, remove_pred_names, add_pred_names,
                                     overdelete, rederive);

                    LOG(INFOL) << "Starting addition materialization";
                    start = std::chrono::system_clock::now();
                    addition.run();
                    sec = std::chrono::system_clock::now() - start;
                    LOG(INFOL) << "Runtime addition = " << sec.count() * 1000 << " milliseconds";
                    addition.getSN()->printCountAllIDBs("");

                    if (vm["storemat_path"].as<string>() != "") {
                        store_mat(vm["storemat_path"].as<string>() + ".add", vm, addition.getSN());
                    }
                } else {
                    LOG(ERRORL) << "For now, ALSO SKIP ADDITION";
                }
            } else {
                LOG(ERRORL) << "For now, SKIP REDERIVE";
            }
        }

#if defined(__linux__) || defined(__linux) || defined(linux) || defined(__gnu_linux__)
        if (vm["monitorThread"].as<bool>()) {
            isFinished = true;
            LOG(INFOL) << "Waiting until logging thread is finished ...";
            monitor.join(); //Wait until the monitor thread is finished
        }
#endif

        if (vm["printRepresentationSize"].as<bool>()) {
            printRepresentationSize(sn);
        }


        if (vm["storemat_path"].as<string>() != "") {
            store_mat(vm["storemat_path"].as<string>(), vm, sn);
        }
#ifdef WEBINTERFACE
        if (webint) {
            //Sleep for max 1 second, to allow the fetching of the last statistics
            LOG(INFOL) << "Sleeping for one second to allow the web interface to get the last stats ...";
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            LOG(INFOL) << "Done.";
            webint->stop();
        }
#endif
    }
}

void execSPARQLQuery(EDBLayer &edb, ProgramArgs &vm) {
    //Parse the rules and create a program
    Program p(&edb);
    string pathRules = vm["rules"].as<string>();
    if (pathRules != "") {
        std::string s = p.readFromFile(pathRules,vm["rewriteMultihead"].as<bool>());
        if (s != "") {
            LOG(ERRORL) << s;
            return;
        }
        p.sortRulesByIDBPredicates();
    }

    //Set up the ruleset and perform the pre-materialization if necessary
    if (pathRules != "") {
        if (!vm["automat"].empty()) {
            //Automatic prematerialization
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            Materialization *mat = new Materialization();
            mat->guessLiteralsFromRules(p, edb);
            mat->getAndStorePrematerialization(edb, p, true,
                    vm["timeoutPremat"].as<int>());
            delete mat;
            std::chrono::duration<double> sec = std::chrono::system_clock::now()
                - start;
            LOG(INFOL) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        } else if (vm["premat"].as<string>() != "") {
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            Materialization *mat = new Materialization();
            mat->loadLiteralsFromFile(p, vm["premat"].as<string>());
            mat->getAndStorePrematerialization(edb, p, false, ~0l);
            p.sortRulesByIDBPredicates();
            delete mat;
            std::chrono::duration<double> sec = std::chrono::system_clock::now()
                - start;
            LOG(INFOL) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        }
    }

    DBLayer *db = NULL;
    if (pathRules == "") {
        PredId_t p = edb.getFirstEDBPredicate();
        string typedb = edb.getTypeEDBPredicate(p);
        if (typedb == "Trident") {
            auto edbTable = edb.getEDBTable(p);
            KB *kb = ((TridentTable*)edbTable.get())->getKB();
            TridentLayer *tridentlayer = new TridentLayer(*kb);
            tridentlayer->disableBifocalSampling();
            db = tridentlayer;
        }
    }
    if (db == NULL) {
        if (pathRules == "") {
            // Use default rule
            std::string s = p.readFromFile(pathRules,vm["rewriteMultihead"].as<bool>());
            if (s != "") {
                LOG(ERRORL) << s;
                return;
            }
            p.sortRulesByIDBPredicates();
        }
        db = new VLogLayer(edb, p, vm["reasoningThreshold"].as<int64_t>(), "TI", "TE");
    }
    string queryFileName = vm["query"].as<string>();
    // Parse the query
    std::fstream inFile;
    inFile.open(queryFileName);//open the input file
    std::stringstream strStream;
    strStream << inFile.rdbuf();//read the file

    VLogUtils::execSPARQLQuery(strStream.str(), vm["explain"].as<bool>(),
            edb.getNTerms(), *db, true, false, NULL, NULL,
            NULL);

    delete db;
}

string selectStrategy(EDBLayer &edb, Program &p, Literal &literal, Reasoner &reasoner, ProgramArgs &vm) {
    string strategy = vm["selectionStrategy"].as<string>();
    if (strategy == "" || strategy == "cardEst") {
        // Use the original cardinality estimation strategy
        ReasoningMode mode = reasoner.chooseMostEfficientAlgo(literal, edb, p, NULL, NULL);
        return mode == TOPDOWN ? "qsqr" : "magic";
    }
    // Add strategies here ...
    LOG(ERRORL) << "Unrecognized selection strategy: " << strategy;
    throw 10;
}

void runLiteralQuery(EDBLayer &edb, Program &p, Literal &literal, Reasoner &reasoner, ProgramArgs &vm) {

    std::chrono::system_clock::time_point startQ1 = std::chrono::system_clock::now();

    string algo = vm["reasoningAlgo"].as<string>();
    int times = vm["repeatQuery"].as<int>();
    bool printResults = vm["printResults"].as<bool>();

    int nVars = literal.getNVars();
    bool onlyVars = nVars > 0;

    if (literal.getPredicate().getType() == EDB) {
        if (algo != "edb") {
            LOG(INFOL) << "Overriding strategy, setting it to edb";
            algo = "edb";
        }
    }

    if (algo == "auto" || algo == "") {
        algo = selectStrategy(edb, p, literal, reasoner, vm);
        LOG(INFOL) << "Selection strategy determined that we go for " << algo;
    }

    TupleIterator *iter;

    if (algo == "edb") {
        iter = reasoner.getEDBIterator(literal, NULL, NULL, edb, onlyVars, NULL);
    } else if (algo == "magic") {
        iter = reasoner.getMagicIterator(literal, NULL, NULL, edb, p, onlyVars, NULL);
    } else if (algo == "qsqr") {
        iter = reasoner.getTopDownIterator(literal, NULL, NULL, edb, p, onlyVars, NULL);
    } else if (algo == "mat") {
        iter = reasoner.getMaterializationIterator(literal, NULL, NULL, edb, p, onlyVars, NULL);
    } else {
        LOG(ERRORL) << "Unrecognized reasoning algorithm: " << algo;
        throw 10;
    }
    long count = 0;
    int sz = iter->getTupleSize();
    if (nVars == 0) {
        cout << (iter->hasNext() ? "TRUE" : "FALSE") << endl;
        count = (iter->hasNext() ? 1 : 0);
    } else {
        while (iter->hasNext()) {
            iter->next();
            count++;
            if (printResults) {
                for (int i = 0; i < sz; i++) {
                    char supportText[MAX_TERM_SIZE];
                    uint64_t value = iter->getElementAt(i);
                    if (i != 0) {
                        cout << " ";
                    }
                    if (!edb.getDictText(value, supportText)) {
                        cerr << "Term " << value << " not found" << endl;
                        cout << value;
                    } else {
                        cout << supportText;
                    }
                }
                cout << endl;
            }
        }
    }
    std::chrono::duration<double> durationQ1 = std::chrono::system_clock::now() - startQ1;
    LOG(INFOL) << "Algo = " << algo << ", cold query runtime = " << (durationQ1.count() * 1000) << " msec, #rows = " << count;

    delete iter;
    if (times > 0) {
        // Redirect output
        ofstream file("/dev/null");
        streambuf* strm_buffer = cout.rdbuf();
        cout.rdbuf(file.rdbuf());
        std::chrono::system_clock::time_point startQ = std::chrono::system_clock::now();
        for (int j = 0; j < times; j++) {
            TupleIterator *iter = reasoner.getIterator(literal, NULL, NULL, edb, p, true, NULL);
            int sz = iter->getTupleSize();
            while (iter->hasNext()) {
                iter->next();
                if (printResults) {
                    for (int i = 0; i < sz; i++) {
                        char supportText[MAX_TERM_SIZE];
                        uint64_t value = iter->getElementAt(i);
                        if (i != 0) {
                            cout << ", ";
                        }
                        if (!edb.getDictText(value, supportText)) {
                            cout << value;
                        } else {
                            cout << supportText;
                        }
                    }
                    cout << endl;
                }
            }
        }
        std::chrono::duration<double> durationQ = std::chrono::system_clock::now() - startQ;
        //Restore stdout
        cout.rdbuf(strm_buffer);
        LOG(INFOL) << "Algo = " << algo << ", average warm query runtime = " << (durationQ.count() / times) * 1000 << " milliseconds";
    }
}

void execLiteralQuery(EDBLayer &edb, ProgramArgs &vm) {
    //Parse the rules and create a program
    Program p(&edb);
    string pathRules = vm["rules"].as<string>();
    if (pathRules != "") {
        std::string s = p.readFromFile(pathRules,vm["rewriteMultihead"].as<bool>());
        if (s != "") {
            LOG(ERRORL) << s;
            return;
        }
        p.sortRulesByIDBPredicates();
    }

    //Set up the ruleset and perform the pre-materialization if necessary
    if (pathRules != "") {
        if (!vm["automat"].empty()) {
            //Automatic prematerialization
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            Materialization *mat = new Materialization();
            mat->guessLiteralsFromRules(p, edb);
            mat->getAndStorePrematerialization(edb, p, true,
                    vm["timeoutPremat"].as<int>());
            delete mat;
            std::chrono::duration<double> sec = std::chrono::system_clock::now()
                - start;
            LOG(INFOL) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        } else if (vm["premat"].as<string>() != "") {
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            Materialization *mat = new Materialization();
            mat->loadLiteralsFromFile(p, vm["premat"].as<string>());
            mat->getAndStorePrematerialization(edb, p, false, ~0l);
            p.sortRulesByIDBPredicates();
            delete mat;
            std::chrono::duration<double> sec = std::chrono::system_clock::now()
                - start;
            LOG(INFOL) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        }
    }

    string query;
    string queryFileName = vm["query"].as<string>();
    if (Utils::exists(queryFileName)) {
        // Parse the query
        std::fstream inFile;
        inFile.open(queryFileName);//open the input file
        std::getline(inFile, query);
        inFile.close();
    } else {
        query = queryFileName;
    }
    Dictionary dictVariables;
    Literal literal = p.parseLiteral(query, dictVariables);
    Reasoner reasoner(vm["reasoningThreshold"].as<int64_t>());
    runLiteralQuery(edb, p, literal, reasoner, vm);
}

int main(int argc, const char** argv) {
    //Init params
    ProgramArgs vm;
    if (!initParams(argc, argv, vm)) {
        return EXIT_FAILURE;
    }
    string full_path = Utils::getFullPathExec();
    //Set logging level
    string ll = vm["logLevel"].as<string>();
    if (ll == "debug") {
        Logger::setMinLevel(DEBUGL);
    } else if (ll == "info") {
        Logger::setMinLevel(INFOL);
    } else if (ll == "warning") {
        Logger::setMinLevel(WARNL);
    } else if (ll == "error") {
        Logger::setMinLevel(ERRORL);
    }

    string cmd = string(argv[1]);

    //Get the path to the EDB layer
    string edbFile = vm["edb"].as<string>();
    if (edbFile == "default") {
        //Get current directory
        string execFile = string(argv[0]);
        string dirExecFile = Utils::parentDir(execFile);
        edbFile = dirExecFile + DIR_SEP + string("edb.conf");
    }

    if (cmd != "load" && !Utils::exists(edbFile)) {
        printErrorMsg(string("I could not find the EDB conf file " + edbFile).c_str());
        return EXIT_FAILURE;
    }

    //set up parallelism in the TBB library
    size_t parallelism = vm["nthreads"].as<int>();
    if (parallelism <= 1) {
        parallelism = 2;    // Otherwise tbb aborts.
        // Actual parallelism will be controlled elsewhere.
    }
    ParallelTasks::setNThreads(parallelism);

    // For profiling:
    int seconds = vm["sleep"].as<int>();
    if (seconds > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(seconds * 1000));
    }

    std::chrono::system_clock::time_point start = std::chrono::system_clock::now();

    LOG(DEBUGL) << "sizeof(EDBLayer) = " << sizeof(EDBLayer);

    if (cmd == "query" || cmd == "queryLiteral") {
        EDBConf conf(edbFile);
        EDBLayer *layer = new EDBLayer(conf, false);

        //Execute the query
        if (cmd == "query") {
            execSPARQLQuery(*layer, vm);
        } else {
            execLiteralQuery(*layer, vm);
        }
        delete layer;
    } else if (cmd == "lookup") {
        EDBConf conf(edbFile);
        EDBLayer *layer = new EDBLayer(conf, false);
        lookup(*layer, vm);
        delete layer;
    } else if (cmd == "mat") {
        EDBConf conf(edbFile);
        EDBLayer *layer = new EDBLayer(conf, ! vm["multithreaded"].empty());
        EDBRemoveLiterals *rm;
        if (! vm["rm"].empty()) {
            std::string path(vm["rm"].as<string>());
            LOG(ERRORL) << "FIXME: currently E has fixed name TE";
            PredId_t remove_pred = layer->getPredID("TE");
            InmemoryTable rmTable(Utils::parentDir(path), Utils::filename(path),
                                  remove_pred, layer);
            rm = new EDBRemoveLiterals(vm["rm"].as<string>(), layer);
            // rm = new EDBRemoveLiterals(rmTable, remove_pred, layer);
            // rm->dump(std::cerr, *layer);
            // Would like to move the thing i.s.o. copy RFHH
            std::unordered_map<PredId_t, const EDBRemoveLiterals *> rm_map;
            rm_map[remove_pred] = rm;
            layer->addRemoveLiterals(rm_map);
        }
        // EDBLayer layer(conf, false);
        launchFullMat(argc, argv, full_path, *layer, vm,
                vm["rules"].as<string>());
        delete layer;
        if (! vm["rm"].empty()) {
            LOG(ERRORL) << "FIXME: need to delete this RemoveLiterals";
            // delete rm;
        }
    } else if (cmd == "rulesgraph") {
        EDBConf conf(edbFile);
        EDBLayer *layer = new EDBLayer(conf, false);
        writeRuleDependencyGraph(*layer, vm["rules"].as<string>(),
                vm["graphfile"].as<string>());
        delete layer;
    } else if (cmd == "load") {
        Loader *loader = new Loader();
        bool onlyCompress = false;
        int sampleMethod = PARSE_COUNTMIN;
        string dictMethod = DICT_HEURISTICS;
        int popArg = 1000;
        int nindices = 6;
        bool aggrIndices = false;
        int fixedStrat = FIXEDSTRAT5;
        bool enableFixedStrat = true;
        bool storePlainList = false;
        double sampleRate = 0.01;
        bool sample = true;
        int ndicts = 1;
        bool canSkipTables = false;
        int thresholdSkipTable = 0;
        string popMethod = "hash";
        if (vm.count("comprinput")) {
            string comprinput = vm["comprinput"].as<string>();
            string comprdict = vm["comprdict"].as<string>();
            LOG(INFOL) << "Creating the KB from " << comprinput << "/" << comprdict;

            ParamsLoad p;
            p.inputformat = "rdf";
            p.onlyCompress = onlyCompress;
            p.inputCompressed = true;
            p.triplesInputDir =  vm["comprinput"].as<string>();
            p.dictDir = vm["comprdict"].as<string>();
            p.tmpDir = vm["output"].as<string>();
            p.kbDir = vm["output"].as<string>();
            p.dictMethod = dictMethod;
            p.sampleMethod = sampleMethod;
            p.sampleArg = popArg;
            p.parallelThreads = vm["maxThreads"].as<int>();
            p.maxReadingThreads = vm["readThreads"].as<int>();
            p.dictionaries = ndicts;
            p.nindices = nindices;
            p.createIndicesInBlocks = false;    // true not working???
            p.aggrIndices = aggrIndices;
            p.canSkipTables = canSkipTables;
            p.enableFixedStrat = enableFixedStrat;
            p.fixedStrat = fixedStrat;
            p.storePlainList = storePlainList;
            p.sample = sample;
            p.sampleRate = sampleRate;
            p.thresholdSkipTable = thresholdSkipTable;
            p.remoteLocation = "";
            p.limitSpace = 0;
            p.graphTransformation = "";
            p.timeoutStats = -1;
            p.storeDicts = true;
            p.relsOwnIDs = false;
            p.flatTree = false;

            loader->load(p);

        } else {
            LOG(INFOL) << "Creating the KB from " << vm["input"].as<string>();


            ParamsLoad p;
            p.inputformat = "rdf";
            p.onlyCompress = false;
            p.inputCompressed = false;
            p.triplesInputDir =  vm["input"].as<string>();
            p.dictDir = "";
            p.tmpDir = vm["output"].as<string>();
            p.kbDir = vm["output"].as<string>();
            p.dictMethod = dictMethod;
            p.sampleMethod = sampleMethod;
            p.sampleArg = popArg;
            p.parallelThreads = vm["maxThreads"].as<int>();
            p.maxReadingThreads = vm["readThreads"].as<int>();
            p.dictionaries = ndicts;
            p.nindices = nindices;
            p.createIndicesInBlocks = false;    // true not working???
            p.aggrIndices = aggrIndices;
            p.canSkipTables = canSkipTables;
            p.enableFixedStrat = enableFixedStrat;
            p.fixedStrat = fixedStrat;
            p.storePlainList = storePlainList;
            p.sample = sample;
            p.sampleRate = sampleRate;
            p.thresholdSkipTable = thresholdSkipTable;
            p.remoteLocation = "";
            p.limitSpace = 0;
            p.graphTransformation = "";
            p.timeoutStats = -1;
            p.storeDicts = true;
            p.relsOwnIDs = false;
            p.flatTree = false;

            loader->load(p);
        }
        delete loader;
    } else if (cmd == "gentq") {
        EDBConf conf(edbFile);
        string rulesFile = vm["rules"].as<string>();
        EDBLayer *layer = new EDBLayer(conf, false);
        Program p(layer);
        //uint8_t vt1 = (uint8_t) p.getIDVar("V1");
        //uint8_t vt2 = (uint8_t) p.getIDVar("V2");
        //uint8_t vt3 = (uint8_t) p.getIDVar("V3");
        //uint8_t vt4 = (uint8_t) p.getIDVar("V4");
        uint8_t vt1 = 0;
        uint8_t vt2 = 1;
        uint8_t vt3 = 2;
        uint8_t vt4 = 3;
        std::vector<uint8_t> vt;
        vt.push_back(vt1);
        vt.push_back(vt2);
        vt.push_back(vt3);
        vt.push_back(vt4);
        std::string s = p.readFromFile(rulesFile);
        if (s != "") {
            cerr << s << endl;
            return 1;
        }
        std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
        std::vector<std::pair<std::string,int>> trainingQueries = ML::generateTrainingQueries(*layer, p, vt, vm);
        std::chrono::duration<double> sec = std::chrono::system_clock::now()- start;
        int nQueries = trainingQueries.size();
        LOG(INFOL) << nQueries << " queries generated in " << sec.count() << " seconds";
        std::string trainingFileName = extractFileName(rulesFile);
        trainingFileName += "-training.log";
        std::ofstream logFile(trainingFileName);
        for (auto it = trainingQueries.begin(); it != trainingQueries.end(); ++it) {
            logFile << it->first <<":"<<it->second << std::endl;
        }
        if (logFile.fail()) {
            LOG(INFOL) << "Error writing to the log file";
        }
        logFile.close();
    } else if (cmd == "server") {
#ifdef WEBINTERFACE
        startServer(argc, argv, full_path, vm);
#else
        cerr << "The program is not compiled with the web interface activated." << endl;
        return 1;
#endif
    }
    std::chrono::duration<double> sec = std::chrono::system_clock::now() - start;
    LOG(INFOL) << "Runtime = " << sec.count() * 1000 << " milliseconds";

    //Print other stats
    LOG(INFOL) << "Max memory used: " << Utils::get_max_mem() << " MB";
    return EXIT_SUCCESS;
}
