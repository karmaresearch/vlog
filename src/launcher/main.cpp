//VLog
#include <vlog/reasoner.h>
#include <vlog/materialization.h>
#include <vlog/seminaiver.h>
#include <vlog/edbconf.h>
#include <vlog/edb.h>
#include <vlog/webinterface.h>
#include <vlog/fcinttable.h>
#include <vlog/exporter.h>

//Used to load a Trident KB
#include <launcher/vloglayer.h>
#include <trident/loader.h>
#include <kognac/utils.h>

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

//Boost
#include <boost/chrono.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/program_options.hpp>
// #include <boost/sort/spreadsort/integer_sort.hpp>

//TBB
// Don't use global_control, to allow for older TBB versions.
// #define TBB_PREVIEW_GLOBAL_CONTROL 1
#include <tbb/task_scheduler_init.h>
// #include <tbb/global_control.h>

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <chrono>
#include <thread>

using namespace std;
namespace timens = boost::chrono;
namespace logging = boost::log;
namespace fs = boost::filesystem;
namespace po = boost::program_options;

void initLogging(logging::trivial::severity_level level) {
    logging::add_common_attributes();
    logging::add_console_log(std::cerr,
            logging::keywords::format =
            (logging::expressions::stream << "["
             << logging::expressions::attr <
             boost::log::attributes::current_thread_id::value_type > (
                 "ThreadID") << " "
             << logging::expressions::format_date_time <
             boost::posix_time::ptime > ("TimeStamp",
                 "%H:%M:%S") << " - "
             << logging::trivial::severity << "] "
             << logging::expressions::smessage));
    boost::shared_ptr<logging::core> core = logging::core::get();
    core->set_filter(logging::trivial::severity >= level);
}

void printHelp(const char *programName, po::options_description &desc) {
    cout << "Usage: " << programName << " <command> [options]" << endl << endl;
    cout << "Possible commands:" << endl;
    cout << "help\t\t produce help message." << endl;
    cout << "mat\t\t perform a full materialization." << endl;
    cout << "query\t\t execute a SPARQL query." << endl;
    cout << "queryLiteral\t\t execute a Literal query." << endl;
    cout << "server\t\t starts in server mode." << endl;
    cout << "load\t\t load a Trident KB." << endl;
    cout << "lookup\t\t lookup for values in the dictionary." << endl << endl;

    cout << desc << endl;
}

inline void printErrorMsg(const char *msg) {
    cout << endl << "*** ERROR: " << msg << "***" << endl << endl
        << "Please type the subcommand \"help\" for instructions (e.g. Vlog help)."
        << endl;
}

bool checkParams(po::variables_map &vm, int argc, const char** argv,
        po::options_description &desc) {

    string cmd;
    if (argc < 2) {
        printErrorMsg("Command is missing!");
        return false;
    } else {
        cmd = argv[1];
    }

    if (cmd != "help" && cmd != "query" && cmd != "lookup" && cmd != "load" && cmd != "queryLiteral"
            && cmd != "mat" && cmd != "rulesgraph" && cmd != "server") {
        printErrorMsg(
                (string("The command \"") + cmd + string("\" is unknown.")).c_str());
        return false;
    }

    if (cmd == "help") {
        printHelp(argv[0], desc);
        return false;
    } else {
        /*** Check specific parameters ***/
        if (cmd == "query" || cmd == "queryLiteral") {
            string queryFile = vm["query"].as<string>();
            if (cmd == "query" && (queryFile == ""  || !fs::exists(queryFile))) {
                printErrorMsg(
                        (string("The file ") + queryFile
                         + string(" doesn't exist.")).c_str());
                return false;
            }

            if (vm["rules"].as<string>().compare("") != 0) {
                string path = vm["rules"].as<string>();
                if (!fs::exists(path)) {
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
            if (!vm.count("input") and !vm.count("comprinput")) {
                printErrorMsg(
                        "The parameter -i (path to the triple files) is not set. Also --comprinput (file with the compressed triples) is not set.");
                return false;
            }

            if (vm.count("comprinput")) {
                string tripleDir = vm["comprinput"].as<string>();
                if (!fs::exists(tripleDir)) {
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
                if (!fs::exists(tripleDir)) {
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
            if (fs::exists(kbdir)) {
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

        } else if (cmd == "mat") {
            string path = vm["rules"].as<string>();
            if (path != "" && !fs::exists(path)) {
                printErrorMsg((string("The rule file '") +
                            path + string("' does not exists")).c_str());
                return false;
            }
        }
    }

    return true;
}

bool initParams(int argc, const char** argv, po::variables_map &vm) {

    po::options_description query_options("Options for <query>, <queryLiteral> or <mat>");
    query_options.add_options()("query,q", po::value<string>()->default_value(""),
            "The path of the file with a query. It is REQUIRED with <query> or <queryLiteral>");
    query_options.add_options()("rules", po::value<string>()->default_value(""),
            "Activate reasoning during query answering using the rules defined at this path. It is REQUIRED in case the command is <mat>. Default is '' (disabled).");
    query_options.add_options()("reasoningThreshold", po::value<long>()->default_value(1000000),
            "This parameter sets a threshold to estimate the reasoning cost of a pattern. This cost can be broadly associated to the cardinality of the pattern. It is used to choose either TopDown or Magic evalution. Default is 1000000 (1M).");
    query_options.add_options()("reasoningAlgo", po::value<string>()->default_value(""),
            "Determines the reasoning algo (only for <queryLiteral>). Possible values are \"qsqr\", \"magic\", \"auto\".");
    query_options.add_options()("selectionStrategy", po::value<string>()->default_value(""),
            "Determines the selection strategy (only for <queryLiteral>, when \"auto\" is specified for the reasoningAlgorithm). Possible values are \"cardEst\", ... (to be extended) .");
    query_options.add_options()("matThreshold", po::value<long>()->default_value(10000000),
            "In case reasoning is activated, this parameter sets a threshold above which a full materialization is performed before we execute the query. Default is 10000000 (10M).");
    query_options.add_options()("automat",
            "Automatically premateralialize some atoms.");
    query_options.add_options()("timeoutPremat", po::value<int>()->default_value(1000000),
            "Timeout used during automatic prematerialization (in microseconds). Default is 1000000 (i.e. one second per query)");
    query_options.add_options()("premat", po::value<string>()->default_value(""),
            "Pre-materialize the atoms in the file passed as argument. Default is '' (disabled).");
    query_options.add_options()("multithreaded",
            "Run multithreaded (currently only supported for <mat>).");
    query_options.add_options()("nthreads", po::value<int>()->default_value(tbb::task_scheduler_init::default_num_threads() / 2),
            string("Set maximum number of threads to use when run in multithreaded mode. Default is " + to_string(tbb::task_scheduler_init::default_num_threads() / 2)).c_str());
    query_options.add_options()("interRuleThreads", po::value<int>()->default_value(0),
            "Set maximum number of threads to use for inter-rule parallelism. Default is 0");

    query_options.add_options()("shufflerules",
            "shuffle rules randomly instead of using heuristics (only for <mat>, and only when running multithreaded).");
    query_options.add_options()("repeatQuery,r",
            po::value<int>()->default_value(0),
            "Repeat the query <arg> times. If the argument is not specified, then the query will not be repeated.");
    query_options.add_options()("storemat_path", po::value<string>()->default_value(""),
            "Directory where to store all results of the materialization. Default is '' (disable).");
    query_options.add_options()("storemat_format", po::value<string>()->default_value("files"),
            "Format in which to dump the materialization. 'files' simply dumps the IDBs in files. 'db' creates a new RDF database. Default is 'files'.");
    query_options.add_options()("explain", po::value<bool>()->default_value(false),
            "Explain the query instead of executing it. Default is false.");
    query_options.add_options()("decompressmat", po::value<bool>()->default_value(false),
            "Decompress the results of the materialization when we write it to a file. Default is false.");

#ifdef WEBINTERFACE
    query_options.add_options()("webinterface", po::value<bool>()->default_value(false),
            "Start a web interface to monitor the execution. Default is false.");
    query_options.add_options()("port", po::value<int>()->default_value(8080), "Port to use for the web interface. Default is 8080");
#endif

    query_options.add_options()("no-filtering",
            "Disable filter optimization.");
    query_options.add_options()("no-intersect",
            "Disable intersection optimization.");
    query_options.add_options()("graphfile", po::value<string>(),
            "Path to store the rule dependency graph");

    po::options_description load_options("Options for <load>");
    load_options.add_options()("input,i", po::value<string>(),
            "Path to the files that contain the compressed triples. This parameter is REQUIRED if already compressed triples/dict are not provided.");
    load_options.add_options()("output,o", po::value<string>(),
            "Path to the KB that should be created. This parameter is REQUIRED.");
    load_options.add_options()("maxThreads",
            po::value<int>()->default_value(Utils::getNumberPhysicalCores()),
            "Sets the maximum number of threads to use during the compression. Default is the number of physical cores");
    load_options.add_options()("readThreads",
            po::value<int>()->default_value(2),
            "Sets the number of concurrent threads that reads the raw input. Default is '2'");
    load_options.add_options()("comprinput", po::value<string>(),
            "Path to a file that contains a list of compressed triples.");
    load_options.add_options()("comprdict", po::value<string>()->default_value(""),
            "Path to a file that contains the dictionary for the compressed triples.");

    po::options_description lookup_options("Options for <lookup>");
    lookup_options.add_options()("text,t", po::value<string>(),
            "Textual term to search")("number,n", po::value<long>(),
                "Numeric term to search");

    po::options_description cmdline_options("Parameters");
    cmdline_options.add(query_options).add(lookup_options).add(load_options);
    cmdline_options.add_options()("logLevel,l", po::value<logging::trivial::severity_level>(),
            "Set the log level (accepted values: trace, debug, info, warning, error, fatal). Default is info.");

    cmdline_options.add_options()("edb,e", po::value<string>()->default_value("default"),
            "Path to the edb conf file. Default is 'edb.conf' in the same directory as the exec file.");
    cmdline_options.add_options()("sleep",
            po::value<int>()->default_value(0),
            "sleep <arg> seconds before starting the run. Useful for attaching profiler.");

    po::store(
            po::command_line_parser(argc, argv).options(cmdline_options).run(),
            vm);

    return checkParams(vm, argc, argv, cmdline_options);
}

void lookup(EDBLayer &layer, po::variables_map &vm) {
    if (vm.count("text")) {
        uint64_t value;
        string textTerm = vm["text"].as<string>();
        if (!layer.getDictNumber((char*) textTerm.c_str(), textTerm.size(), value)) {
            cout << "Term " << textTerm << " not found" << endl;
        } else {
            cout << value << endl;
        }
    } else {
        uint64_t key = vm["number"].as<long>();
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
    BOOST_LOG_TRIVIAL(info) << " Write the graph file to " << filegraph;
    Program p(db.getNTerms(), &db);
    p.readFromFile(pathRules);
    std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(db,
            &p, true, true, false, 1, 1, false);

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

void startServer(int argc,
        const char** argv,
        string pathExec,
        po::variables_map &vm) {
    std::unique_ptr<WebInterface> webint;
    int port = vm["port"].as<int>();
    webint = std::unique_ptr<WebInterface>(
            new WebInterface(NULL, pathExec + "/webinterface",
                flattenAllArgs(argc, argv),
                vm["edb"].as<string>()));
    webint->start("0.0.0.0", to_string(port));
    BOOST_LOG_TRIVIAL(info) << "Server is launched at 0.0.0.0:" << to_string(port);
    webint->join();
}

void launchFullMat(int argc,
        const char** argv,
        string pathExec,
        EDBLayer &db,
        po::variables_map &vm,
        std::string pathRules) {
    //Load a program with all the rules
    Program p(db.getNTerms(), &db);
    p.readFromFile(pathRules);

    //Set up the ruleset and perform the pre-materialization if necessary
    {
        if (!vm["automat"].empty()) {
            //Automatic prematerialization
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization mat;
            mat.guessLiteralsFromRules(p, db);
            mat.getAndStorePrematerialization(db, p, true,
                    vm["timeoutPremat"].as<int>());
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        } else if (vm["premat"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization mat;
            mat.loadLiteralsFromFile(p, vm["premat"].as<string>());
            mat.getAndStorePrematerialization(db, p, false, ~0l);
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
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

        //Execute the materialization
        std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(db,
                &p, vm["no-intersect"].empty(),
                vm["no-filtering"].empty(),
                ! vm["multithreaded"].empty(),
                nthreads,
                interRuleThreads,
                ! vm["shufflerules"].empty());

#ifdef WEBINTERFACE
        //Start the web interface if requested
        std::unique_ptr<WebInterface> webint;
        if (vm["webinterface"].as<bool>()) {
            webint = std::unique_ptr<WebInterface>(
                    new WebInterface(sn, pathExec + "/webinterface",
                        flattenAllArgs(argc, argv),
                        vm["edb"].as<string>()));
            int port = vm["port"].as<int>();
            webint->start("localhost", to_string(port));
        }
#endif

        BOOST_LOG_TRIVIAL(info) << "Starting full materialization";
        timens::system_clock::time_point start = timens::system_clock::now();
        sn->run();
        boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Runtime materialization = " << sec.count() * 1000 << " milliseconds";
        sn->printCountAllIDBs();

        if (vm["storemat_path"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();

            Exporter exp(sn);

            if (vm["storemat_format"].as<string>() == "files") {
                sn->storeOnFiles(vm["storemat_path"].as<string>(),
                        vm["decompressmat"].as<bool>(), 0);
            } else if (vm["storemat_format"].as<string>() == "db") {
                //I will store the details on a Trident index
                exp.generateTridentDiffIndex(vm["storemat_path"].as<string>());
            } else if (vm["storemat_format"].as<string>() == "nt") {
                exp.generateNTTriples(vm["storemat_path"].as<string>(), vm["decompressmat"].as<bool>());
            } else {
                BOOST_LOG_TRIVIAL(error) << "Option 'storemat_format' not recognized";
                throw 10;
            }

            boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
            BOOST_LOG_TRIVIAL(info) << "Time to index and store the materialization on disk = " << sec.count() << " seconds";
        }
#ifdef WEBINTERFACE
        if (webint) {
            //Sleep for max 1 second, to allow the fetching of the last statistics
            BOOST_LOG_TRIVIAL(info) << "Sleeping for one second to allow the web interface to get the last stats ...";
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            BOOST_LOG_TRIVIAL(info) << "Done.";
            webint->stop();
        }
#endif
    }
}

void execSPARQLQuery(EDBLayer &edb, po::variables_map &vm) {
    //Parse the rules and create a program
    Program p(edb.getNTerms(), &edb);
    string pathRules = vm["rules"].as<string>();
    if (pathRules != "") {
        p.readFromFile(pathRules);
        p.sortRulesByIDBPredicates();
    }

    //Set up the ruleset and perform the pre-materialization if necessary
    if (pathRules != "") {
        if (!vm["automat"].empty()) {
            //Automatic prematerialization
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization *mat = new Materialization();
            mat->guessLiteralsFromRules(p, edb);
            mat->getAndStorePrematerialization(edb, p, true,
                    vm["timeoutPremat"].as<int>());
            delete mat;
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        } else if (vm["premat"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization *mat = new Materialization();
            mat->loadLiteralsFromFile(p, vm["premat"].as<string>());
            mat->getAndStorePrematerialization(edb, p, false, ~0l);
            p.sortRulesByIDBPredicates();
            delete mat;
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
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
            p.readFromFile(pathRules);
            p.sortRulesByIDBPredicates();
        }
        db = new VLogLayer(edb, p, vm["reasoningThreshold"].as<long>(), "TI", "TE");
    }
    string queryFileName = vm["query"].as<string>();
    // Parse the query
    std::fstream inFile;
    inFile.open(queryFileName);//open the input file
    std::stringstream strStream;
    strStream << inFile.rdbuf();//read the file

    WebInterface::execSPARQLQuery(strStream.str(), vm["explain"].as<bool>(),
            edb.getNTerms(), *db, true, false, NULL, NULL,
            NULL);
    delete db;

    /*QueryDict queryDict(edb.getNTerms());
      QueryGraph queryGraph;
      bool parsingOk;

      SPARQLLexer lexer(strStream.str());
      SPARQLParser parser(lexer);
      boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
      parseQuery(parsingOk, parser, queryGraph, queryDict, db);
      if (!parsingOk) {
      boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
      BOOST_LOG_TRIVIAL(info) << "Runtime query: 0ms.";
      BOOST_LOG_TRIVIAL(info) << "Runtime total: " << duration.count() * 1000 << "ms.";
      BOOST_LOG_TRIVIAL(info) << "# rows = 0";
      return;
      }

    // Run the optimizer
    PlanGen *plangen = new PlanGen();
    Plan* plan = plangen->translate(db, queryGraph);
    // delete plangen;  Commented out, because this also deletes all plans!
    // In particular, it corrupts the current plan.
    // --Ceriel
    if (!plan) {
    cerr << "internal error plan generation failed" << endl;
    delete plangen;
    return;
    }
    bool explain = vm["explain"].as<bool>();
    if (explain)
    plan->print(0);

    // Build a physical plan
    Runtime runtime(db, NULL, &queryDict);
    Operator* operatorTree = CodeGen().translate(runtime, queryGraph, plan, false);

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
boost::chrono::system_clock::time_point startQ = boost::chrono::system_clock::now();
if (operatorTree->first()) {
while (operatorTree->next());
}
boost::chrono::duration<double> durationQ = boost::chrono::system_clock::now() - startQ;
boost::chrono::duration<double> duration = boost::chrono::system_clock::now() - start;
BOOST_LOG_TRIVIAL(info) << "Runtime query: " << durationQ.count() * 1000 << "ms.";
BOOST_LOG_TRIVIAL(info) << "Runtime total: " << duration.count() * 1000 << "ms.";
ResultsPrinter *p = (ResultsPrinter*) operatorTree;
long nElements = p->getPrintedRows();
BOOST_LOG_TRIVIAL(info) << "# rows = " << nElements;

delete plangen;
delete operatorTree;

int times = vm["repeatQuery"].as<int>();
if (times > 0) {
    // Redirect output
    ofstream file("/dev/null");
    streambuf* strm_buffer = cout.rdbuf();
    cout.rdbuf(file.rdbuf());

    for (int i = 0; i < times; i++) {
    PlanGen *plangen = new PlanGen();
    Plan* plan = plangen->translate(db, queryGraph);
    Runtime runtime(db, NULL, &queryDict);
    operatorTree = CodeGen().translate(runtime, queryGraph, plan, false);
    startQ = boost::chrono::system_clock::now();
    if (operatorTree->first()) {
        while (operatorTree->next());
    }
    durationQ += boost::chrono::system_clock::now() - startQ;
    p = (ResultsPrinter*) operatorTree;
    long n1 = p->getPrintedRows();
    if (n1 != nElements) {
        BOOST_LOG_TRIVIAL(error) << "Number of records (" << n1 << ") is not the same. This should not happen...";
    }
    delete plangen;
    delete operatorTree;
}
BOOST_LOG_TRIVIAL(info) << "Repeated query runtime = " << (durationQ.count() / times) * 1000
<< " milliseconds";
//Restore stdout
cout.rdbuf(strm_buffer);
}
}*/
}

string selectStrategy(EDBLayer &edb, Program &p, Literal &literal, Reasoner &reasoner, po::variables_map &vm) {
    string strategy = vm["selectionStrategy"].as<string>();
    if (strategy == "" || strategy == "cardEst") {
	// Use the original cardinality estimation strategy
	ReasoningMode mode = reasoner.chooseMostEfficientAlgo(literal, edb, p, NULL, NULL);
	return mode == TOPDOWN ? "qsqr" : "magic";
    }
    // Add strategies here ...
    BOOST_LOG_TRIVIAL(error) << "Unrecognized selection strategy: " << strategy;
    throw 10;
}

void runLiteralQuery(EDBLayer &edb, Program &p, Literal &literal, Reasoner &reasoner, po::variables_map &vm) {

    boost::chrono::system_clock::time_point startQ1 = boost::chrono::system_clock::now();

    string algo = vm["reasoningAlgo"].as<string>();
    int times = vm["repeatQuery"].as<int>();

    int nVars = literal.getNVars();
    bool onlyVars = nVars > 0;

    if (algo == "auto" || algo == "") {
	algo = selectStrategy(edb, p, literal, reasoner, vm);
	BOOST_LOG_TRIVIAL(info) << "Selection strategy determined that we go for " << algo;
    }

    TupleIterator *iter;

    if (algo == "magic") {
        iter = reasoner.getMagicIterator(literal, NULL, NULL, edb, p, onlyVars, NULL);
    } else if (algo == "qsqr") {
        iter = reasoner.getTopDownIterator(literal, NULL, NULL, edb, p, onlyVars, NULL);
    } else {
	BOOST_LOG_TRIVIAL(error) << "Unregocnized reasoning algorithm: " << algo;
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
    boost::chrono::duration<double> durationQ1 = boost::chrono::system_clock::now() - startQ1;
    BOOST_LOG_TRIVIAL(info) << "Algo = " << algo << ", query runtime = " << (durationQ1.count() * 1000) << " msec, #rows = " << count;

    delete iter;
    if (times > 0) {
        // Redirect output
        ofstream file("/dev/null");
        streambuf* strm_buffer = cout.rdbuf();
        cout.rdbuf(file.rdbuf());
        boost::chrono::system_clock::time_point startQ = boost::chrono::system_clock::now();
        for (int j = 0; j < times; j++) {
            TupleIterator *iter = reasoner.getIterator(literal, NULL, NULL, edb, p, true, NULL);
            int sz = iter->getTupleSize();
            while (iter->hasNext()) {
                iter->next();
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
            }
            cout << endl;
        }
        boost::chrono::duration<double> durationQ = boost::chrono::system_clock::now() - startQ;
        //Restore stdout
        cout.rdbuf(strm_buffer);
        BOOST_LOG_TRIVIAL(info) << "Algo = " << algo << ", repeated query runtime = " << (durationQ.count() / times) * 1000 << " milliseconds";
    }
}

void execLiteralQuery(EDBLayer &edb, po::variables_map &vm) {
    //Parse the rules and create a program
    Program p(edb.getNTerms(), &edb);
    string pathRules = vm["rules"].as<string>();
    if (pathRules != "") {
        p.readFromFile(pathRules);
        p.sortRulesByIDBPredicates();
    }

    //Set up the ruleset and perform the pre-materialization if necessary
    if (pathRules != "") {
        if (!vm["automat"].empty()) {
            //Automatic prematerialization
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization *mat = new Materialization();
            mat->guessLiteralsFromRules(p, edb);
            mat->getAndStorePrematerialization(edb, p, true,
                    vm["timeoutPremat"].as<int>());
            delete mat;
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        } else if (vm["premat"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization *mat = new Materialization();
            mat->loadLiteralsFromFile(p, vm["premat"].as<string>());
            mat->getAndStorePrematerialization(edb, p, false, ~0l);
            p.sortRulesByIDBPredicates();
            delete mat;
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                sec.count() * 1000 << " milliseconds";
        }
    }

    string query;
    string queryFileName = vm["query"].as<string>();
    if (fs::exists(queryFileName)) {
        // Parse the query
        std::fstream inFile;
        inFile.open(queryFileName);//open the input file
        std::getline(inFile, query);
        inFile.close();
    } else {
        query = queryFileName;
    }
    Literal literal = p.parseLiteral(query);
    Reasoner reasoner(vm["reasoningThreshold"].as<long>());
    runLiteralQuery(edb, p, literal, reasoner, vm);
}

int main(int argc, const char** argv) {

    //Init params
    po::variables_map vm;
    if (!initParams(argc, argv, vm)) {
        return EXIT_FAILURE;
    }
    fs::path full_path( fs::initial_path<fs::path>());
    //full_path = fs::system_complete(fs::path( argv[0]));

    //Init logging system
    logging::trivial::severity_level level =
        vm.count("logLevel") ?
        vm["logLevel"].as<logging::trivial::severity_level>() :
        logging::trivial::info;
    initLogging(level);

    string cmd = string(argv[1]);

    //Get the path to the EDB layer
    string edbFile = vm["edb"].as<string>();
    if (edbFile == "default") {
        //Get current directory
        fs::path execFile(argv[0]);
        fs::path dirExecFile = execFile.parent_path();
        edbFile = dirExecFile.string() + string("/edb.conf");
        if (cmd != "load" && !fs::exists(edbFile)) {
            printErrorMsg(string("I could not find the EDB conf file " + edbFile).c_str());
            return EXIT_FAILURE;
        }
    }

    //set up parallelism in the TBB library
    size_t parallelism = vm["nthreads"].as<int>();
    if (parallelism <= 1) {
        parallelism = 2;    // Otherwise tbb aborts.
        // Actual parallelism will be controlled elsewhere.
    }
    // Allow for older tbb versions: don't use global_control.
    // tbb::global_control c(tbb::global_control::max_allowed_parallelism, parallelism);
    tbb::task_scheduler_init init(parallelism);
    /*if (!vm["multithreaded"].empty()) {
      const size_t parallelism = vm["nthreads"].as<int>();
      if (parallelism > 1) {
    //tbb::global_control c(tbb::global_control::max_allowed_parallelism, parallelism);
    c.max_allowed_parallelism = parallelism;
    }
    }*/

    // For profiling:
    int seconds = vm["sleep"].as<int>();
    if (seconds > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(seconds * 1000));
    }

    timens::system_clock::time_point start = timens::system_clock::now();

    BOOST_LOG_TRIVIAL(debug) << "sizeof(EDBLayer) = " << sizeof(EDBLayer);

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
        // EDBLayer layer(conf, false);
        launchFullMat(argc, argv, full_path.string(), *layer, vm,
                vm["rules"].as<string>());
        delete layer;
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
        int fixedStrat = StorageStrat::FIXEDSTRAT5;
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
            BOOST_LOG_TRIVIAL(info) << "Creating the KB from " << comprinput << "/" << comprdict;

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
            p.logPtr = NULL;
            p.remoteLocation = "";
            p.limitSpace = 0;
            p.graphTransformation = "";

            loader->load(p);

            /*loader->load("rdf", onlyCompress, true, vm["comprinput"].as<string>(),
              vm["comprdict"].as<string>() , vm["output"].as<string>(),
              vm["output"].as<string>(),
              dictMethod, sampleMethod,
              popArg,
              vm["maxThreads"].as<int>(), vm["readThreads"].as<int>(),
              ndicts, nindices,
              true,
              aggrIndices, canSkipTables, enableFixedStrat,
              fixedStrat, storePlainList,
              sample, sampleRate, thresholdSkipTable, NULL, "", 0, "");*/

        } else {
            BOOST_LOG_TRIVIAL(info) << "Creating the KB from " << vm["input"].as<string>();


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
            p.logPtr = NULL;
            p.remoteLocation = "";
            p.limitSpace = 0;
            p.graphTransformation = "";
            p.timeoutStats = 0;
            p.storeDicts = true;

            loader->load(p);


            /*loader->load("rdf", onlyCompress, false, vm["input"].as<string>(), "", vm["output"].as<string>(),
              vm["output"].as<string>(),
              dictMethod, sampleMethod,
              popArg,
              vm["maxThreads"].as<int>(), vm["readThreads"].as<int>(),
              ndicts, nindices,
              true,
              aggrIndices, canSkipTables, enableFixedStrat,
              fixedStrat, storePlainList,
              sample, sampleRate, thresholdSkipTable, NULL, "", 0, "");*/
        }
        delete loader;
    } else if (cmd == "server") {
        startServer(argc, argv, full_path.string(), vm);
    }
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(info) << "Runtime = " << sec.count() * 1000 << " milliseconds";

    //Print other stats
    BOOST_LOG_TRIVIAL(info) << "Max memory used: " << Utils::get_max_mem() << " MB";
    return EXIT_SUCCESS;
}
