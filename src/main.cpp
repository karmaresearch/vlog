/*
   Copyright (C) 2015 Jacopo Urbani.

   This file is part of Vlog.

   Vlog is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 2 of the License, or
   (at your option) any later version.

   Vlog is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with Vlog.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <trident/loader.h>

#include <trident/sparql/query.h>
#include <trident/kb/kb.h>
#include <trident/kb/statistics.h>
#include <trident/kb/inserter.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/querier.h>
#include <trident/storage/storagestrat.h>
#include <trident/sparql/sparqloperators.h>
// #include <vlog/plan.h>
#include <vlog/reasoner.h>
#include <vlog/materialization.h>
#include <vlog/seminaiver.h>

#include <tridentcompr/utils/utils.h>

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

#include <iostream>
#include <cstdlib>
#include <sstream>
#include <fstream>

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
    cout << "mat\t\t perform a materialization." << endl;
    cout << "query\t\t query the KB." << endl;
    cout << "load\t\t load the KB." << endl;
    cout << "compress\t\t only perform dictionary encoding." << endl;
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

    if (cmd != "help" && cmd != "query" && cmd != "lookup" && cmd != "load"
            && cmd != "compress" && cmd != "mat") {
        printErrorMsg(
            (string("The command \"") + cmd + string("\" is unknown.")).c_str());
        return false;
    }

    if (cmd == "help") {
        printHelp(argv[0], desc);
        return false;
    } else {
        /*** Check common parameters ***/
        if (!vm.count("input")) {
            printErrorMsg("The parameter -i (the knowledge base) is not set.");
            return false;
        }

        //Check if the directory exists
        string kbDir = vm["input"].as<string>();
        if ((cmd == "query" || cmd == "lookup") && !fs::is_directory(kbDir)) {
            printErrorMsg(
                (string("The directory ") + kbDir
                 + string(" does not exist.")).c_str());
            return false;
        }

        //Check if the directory is not empty
        if (cmd == "query" || cmd == "lookup") {
            if (fs::is_empty(kbDir)) {
                printErrorMsg(
                    (string("The directory ") + kbDir + string(" is empty.")).c_str());
                return false;
            }
        }
        /*** Check specific parameters ***/
        if (cmd == "query") {
            string queryFile = vm["query"].as<string>();
            if (queryFile != "" && !fs::exists(queryFile)) {
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
        } else if (cmd == "load" || cmd == "compress") {
            if (!vm.count("tripleFiles")) {
                printErrorMsg(
                    "The parameter -f (path to the triple files) is not set.");
                return false;
            }

            string tripleDir = vm["tripleFiles"].as<string>();
            if (!fs::exists(tripleDir)) {
                printErrorMsg(
                    (string("The path ") + tripleDir
                     + string(" does not exist.")).c_str());
                return false;
            }

            string dictMethod = vm["dictMethod"].as<string>();
            if (dictMethod != DICT_HEURISTICS && dictMethod != DICT_HASH && dictMethod != DICT_SMART) {
                printErrorMsg("The parameter dictMethod (-d) can be either 'hash', 'heuristics', or 'smart'");
                return false;
            }

            string sampleMethod = vm["popMethod"].as<string>();
            if (sampleMethod != "sample" && sampleMethod != "hash") {
                printErrorMsg(
                    "The method to identify the popular terms can only be either 'sample' or 'hash'");
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

            if (vm["ndicts"].as<int>() < 1) {
                printErrorMsg(
                    "The number of dictionary partitions must be at least 1");
                return false;
            }
        } else if (cmd == "mat") {
            string path = vm["rules"].as<string>();
            if (!fs::exists(path)) {
                printErrorMsg((string("The rule file '") + path + string("' does not exists")).c_str());
                return false;
            }
        }
    }

    return true;
}

bool initParams(int argc, const char** argv, po::variables_map &vm) {

    po::options_description query_options("Options for <query> or <mat>");
    query_options.add_options()("query,q", po::value<string>()->default_value(""),
                                "The path of the file with a SPARQL query. If not set then the query is read from STDIN.");
    query_options.add_options()("rules", po::value<string>()->default_value(""),
                                "Activate reasoning during query answering using the rules defined at this path. It is REQUIRED in case the command is <mat>. Default is '' (disabled).");
    query_options.add_options()("reasoningThreshold", po::value<long>()->default_value(1000000),
                                "This parameter sets a threshold to estimate the reasoning cost of a pattern. This cost can be broadly associated to the cardinality of the pattern. It is used to choose either TopDown or Magic evalution. Default is 1000000 (1M).");
    query_options.add_options()("matThreshold", po::value<long>()->default_value(10000000),
                                "In case reasoning is activated, this parameter sets a threshold above which a full materialization is performed before we execute the query. Default is 10000000 (10M).");


    query_options.add_options()("automat",
                                "Automatically premateralialize some atoms.");

    query_options.add_options()("timeoutPremat", po::value<int>()->default_value(1000000),
                                "Timeout used during automatic prematerialization (in microseconds). Default is 1000000 (i.e. one second per query)");
    query_options.add_options()("premat", po::value<string>()->default_value(""),
                                "Pre-materialize the atoms in the file passed as argument. Default is '' (disabled).");
    query_options.add_options()("repeatQuery,r",
                                po::value<int>()->default_value(0),
                                "Repeat the query <arg> times. If the argument is not specified, then the query will not be repeated.");
    query_options.add_options()("minStoreMat",
                                po::value<int>()->default_value(0),
                                "Min level used when we decompress the inferred data. Default value to 0");
    //query_options.add_options()("planning",
    //                            po::value<int>()->default_value(SIMPLE),
    //                            "Type of join to use. It can be 0=SIMPLE, 1=BOTTOMUP, 2=NONE. Default is SIMPLE.");
    query_options.add_options()("storemat", po::value<string>()->default_value(""),
                                "Directory where to store all results of the materialization. Default is '' (disable).");
    query_options.add_options()("decompressmat", po::value<bool>()->default_value(false),
                                "Decompress the results of the materialization when we write it to a file. Default is false.");

    query_options.add_options()("no-filtering",
                                "Disable filter optimization.");
    query_options.add_options()("no-intersect",
                                "Disable intersection optimization.");

    po::options_description load_options("Options for <load> or <compress>");
    load_options.add_options()("tripleFiles,f", po::value<string>(),
                               "Path to the files that contain the compressed triples. This parameter is REQUIRED.");
    load_options.add_options()("dictMethod,d", po::value<string>()->default_value(DICT_HEURISTICS),
                               "Method to perform dictionary encoding. It can b: \"hash\", \"heuristics\", or \"smart\". Default is heuristics.");
    load_options.add_options()("popMethod",
                               po::value<string>()->default_value("hash"),
                               "Method to use to identify the popular terms. Can be either 'sample' or 'hash'. Default is 'hash'");
    load_options.add_options()("maxThreads",
                               po::value<int>()->default_value(Utils::getNumberPhysicalCores()),
                               "Sets the maximum number of threads to use during the compression. Default is the number of physical cores");
    load_options.add_options()("readThreads",
                               po::value<int>()->default_value(2),
                               "Sets the number of concurrent threads that reads the raw input. Default is '2'");
    load_options.add_options()("ndicts",
                               po::value<int>()->default_value(1),
                               "Sets the number dictionary partitions. Default is '1'");
    load_options.add_options()("sample",
                               po::value<bool>()->default_value(true),
                               "Store a little sample of the data, to improve query optimization. Default is ENABLED");
    load_options.add_options()("sampleRate",
                               po::value<double>()->default_value(0.05),
                               "If the sampling is enabled, this parameter sets the sample rate. Default is 0.05 (i.e 5%)");
    load_options.add_options()("storeplainlist",
                               po::value<bool>()->default_value(false),
                               "Next to the indices, stores also a dump of all the input in a single file. This improves scan queries. Default is DISABLED");
    load_options.add_options()("nindices",
                               po::value<int>()->default_value(6),
                               "Set the number of indices to use. Can be 1,3,4,6. Default is '6'");
    load_options.add_options()("aggrIndices", po::value<bool>()->default_value(false),
                               "Use aggredated indices.");
    load_options.add_options()("enableFixedStrat", po::value<bool>()->default_value(true),
                               "Should we store the tables with a fixed layout?");
    string textStrat =  "Fixed strategy to use. Only for advanced users. Three possible values are: i) " + to_string(StorageStrat::FIXEDSTRAT1) + " for a standard cluster layout, ii) " + to_string(StorageStrat::FIXEDSTRAT2) + " for a standard row layout, iii) " + to_string(StorageStrat::FIXEDSTRAT3) + " for a column-oriented layout.";
    load_options.add_options()("fixedStrat",
                               po::value<int>()->default_value(StorageStrat::FIXEDSTRAT3), textStrat.c_str());

    load_options.add_options()("popArg", po::value<int>()->default_value(1000),
                               "Argument for the method to identify the popular terms. If the method is sample, then it represents the sample percentage (x/10000)."
                               " If it it hash, then it indicates the number of popular terms.");

    po::options_description lookup_options("Options for <lookup>");
    lookup_options.add_options()("text,t", po::value<string>(),
                                 "Textual term to search")("number,n", po::value<long>(),
                                         "Numeric term to search");

    po::options_description cmdline_options("Generic options");
    cmdline_options.add(query_options).add(lookup_options).add(load_options);
    cmdline_options.add_options()("input,i", po::value<string>(),
                                  "The path of the KB directory. This parameter is REQUIRED.")(
                                      "logLevel,l", po::value<logging::trivial::severity_level>(),
                                      "Set the log level (accepted values: trace, debug, info, warning, error, fatal). Default is info.");

    po::store(
        po::command_line_parser(argc, argv).options(cmdline_options).run(),
        vm);

    return checkParams(vm, argc, argv, cmdline_options);
}

/*
void parseQuery(DictMgmt *dict, string queryFile, Query *query) {
    //Process the query
    timens::system_clock::time_point start = timens::system_clock::now();

    string sQuery = "";
    if (queryFile == "") {
        std::cout << "Type SPARQL query (terminate with CTRL-D)" << std::endl;
        std::string line;
        while (std::getline(std::cin, line)) {
            sQuery += line + std::string(" ");
        }
    } else {
        //Read the query from file
        ifstream file(queryFile);
        string line;
        while (std::getline(file, line)) {
            sQuery += line + std::string(" ");
        }
        file.close();
    }


    switch (query->parseQuery(dict, sQuery)) {
    case PARSE_ERROR_SYNTAX:
        BOOST_LOG_TRIVIAL(error) << "Syntax error in the query.";
        exit(EXIT_FAILURE);
    case PARSE_URI_NOT_FOUND:
        BOOST_LOG_TRIVIAL(info) << "One constant in the query was not found in the dictionary. Query is finished.";
        exit(EXIT_FAILURE);
    }
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                          - start;
    BOOST_LOG_TRIVIAL(info) << "Time parsing the query = " << sec.count() * 1000
                            << " milliseconds";
}

void execQuery(Querier *q, po::variables_map &vm, Query &query, KB &kb) {
    long nElements = 0;
    DictMgmt *dict = kb.getDictMgmt();
    char bufferTerm[MAX_TERM_SIZE];

    bool reasoning = false;
    std::unique_ptr<EDBLayer> pLayer;
    std::unique_ptr<Program> pProgram;
    std::string pathRules = vm["rules"].as<string>();
    long reasoningThreshold = vm["reasoningThreshold"].as<long>();
    long matThreshold = vm["matThreshold"].as<long>();
    if (pathRules.compare("") != 0) {
        reasoning = true;
        //Load a program with all the rules
        pProgram = std::unique_ptr<Program>(new Program(&kb));
        pProgram->readFromFile(pathRules);
        pProgram->sortRulesByIDBPredicates();

        //Set up knowledge base
        std::string kbPred("TE");
        pLayer = std::unique_ptr<EDBLayer>(new EDBLayer(q, pProgram->getPredicate(kbPred)));
        if (vm["premat"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization mat;
            mat.loadLiteralsFromFile(*pProgram, vm["premat"].as<string>());
            mat.getAndStorePrematerialization(*pLayer, *pProgram, dict, false,
                                              ~0l);
            pProgram->sortRulesByIDBPredicates();
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                                  - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " << sec.count() * 1000 << " milliseconds";
        } else if (!vm["automat"].empty()) {
            //Automatic prematerialization
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization mat;
            mat.guessLiteralsFromRules(*pProgram, dict, *pLayer);
            mat.getAndStorePrematerialization(*pLayer, *pProgram, dict,
                                              true,
                                              vm["timeoutPremat"].as<int>());
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                                  - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                                    sec.count() * 1000 << " milliseconds";
        }
    }

    TridentQueryPlan plan(&kb, q, reasoning, reasoningThreshold, matThreshold, pLayer.get(),
                          pProgram.get());

    timens::system_clock::time_point startOpt = timens::system_clock::now();
    plan.create(query, vm["planning"].as<int>());
    boost::chrono::duration<double> secOpt = boost::chrono::system_clock::now()
            - startOpt;
    BOOST_LOG_TRIVIAL(info) << "Runtime query optimization = " << secOpt.count() * 1000 << " milliseconds";

    //Output plan
#ifdef DEBUG
    BOOST_LOG_TRIVIAL(debug) << "Translated plan:";
    plan.print();
#endif

    {
        q->resetCounters();
        timens::system_clock::time_point start = timens::system_clock::now();
        TupleIterator *root = plan.getIterator();
        //Execute the query
        const uint8_t nvars = (uint8_t) root->getTupleSize();
        while (root->hasNext()) {
            root->next();
            for (uint8_t i = 0; i < nvars; ++i) {
                dict->getText(root->getElementAt(i), bufferTerm);
                std::cout << bufferTerm << ' ';
            }
            std::cout << '\n';
            nElements++;
        }
        boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                              - start;
        //Print stats
        BOOST_LOG_TRIVIAL(info) << "Runtime = " << sec.count() * 1000 << " milliseconds";
        BOOST_LOG_TRIVIAL(info) << "# rows = " << nElements;
        BOOST_LOG_TRIVIAL(debug) << "# Read Index Blocks = " << kb.getStats().getNReadIndexBlocks();
        BOOST_LOG_TRIVIAL(debug) << " Read Index Bytes from disk = " << kb.getStats().getNReadIndexBytes();
        Querier::Counters c = q->getCounters();
        BOOST_LOG_TRIVIAL(debug) << "RowLayouts: " << c.statsRow << " ClusterLayouts: " << c.statsCluster << " ColumnLayouts: " << c.statsColumn;
        BOOST_LOG_TRIVIAL(debug) << "AggrIndices: " << c.aggrIndices << " NotAggrIndices: " << c.notAggrIndices << " CacheIndices: " << c.cacheIndices;
        BOOST_LOG_TRIVIAL(debug) << "Permutations: spo " << c.spo << " ops " << c.ops << " pos " << c.pos << " sop " << c.sop << " osp " << c.osp << " pso " << c.pso;

        plan.releaseIterator(root);
    }
    //Print stats dictionary
    long nblocks = 0;
    long nbytes = 0;
    for (int i = 0; i < kb.getNDictionaries(); ++i) {
        nblocks = kb.getStatsDict()[i].getNReadIndexBlocks();
        nbytes = kb.getStatsDict()[i].getNReadIndexBytes();
    }
    BOOST_LOG_TRIVIAL(debug) << "# Read Dictionary Blocks = " << nblocks;
    BOOST_LOG_TRIVIAL(debug) << "# Read Dictionary Bytes from disk = " << nbytes;
    BOOST_LOG_TRIVIAL(debug) << "Process IO Read bytes = " << Utils::getIOReadBytes();

    //Repeat the query if requested
    int times = vm["repeatQuery"].as<int>();
    if (times > 0) {

        //Redirect output
        ofstream file("/dev/null");
        streambuf* strm_buffer = cout.rdbuf();
        cout.rdbuf(file.rdbuf());

        timens::system_clock::time_point start = timens::system_clock::now();
        for (int i = 0; i < times; ++i) {
            TupleIterator *root = plan.getIterator();
            long n = 0;
            const uint8_t nvars = (uint8_t) root->getTupleSize();
            while (root->hasNext()) {
                root->next();
                for (uint8_t i = 0; i < nvars; ++i) {
                    dict->getText(root->getElementAt(i), bufferTerm);
                    std::cout << bufferTerm << ' ';
                }
                std::cout << '\n';
                n++;
            }
            if (n != nElements) {
                BOOST_LOG_TRIVIAL(error) << "Number of records (" << n << ") is not the same. This should not happen...";
            }
            plan.releaseIterator(root);
        }
        boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Repeated query runtime = " << (sec.count() / times) * 1000
                                << " milliseconds";

        //Restore stdout
        cout.rdbuf(strm_buffer);
    }
}
*/

void lookup(DictMgmt *dict, po::variables_map &vm) {
    if (vm.count("text")) {
        nTerm value;
        string textTerm = vm["text"].as<string>();
        if (!dict->getNumber((char*) textTerm.c_str(), textTerm.size(), &value)) {
            cout << "Term " << textTerm << " not found" << endl;
        } else {
            cout << value << endl;
        }
    } else {
        nTerm key = vm["number"].as<long>();
        char supportText[4096];
        if (!dict->getText(key, supportText)) {
            cout << "Term " << key << " not found" << endl;
        } else {
            cout << supportText << endl;
        }
    }
}

void launchFullMat(KB *kb, po::variables_map &vm, std::string pathRules) {
    //Load a program with all the rules
    Program p(kb);
    p.readFromFile(pathRules);

    //Set up the ruleset and perform the pre-materialization if necessary
    std::string kbPred("TE");
    Querier *q = kb->query();
    {
        EDBLayer layer(q, p.getPredicate(kbPred));

        if (!vm["automat"].empty()) {
            //Automatic prematerialization
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization mat;
            mat.guessLiteralsFromRules(p, kb->getDictMgmt(), layer);
            mat.getAndStorePrematerialization(layer, p, kb->getDictMgmt(),
                                              true,
                                              vm["timeoutPremat"].as<int>());
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                                  - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                                    sec.count() * 1000 << " milliseconds";
        } else if (vm["premat"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();
            Materialization mat;
            mat.loadLiteralsFromFile(p,  vm["premat"].as<string>());
            mat.getAndStorePrematerialization(layer, p, kb->getDictMgmt(),
                    false, ~0l);
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                                  - start;
            BOOST_LOG_TRIVIAL(info) << "Runtime pre-materialization = " <<
                                    sec.count() * 1000 << " milliseconds";
        }

        //Execute the materialization
        std::shared_ptr<SemiNaiver> sn = Reasoner::fullMaterialization(kb, layer, &p, vm["no-intersect"].empty(), vm["no-filtering"].empty());

        if (vm["storemat"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();
            sn->storeOnFiles(vm["storemat"].as<string>(), vm["decompressmat"].as<bool>(),vm["minStoreMat"].as<int>());
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
            BOOST_LOG_TRIVIAL(info) << "Time to store the mat. on files = " << sec.count() << " seconds";
        }
    }
    delete q;
}

int main(int argc, const char** argv) {

    //Init params
    po::variables_map vm;
    if (!initParams(argc, argv, vm)) {
        return EXIT_FAILURE;
    }

    //Init logging system
    logging::trivial::severity_level level =
        vm.count("logLevel") ?
        vm["logLevel"].as<logging::trivial::severity_level>() :
        logging::trivial::info;
    initLogging(level);

    //Init the knowledge base
    string cmd = string(argv[1]);
    string kbDir = vm["input"].as<string>();

    timens::system_clock::time_point start = timens::system_clock::now();

    if (cmd == "query") {
	/*
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        Querier *q = kb.query();
        //Parse the query
        Query query;
        parseQuery(kb.getDictMgmt(), vm["query"].as<string>(), &query);
        //I don't need the dictionary anymore. I can free up some memory...

        //Execute the query
        execQuery(q, vm, query, kb);

        delete q;
	*/
    } else if (cmd == "lookup") {
        KBConfig config;
        KB kb(kbDir.c_str(), true, false, true, config);
        lookup(kb.getDictMgmt(), vm);
    } else if (cmd == "mat") {
	// BOOST_LOG_TRIVIAL(info) << "Starting materialization of " << kbDir.c_str();
        KBConfig config;
        //Decrease the memory consumption of the database to give space to derivations
        KB kb(kbDir.c_str(), true, true, true, config);
        launchFullMat(&kb, vm, vm["rules"].as<string>());
	//BOOST_LOG_TRIVIAL(info) << "Done with materialization of " << kbDir.c_str();
    } else if (cmd == "load" || cmd == "compress") {
        Loader loader;
        bool onlyCompress = cmd == "compress";
        int sampleMethod;
        if (vm["popMethod"].as<string>() == string("sample")) {
            sampleMethod = PARSE_SAMPLE;
        } else {
            sampleMethod = PARSE_COUNTMIN;
        }
	if (cmd == "load") {
	    BOOST_LOG_TRIVIAL(info) << "Creating index " << kbDir.c_str()
		<< " from " << vm["tripleFiles"].as<string>();
	}
        string dictMethod = vm["dictMethod"].as<string>();
        loader.load(onlyCompress, vm["tripleFiles"].as<string>(), kbDir, dictMethod, sampleMethod,
                    vm["popArg"].as<int>(),
                    vm["maxThreads"].as<int>(), vm["readThreads"].as<int>(),
                    vm["ndicts"].as<int>(), vm["nindices"].as<int>(),
                    vm["aggrIndices"].as<bool>(), vm["enableFixedStrat"].as<bool>(),
                    vm["fixedStrat"].as<int>(), vm["storeplainlist"].as<bool>(),
                    vm["sample"].as<bool>(), vm["sampleRate"].as<double>());
	if (cmd == "load") {
	    BOOST_LOG_TRIVIAL(info) << "Done creating the index";
	}
	boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
	BOOST_LOG_TRIVIAL(info) << "Runtime = " << sec.count() * 1000 << " milliseconds";
    }

    //Print other stats
    BOOST_LOG_TRIVIAL(info) << "Max memory used: " << Utils::get_max_mem() << " MB";
    return EXIT_SUCCESS;
}
