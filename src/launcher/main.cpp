//VLog
#include <vlog/reasoner.h>
#include <vlog/materialization.h>
#include <vlog/seminaiver.h>
#include <vlog/edbconf.h>
#include <vlog/edb.h>
#include <vlog/webinterface.h>

//Used to load a Trident KB
#include <trident/loader.h>
#include <tridentcompr/utils/utils.h>

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

    if (cmd != "help" && cmd != "lookup" && cmd != "load"
            && cmd != "mat") {
        printErrorMsg(
            (string("The command \"") + cmd + string("\" is unknown.")).c_str());
        return false;
    }

    if (cmd == "help") {
        printHelp(argv[0], desc);
        return false;
    } else {
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
        } else if (cmd == "load") {
            if (!vm.count("input")) {
                printErrorMsg(
                    "The parameter -i (path to the triple files) is not set.");
                return false;
            }
            string tripleDir = vm["input"].as<string>();
            if (!fs::exists(tripleDir)) {
                printErrorMsg(
                    (string("The path ") + tripleDir
                     + string(" does not exist.")).c_str());
                return false;
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
            if (!fs::exists(path)) {
                printErrorMsg((string("The rule file '") +
                               path + string("' does not exists")).c_str());
                return false;
            }
        }
    }

    return true;
}

bool initParams(int argc, const char** argv, po::variables_map &vm) {

    po::options_description query_options("Options for <mat>");
    query_options.add_options()("rules", po::value<string>()->default_value(""),
                                "Activate reasoning with the rules defined at this path. It is REQUIRED in case the command is <mat>. Default is '' (disabled).");
    query_options.add_options()("automat",
                                "Automatically premateralialize some atoms.");
    query_options.add_options()("timeoutPremat", po::value<int>()->default_value(1000000),
                                "Timeout used during automatic prematerialization (in microseconds). Default is 1000000 (i.e. one second per query)");
    query_options.add_options()("premat", po::value<string>()->default_value(""),
                                "Pre-materialize the atoms in the file passed as argument. Default is '' (disabled).");
    query_options.add_options()("storemat", po::value<string>()->default_value(""),
                                "Directory where to store all results of the materialization. Default is '' (disable).");
    query_options.add_options()("decompressmat", po::value<bool>()->default_value(false),
                                "Decompress the results of the materialization when we write it to a file. Default is false.");
    query_options.add_options()("webinterface", po::value<bool>()->default_value(false),
                                "Start a web interface to monitor the execution. Default is false.");

    query_options.add_options()("no-filtering",
                                "Disable filter optimization.");
    query_options.add_options()("no-intersect",
                                "Disable intersection optimization.");

    po::options_description load_options("Options for <load>");
    load_options.add_options()("input,i", po::value<string>(),
                               "Path to the files that contain the compressed triples. This parameter is REQUIRED.");
    load_options.add_options()("output,o", po::value<string>(),
                               "Path to the KB that should be created. This parameter is REQUIRED.");
    load_options.add_options()("maxThreads",
                               po::value<int>()->default_value(Utils::getNumberPhysicalCores()),
                               "Sets the maximum number of threads to use during the compression. Default is the number of physical cores");
    load_options.add_options()("readThreads",
                               po::value<int>()->default_value(2),
                               "Sets the number of concurrent threads that reads the raw input. Default is '2'");

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

        //Execute the materialization
        std::shared_ptr<SemiNaiver> sn = Reasoner::getSemiNaiver(db,
                                         &p, vm["no-intersect"].empty(),
                                         vm["no-filtering"].empty());

        //Start the web interface if requested
        std::unique_ptr<WebInterface> webint;
        if (vm["webinterface"].as<bool>()) {
            webint = std::unique_ptr<WebInterface>(
                         new WebInterface(sn, pathExec + "/webinterface",
                                          flattenAllArgs(argc, argv)));
            webint->start("localhost", "8088");
        }

        BOOST_LOG_TRIVIAL(info) << "Starting full materialization";
        timens::system_clock::time_point start = timens::system_clock::now();
        sn->run();
        boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
        BOOST_LOG_TRIVIAL(info) << "Runtime materialization = " << sec.count() * 1000 << " milliseconds";
        sn->printCountAllIDBs();

        if (vm["storemat"].as<string>() != "") {
            timens::system_clock::time_point start = timens::system_clock::now();
            sn->storeOnFiles(vm["storemat"].as<string>(), vm["decompressmat"].as<bool>(), 0);
            boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
            BOOST_LOG_TRIVIAL(info) << "Time to store the mat. on files = " << sec.count() << " seconds";
        }

        if (webint) {
            //Sleep for max 1 second, to allow the fetching of the last statistics
            BOOST_LOG_TRIVIAL(info) << "Sleeping for one second to allow the web interface to get the last stats ...";
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            BOOST_LOG_TRIVIAL(info) << "Done.";
            webint->stop();
        }
    }
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

    timens::system_clock::time_point start = timens::system_clock::now();

    if (cmd == "lookup") {
        EDBConf conf(edbFile);
        EDBLayer layer(conf);
        lookup(layer, vm);
    } else if (cmd == "mat") {
        EDBConf conf(edbFile);
        EDBLayer layer(conf);
        launchFullMat(argc, argv, full_path.string(), layer, vm,
                      vm["rules"].as<string>());
    } else if (cmd == "load") {
        Loader loader;
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
        BOOST_LOG_TRIVIAL(info) << "Creating the KB from " << vm["input"].as<string>();
        loader.load(onlyCompress, vm["input"].as<string>(), vm["output"].as<string>(),
                    dictMethod, sampleMethod,
                    popArg,
                    vm["maxThreads"].as<int>(), vm["readThreads"].as<int>(),
                    ndicts, nindices,
                    aggrIndices, canSkipTables, enableFixedStrat,
                    fixedStrat, storePlainList,
                    sample, sampleRate, thresholdSkipTable);
    }
    boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(info) << "Runtime = " << sec.count() * 1000 << " milliseconds";

//Print other stats
    BOOST_LOG_TRIVIAL(info) << "Max memory used: " << Utils::get_max_mem() << " MB";
    return EXIT_SUCCESS;
}
