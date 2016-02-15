#include <vlog/webinterface.h>

#include <tridentcompr/utils/utils.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include <string>
#include <fstream>
#include <chrono>
#include <thread>

using boost::property_tree::ptree;
using boost::property_tree::read_json;
using boost::property_tree::write_json;

void WebInterface::startThread(string address, string port) {
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
        boost::asio::async_read_until(socket, buffer, "\r\n\r\n",
                                      boost::bind(&Server::readHeader, shared_from_this(),
                                              boost::asio::placeholders::error,
                                              boost::asio::placeholders::bytes_transferred));
    }
}

void WebInterface::Server::readHeader(boost::system::error_code const &err,
                                      size_t bytes) {
    inter->setActive();

    ss << &buffer;
    req = ss.str();
    //Get the page
    string page;
    if (boost::starts_with(req, "GET")) {
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
        } else if (path.size() > 1) {
            page = inter->getPage(path);
        }
    }

    if (page == "") {
        //return the main page
        page = inter->getDefaultPage();
    }
    res = "HTTP/1.1 200 OK\r\nContent-Length: " + to_string(page.size()) + "\r\n\r\n" + page;

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
        cachehtml.insert(make_pair(f, contentFile));
        return contentFile;
    }

    return "Error! I cannot find the page to show.";
}
