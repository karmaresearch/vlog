#ifndef _WEB_INTERFACE_H
#define _WEB_INTERFACE_H

/* Code inspired by the tutorial available at http://pastebin.com/1KLsjJLZ */

#include <vlog/seminaiver.h>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

#include <map>

class WebInterface {
private:
    std::shared_ptr<SemiNaiver> sn;
    string dirhtmlfiles;
    boost::thread t;
    string cmdArgs;

    boost::asio::io_service io;
    boost::asio::ip::tcp::acceptor acceptor;
    boost::asio::ip::tcp::resolver resolver;

    bool isActive;

    map<string, string> cachehtml;

    class Server: public boost::enable_shared_from_this<Server> {
    private:
        boost::asio::streambuf buffer;
        std::ostringstream ss;
        std::string res, req;

        WebInterface *inter;

    public:
        boost::asio::ip::tcp::socket socket;
        Server(boost::asio::io_service &io, WebInterface *inter):
            inter(inter), socket(io) {};
        void writeHandler(const boost::system::error_code &err, std::size_t bytes);
        void readHeader(boost::system::error_code const &err, size_t bytes);
        void acceptHandler(const boost::system::error_code &err);
    };

    void startThread(string address, string port);

public:
    WebInterface(std::shared_ptr<SemiNaiver> sn, string htmlfiles, string cmdArgs) : sn(sn),
        dirhtmlfiles(htmlfiles), cmdArgs(cmdArgs), acceptor(io), resolver(io), isActive(false) {
    }

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

    std::shared_ptr<SemiNaiver> getSemiNaiver() {
        return sn;
    }

    string getCommandLineArgs() {
        return cmdArgs;
    }
};

#endif
