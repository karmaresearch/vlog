#ifndef _TG_GRAPH_PATH_H
#define _TG_GRAPH_PATH_H

#include <vector>
#include <iterator>
#include <string>
#include <inttypes.h>

template<char delimiter>
class WordDelimitedBy : public std::string {};

class TGPath {
    public:
        uint32_t ruleid;
        std::vector<std::string> inputs;
        std::string output;
};

class TGPaths {
    private:
        std::vector<TGPath> paths;

    public:
        TGPaths();

        void readFrom(std::string filepath);

        TGPaths(std::string filepath);

        void writeTo(std::string filepath);

        size_t getNPaths() const;

        const TGPath &getPath(const uint32_t pathID) const;
};

#endif
