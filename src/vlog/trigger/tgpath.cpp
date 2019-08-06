#include <vlog/trigger/tgpath.h>
#include <fstream>
#include <sstream>
#include <iterator>

TGPaths::TGPaths() {
}

TGPaths::TGPaths(std::string filepath) {
    readFrom(filepath);
}

void TGPaths::readFrom(std::string filepath) {
    std::string line;
    std::ifstream fin;
    fin.open(filepath);
    while(std::getline(fin, line)) {
        //Parse line. Structure is <ruleID>\t<outputSet>\t<inputSsets>\t...
        std::istringstream iss(line);
        std::vector<std::string> tokens(
                (std::istream_iterator<WordDelimitedBy<'\t'>>(iss)),
                std::istream_iterator<WordDelimitedBy<'\t'>>());
        TGPath p;
        p.ruleid = std::stoi(tokens[0]);
        for(int i = 1; i < tokens.size() - 1; ++i) {
            p.inputs.push_back(tokens[i]);
        }
        p.output = tokens[tokens.size() - 1];
        paths.push_back(p);
    }
    fin.close();
}

void TGPaths::writeTo(std::string filepath) {
    //TODO ...
}

size_t TGPaths::getNPaths() const {
    return paths.size();
}

const TGPath &TGPaths::getPath(const uint32_t pathID) const {
    return paths[pathID];
}
