#ifndef _ML_H
#define _ML_H

#include <vlog/concepts.h>
#include <kognac/progargs.h>

typedef enum QueryType{
    QUERY_TYPE_MIXED = 0,
    QUERY_TYPE_GENERIC = 100,
    QUERY_TYPE_BOOLEAN = 1000
}QueryType;

class ML {
    public:
        static std::string makeGenericQuery(Program& p, PredId_t predId,
                uint8_t predCard);

        static std::pair<std::string, int> makeComplexQuery(
                Program& p,
                Literal& l,
                vector<Substitution>& sub, EDBLayer& db);

        template <typename Generic>
            static std::vector<std::vector<Generic>> powerset(std::vector<Generic>& set) {
                std::vector<std::vector<Generic>> output;
                uint16_t setSize = set.size();
                uint16_t powersetSize = pow((uint16_t)2, setSize) - 1;
                for (int i = 1; i <= powersetSize; ++i) {
                    std::vector<Generic> element;
                    for (int j = 0; j < setSize; ++j) {
                        if (i & (1<<j)) {
                            element.push_back(set[j]);
                        }
                    }
                    output.push_back(element);
                }
                return output;
            }

        static PredId_t getMatchingIDB(EDBLayer& db, Program &p, vector<uint64_t>& tuple);

        static std::vector<std::pair<std::string, int>> generateTrainingQueries(
                EDBLayer &db,
                Program &p,
                std::vector<uint8_t>& vt,
                ProgramArgs &vm);
};

#endif
