#ifndef _SEMINAIVER_TRIGGER_H
#define _SEMINAIVER_TRIGGER_H

#include <vlog/seminaiver.h>

#include <vector>

class TriggerSemiNaiver: public SemiNaiver {
    private:
        size_t unique_unary(std::vector<Term_t> &unaryBuffer,
                std::vector<std::shared_ptr<const FCInternalTable>> &tables);

        size_t unique_binary(std::vector<std::pair<Term_t, Term_t>>  &binaryBuffer,
                std::vector<std::shared_ptr<const FCInternalTable>> &tables);

    public:
        TriggerSemiNaiver(EDBLayer &layer,
                Program *program, TypeChase chase) :
            SemiNaiver(layer, program, false, false, false, chase, 1, false, false) {
            }

        VLIBEXP void run(std::string trigger_paths);

        VLIBEXP size_t unique();

};

#endif
