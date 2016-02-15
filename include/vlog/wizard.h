#ifndef _WIZARD_H
#define _WIZARD_H

#include <vlog/concepts.h>

class Wizard {
private:

    Literal getMagicRelation(const bool priority, std::shared_ptr<Program> newProgram,
                             const Literal &head);

public:

    std::shared_ptr<Program> getAdornedProgram(Literal &query, Program &program);

    std::shared_ptr<Program> doMagic(const Literal &query,
                                     const std::shared_ptr<Program> inputProgram,
                                     std::pair<PredId_t, PredId_t> &inputOutputRelIDs);

};

#endif
