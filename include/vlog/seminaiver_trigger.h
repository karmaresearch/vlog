#ifndef _SEMINAIVER_TRIGGER_H
#define _SEMINAIVER_TRIGGER_H

#include <vlog/seminaiver.h>

#include <vector>

class TriggerSemiNaiver: public SemiNaiver {
    public:
        TriggerSemiNaiver(EDBLayer &layer,
                Program *program, bool restrictedChase) :
           SemiNaiver(layer, program, false, false, false, restrictedChase, 1, false) {
        }

    VLIBEXP void run(std::string trigger_paths);

};

#endif
