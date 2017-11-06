#ifndef _CHASE_MGMT_H
#define _CHASE_MGMT_H

#include <vector>
#include <vlog/column.h>

class ChaseMgmt {
    private:
        uint64_t counter;

    public:
        ChaseMgmt(uint64_t startcounter);

        std::shared_ptr<Column> getNewOrExistingIDs(
                uint32_t ruleid, 
                uint8_t var,
                std::vector<std::shared_ptr<Column>> &columns,
                uint64_t size);

};

#endif
