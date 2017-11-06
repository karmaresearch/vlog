#include <vlog/chasemgmt.h>

ChaseMgmt::ChaseMgmt(uint64_t startcounter) {
    this->counter = startcounter;
}

std::shared_ptr<Column> ChaseMgmt::getNewOrExistingIDs(
        uint32_t ruleid,
        uint8_t var,
        std::vector<std::shared_ptr<Column>> &columns,
        uint64_t size) {

    //uint64_t out = counter;
    //counter += size;
    //return out;

}
