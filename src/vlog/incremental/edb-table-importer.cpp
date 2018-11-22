#include <vlog/incremental/edb-table-importer.h>


EDBimporter::EDBimporter(PredId_t predid, EDBLayer *layer,
                         const std::shared_ptr<SemiNaiver> prevSN) :
        predid(predid), layer(layer), prevSemiNaiver(prevSN),
        edbTable(prevSN->getEDBLayer().getEDBTable(predid)) {
    LOG(DEBUGL) << "EDBImporter constructor";
}

EDBimporter::~EDBimporter() {
    LOG(DEBUGL) << "EDBImporter destructor";
}
