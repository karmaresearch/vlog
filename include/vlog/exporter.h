#include <vlog/seminaiver.h>
#include <vlog/consts.h>

#include <trident/tree/root.h>

struct _EDBPredicates {
    PredId_t id;
    size_t ruleid;
    long triple[3];
    uint8_t nPosToCopy;
    uint8_t posToCopy[3];
};

class Exporter {
private:
    std::shared_ptr<SemiNaiver> sn;

    void extractTriples(std::vector <uint64_t> &all_s,
                        std::vector <uint64_t> &all_p,
                        std::vector <uint64_t> &all_o);

    void copyTable(std::vector<uint64_t> &all_s,
                   std::vector<uint64_t> &all_p,
                   std::vector<uint64_t> &all_o,
                   std::vector<_EDBPredicates>::iterator it,
                   std::shared_ptr<const FCInternalTable> intTable,
                   const long nrows,
                   long triple[3]);

public:
    Exporter(std::shared_ptr<SemiNaiver> sn) : sn(sn) {}

    VLIBEXP void generateTridentDiffIndex(string outputdir);

    //void generateTridentDiffIndexTabByTab(string outputdir);

    VLIBEXP void generateNTTriples(string outputdir, bool decompress);
};
