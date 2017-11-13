#include <vlog/exporter.h>
#include <vlog/seminaiver.h>
#include <vlog/trident/tridenttable.h>

#include <kognac/utils.h>
#include <trident/tree/root.h>
#include <trident/kb/kbconfig.h>
#include <trident/kb/updater.h>

#include <inttypes.h>
#include <vector>
#include <fstream>
#include <zstr/zstr.hpp>

struct AggrIndex {
    uint64_t first, second;
    size_t begin, end;
};

void Exporter::extractTriples(std::vector <uint64_t> &all_s,
        std::vector <uint64_t> &all_p,
        std::vector <uint64_t> &all_o) {

    //Get all rules that define IDB predicates from the standard EDB ternary triple
    //e.g. P1(a,b) :- TE(a, rdf:type, b)
    PredId_t edbpred = sn->getEDBLayer().getFirstEDBPredicate();
    std::vector<Rule> rules = sn->getProgram()->getAllRules();
    std::vector<_EDBPredicates> predicatesToExtract;
    int ruleid = 0;
    for (auto it = rules.begin(); it != rules.end(); ++it) {
        const auto &body = it->getBody();
        if (body.size() == 1 && body[0].getPredicate().getId() == edbpred) {
            VTuple t = body[0].getTuple();
            _EDBPredicates pred;
            pred.triple[0] = pred.triple[1] = pred.triple[2] = -1;
            pred.nPosToCopy = 0;
            if (t.get(0).isVariable()) {
                pred.posToCopy[pred.nPosToCopy++] = 0;
            } else {
                pred.triple[0] = t.get(0).getValue();
            }
            if (t.get(1).isVariable()) {
                pred.posToCopy[pred.nPosToCopy++] = 1;
            } else {
                pred.triple[1] = t.get(1).getValue();
            }
            if (t.get(2).isVariable()) {
                pred.posToCopy[pred.nPosToCopy++] = 2;
            } else {
                pred.triple[2] = t.get(2).getValue();
            }
            pred.ruleid = ruleid;

            for(const auto &h : it->getHeads()) {
                _EDBPredicates pnew = pred;
                pnew.id = h.getPredicate().getId();
                LOG(DEBUGL) << "Pred.id " << pnew.id << " rule: " << it->toprettystring(sn->getProgram(), &sn->getEDBLayer()) << " triple " << pred.triple[0] << " " << pred.triple[1] << " " << pred.triple[2] << " npostocopy: " << (int)pred.nPosToCopy << " pos1: " << (int)pred.posToCopy[0] << " pos2:" << (int)pred.posToCopy[1];
                predicatesToExtract.push_back(pnew);
            }
        }
        ruleid++;
    }

    //How many tuples should I store?
    long count = 0;
    for (auto it = predicatesToExtract.begin(); it != predicatesToExtract.end();
            ++it) {
        count += sn->getSizeTable(it->id);
    }
    LOG(DEBUGL) << "Copying up to " << count << " triples ...";

    //Get tables for all such predicates, put the subjects in a large array
    all_s.reserve(count);
    all_p.reserve(count);
    all_o.reserve(count);

    long ntables = 0;
    for (auto it = predicatesToExtract.begin(); it != predicatesToExtract.end();
            ++it) {
        LOG(DEBUGL) << "Get table for pred " << it->id;
        FCIterator tableItr = sn->getTable(it->id);
        long triple[3];
        triple[0] = it->triple[0];
        triple[1] = it->triple[1];
        triple[2] = it->triple[2];

        bool isFirst = true;
        while (!tableItr.isEmpty()) {
            std::shared_ptr<const FCInternalTable> intTable = tableItr.getCurrentTable();
            size_t nrows = intTable->getNRows();
            if (isFirst) {
                isFirst = false;
                if (tableItr.getCurrentBlock()->rule->ruleid == it->ruleid) {
                    //Skip the first table since they are all duplicates
                    LOG(DEBUGL) << "Skipping table of " << nrows << " nrows, iter = " << tableItr.getCurrentIteration();
                    tableItr.moveNextCount();
                    continue;
                }
            }

            ntables++;
            LOG(DEBUGL) << "Copying table of " << nrows << " nrows, iter = " << tableItr.getCurrentIteration();

            uint8_t currentPosToCopy = 0;
            for (int i = 0; i < 3; ++i) {
                std::vector<uint64_t> *out;
                if (i == 0)
                    out = &all_s;
                else if (i == 1)
                    out = &all_p;
                else
                    out = &all_o;

                if (currentPosToCopy < it->nPosToCopy) {
                    if (it->posToCopy[currentPosToCopy] == i) {
                        auto column = intTable->getColumn(currentPosToCopy);
                        if (column->isBackedByVector()) {
                            //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
                            const std::vector<Term_t> &vec = column->getVectorRef();
                            assert(sizeof(Term_t) == sizeof(uint64_t));
                            assert(vec.size() == nrows);
                            out->resize(out->size() + nrows);
                            memcpy(&(out->at(out->size() - nrows)), &(vec[0]), sizeof(Term_t) * nrows);
                            //std::chrono::duration<double> sec = std::chrono::system_clock::now()
                            //                                      - start;
                            //LOG(INFOL) << "Runtime memcpy = " << sec.count() * 1000 << "-" << nrows;
                        } else {
                            //Copy one by one
                            //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
#if INCORRECT_CODE
                            // Unfortunately, this code is not correct, but I don't know why yet,
                            // but I think the rawarray pointer is wrong. TODO!
                            // --Ceriel

                            if (column->isEDB()) {
                                //Get underlying representation of a column in Trident. This is a dirty hack, but extremely fast!
                                EDBColumn *col = (EDBColumn*) column.get();
                                const char *rawarray = col->getUnderlyingArray();
                                std::pair<uint8_t, std::pair<uint8_t, uint8_t>> sizeelements = col->getSizeElemUnderlyingArray();
                                const uint8_t totalsize = sizeelements.first +
                                    sizeelements.second.first +
                                    sizeelements.second.second;

                                //Enlarge array
                                out->resize(out->size() + nrows);
                                uint64_t *begin = &(out->at(out->size() - nrows));

                                if (sizeelements.second.first != 0 && col->containsDuplicates()) {
                                    //I must read the first column of a table.
                                    //I must read also the counter and add a
                                    //corresponding number of duplicates.

                                    const uint8_t sizecount = sizeelements.second.first;
                                    for (uint64_t j = 0; j < nrows; ) {
                                        const uint64_t el = Utils::decode_longFixedBytes(rawarray, sizeelements.first);
                                        begin[j++] = el;
                                        uint64_t count = Utils::decode_longFixedBytes(rawarray + sizeelements.first, sizecount);
                                        while (--count > 0) {
                                            begin[j++] = el;
                                        }
                                        rawarray += totalsize;
                                    }

                                } else {
                                    for (uint64_t j = 0; j < nrows; ++j) {
                                        const uint64_t el = Utils::decode_longFixedBytes(rawarray, sizeelements.first);
                                        begin[j] = el;
                                        rawarray += totalsize;
                                    }
                                }
                            } else
#endif
                            {
                                auto r = column->getReader();
                                while (r->hasNext()) {
                                    auto value = r->next();
                                    out->push_back(value);
                                }
                            }
                            //std::chrono::duration<double> sec = std::chrono::system_clock::now()
                            //                                      - start;
                            //LOG(INFOL) << "Runtime copy one-by-one = " << sec.count() * 1000 << "-" << nrows;
                        }

                        currentPosToCopy++;
                        continue;
                    }
                }

                //Copy a constant value
                //std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
                out->resize(out->size() + nrows);
                uint64_t *begin = &(out->at(out->size() - nrows));
                uint64_t j = 0;
                while (j < nrows) {
                    begin[j++] = triple[i];
                }
                //std::chrono::duration<double> sec = std::chrono::system_clock::now()
                //                                      - start;
                //LOG(INFOL) << "Runtime constant = " << sec.count() * 1000 << "-" << nrows;
            }
            assert(all_s.size() == all_p.size());
            assert(all_p.size() == all_o.size());

            tableItr.moveNextCount();
        }
    }
    LOG(DEBUGL) << "Merged " << ntables << " tables";
}

/*void Exporter::generateTridentDiffIndexTabByTab(string outputdir) {
  std::vector<uint64_t> all_s;
  std::vector<uint64_t> all_p;
  std::vector<uint64_t> all_o;
  PredId_t idpred = sn->getEDBLayer().getFirstEDBPredicate();
  std::shared_ptr<EDBTable> table = sn->getEDBLayer().getEDBTable(idpred);
  Querier *q = ((TridentTable*)table.get())->getQuerier();
  KB *kb = ((TridentTable*)table.get())->getKB();
  int idxUpdate = 0;

//Get all rules that define IDB predicates from the standard EDB ternary triple
//e.g. P1(a,b) :- TE(a, rdf:type, b)
PredId_t edbpred = sn->getEDBLayer().getFirstEDBPredicate();
std::vector<Rule> rules = sn->getProgram()->getAllRules();
std::vector<_EDBPredicates> predicatesToExtract;
int ruleid = 0;
for (auto it = rules.begin(); it != rules.end(); ++it) {
const auto &body = it->getBody();
if (body.size() == 1 && body[0].getPredicate().getId() == edbpred) {
VTuple t = body[0].getTuple();
_EDBPredicates pred;
pred.nPosToCopy = 0;
pred.triple[0] = pred.triple[1] = pred.triple[2] = -1;
if (t.get(0).isVariable()) {
pred.posToCopy[pred.nPosToCopy++] = 0;
} else {
pred.triple[0] = t.get(0).getValue();
}
if (t.get(1).isVariable()) {
pred.posToCopy[pred.nPosToCopy++] = 1;
} else {
pred.triple[1] = t.get(1).getValue();
}
if (t.get(2).isVariable()) {
pred.posToCopy[pred.nPosToCopy++] = 2;
} else {
pred.triple[2] = t.get(2).getValue();
}
pred.id = it->getHead().getPredicate().getId();
pred.ruleid = ruleid;
predicatesToExtract.push_back(pred);
}
ruleid++;
}

//How many tuples should I store?
long count = 0;
for (auto it = predicatesToExtract.begin(); it != predicatesToExtract.end();
++it) {
count += sn->getSizeTable(it->id);
}
LOG(DEBUGL) << "Copying up to " << count << " triples ...";

//Get tables for all such predicates, put the subjects in a large array
all_s.reserve(count);
all_p.reserve(count);
all_o.reserve(count);

long ntables = 0;
for (auto it = predicatesToExtract.begin(); it != predicatesToExtract.end();
++it) {
LOG(DEBUGL) << "Get table for pred " << it->id;
FCIterator tableItr = sn->getTable(it->id);
long triple[3];
triple[0] = it->triple[0];
triple[1] = it->triple[1];
triple[2] = it->triple[2];

bool isFirst = true;
while (!tableItr.isEmpty()) {
if (isFirst) {
isFirst = false;
if (tableItr.getCurrentBlock()->rule->ruleid == it->ruleid) {
    //Skip the first table since they are all duplicates
    tableItr.moveNextCount();
    continue;
}
}

ntables++;
std::shared_ptr<const FCInternalTable> intTable = tableItr.getCurrentTable();
size_t nrows = intTable->getNRows();
LOG(INFOL) << "Copying table of " << nrows <<
" postocopy " << (int)it->nPosToCopy;
copyTable(all_s, all_p, all_o, it, intTable, nrows, triple);

string diffdir = outputdir + string("/") + to_string(idxUpdate++);
fs::create_directories(diffdir);
LOG(INFOL) << "Creating the index " << diffdir
<< " posToCopy=" << (int)it->nPosToCopy;

if (it->nPosToCopy == 1) {
    std::vector<uint64_t> *out;
    if (it->posToCopy[0] == 0) {
        out = &all_s;
    } else if (it->posToCopy[0] == 1) {
        out = &all_p;
    } else {
        out = &all_o;
    }
    DiffIndex1::createDiffIndex(diffdir, true, q, triple, *out, it->posToCopy[0]);
} else {
    DiffIndex3::createDiffIndex(DiffIndex::TypeUpdate::ADDITION,
            diffdir, outputdir, all_s, all_p,
            all_o, true, q, true);
}

//Add the index to the KB
kb->addDiffIndex(diffdir, q);

all_s.clear();
all_p.clear();
all_o.clear();

tableItr.moveNextCount();
}
}
}*/

void Exporter::copyTable(std::vector<uint64_t> &all_s,
        std::vector<uint64_t> &all_p,
        std::vector<uint64_t> &all_o,
        std::vector<_EDBPredicates>::iterator it,
        std::shared_ptr<const FCInternalTable> intTable,
        const long nrows,
        long triple[3]) {

    uint8_t currentPosToCopy = 0;
    for (int i = 0; i < 3; ++i) {
        std::vector<uint64_t> *out;
        if (i == 0)
            out = &all_s;
        else if (i == 1)
            out = &all_p;
        else
            out = &all_o;

        if (currentPosToCopy < it->nPosToCopy) {
            if (it->posToCopy[currentPosToCopy] == i) {
                auto column = intTable->getColumn(currentPosToCopy);
                if (column->isBackedByVector()) {
                    const std::vector<Term_t> &vec = column->getVectorRef();
                    assert(sizeof(Term_t) == sizeof(uint64_t));
                    out->resize(out->size() + nrows);
                    memcpy(&(out->at(out->size() - nrows)), &(vec[0]), sizeof(Term_t) * nrows);
                } else {
                    auto r = column->getReader();
                    while (r->hasNext()) {
                        auto value = r->next();
                        out->push_back(value);
                    }
                }

                currentPosToCopy++;
                continue;
            }
        }

        //Copy a constant value
        out->resize(out->size() + nrows);
        uint64_t *begin = &(out->at(out->size() - nrows));
        uint64_t j = 0;
        while (j < nrows) {
            begin[j++] = triple[i];
        }
    }
}

void Exporter::generateTridentDiffIndex(string outputdir) {
    std::vector<uint64_t> all_s;
    std::vector<uint64_t> all_p;
    std::vector<uint64_t> all_o;

    extractTriples(all_s, all_p, all_o);

    PredId_t idpred = sn->getEDBLayer().getFirstEDBPredicate();
    std::shared_ptr<EDBTable> table = sn->getEDBLayer().getEDBTable(idpred);
    Querier *q = ((TridentTable*)table.get())->getQuerier();

    string diffdir = Updater::getPathForUpdate(outputdir);

    DiffIndex3::createDiffIndex(DiffIndex::TypeUpdate::ADDITION,
            diffdir, outputdir + "/_diff",
            all_s, all_p, all_o, false, q, true);
    //Write the type of file
    string flagup = diffdir + "/ADD";
    //write also an additional dictionary (if any) TODO!
    // writeDict(kb.getDictMgmt(), locationupdate, tmpdict);
    ofstream ofs(flagup);
    ofs.close();
}

void Exporter::generateNTTriples(string outputdir, bool decompress) {
    std::vector<uint64_t> all_s;
    std::vector<uint64_t> all_p;
    std::vector<uint64_t> all_o;
    extractTriples(all_s, all_p, all_o);

    LOG(INFOL) << "Exporting the materialization in N-Triples format ...";
    EDBLayer &edb = sn->getEDBLayer();

    //Store the raw dataset in a text file for debug purposes
    Utils::create_directories(outputdir);
    std::unique_ptr<zstr::ofstream> out;

    char supportBuffer[MAX_TERM_SIZE];
    size_t idx = 0;
    for (int i = 0; i < all_s.size(); ++i) {
        if (i % 10000000 == 0) {
            if (i > 0) {
                LOG(INFOL) << "So far exported " << i << " triples ...";
            }
            //Create the file. Close the previous one
            string filename = outputdir + "/out-" + to_string(idx++) + ".nt.gz";
            LOG(DEBUGL) << "Creating file " << filename;
            out = std::unique_ptr<zstr::ofstream>(new zstr::ofstream(filename));
        }
        if (decompress) {
            if (edb.getDictText(all_s[i], supportBuffer)) {
                *out << supportBuffer << " ";
            } else {
                std::string t = sn->getProgram()->getFromAdditional(all_s[i]);
                if (t == std::string("")) t = std::to_string(all_s[i]);
                *out << t << " ";
            }
            if (edb.getDictText(all_p[i], supportBuffer)) {
                *out << supportBuffer << " ";
            } else {
                std::string t = sn->getProgram()->getFromAdditional(all_p[i]);
                if (t == std::string("")) t = std::to_string(all_p[i]);
                *out << t << " ";
            }
            if (edb.getDictText(all_o[i], supportBuffer)) {
                *out << supportBuffer << " ." << endl;
            } else {
                std::string t = sn->getProgram()->getFromAdditional(all_o[i]);
                if (t == std::string("")) t = std::to_string(all_o[i]);
                *out << t << " ." << endl;
            }
        } else {
            *out << all_s[i];
            *out << " ";
            *out << all_p[i];
            *out << " ";
            *out << all_o[i];
            *out << endl;
        }
    }
}
