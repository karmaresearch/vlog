#include <vlog/embeddings/embtable.h>
#include <vlog/embeddings/embiterator.h>

#include <kognac/utils.h>

EmbTable::EmbTable(PredId_t predid,
        std::string predname,
        EDBLayer *layer,
        std::string pathfile,
        std::string startRange,
        std::string dictPredName) :
    predid(predid),
    layer(layer),
    pathfile(pathfile),
    startRange(stoi(startRange)),
    dictPredName(dictPredName) {
        prefix = "EMB-" + predname + "-";
        emb = Embeddings<double>::load(pathfile);
        N = emb->getN();
        auto dictPredId = layer->getPredID(dictPredName);
        dictTable = layer->getEDBTable(dictPredId);

        std::string filemappings = pathfile + ".map";
        if (Utils::exists(filemappings)) {
            std::ifstream ifs(filemappings);
            std::string line;
            size_t idx = 0;
            while (std::getline(ifs, line)) {
                //Each line reports the predicate ID used for the embedding
                auto termid = stol(line);
                predIDsMap.insert(std::make_pair(termid, idx));
                predIDsList.push_back(termid);
                invPredIDsMap.insert(std::make_pair(idx, termid));
                idx += 1;
            }
            ifs.close();
            usePredicateMappings = true;
        } else {
            usePredicateMappings = false;
        }
    }

void EmbTable::query(QSQQuery *query, TupleTable *outputTable,
        std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    LOG(ERRORL) << "EmbTable: Not supported";
    throw 10;
}

size_t EmbTable::estimateCardinality(const Literal &query) {
    return getCardinality(query);
}

Term_t EmbTable::getEntity(uint64_t embid) {
    embid = embid - startRange;
    if (usePredicateMappings) {
        return invPredIDsMap[embid];
    } else {
        return embid;
    }
}

size_t EmbTable::getCardinality(const Literal &query) {
    auto v1 = query.getTermAtPos(0);
    auto v2 = query.getTermAtPos(1);
    int64_t v1value = v1.getValue();
    int64_t v2value = v2.getValue() - startRange;
    if (!v1.isVariable()) {
        if (predIDsMap.count(v1value) ||
                (!usePredicateMappings && v1value >= 0 && v1value < N)) {
            if (!v2.isVariable()) {
                if (v2value == v1value) {
                    return 1;
                } else {
                    return 0;
                }
            } else {
                return 1;
            }
        } else {
            return 0;
        }
    } else if (!v2.isVariable()) {
        if (predIDsMap.count(v2value) ||
                (!usePredicateMappings && v2value >= 0 && v2value < N)) {
            return 1;
        } else {
            return 0;
        }
    } else if (v1.getId() != v2.getId()) {
        return N;
    } else {
        return 0;
    }
}

size_t EmbTable::getCardinalityColumn(const Literal &query, uint8_t posColumn) {
    return getCardinality(query);
}

bool EmbTable::isEmpty(const Literal &query, std::vector<uint8_t> *posToFilter,
        std::vector<Term_t> *valuesToFilter) {
    if (posToFilter == NULL && valuesToFilter == NULL) {
        return getCardinality(query) == 0;
    } else {
        LOG(ERRORL) << "EmbTable: Not supported";
        throw 10;
    }
}

EDBIterator *EmbTable::getIterator(const Literal &query) {
    auto v1 = query.getTermAtPos(0);
    auto v2 = query.getTermAtPos(1);
    int64_t v1value = v1.getValue();
    int64_t v2value = v2.getValue() - startRange;
    if (!v1.isVariable()) {
        if (predIDsMap.count(v1value) ||
                (!usePredicateMappings && v1value >= 0 && v1value < N)) {
            if (!v2.isVariable()) {
                if (v2value == v1value) {
                    if (usePredicateMappings) {
                        auto posV1value = predIDsMap[v1value];
                        return new EmbIterator(predid, posV1value,
                                posV1value + 1, startRange, predIDsList.data());
                    } else {
                        return new EmbIterator(
                                predid, v1value, v1value + 1, startRange);
                    }
                } else {
                    return new EmbIterator(predid, 0, 0, 0); //None
                }
            } else {
                if (usePredicateMappings) {
                    auto posV1value = predIDsMap[v1value];
                    return new EmbIterator(predid, posV1value,
                            posV1value + 1, startRange, predIDsList.data());
                } else {
                    return new EmbIterator(
                            predid, v1value, v1value + 1, startRange);
                }
            }
        } else {
            return new EmbIterator(predid, 0, 0, 0); //None
        }
    } else if (!v2.isVariable()) {
        if (predIDsMap.count(v2value) ||
                (!usePredicateMappings && v2value >= 0 && v2value < N)) {
            if (usePredicateMappings) {
                return new EmbIterator(predid, v2value,
                        v2value + 1, startRange, predIDsList.data());
            } else {
                return new EmbIterator(
                        predid, v2value, v2value + 1, startRange);
            }

        } else {
            return new EmbIterator(predid, 0, 0, 0); //None
        }
    } else if (v1.getId() != v2.getId()) {
        if (usePredicateMappings) {
            return new EmbIterator(predid, 0, N, startRange, predIDsList.data());
        } else {
            return new EmbIterator(predid, 0, N, startRange);
        }
    } else {
        return new EmbIterator(predid, 0, 0, 0); //None
    }

}

EDBIterator *EmbTable::getSortedIterator(const Literal &query,
        const std::vector<uint8_t> &fields) {
    return getIterator(query);
}

bool EmbTable::getDictNumber(const char *text, const size_t sizeText,
        uint64_t &id) {
    if (sizeText > prefix.size()) {
        std::string s = std::string(text, sizeText);
        auto beg = s.find(prefix);
        if (beg == 0) {
            std::string sid = s.substr(prefix.size());
            try {
                auto idx = stoi(sid);
                if (idx >= 0 && idx < N) {
                    id = startRange + idx;
                    return true;
                }
            } catch (std::exception e) {
            }
            return false;
        }
    }
    return dictTable->getDictNumber(text, sizeText, id);
}

bool EmbTable::getDictText(const uint64_t id, char *text) {
    if (id >= startRange && id < startRange + N) {
        std::string v = prefix + std::to_string(id - startRange);
        strcpy(text, v.c_str());
        return true;
    } else {
        return dictTable->getDictText(id, text);
    }
}

bool EmbTable::getDictText(const uint64_t id, std::string &text) {
    if (id >= startRange && id < startRange + N) {
        text = prefix + std::to_string(id - startRange);
        return true;
    } else {
        return dictTable->getDictText(id, text);
    }
}

uint64_t EmbTable::getNTerms() {
    return N * 2;
}

void EmbTable::releaseIterator(EDBIterator *itr) {
    delete itr;
}

uint64_t EmbTable::getSize() {
    return emb->getN();
}

EmbTable::~EmbTable() {
}
