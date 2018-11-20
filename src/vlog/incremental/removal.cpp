#include <vlog/incremental/removal.h>

#include <vlog/incremental/edb-table-from-idb.h>

#include <vlog/edb.h>
#if 0
#include <vlog/concepts.h>
#include <vlog/idxtupletable.h>
#include <vlog/column.h>
#endif

#include <climits>


EDBRemoveLiterals::EDBRemoveLiterals(const std::string &file,
                                     EDBLayer *layer) :
        layer(layer), num_rows(0) {
    std::ifstream infile(file);
    std::string token;
    std::vector<Term_t> terms;
    PredId_t pred;
    while (infile >> token) {
        LOG(DEBUGL) << "Read token '" << token << "'";
        uint64_t val;
        if (token == ".") {
            insert(terms);
            terms.clear();
        } else {
            layer->getOrAddDictNumber(token.c_str(), token.size(), val);
            terms.push_back(val);
        }
    }
}

// Looks up the table in layer
EDBRemoveLiterals::EDBRemoveLiterals(PredId_t predid,
                                     EDBLayer *layer) {
    const std::shared_ptr<EDBTable> table = layer->getEDBTable(predid);
    uint8_t arity = table->getArity();
    Predicate pred(predid, 0, EDB, arity);
    VTuple t = VTuple(arity);
    for (uint8_t i = 0; i < t.getSize(); ++i) {
        t.set(VTerm(i + 1, 0), i);
    }
    Literal lit(pred, t);

    EDBIterator *itr = table->getIterator(lit);
    std::vector<Term_t> terms(arity);
    while (itr->hasNext()) {
        itr->next();
        for (uint8_t m = 0; m < arity; ++m) {
            terms[m] = itr->getElementAt(m);
        }
        insert(terms);
    }
    table->releaseIterator(itr);

    // dump(std::cerr, *layer);
}

EDBRemoveItem *EDBRemoveLiterals::insert_recursive(const std::vector<Term_t> &terms, size_t offset) {
    EDBRemoveItem *item;
    if (offset == terms.size()) {
        item = NULL;
    } else {
        item = new EDBRemoveItem();
        item->has[terms[offset]] = insert_recursive(terms, offset + 1);
    }

    return item;
}


void EDBRemoveLiterals::insert(const std::vector<Term_t> &terms) {
    removed.has[terms[0]] = insert_recursive(terms, 1);
    ++num_rows;
}


static void dump_map(std::ostream &os,
                     const std::unordered_map<Term_t, EDBRemoveItem *> &m) {
    for (auto &r : m) {
        os << "    " << r.first << " -> " << r.second << std::endl;
    }
}

bool EDBRemoveLiterals::present(const std::vector<Term_t> &terms) const {
    const EDBRemoveItem *m = &removed;
    for (auto i = 0; i < terms.size(); ++i) {
        const auto f = m->has.find(terms[i]);
        if (f == m->has.end()) {
            return false;
        }
        m = f->second;
    }

#ifdef DEBUG
    ostringstream os;
    os << "Hit row in removed: ";
    os << "[";
    for (auto t : terms) {
        os << t << " ";
    }
    os << "]";
    LOG(DEBUGL) << os.str();
#endif

    return true;
}


std::ostream &EDBRemoveLiterals::dump_recursive_name(
        std::ostream &of,
        const EDBLayer &layer,
        const EDBRemoveItem *item) const {
    if (item == NULL) {
        return of;
    }

    for (auto rm = item->has.begin(); rm != item->has.end(); ++rm) {
        char name[1024];
        layer.getDictText(rm->first, name);
        of << name << ",";
        dump_recursive_name(of, layer, rm->second);
        of << std::endl;
    }

    return of;
}


std::ostream &EDBRemoveLiterals::dump_recursive(
        std::ostream &of,
        const EDBRemoveItem *item) const {
    if (item == NULL) {
        return of;
    }

    for (auto rm = item->has.begin(); rm != item->has.end(); ++rm) {
        of << rm->first << ",";
        dump_recursive(of, rm->second);
        of << std::endl;
    }

    return of;
}

std::ostream &EDBRemoveLiterals::dump(
        std::ostream &of,
        const EDBLayer &layer) const {
    dump_recursive(of, &removed);
    dump_recursive_name(of, layer, &removed);

    return of;
}
