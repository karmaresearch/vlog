#include <vlog/fcinttable.h>
#include <vlog/concepts.h>

EDBFCInternalTable::EDBFCInternalTable(const size_t iteration,
        const Literal &literal, EDBLayer *layer)
    : iteration(iteration),
    nfields(literal.getNVars()),
    query(QSQQuery(literal)),
    layer(layer) {

        int j = 0;
        defaultSorting.clear();
        for (int i = 0; i < literal.getTupleSize(); ++i) {
            VTerm t = literal.getTermAtPos(i);
            if (t.isVariable()) {
                defaultSorting.push_back(j);
                posFields[j++] = i;
            }
        }
    }

size_t EDBFCInternalTable::getNRows() const {
    return layer->getCardinality(*query.getLiteral());
}

bool EDBFCInternalTable::isEmpty() const {
    const Literal l = *query.getLiteral();
    bool retval = layer->isEmpty(l, NULL, NULL);
    LOG(DEBUGL) << "isEmpty: literal = " << l.tostring(NULL, layer) << ", returns: " << retval;
    return retval;
}

std::pair<std::shared_ptr<const Segment>,
    std::shared_ptr<const Segment>>
    EDBFCInternalTable::replaceAllTermsWithMap(EGDTermMap &map,
            bool replace) const {

        SegmentInserter oldTuples(nfields);
        SegmentInserter newTuples(nfields);
        auto itr = getSortedIterator();
        Term_t row[nfields];
        while (itr->hasNext()) {
            itr->next();
            for(size_t i = 0; i < nfields; ++i) {
                auto v = itr->getCurrentValue(i);
                row[i] = v;
            }
            replaceRow(oldTuples, replace, newTuples, row, 0, nfields, map);
        }
        releaseIterator(itr);

        if (replace) {
            return std::make_pair(oldTuples.getSegment(),
                    newTuples.getSortedAndUniqueSegment());
        } else {
            return std::make_pair(std::shared_ptr<const Segment>(),
                    newTuples.getSortedAndUniqueSegment());
        }
    }

uint8_t EDBFCInternalTable::getRowSize() const {
    return nfields;
}

FCInternalTableItr *EDBFCInternalTable::getIterator() const {
    EDBFCInternalTableItr *itr = new EDBFCInternalTableItr();
    const Literal &l = *query.getLiteral();
    EDBIterator *edbItr = layer->getSortedIterator(l, defaultSorting);
    itr->init(iteration, defaultSorting, nfields, posFields, edbItr, layer, query.getLiteral());
    return itr;
}

FCInternalTableItr *EDBFCInternalTable::getSortedIterator() const {
    return getIterator();
}

std::shared_ptr<const FCInternalTable> EDBFCInternalTable::merge(std::shared_ptr<const FCInternalTable> t, int nthreads) const {
    assert(false);
    throw 10;
}

bool EDBFCInternalTable::isSorted() const {
    return true;
}

size_t EDBFCInternalTable::getRepresentationSize(std::set<uint64_t> &IDs) const {
    return nfields;
}

size_t EDBFCInternalTable::estimateNRows(const uint8_t nconstantsToFilter,
        const uint8_t *posConstantsToFilter,
        const Term_t *valuesConstantsToFilter) const {
    if (nconstantsToFilter == 0)
        return layer->estimateCardinality(*(query.getLiteral()));

    //Create a new literal adding the constants
    VTuple t = query.getLiteral()->getTuple();
    for (int i = 0; i < nconstantsToFilter; ++i) {
        t.set(VTerm(0, valuesConstantsToFilter[i]), posConstantsToFilter[i]);
    }
    const Literal newLiteral(query.getLiteral()->getPredicate(), t);
    //const QSQQuery query(newLiteral);
    return layer->estimateCardinality(newLiteral);
}

std::shared_ptr<Column> EDBFCInternalTable::getColumn(
        const uint8_t columnIdx) const {
    //bool unq = query.getLiteral()->getNVars() == 2;
    std::vector<uint8_t> presortFields;
    for (int i = 0; i < columnIdx; ++i)
        presortFields.push_back(i);

    return std::shared_ptr<Column>(new EDBColumn(*layer,
                *query.getLiteral(),
                columnIdx,
                presortFields,
                //unq));
           false));
}

bool EDBFCInternalTable::isColumnConstant(const uint8_t columnid) const {
    return false;
}

Term_t EDBFCInternalTable::getValueConstantColumn(const uint8_t columnid) const {
    throw 10; //should never be called
}

std::shared_ptr<const FCInternalTable> EDBFCInternalTable::filter(
        const uint8_t nPosToCopy, const uint8_t *posVarsToCopy,
        const uint8_t nPosToFilter, const uint8_t *posConstantsToFilter,
        const Term_t *valuesConstantsToFilter, const uint8_t nRepeatedVars,
        const std::pair<uint8_t, uint8_t> *repeatedVars, int nthreads) const {

    //Create a new literal adding the constants
    VTuple t = query.getLiteral()->getTuple();
    for (int i = 0; i < nPosToFilter; ++i) {
        t.set(VTerm(0, valuesConstantsToFilter[i]), posConstantsToFilter[i]);
    }
    for (int i = 0; i < nRepeatedVars; ++i) {
        t.set(t.get(repeatedVars[i].first), repeatedVars[i].second);
    }
    const Literal newLiteral(query.getLiteral()->getPredicate(), t);

    // LOG(DEBUGL) << "EDBFCInternalTable";

    FCInternalTable *filteredTable = new EDBFCInternalTable(iteration,
            newLiteral, layer);
    if (filteredTable->isEmpty()) {
        delete filteredTable;
        return NULL;
    } else {
        assert(filteredTable->getRowSize() == nPosToCopy);
        return std::shared_ptr<const FCInternalTable>(filteredTable);
    }
}

FCInternalTableItr *EDBFCInternalTable::sortBy(const std::vector<uint8_t> &fields) const {
    EDBFCInternalTableItr *itr = new EDBFCInternalTableItr();
    const Literal &l = *query.getLiteral();
    EDBIterator *edbItr = layer->getSortedIterator(l, fields);
    itr->init(iteration, fields, nfields, posFields, edbItr, layer, query.getLiteral());
    return itr;
}

FCInternalTableItr *EDBFCInternalTable::sortBy(const std::vector<uint8_t> &fields,
        const int nthreads) const {
    //Ignore the nthreads parameter
    return sortBy(fields);
}

void EDBFCInternalTable::releaseIterator(FCInternalTableItr *itr) const {
    EDBFCInternalTableItr *castedItr = (EDBFCInternalTableItr*)itr;
    layer->releaseIterator(castedItr->getEDBIterator());
    delete castedItr;
}

EDBFCInternalTable::~EDBFCInternalTable() {
}

void EDBFCInternalTableItr::init(const size_t iteration,
        const std::vector<uint8_t> &fields,
        const uint8_t nfields,
        uint8_t const *posFields,
        EDBIterator *itr,
        EDBLayer *layer,
        const Literal *query) {
    this->iteration = iteration;
    this->edbItr = itr;
    this->fields = fields;
    this->nfields = nfields;
    this->layer = layer;
    this->query = query;
    // LOG(DEBUGL) << "EDB iter: nfields = " << (int) this->nfields;
    for (int i = 0; i < nfields; ++i) {
        // LOG(DEBUGL) << "EDB iter: posfields[" << i << "] = " << (int) posFields[i];
        this->posFields[i] = posFields[i];
    }
    compiled = false;
}

FCInternalTableItr *EDBFCInternalTableItr::copy() const {
    EDBFCInternalTableItr *itr = new EDBFCInternalTableItr();
    EDBIterator *edbItr = layer->getSortedIterator(*query, fields);
    itr->init(iteration, fields, nfields, posFields, edbItr, layer, query);
    return itr;
}

EDBIterator *EDBFCInternalTableItr::getEDBIterator() {
    return edbItr;
}

inline bool EDBFCInternalTableItr::hasNext() {
    bool response = edbItr->hasNext();
    return response;
}

inline void EDBFCInternalTableItr::next() {
    edbItr->next();
    compiled = false;
}

uint8_t EDBFCInternalTableItr::getNColumns() const {
    return nfields;
}

inline Term_t EDBFCInternalTableItr::getCurrentValue(const uint8_t pos) {
    return edbItr->getElementAt(posFields[pos]);
}

std::vector<std::shared_ptr<Column>> EDBFCInternalTableItr::getColumn(
        const uint8_t ncolumns, const uint8_t *columns) {
    std::vector<std::shared_ptr<Column>> output;

    //Fields are the fields on which this table should be sorted
    std::vector<uint8_t> presortFields = fields;
    for (int i = 0; i < ncolumns; ++i) {
        int columnNo = columns[i];
        std::vector<uint8_t> columnpresort;
        for(int j = 0; j < presortFields.size(); ++j) {
            if (presortFields[j] != columnNo)
                columnpresort.push_back(presortFields[j]);
            else
                break;
        }

        output.push_back(std::shared_ptr<Column>(
                    new EDBColumn(*layer, *query, columnNo, columnpresort, false)));
        //Add it only if it is not there
        bool found = false;
        for(int j = 0; j < presortFields.size() && !found; ++j)
            if (presortFields[j] == columnNo)
                found = true;

        if (!found) {
            presortFields.push_back(columnNo);
        }
    }
    return output;
}

std::vector<std::shared_ptr<Column>> EDBFCInternalTableItr::getAllColumns() {
    std::vector<uint8_t> idxs;
    for (int i = 0; i < nfields; ++i) {
        idxs.push_back(i);
    }
    return getColumn(nfields, &(idxs[0]));
}
