#include <vlog/column.h>
#include <vlog/chasemgmt.h>

FunctionalColumn::FunctionalColumn(std::shared_ptr<ChaseMgmt> chase,
        std::vector<std::shared_ptr<Column>> &columns) {
    if (columns.size() == 0)  {
        this->nvalues = 0;
    } else {
        this->nvalues = columns[0]->size();
    }
    this->chase = chase;
    //startvalue = chase->getNewOrExistingID(columns, this->nvalues);
}

bool FunctionalColumn::isEmpty() const {
    return nvalues == 0;
}

bool FunctionalColumn::isEDB() const {
    return false;
}

size_t FunctionalColumn::size() const {
    return nvalues;
}

size_t FunctionalColumn::estimateSize() const {
    return nvalues;
}

Term_t FunctionalColumn::getValue(const size_t pos) const {
    return startvalue + pos;
}

bool FunctionalColumn::supportsDirectAccess() const {
    return true;
}

bool FunctionalColumn::isIn(const Term_t t) const {
    if (t >= startvalue && t < startvalue + nvalues) {
        return true;
    } else {
        return false;
    }
}

std::unique_ptr<ColumnReader> FunctionalColumn::getReader() const {
    LOG(ERRORL) << "Not implemented";
    throw 10;
}

std::shared_ptr<Column> FunctionalColumn::sort() const {
    return std::shared_ptr<Column>(new FunctionalColumn(*this));
}

std::shared_ptr<Column> FunctionalColumn::sort(const int nthreads) const {
    return std::shared_ptr<Column>(new FunctionalColumn(*this));
}

std::shared_ptr<Column> FunctionalColumn::unique() const {
    return std::shared_ptr<Column>(new FunctionalColumn(*this));
}

bool FunctionalColumn::isConstant() const {
    return nvalues < 2;
}
