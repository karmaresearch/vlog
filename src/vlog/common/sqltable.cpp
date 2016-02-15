#include <sstream>
#include <string>

#include <vlog/sqltable.h>


std::vector<std::shared_ptr<Column>> SQLTable::checkNewIn(const Literal &l1,
                                  std::vector<uint8_t> &posInL1,
                                  const Literal &l2,
std::vector<uint8_t> posInL2) {

    BOOST_LOG_TRIVIAL(debug) << "checkNewIn version 1";
    std::vector<uint8_t> posVars1 = l1.getPosVars();
    std::vector<uint8_t> fieldsToSort1;
    for (int i = 0; i < posInL1.size(); i++) {
	fieldsToSort1.push_back(posVars1[posInL1[i]]);
    }
    EDBIterator *iter1 = getSortedIterator(l1, fieldsToSort1);

    std::vector<uint8_t> posVars2 = l2.getPosVars();
    std::vector<uint8_t> fieldsToSort2;
    for (int i = 0; i < posInL2.size(); i++) {
	fieldsToSort2.push_back(posVars2[posInL2[i]]);
    }
    EDBIterator *iter2 = getSortedIterator(l2, fieldsToSort2);

    std::vector<std::shared_ptr<ColumnWriter>> cols;
    for (int i = 0; i < fieldsToSort1.size(); i++) {
	cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));
    }

    bool more = false;
    if (iter1->hasNext() && iter2->hasNext()) {
	iter1->next();
	iter2->next();
	while (true) {
	    bool equal = true;
	    bool lt = false;
	    for (int i = 0; i < fieldsToSort1.size(); i++) {
		if (iter1->getElementAt(fieldsToSort1[i]) != iter2->getElementAt(fieldsToSort2[i])) {
		    equal = false;
		    lt = iter1->getElementAt(fieldsToSort1[i]) != iter2->getElementAt(fieldsToSort2[i]);
		    break;
		}
	    }
	    if (equal) {
		if (iter1->hasNext()) {
		    iter1->next();
		    if (iter2->hasNext()) {
			iter2->next();
		    } else {
			more = true;
			break;
		    }
		} else {
		    break;
		}
	    } else if (lt) {
		for (int i = 0; i < fieldsToSort1.size(); i++) {
		    cols[i]->add(iter1->getElementAt(fieldsToSort1[i]));
		}
		if (iter1->hasNext()) {
		    iter1->next();
		} else {
		    break;
		}
	    } else {
		if (iter2->hasNext()) {
		    iter2->next();
		} else {
		    more = true;
		    break;
		}
	    }
	}
    } else {
	more = iter1->hasNext();
    }

    if (more) {
	for (int i = 0; i < fieldsToSort1.size(); i++) {
	    cols[i]->add(iter1->getElementAt(fieldsToSort1[i]));
	}
	while (iter1->hasNext()) {
	    iter1->next();
	    for (int i = 0; i < fieldsToSort1.size(); i++) {
		cols[i]->add(iter1->getElementAt(fieldsToSort1[i]));
	    }
	}
    }

    std::vector<std::shared_ptr<Column>> output;
    for (auto &writer : cols) {
	output.push_back(writer->getColumn());
    }

    iter1->clear();
    iter2->clear();
    delete iter1;
    delete iter2;
    return output;
}

std::vector<std::shared_ptr<Column>> SQLTable::checkNewIn(
                                      std::vector <
                                      std::shared_ptr<Column >> &checkValues,
                                      const Literal &l,
std::vector<uint8_t> posInL) {

    BOOST_LOG_TRIVIAL(debug) << "checkNewIn version 2";

    std::vector<uint8_t> posVars = l.getPosVars();
    std::vector<uint8_t> fieldsToSort;
    for (int i = 0; i < posInL.size(); i++) {
	fieldsToSort.push_back(posVars[posInL[i]]);
    }

    EDBIterator *iter = getSortedIterator(l, fieldsToSort);

    int sz = checkValues.size();

    std::vector<std::shared_ptr<ColumnWriter>> cols;
    for (int i = 0; i < fieldsToSort.size(); i++) {
	cols.push_back(std::shared_ptr<ColumnWriter>(new ColumnWriter()));
    }

    std::vector<std::unique_ptr<ColumnReader>> valuesToChReader;

    for (int i = 0; i < checkValues.size(); i++) {
	std::shared_ptr<Column> valuesToCh = checkValues[i];
	valuesToChReader.push_back(valuesToCh->getReader());
    }

    std::vector<Term_t> prevcv;
    std::vector<Term_t> cv;
    std::vector<Term_t> vi;

    for (int i = 0; i < sz; i++) {
	prevcv.push_back((Term_t) -1);
	cv.push_back((Term_t) -1);
	vi.push_back((Term_t) -1);
    }

    if (iter->hasNext()) {
	iter->next();
	for (int i = 0; i < sz; i++) {
	    if (! valuesToChReader[i]->hasNext()) {
		throw 10;
	    }
	    cv[i] = valuesToChReader[i]->next();
	}

	bool equal;

	while (true) {
	    for (int i = 0; i < sz; i++) {
		vi[i] = iter->getElementAt(fieldsToSort[i]);
	    }
	    equal = true;
	    bool lt = false;
	    for (int i = 0; i < sz; i++) {
		if (vi[i] == cv[i]) {
		} else {
		    if (vi[i] < cv[i]) {
			lt = true;
		    }
		    equal = false;
		    break;
		}
	    }
	    if (equal) {
		for (int i = 0; i < sz; i++) {
		    prevcv[i] = cv[i];
		}
		if (iter->hasNext()) {
		    iter->next();
		    for (int i = 0; i < sz; i++) {
                        if (!valuesToChReader[i]->hasNext()) {
                            cv[0] = (Term_t) - 1;
                            break;
                        } else {
                            cv[i] = valuesToChReader[i]->next();
                        }
                    }
		    if (cv[0] == (Term_t) -1) {
			break;
		    }
		} else {
		    break;
		}
	    } else if (lt) {
		if (iter->hasNext()) {
		    iter->next();
		} else {
		    break;
		}
	    } else {
		equal = true;
		for (int i = 0; i < sz; i++) {
		    if (cv[i] != prevcv[i]) {
			equal = false;
			break;
		    }
		}
		if (! equal) {
		    for (int i = 0; i < sz; i++) {
			cols[i]->add(cv[i]);
                        prevcv[i] = cv[i];
                    }
		}
		for (int i = 0; i < sz; i++) {
		    if (!valuesToChReader[i]->hasNext()) {
			cv[0] = (Term_t) - 1;
			break;
		    } else {
			cv[i] = valuesToChReader[i]->next();
		    }
		}
		if (cv[0] == (Term_t) -1) {
		    break;
		}
            }
        }

        while (cv[0] != (Term_t) - 1) {
	    equal = true;
	    for (int i = 0; i < sz; i++) {
		if (cv[i] != prevcv[i]) {
		    equal = false;
		    break;
		}
	    }
	    if (! equal) {
		for (int i = 0; i < sz; i++) {
		    cols[i]->add(cv[i]);
		    prevcv[i] = cv[i];
		}
	    }
	    for (int i = 0; i < sz; i++) {
		if (!valuesToChReader[i]->hasNext()) {
		    cv[0] = (Term_t) - 1;
		    break;
		} else {
		    cv[i] = valuesToChReader[i]->next();
		}
	    }
	}
    }

    std::vector<std::shared_ptr<Column>> output;
    for (auto &el : cols)
        output.push_back(el->getColumn());
    iter->clear();
    delete iter;
    return output;
}

std::shared_ptr<Column> SQLTable::checkIn(
    std::vector<Term_t> &values,
    const Literal &l,
    uint8_t posInL,
    size_t &sizeOutput) {

    if (l.getNVars() == 0 || l.getNVars() == l.getTupleSize()) {
	BOOST_LOG_TRIVIAL(error) << "SQLTable::checkIn() with getNVars() == " << l.getNVars() << " is not supported.";
	throw 10;
    }

    std::vector<uint8_t> posVars = l.getPosVars();
    std::vector<uint8_t> fieldsToSort;
    fieldsToSort.push_back(posVars[posInL]);
    EDBIterator *iter = getSortedIterator(l, fieldsToSort);

    //Output
    std::unique_ptr<ColumnWriter> col(new ColumnWriter());
    size_t idx1 = 0;
    const uint8_t varIndex = posVars[posInL];
    sizeOutput = 0;
    while (iter->hasNext()) {
	iter->next();
	const Term_t v2 = iter->getElementAt(varIndex);
	while (values[idx1] < v2) {
	    idx1++;
	    if (idx1 == values.size()) {
		break;
	    }
	}
	if (values[idx1] == v2) {
	    col->add(v2);
	    sizeOutput++;
	    idx1++;
	    if (idx1 == values.size()) {
		break;
	    }
	}
    }
    iter->clear();
    delete iter;
    return col->getColumn();
}

string SQLTable::literalConstraintsToSQLQuery(const Literal &q,
        const std::vector<string> &fieldTables) {
    string cond = "";
    int idxField = 0;
    while (idxField < q.getTupleSize()) {
        if (!q.getTermAtPos(idxField).isVariable()) {
            if (cond != "") {
                cond += " and ";
            }
            cond += fieldTables[idxField] + "=" +
                    to_string(q.getTermAtPos(idxField).getValue());
        }
        idxField++;
    }
    return cond;
}

string SQLTable::repeatedToSQLQuery(const Literal &q,
        const std::vector<string> &fieldTables) {
    string cond = "";
    std::vector<std::pair<uint8_t, uint8_t>> repeated = q.getRepeatedVars();
    for (int i = 0; i < repeated.size(); i++) {
	if (i != 0) {
	    cond += " and ";
	}
	cond += fieldTables[repeated[i].first] + "=" + fieldTables[repeated[i].second];
    }
    return cond;
}

size_t SQLTable::estimateCardinality(const Literal &query) {
    //TODO: This should be improved
    return getCardinality(query);
}

void SQLTable::releaseIterator(EDBIterator *itr) {
    delete itr;
}

