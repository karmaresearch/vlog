#include <launcher/vlogscan.h>


uint64_t VLogScan::getValue1() {
    return iterator->getElementAt(value1_index);
}

uint64_t VLogScan::getValue2() {
    assert(aggr != DBLayer::Aggr_t::AGGR_SKIP_2LAST);
    return iterator->getElementAt(value2_index);
}

uint64_t VLogScan::getValue3() {
    assert(aggr == DBLayer::Aggr_t::AGGR_NO);
    return iterator->getElementAt(value3_index);
}

uint64_t VLogScan::getCount() {
    TupleTableItr *it = (TupleTableItr *) (iterator.get());
    return it->count();
}

bool VLogScan::next() {
    if (iterator->hasNext()) {
        iterator->next();
        // LOG(DEBUGL) << "Iterator = " << iterator.get() << ", value3 = " << getValue3();
        return true;
    }
    return false;
}

bool VLogScan::first() {
    return VLogScan::first(0, false);
}

bool VLogScan::first(uint64_t first, bool constrained) {
    if (aggr == DBLayer::Aggr_t::AGGR_SKIP_2LAST) {
	// TODO: support this?
	throw 10;
    }
    return VLogScan::first(first, constrained, 0, false);
}

bool VLogScan::first(uint64_t first, bool constrained1, uint64_t second, bool constrained2) {
    bool resp = VLogScan::first(first, constrained1, second, constrained2, 0, false);

    //I must instruct the tupletableitr to exclude the last column
    if (resp && aggr != DBLayer::Aggr_t::AGGR_NO) {
        ((TupleTableItr*)iterator.get())->skipLastColumn();
    }
    return resp;
}

bool VLogScan::first(uint64_t first, bool constrained1, uint64_t second, bool constrained2,
                     uint64_t third, bool constrained3) {

    Literal query = getLiteral(order, first, constrained1, second, constrained2,
                               third, constrained3);
    std::vector<uint8_t> *keypos = NULL;
    std::vector<uint64_t> *keys = NULL;
    int bitset = 0;
    if (hint != NULL) {
	keys = hint->getKeys(&bitset);
	if (keys != NULL) {
	    LOG(DEBUGL) << "I have some keys: size = " << keys->size() << ", bitset = " << bitset;
	    LOG(DEBUGL) << "Literal = " << query.tostring();
	    // Assumes a single join variable.
	    keypos = new std::vector<uint8_t>();
	    uint8_t varNo = 0;
	    int flag = 1;
	    for (int i = 0; i < 3; i++) {
		VTerm t = query.getTermAtPos(i);
		if (t.isVariable()) {
		    if (bitset & flag) {
			keypos->push_back(varNo);
			LOG(DEBUGL) << "Variable number = " << (int) varNo;
			break;
		    }
		    varNo++;
		}
		flag <<= 1;
	    }
	}
    }

    //If the order requires sorted data, then I must sort it
    std::vector<uint8_t> sortByFields;
    switch (order) {
    case DBLayer::DataOrder::Order_Object_Predicate_Subject:
        sortByFields.push_back(2);
        sortByFields.push_back(1);
        sortByFields.push_back(0);
        break;
    case DBLayer::DataOrder::Order_Object_Subject_Predicate:
        sortByFields.push_back(2);
        sortByFields.push_back(0);
        sortByFields.push_back(1);
        break;
    case DBLayer::DataOrder::Order_Predicate_Object_Subject:
        sortByFields.push_back(1);
        sortByFields.push_back(2);
        sortByFields.push_back(0);
        break;
    case DBLayer::DataOrder::Order_Predicate_Subject_Object:
        sortByFields.push_back(1);
        sortByFields.push_back(0);
        sortByFields.push_back(2);
        break;
    case DBLayer::DataOrder::Order_Subject_Object_Predicate:
        sortByFields.push_back(0);
        sortByFields.push_back(2);
        sortByFields.push_back(1);
        break;
    case DBLayer::DataOrder::Order_Subject_Predicate_Object:
        sortByFields.push_back(0);
        sortByFields.push_back(1);
        sortByFields.push_back(2);
        break;
    case DBLayer::DataOrder::Order_No_Order_OPS:
    case DBLayer::DataOrder::Order_No_Order_OSP:
    case DBLayer::DataOrder::Order_No_Order_POS:
    case DBLayer::DataOrder::Order_No_Order_PSO:
    case DBLayer::DataOrder::Order_No_Order_SPO:
    case DBLayer::DataOrder::Order_No_Order_SOP:
        break;
    }

    if (keypos != NULL) {
	assert(keys != NULL);
	if (keys->size() == 0) {
	    return false;
	}
    }

    TupleIterator *tmpitr = r->getIterator(
                                    query, keypos, keys, layer, p,
                                    false, &sortByFields);
    iterator = std::unique_ptr<TupleIterator>(tmpitr);

    if (iterator && iterator->hasNext()) {
        iterator->next();
        return true;

    }
    return false;
}

Literal VLogScan::getLiteral(DBLayer::DataOrder order, uint64_t first,
                             bool constrained1, uint64_t second, bool constrained2,
                             uint64_t third, bool constrained3) {
    LOG(DEBUGL) << "getLiteral: first = " << first << ", second = " << second << ", third = " << third;

    VTerm terms[3];
    uint8_t vardidx = 1;
    if (constrained1) {
        terms[0] = VTerm(0, first);
    } else {
        terms[0] = VTerm(vardidx++, 0);
    }
    if (constrained2) {
        terms[1] = VTerm(0, second);
    } else {
        terms[1] = VTerm(vardidx++, 0);
    }
    if (constrained3) {
        terms[2] = VTerm(0, third);
    } else {
        terms[2] = VTerm(vardidx++, 0);
    }

    //Create a ternary query
    VTuple tuple(3);
    switch (order) {
    case DBLayer::Order_No_Order_SPO:
    case DBLayer::Order_Subject_Predicate_Object:
        tuple.set(terms[0], 0);
        tuple.set(terms[1], 1);
        tuple.set(terms[2], 2);
        break;
    case DBLayer::Order_No_Order_SOP:
    case DBLayer::Order_Subject_Object_Predicate:
        tuple.set(terms[0], 0);
        tuple.set(terms[2], 1);
        tuple.set(terms[1], 2);
        break;
    case DBLayer::Order_No_Order_POS:
    case DBLayer::Order_Predicate_Object_Subject:
        tuple.set(terms[2], 0);
        tuple.set(terms[0], 1);
        tuple.set(terms[1], 2);
        break;
    case DBLayer::Order_No_Order_PSO:
    case DBLayer::Order_Predicate_Subject_Object:
        tuple.set(terms[1], 0);
        tuple.set(terms[0], 1);
        tuple.set(terms[2], 2);
        break;
    case DBLayer::Order_No_Order_OPS:
    case DBLayer::Order_Object_Predicate_Subject:
        tuple.set(terms[2], 0);
        tuple.set(terms[1], 1);
        tuple.set(terms[0], 2);
        break;
    case DBLayer::Order_No_Order_OSP:
    case DBLayer::Order_Object_Subject_Predicate:
        tuple.set(terms[1], 0);
        tuple.set(terms[2], 1);
        tuple.set(terms[0], 2);
        break;
    }
    return Literal(Predicate(predQuery, Predicate::calculateAdornment(tuple)), tuple);

}
