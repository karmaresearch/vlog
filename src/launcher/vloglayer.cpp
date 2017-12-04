#include <launcher/vloglayer.h>
#include <launcher/vlogscan.h>

#include <cmath>

// #define TEST_LUBM

bool VLogLayer::lookup(const std::string& text,
        ::Type::ID type,
        unsigned subType,
        uint64_t& id) {

    uint64_t longId;
    bool resp = false;
    if (type == ::Type::ID::URI) {
        string uri = "<" + text + ">";
        resp = edb.getDictNumber(uri.c_str(), uri.size(), longId);
    } else {
        resp = edb.getDictNumber(text.c_str(), text.size(), longId);
        if (! resp) {
            string tp = "";
            switch(type) {
                case ::Type::ID::String:
                    tp = "http://www.w3.org/2001/XMLSchema#string";
                    break;
                case ::Type::ID::Integer:
                    tp = "http://www.w3.org/2001/XMLSchema#integer";
                    break;
                case ::Type::ID::Decimal:
                    tp = "http://www.w3.org/2001/XMLSchema#decimal";
                    break;
                case ::Type::ID::Double:
                    tp = "http://www.w3.org/2001/XMLSchema#double";
                    break;
                case ::Type::ID::Boolean:
                    tp = "http://www.w3.org/2001/XMLSchema#boolean";
                    break;
                case ::Type::ID::Date:
                    tp = "http://www.w3.org/2001/XMLSchema#date";
                    break;
                default:
                    // TODO?
                    break;
            }
            string txt = "\"" + text + "\"^^<" + tp + ">";
            resp = edb.getDictNumber(txt.c_str(), txt.size(), longId);
        }
    }

    if (resp)
        id = (uint64_t) longId;
    return resp;
}

bool VLogLayer::lookupById(uint64_t id,
        const char*& start,
        const char*& stop,
        ::Type::ID& type,
        unsigned& subType) {
    if (edb.getDictText(id, tmpText)) {
        start = tmpText;
        stop = tmpText + strlen(tmpText);
        if (start[0] == '<') {
            start++;
            stop--;
            type = ::Type::ID::URI;
        } else {
            type = ::Type::ID::Literal;
        }
        subType = 0;
        return true;
    }
    return false;
}

bool VLogLayer::lookupById(uint64_t id,
        char* start,
        size_t &len,
        ::Type::ID& type,
        unsigned& subType) {
    LOG(ERRORL) << "Not implemented yet";
    throw 10;
}



uint64_t VLogLayer::getNextId() {
    return edb.getNTerms() + 1;
}

double VLogLayer::getScanCost(DBLayer::DataOrder order,
        uint64_t value1,
        uint64_t value1C,
        uint64_t value2,
        uint64_t value2C,
        uint64_t value3,
        uint64_t value3C) {

    //First I must swap the values
    uint64_t s, sc, p, pc, o, oc;
    double cost = 1;

    switch (order) {
        case DBLayer::Order_No_Order_SPO:
        case DBLayer::Order_Subject_Predicate_Object:
            sc = value1C;
            s = value1;
            pc = value2C;
            p = value2;
            oc = value3C;
            o = value3;
            break;
        case DBLayer::Order_No_Order_SOP:
        case DBLayer::Order_Subject_Object_Predicate:
            sc = value1C;
            s = value1;
            oc = value2C;
            o = value2;
            pc = value3C;
            p = value3;
            break;
        case DBLayer::Order_No_Order_POS:
        case DBLayer::Order_Predicate_Object_Subject:
            pc = value1C;
            p = value1;
            oc = value2C;
            o = value2;
            sc = value3C;
            s = value3;
            break;
        case DBLayer::Order_No_Order_PSO:
        case DBLayer::Order_Predicate_Subject_Object:
            pc = value1C;
            p = value1;
            sc = value2C;
            s = value2;
            oc = value3C;
            o = value3;
            break;
        case DBLayer::Order_No_Order_OPS:
        case DBLayer::Order_Object_Predicate_Subject:
            oc = value1C;
            o = value1;
            pc = value2C;
            p = value2;
            sc = value3C;
            s = value3;
            break;
        case DBLayer::Order_No_Order_OSP:
        case DBLayer::Order_Object_Subject_Predicate:
            oc = value1C;
            o = value1;
            sc = value2C;
            s = value2;
            pc = value3C;
            p = value3;
            break;
    }

    int nvars = 1;
    switch (order) {
        case DBLayer::Order_No_Order_SPO:
        case DBLayer::Order_Subject_Predicate_Object:
            if (s != ~0ul) {
                nvars = 3;
            } else if (p != ~0ul) {
                nvars = 2;
            }
            break;
        case DBLayer::Order_No_Order_SOP:
        case DBLayer::Order_Subject_Object_Predicate:
            if (s != ~0ul) {
                nvars = 3;
            } else if (o != ~0ul) {
                nvars = 2;
            }
            break;
        case DBLayer::Order_No_Order_POS:
        case DBLayer::Order_Predicate_Object_Subject:
            if (p != ~0ul) {
                nvars = 3;
            } else if (o != ~0ul) {
                nvars = 2;
            }
            break;
        case DBLayer::Order_No_Order_PSO:
        case DBLayer::Order_Predicate_Subject_Object:
            if (p != ~0ul) {
                nvars = 3;
            } else if (s != ~0ul) {
                nvars = 2;
            }
            break;
        case DBLayer::Order_No_Order_OPS:
        case DBLayer::Order_Object_Predicate_Subject:
            if (o != ~0ul) {
                nvars = 3;
            } else if (p != ~0ul) {
                nvars = 2;
            }
            break;
        case DBLayer::Order_No_Order_OSP:
        case DBLayer::Order_Object_Subject_Predicate:
            if (o != ~0ul) {
                nvars = 3;
            } else if (s != ~0ul) {
                nvars = 2;
            }
            break;
    }

    int varCount = 0;
    VTuple tuple(3);
    if (s == ~0ul) {
        tuple.set(VTerm(0, sc), 0);
    } else {
        tuple.set(VTerm(1, 0), 0);
        varCount++;
    }
    if (p == ~0ul) {
        tuple.set(VTerm(0, pc), 1);
    } else {
        tuple.set(VTerm(2, 0), 1);
        varCount++;
    }
    if (o == ~0ul) {
        tuple.set(VTerm(0, oc), 2);
    } else {
        tuple.set(VTerm(3, 0), 2);
        varCount++;
    }

    double costImplicit = getCardinality(sc, pc, oc);

    if (order == DBLayer::DataOrder::Order_Subject_Predicate_Object ||
            order == DBLayer::DataOrder::Order_Subject_Object_Predicate ||
            order == DBLayer::DataOrder::Order_Predicate_Object_Subject ||
            order == DBLayer::DataOrder::Order_Predicate_Subject_Object ||
            order == DBLayer::DataOrder::Order_Object_Predicate_Subject ||
            order == DBLayer::DataOrder::Order_Object_Subject_Predicate) {
        if (costImplicit > 0) {
            cost *= (1 + log(costImplicit));
        }
    }

    if (nvars > varCount) {
        cost *= (nvars + 1);
    }

#ifdef DEBUG
    Literal query(Predicate(predQueries, Predicate::calculateAdornment(tuple)), tuple);
    LOG(DEBUGL) << "Literal: " << query.tostring(&(this->p), &edb) << ", cost = " << cost;
#endif
    return cost;
}

double VLogLayer::getScanCost(DBLayer::DataOrder order,
        uint64_t value1,
        uint64_t value1C,
        uint64_t value2,
        uint64_t value2C) {
    return getScanCost(order, value1, value1C, value2, value2C, 0, ~0ul);
}

double VLogLayer::getScanCost(DBLayer::DataOrder order,
        uint64_t value1,
        uint64_t value1C) {
    return getScanCost(order, value1, value1C, 0, ~0ul, 0, ~0ul);
}

double VLogLayer::getJoinSelectivity(bool value1L,
        uint64_t value1CL,
        bool value2L,
        uint64_t value2CL,
        bool value3L,
        uint64_t value3CL,
        bool value1R,
        uint64_t value1CR,
        bool value2R,
        uint64_t value2CR,
        bool value3R,
        uint64_t value3CR) {

    uint64_t v1l = value1L ? value1CL : ~0ul;
    uint64_t v2l = value2L ? value2CL : ~0ul;
    uint64_t v3l = value3L ? value3CL : ~0ul;
    uint64_t v1r = value1R ? value1CR : ~0ul;
    uint64_t v2r = value2R ? value2CR : ~0ul;
    uint64_t v3r = value3R ? value3CR : ~0ul;
    uint64_t cardl = getCardinality(v1l, v2l, v3l);
    uint64_t cardr = getCardinality(v1r, v2r, v3r);
    double max = (double) cardl * (double) cardr;
    if (max == 0) {
        return 0;
    }
    std::vector<uint64_t> varsl;
    std::vector<uint64_t> varsr;
    if (! value1L) varsl.push_back(value1CL);
    if (! value2L) varsl.push_back(value2CL);
    if (! value3L) varsl.push_back(value3CL);
    if (! value1R) varsr.push_back(value1CR);
    if (! value2R) varsr.push_back(value2CR);
    if (! value3R) varsr.push_back(value3CR);
    if (varsl.size() == 1 && varsr.size() == 1) {
        double retval = cardl < cardr ? cardl/max : cardr/max;
        return retval;
    }
    if (varsl.size() == 1) {
        double retval = cardr/max;
        return retval;
    }
    if (varsr.size() == 1) {
        double retval = cardl/max;
        return retval;
    }

    // Now check if join is over same variables ...
    if (varsl.size() == varsr.size()) {
        for (int i = 0; i < varsl.size(); i++) {
            bool found = false;
            for (int j = 0; j < varsr.size(); j++) {
                if (varsl[i] == varsr[j]) {
                    found = true;
                    break;
                }
            }
            if (! found) {
                if (cardl == cardr && cardl > 10) {
                    // Assume it is the same ...
                    double retval = cardl/max;
                    return retval;
                }
                return 1;
            }
        }
        // Same variables, so resulting cardinality not higher than smallest one.
        double retval = cardl < cardr ? cardl/max : cardr/max;
        LOG(DEBUGL) << "joinselectivity = " << retval;
        return retval;
    }

    if (cardl == cardr && cardl > 10) {
        // Assume it is the same ...
        double retval = cardl/max;
        return retval;
    }
    return 1;
}

uint64_t VLogLayer::getCardinality(uint64_t c1,
        uint64_t c2,
        uint64_t c3) {
    VTuple tuple(3);
    tuple.set(VTerm(~c1 ? 0 : 1, c1), 0);
    tuple.set(VTerm(~c2 ? 0 : 1, c2), 1);
    tuple.set(VTerm(~c3 ? 0 : 1, c3), 2);
    return getCardinality(tuple);
}

uint64_t VLogLayer::getCardinality(VTuple tuple) {
    auto got = idbCardinalities.find(tuple);
    double costImplicit;
    Literal idbquery(Predicate(predQueries,
                Predicate::calculateAdornment(tuple)), tuple);

    if (got == idbCardinalities.end()) {
        costImplicit = reasoner.estimate(idbquery, NULL, NULL, edb, this->p);
        idbCardinalities[tuple] = costImplicit;
    } else {
        costImplicit = got->second;
    }
    return (uint64_t) costImplicit;
}

uint64_t VLogLayer::getCardinality() {
    return ~0ul;
}

std::unique_ptr<DBLayer::Scan> VLogLayer::getScan(const DBLayer::DataOrder order,
        const DBLayer::Aggr_t aggr,
        DBLayer::Hint *hint) {
    //DataOrder is ignored
    return std::unique_ptr<DBLayer::Scan>(new VLogScan(order, aggr, hint,
                predQueries,
                edb, p, &reasoner));
}

void VLogLayer::init() {
#ifdef TEST_LUBM
    // Pre-initialize the cache with the exact cardinalities for lubm_125.
    // All cardinality requests as they are asked while running q1-q14 are here.

    VTuple t(3);
    VTerm v1(1, ~0ul);
    VTerm v2(2, ~0ul);
    VTerm v3(3, ~0ul);

    uint64_t associateProf0;
    uint64_t assistantProf0;
    uint64_t university0;
    uint64_t graduateCourse0;
    uint64_t department0;

    uint64_t teacherOf;
    uint64_t hasAlumnus;
    uint64_t university;
    uint64_t takesCourse;
    uint64_t course;
    uint64_t researchGroup;
    uint64_t chair;
    uint64_t person;
    uint64_t faculty;
    uint64_t professor;
    uint64_t student;
    uint64_t graduateStudent;
    uint64_t undergraduateStudent;
    uint64_t publication;
    uint64_t worksFor;
    uint64_t department;
    uint64_t advisor;
    uint64_t undergraduateDegreeFrom;
    uint64_t name;
    uint64_t publicationAuthor;
    uint64_t telephone;
    uint64_t emailAddress;
    uint64_t memberOf;
    uint64_t subOrganizationOf;

    bool tmp = lookup("http://www.Department0.University0.edu/AssociateProfessor0", ::Type::ID::URI, 0, associateProf0);
    assert(tmp);
    tmp = lookup("http://www.Department0.University0.edu/AssistantProfessor0", ::Type::ID::URI, 0, assistantProf0);
    assert(tmp);
    tmp = lookup("http://www.Department0.University0.edu", ::Type::ID::URI, 0, department0);
    assert(tmp);
    tmp = lookup("http://www.University0.edu", ::Type::ID::URI, 0, university0);
    assert(tmp);
    tmp = lookup("http://www.Department0.University0.edu/GraduateCourse0", ::Type::ID::URI, 0, graduateCourse0);
    assert(tmp);

    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf", ::Type::ID::URI, 0, teacherOf);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#hasAlumnus", ::Type::ID::URI, 0, hasAlumnus);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University", ::Type::ID::URI, 0, university);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course", ::Type::ID::URI, 0, course);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#ResearchGroup", ::Type::ID::URI, 0, researchGroup);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent", ::Type::ID::URI, 0, graduateStudent);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Chair", ::Type::ID::URI, 0, chair);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Person", ::Type::ID::URI, 0, person);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Faculty", ::Type::ID::URI, 0, faculty);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Professor", ::Type::ID::URI, 0, professor);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student", ::Type::ID::URI, 0, student);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent", ::Type::ID::URI, 0, undergraduateStudent);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication", ::Type::ID::URI, 0, publication);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor", ::Type::ID::URI, 0, worksFor);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department", ::Type::ID::URI, 0, department);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse", ::Type::ID::URI, 0, takesCourse);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor", ::Type::ID::URI, 0, advisor);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom", ::Type::ID::URI, 0, undergraduateDegreeFrom);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name", ::Type::ID::URI, 0, name);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor", ::Type::ID::URI, 0, publicationAuthor);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress", ::Type::ID::URI, 0, emailAddress);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf", ::Type::ID::URI, 0, memberOf);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone", ::Type::ID::URI, 0, telephone);
    assert(tmp);
    tmp = lookup("http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf", ::Type::ID::URI, 0, subOrganizationOf);
    assert(tmp);

    // <http://www.Department0.University0.edu/AssociateProfessor0>,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf>,?3
    t.set(VTerm(0,associateProf0),0); t.set(VTerm(0,teacherOf),1); t.set(v3, 2);
    // getCardinality(t);
    idbCardinalities[t] = 4;

    // <http://www.University0.edu>,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#hasAlumnus>,?3
    t.set(VTerm(0,university0),0); t.set(VTerm(0,hasAlumnus),1); t.set(v3, 2);
    // getCardinality(t);
    idbCardinalities[t] = 582;

    // <http://www.Department0.University0.edu/AssociateProfessor0>,?2,?3
    t.set(VTerm(0,associateProf0),0); t.set(v2,1); t.set(v3, 2);
    // getCardinality(t);
    idbCardinalities[t] = 22;

    // <http://www.University0.edu>,?2,?3
    t.set(VTerm(0,university0),0); t.set(v2,1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 585;

    // ?1,http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse>,<http://www.Department0.University0.edu/GraduateCourse0>
    t.set(v1,0); t.set(VTerm(0,takesCourse),1); t.set(VTerm(0,graduateCourse0),2);
    // getCardinality(t);
    idbCardinalities[t] = 4;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,university),2);
    // getCardinality(t);
    idbCardinalities[t] = 1000;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,graduateStudent),2);
    // getCardinality(t);
    idbCardinalities[t] = 314796;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,course),2);
    // getCardinality(t);
    idbCardinalities[t] = 270784;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#ResearchGroup>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,researchGroup),2);
    // getCardinality(t);
    idbCardinalities[t] = 37468;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Chair>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,chair),2);
    // getCardinality(t);
    idbCardinalities[t] = 2504;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Faculty>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,faculty),2);
    // getCardinality(t);
    idbCardinalities[t] = 90191;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Person>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,person),2);
    // getCardinality(t);
    idbCardinalities[t] = 1396711;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Professor>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,professor),2);
    // getCardinality(t);
    idbCardinalities[t] = 75195;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,student),2);
    // getCardinality(t);
    idbCardinalities[t] = 1306520;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,publication),2);
    // getCardinality(t);
    idbCardinalities[t] = 1009219;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,undergraduateStudent),2);
    // getCardinality(t);
    idbCardinalities[t] = 991724;

    // ?1,rdf:type,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(VTerm(0,department),2);
    // getCardinality(t);
    idbCardinalities[t] = 2504;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>,<http://www.Department0.University0.edu>
    t.set(v1,0); t.set(VTerm(0,worksFor),1); t.set(VTerm(0,department0),2);
    // getCardinality(t);
    idbCardinalities[t] = 41;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor>,<http://www.Department0.University0.edu/AssistantProfessor0>
    t.set(v1,0); t.set(VTerm(0,publicationAuthor),1); t.set(VTerm(0,assistantProf0),2);
    // getCardinality(t);
    idbCardinalities[t] = 6;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>,<http://www.University0.edu>
    t.set(v1,0); t.set(VTerm(0,subOrganizationOf),1); t.set(VTerm(0,university0),2);
    // getCardinality(t);
    idbCardinalities[t] = 239;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>,<http://www.Department0.University0.edu>
    t.set(v1,0); t.set(VTerm(0,memberOf),1); t.set(VTerm(0,department0),2);
    // getCardinality(t);
    idbCardinalities[t] = 719;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#takesCourse>,?3
    t.set(v1,0); t.set(VTerm(0,takesCourse),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 3605783;

    // ?1,rdf:type,?3
    t.set(v1,0); t.set(VTerm(0,1),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 8346042;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#advisor>,?3
    t.set(v1,0); t.set(VTerm(0,advisor),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 513115;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#undergraduateDegreeFrom>,?3
    t.set(v1,0); t.set(VTerm(0,undergraduateDegreeFrom),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 404987;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#teacherOf>,?3
    t.set(v1,0); t.set(VTerm(0,teacherOf),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 270784;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#worksFor>,?3
    t.set(v1,0); t.set(VTerm(0,worksFor),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 90191;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#name>,?3
    t.set(v1,0); t.set(VTerm(0,name),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 2679343;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#publicationAuthor>,?3
    t.set(v1,0); t.set(VTerm(0,publicationAuthor),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 1797808;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#subOrganizationOf>,?3
    t.set(v1,0); t.set(VTerm(0,subOrganizationOf),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 77440;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#emailAddress>,?3
    t.set(v1,0); t.set(VTerm(0,emailAddress),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 1396711;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#hasAlumnus>,?3
    t.set(v1,0); t.set(VTerm(0,hasAlumnus),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 585071;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#telephone>,?3
    t.set(v1,0); t.set(VTerm(0,telephone),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 1396711;

    // ?1,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#memberOf>,?3
    t.set(v1,0); t.set(VTerm(0,memberOf),1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 1309024;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,university),2);
    // getCardinality(t);
    idbCardinalities[t] = 1007;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,graduateStudent),2);
    // getCardinality(t);
    idbCardinalities[t] = 314798;

    // ?1,?2,<http://www.Department0.University0.edu/AssistantProfessor0>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,assistantProf0),2);
    // getCardinality(t);
    idbCardinalities[t] = 25;

    // ?1,?2,<http://www.Department0.University0.edu/GraduateCourse0>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,graduateCourse0),2);
    // getCardinality(t);
    idbCardinalities[t] = 5;

    // ?1,?2,<http://www.Department0.University0.edu>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,department0),2);
    // getCardinality(t);
    idbCardinalities[t] = 771;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,course),2);
    // getCardinality(t);
    idbCardinalities[t] = 270792;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#ResearchGroup>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,researchGroup),2);
    // getCardinality(t);
    idbCardinalities[t] = 37472;

    // ?1,?2,<http://www.University0.edu>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,university0),2);
    // getCardinality(t);
    idbCardinalities[t] = 1404;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Chair>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,chair),2);
    // getCardinality(t);
    idbCardinalities[t] = 2506;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Faculty>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,faculty),2);
    // getCardinality(t);
    idbCardinalities[t] = 90295;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Person>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,person),2);
    // getCardinality(t);
    idbCardinalities[t] = 1396711;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Professor>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,professor),2);
    // getCardinality(t);
    idbCardinalities[t] = 75205;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Student>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,student),2);
    // getCardinality(t);
    idbCardinalities[t] = 1306524;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,publication),2);
    // getCardinality(t);
    idbCardinalities[t] = 1009237;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,undergraduateStudent),2);
    // getCardinality(t);
    idbCardinalities[t] = 991726;

    // ?1,?2,<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>
    t.set(v1,0); t.set(v2,1); t.set(VTerm(0,department),2);
    // getCardinality(t);
    idbCardinalities[t] = 2507;

    // ?1,?2,?3
    t.set(v1,0); t.set(v2,1); t.set(v3,2);
    // getCardinality(t);
    idbCardinalities[t] = 24873388;
#endif
}
