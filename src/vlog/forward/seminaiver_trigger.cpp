#include <vlog/seminaiver_trigger.h>
#include <vlog/trigger/tgpath.h>

#include <unordered_map>
#include <queue>

void TriggerSemiNaiver::run(std::string trigger_paths) {
    //Create all the execution plans, etc.
    std::vector<RuleExecutionDetails> allrules;
    int ruleid = 0;

    for (auto& rule : program->getAllRules()) {
        allrules.push_back(RuleExecutionDetails(rule, ruleid++));
    }

    for(auto &r : allrules) {
        r.createExecutionPlans(checkCyclicTerms);
        r.calculateNVarsInHeadFromEDB();
    }

    //Set up chase data structure
    chaseMgmt = std::shared_ptr<ChaseMgmt>(new ChaseMgmt(allrules,
                typeChase, checkCyclicTerms, -1));

    //Do not check for duplicates anymore
    setIgnoreDuplicatesElimination();

    LOG(DEBUGL) << "First load the trigger_path file";
    TGPaths paths(trigger_paths);
    LOG(DEBUGL) << "There are " << paths.getNPaths() << " paths to execute";

    size_t iteration = 0;
    std::unordered_map<std::string, size_t> iterations;
    std::vector<StatIteration> costRules;

    for(size_t i = 0; i < paths.getNPaths(); ++i) {
        LOG(DEBUGL) << "Executing path " << i;
        const TGPath &path = paths.getPath(i);
        //LOG(DEBUGL) << "Rule " << path.ruleid << " inputs " << path.inputs.size() << " output " << path.output;

        //Set up the inputs
        auto &ruleDetails = allrules[path.ruleid];

        //Create range vector corresponding to the inputs
        std::vector<std::pair<size_t, size_t>> ranges;
        for(auto &input : path.inputs) {
            if (input == "INPUT" || input.find("EDB") == 0) {
                ranges.push_back(std::make_pair(0, (size_t) - 1));
            } else {
                //Get the range from the map
                if (!iterations.count(input)) {
                    LOG(ERRORL) << "This should not happen! " << input << " never found before";
                    throw 10;
                }
                size_t it = iterations[input];
                ranges.push_back(std::make_pair(it, it));
            }
        }

        ruleDetails.createExecutionPlans(ranges, false);

        //Invoke the execution of the rule using the inputs specified
        std::chrono::system_clock::time_point start =
            std::chrono::system_clock::now();
        executeRule(ruleDetails, iteration, 0, NULL);
        std::chrono::duration<double> sec = std::chrono::system_clock::now()
            - start;
        StatIteration stat;
        stat.iteration = i;
        stat.rule = &ruleDetails.rule;
        stat.time = sec.count() * 1000;
        stat.derived = false;
        costRules.push_back(stat);

        iterations.insert(std::make_pair(path.output, iteration));
        iteration += 1;
    }
    LOG(INFOL) << "Triggers: " << triggers;

#ifdef DEBUG
    std::sort(costRules.begin(), costRules.end());
    int i = 0;
    double sum = 0;
    double sum10 = 0;
    for (auto &el : costRules) {
        LOG(DEBUGL) << "Cost iteration " << el.iteration << " " <<
            el.time;
        i++;
        if (i >= 20)
            break;

        sum += el.time;
        if (i <= 10)
            sum10 += el.time;
    }
    LOG(DEBUGL) << "Sum first 20 rules: " << sum
        << " first 10:" << sum10;
#endif
}

size_t TriggerSemiNaiver::unique_unary(std::vector<Term_t> &unaryBuffer, std::vector<std::shared_ptr<const FCInternalTable>> &tables) {
    if (tables.size() == 1) {
        //Might be already sorted and unique
        size_t n = 0;
        size_t prev = 0;
        bool first = true;
        bool unique = true;
        auto ctable = tables[0];
        auto itrtable = ctable->getIterator();
        while (itrtable->hasNext()) {
            itrtable->next();
            size_t value = itrtable->getCurrentValue(0);
            if (first) {
                first = false;
                prev = value;
            } else {
                if (prev >= value) {
                    unique = false;
                    break;
                }
                prev = value;
            }
            n++;
        }
        ctable->releaseIterator(itrtable);
        if (unique) {
            return 0;
        }
    }

    size_t idx = 0;
    for(int i = 0; i < tables.size(); ++i) {
        auto t = tables[i];
        auto itr = t->getIterator();
        auto columns = itr->getAllColumns();
        if (columns.size() != 1) {
            LOG(ERRORL) << "This should not happen";
        }
        auto c = columns[0];
        bool fastCopy = false;
        if (c->isEDB()) {
            EDBColumn *c1 = ((EDBColumn*)c.get());
            auto &literal = c1->getLiteral();
            PredId_t predc = literal.getPredicate().getId();
            //Consult the EDB layer to see whether we can get a vector
            auto edbTable = layer.getEDBTable(predc);
            if (edbTable->useSegments()) {
                auto segment = edbTable->getSegment();
                uint8_t pos = c1->posColumnInLiteral();
                auto column = segment->getColumn(pos);
                if (column->isBackedByVector()) {
                    const auto &vec = column->getVectorRef();
                    memcpy((char*)unaryBuffer.data() + sizeof(Term_t) * idx, (char*)vec.data(), sizeof(Term_t) * vec.size());
                    fastCopy = true;
                    idx += vec.size();
                    //iterators.push_back(make_pair(vec.begin(), vec.end()));
                } else {
                    LOG(WARNL) << "Problem";
                }
            } else {
                LOG(WARNL) << "Problem";
            }
        } else {
            //Maybe it is still backed by a vector
            if (c->isBackedByVector()) {
                const auto &vec = c->getVectorRef();
                memcpy((char*)unaryBuffer.data() + sizeof(Term_t) * idx,
                        (char*)vec.data(), sizeof(Term_t) * vec.size());
                fastCopy = true;
                idx += vec.size();
            } else {
                LOG(WARNL) << "Problem";
            }
        }
        t->releaseIterator(itr);
    }
    std::sort(unaryBuffer.begin(), unaryBuffer.end());
    auto last = std::unique(unaryBuffer.begin(), unaryBuffer.end());
    return unaryBuffer.end() - last;
}

typedef std::pair<std::vector<Term_t>::const_iterator, std::vector<Term_t>::const_iterator> UniqueVector;
bool _customPairSorter(const std::pair<UniqueVector, UniqueVector> &a, const std::pair<UniqueVector, UniqueVector> &b) {
    size_t carda = a.first.second - a.first.first;
    size_t cardb = b.first.second - b.first.first;
    return carda > cardb;
}

size_t TriggerSemiNaiver::unique_binary(std::vector<std::pair<Term_t, Term_t>>  &binaryBuffer,
        std::vector<std::shared_ptr<const FCInternalTable>> &tables) {
    size_t out = 0;
    //Get vectors
    //binaryBuffer.clear();
    std::vector<std::pair<UniqueVector, UniqueVector>> vectors;
    for (auto ctable : tables) {
        auto itr = ctable->getIterator();
        auto columns = itr->getAllColumns();
        if (columns.size() != 2) {
            LOG(ERRORL) << "Problem. I should have only two columns";
        }

        std::vector<UniqueVector> rawColumns;
        for (auto &c : itr->getAllColumns()) {
            if (c->isEDB()) {
                EDBColumn *c1 = ((EDBColumn*)c.get());
                auto &literal = c1->getLiteral();
                PredId_t predc = literal.getPredicate().getId();
                //Consult the EDB layer to see whether we can get a vector
                auto edbTable = layer.getEDBTable(predc);
                if (edbTable->useSegments()) {
                    auto segment = edbTable->getSegment();
                    uint8_t pos = c1->posColumnInLiteral();
                    auto column = segment->getColumn(pos);
                    if (column->isBackedByVector()) {
                        const auto &vec = column->getVectorRef();
                        rawColumns.push_back(std::make_pair(vec.cbegin(), vec.cend()));
                    } else {
                        LOG(ERRORL) << "Problem";
                    }
                } else {
                    LOG(ERRORL) << "Problem";
                }

            } else {
                if (c->isBackedByVector()) {
                    const auto &vec = c->getVectorRef();
                    rawColumns.push_back(std::make_pair(vec.cbegin(), vec.cend()));
                } else {
                    LOG(ERRORL) << "Problem";
                }
            }
        }
        ctable->releaseIterator(itr);
        vectors.push_back(std::make_pair(rawColumns[0], rawColumns[1]));
    }

    //Sort them by cardinality
    //LOG(INFOL) << "Sorting vectors by cardinality " << vectors.size();
    std::sort(vectors.begin(), vectors.end(), _customPairSorter);

    //Check for duplicates
    for(int i = vectors.size() - 1; i >= 0; i--) {
        const auto &v = vectors[i];
        bool found = false;
        for (int j = i - 1; j>= 0; j--) {
            const auto &z = vectors[j];
            //Check if the columns are the same
            if (z.first.first == v.first.first && z.second.first == v.second.first &&
                    z.first.second == v.first.second && z.second.second == v.second.second) {
                found = true;
                break;
            }
        }
        if (found) {
            //LOG(INFOL) << "Removing one pair of vectors ...";
            //Remove the pair of vectors
            out += v.first.second - v.first.first;
            vectors.erase(vectors.begin() + i);
        }
    }

    //LOG(INFOL) << "N vectors " << vectors.size();

    //Are the vectors internally sorted?
    std::vector<std::vector<Term_t>> sortedBuffers;
    for(int i = 0; i < vectors.size(); ++i) {
        bool sorted = true;
        auto &v = vectors[i];
        size_t size = v.first.second - v.first.first;
        for(size_t j = 1; j < size; ++j) {
            Term_t prev1 = *(v.first.first + j - 1);
            Term_t prev2 = *(v.second.first + j - 1);
            Term_t cur1 = *(v.first.first + j);
            Term_t cur2 = *(v.second.first + j);
            if (cur1 < prev1 || (cur1 == prev1 && cur2 < prev2)) {
                sorted = false;
                break;
            }
        }
        if (!sorted) {
            //LOG(INFOL) << "Must sort one segment";
            std::vector<std::pair<Term_t, Term_t>> tmpBuffer;
            for(size_t j = 0; j < size; ++j) {
                Term_t cur1 = *(v.first.first + j);
                Term_t cur2 = *(v.second.first + j);
                tmpBuffer.push_back(std::make_pair(cur1, cur2));
            }
            std::sort(tmpBuffer.begin(), tmpBuffer.end());
            sortedBuffers.emplace_back();
            sortedBuffers.emplace_back();
            std::vector<Term_t> &vector1 = sortedBuffers[sortedBuffers.size() - 2];
            std::vector<Term_t> &vector2 = sortedBuffers[sortedBuffers.size() - 1];
            for(auto &el : tmpBuffer) {
                vector1.push_back(el.first);
                vector2.push_back(el.second);
            }
            //Replace the pointers
            v.first.first = vector1.begin();
            v.first.second = vector1.end();
            v.second.first = vector2.begin();
            v.second.second = vector2.end();
        }
    }

    //Check whether there is some overlap. New elements are added to the vector
    for(int i = vectors.size() - 1; i >= 0; i--) {
        const auto &v = vectors[i];
        bool firstIteration = true;
        std::vector<Term_t> prevbuffer1;
        std::vector<Term_t> prevbuffer2;

        for (int j = i - 1; j >= 0; j--) {
            std::vector<Term_t> tmpbuffer1;
            std::vector<Term_t> tmpbuffer2;

            const auto &z = vectors[j];
            //Check whether all v is contained in z
            size_t cardv = v.first.second - v.first.first;
            size_t cardz = z.first.second - z.first.first;
            auto zb1 = z.first.first;
            auto ze1 = z.first.second;
            auto zb2 = z.second.first;
            auto ze2 = z.second.second;

            std::vector<Term_t>::const_iterator vb1;
            std::vector<Term_t>::const_iterator ve1;
            std::vector<Term_t>::const_iterator vb2;
            std::vector<Term_t>::const_iterator ve2;
            if (firstIteration) {
                vb1 = v.first.first;
                ve1 = v.first.second;
                vb2 = v.second.first;
                ve2 = v.second.second;
                firstIteration = false;
            } else {
                vb1 = prevbuffer1.begin();
                ve1 = prevbuffer1.end();
                vb2 = prevbuffer2.begin();
                ve2 = prevbuffer2.end();
            }

            size_t overlap = 0;
            while (vb1 != ve1 && zb1 != ze1) {
                Term_t value_v1 = *vb1;
                Term_t value_z1 = *zb1;
                Term_t value_v2 = *vb2;
                Term_t value_z2 = *zb2;

                if (value_z1 < value_v1 || (value_z1 == value_v1 && value_z2 < value_v2)) {
                    //Advance z
                    zb1++;
                    zb2++;
                } else if (value_z1 > value_v1 || (value_z1 == value_v1 && value_z2 > value_v2)) {
                    tmpbuffer1.push_back(*vb1);
                    tmpbuffer2.push_back(*vb2);
                    //Advance v
                    vb1++;
                    vb2++;
                } else {
                    overlap++;
                    //Advance both
                    zb1++;
                    zb2++;
                    vb1++;
                    vb2++;
                }
            }
            while (vb1 != ve1) {
                tmpbuffer1.push_back(*vb1);
                tmpbuffer2.push_back(*vb2);
                vb1++;
                vb2++;
            }
            prevbuffer1.swap(tmpbuffer1);
            prevbuffer2.swap(tmpbuffer2);
            out += overlap;
        }

        //Copy prevbuffer1 into out
        /*if (i > 0) {
          for(size_t i = 0; i < prevbuffer1.size(); ++i) {
          binaryBuffer.push_back(std::make_pair(prevbuffer1[i], prevbuffer2[i]));
          }
          } else {
          auto vb1 = v.first.first;
          auto ve1 = v.first.second;
          auto vb2 = v.second.first;
          auto ve2 = v.second.second;
          while (vb1 != ve1) {
          binaryBuffer.push_back(std::make_pair(*vb1, *vb2));
          vb1++; vb2++;
          }
          }*/
    }

    //DEBUG
    /*size_t out2 = 0;
      size_t idx = 0;
      for (auto ctable : tables) {
      auto itrtable = ctable->getIterator();
      while (itrtable->hasNext()) {
      itrtable->next();
      uint64_t value1 = itrtable->getCurrentValue(0);
      uint64_t value2 = itrtable->getCurrentValue(1);
      binaryBuffer[idx++] = std::make_pair(value1, value2);
      }
      ctable->releaseIterator(itrtable);
      }
      std::sort(binaryBuffer.begin(), binaryBuffer.end());
      auto last = std::unique(binaryBuffer.begin(), binaryBuffer.end());
      out2 += binaryBuffer.end() - last;

      LOG(INFOL) << "OUT2=" << out2 << " OUT=" << out;*/

    return out;
}

size_t TriggerSemiNaiver::unique() {
    size_t out = 0;
    std::vector<Term_t> unaryBuffer;
    std::vector<std::pair<Term_t, Term_t>> binaryBuffer;
    for(PredId_t pid : program->getAllPredicateIDs()) {
        if (!program->isPredicateIDB(pid)) {
            continue;
        }
        if (!isEmpty(pid)) {
            //std::string predname = program->getPredicateName(pid);
            std::vector<std::shared_ptr<const FCInternalTable>> tables;
            FCIterator itr = getTable(pid);
            while (!itr.isEmpty()) {
                tables.push_back(itr.getCurrentTable());
                itr.moveNextCount();
            }
            int arity = program->getPredicate(pid).getCardinality();
            size_t n = getSizeTable(pid);
            std::chrono::system_clock::time_point start = std::chrono::system_clock::now();
            if (arity == 1) {
                unaryBuffer.resize(n);
                size_t res = unique_unary(unaryBuffer, tables);
                out += res;
            } else if (arity == 2) {
                //LOG(INFOL) << "****** Pred " << pid << " " << predname;
                //binaryBuffer.resize(n);
                size_t res = unique_binary(binaryBuffer, tables);
                out += res;
                //std::chrono::duration<double> secDel = std::chrono::system_clock::now()
                //    - start;
                //LOG(INFOL) << "Time: " << secDel.count() * 1000;
            } else {
                LOG(INFOL) << "Cardinality " << arity << " not supported";
                throw 10;
            }
        }
    }
    return out;
}
