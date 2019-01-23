#include <vlog/trigger/tg.h>
#include <kognac/logs.h>

TriggerGraph::TriggerGraph() {
}

void TriggerGraph::createLinear(EDBLayer &db, Program &p) {
    LOG(ERRORL) << "Not yet implemented";
}

void TriggerGraph::createKBound(EDBLayer &db, Program &p) {
    LOG(ERRORL) << "Not yet implemented";
}

void TriggerGraph::processNode(const Node &n, std::ostream &out) {
    for (const auto child : n.incoming) {
        processNode(*child, out);
    }
    //Write a line
    out << std::to_string(n.ruleID) << "\t";
    for (const auto child : n.incoming) {
        out << child->label << "\t";
    }
    out << n.label << std::endl;
}

void TriggerGraph::saveAllPaths(std::ostream &out) {
    for(const auto &n : nodes) {
        if (n.outgoing.size() == 0) {
            if (n.incoming.size() != 0) {
                processNode(n, out);
            }
        }
    }
}
