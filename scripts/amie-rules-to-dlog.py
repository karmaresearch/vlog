import sys
predicates = {}
counter = 0

def processAtom(atom):
    global predicates
    global counter
    terms = atom.split()
    for term in terms:
    #    print ("term : " , term)
        if term.startswith("<") and term.endswith(">") and not term.startswith("?"):
            if term not in predicates:
                predicates[term] = "RP" + str(counter)
                counter += 1

def processRule(rule):
    index = rule.find("=>")
    if -1 == index:
        return None
    body = rule[0:index]
    head = rule[index+2:]
    processAtom(body)
    processAtom(head)


def newAtomFormat(oldTerm, variableMap):
    newAtom = predicates[oldTerm[1]] + "(" + \
            variableMap[oldTerm[0]] + "," + variableMap[oldTerm[2]]+")"
    return newAtom

def convertRule(rule):
    index = rule.find("=>")
    if -1 == index:
        return None
    body = rule[0:index]
    head = rule[index+2:]

    variableMap = {}
    varCounter = 1
    bodyTerms = body.split()
    for bt in bodyTerms:
        if bt not in predicates and  bt not in variableMap:
            variableMap[bt] = "X" + str(varCounter)
            varCounter += 1

    headTerms = head.split()
    for bt in headTerms:
        if bt not in predicates and  bt not in variableMap:
            variableMap[bt] = "X" + str(varCounter)
            varCounter += 1

    # construct the rule in vlog format
    newHead = newAtomFormat(headTerms, variableMap)

    assert(len(bodyTerms) % 3 == 0)
    bodyAtoms = []
    for i in range(len(bodyTerms) // 3):
        newBodyAtom = newAtomFormat(bodyTerms[i*3:(i*3)+3], variableMap)
        bodyAtoms.append(newBodyAtom)

    newRule = newHead + " :- " + ','.join(bodyAtoms)
    #print(newRule)
    return newRule

rulesFile = sys.argv[1]
with open(rulesFile, "r") as fin:
    lines = fin.readlines()
    for line in lines:
        processRule(line)

newRules = []
for k,v in predicates.items():
    #print(k , " : " , v)
    newRule = v + "(A,B) :- TE(A," + k + ",B)"
    newRules.append(newRule)

with open(rulesFile, "r") as fin:
    lines = fin.readlines()
    for line in lines:
        newRule = convertRule(line)
        newRules.append(newRule)

outfile = rulesFile.split(".")[0] + ".dlog"
with open(outfile, "w") as fout:
    for rule in newRules:
        fout.write(rule + "\n")
