import sys
rulesin = sys.argv[1]
rulesout = rulesin + '-vlog'

prefixes = {}
predid = 0
binaryPredicates = {}
unaryPredicates = {}
rules = []

def translatePredicate(pred, unary, predid):
    if unary:
        if pred not in unaryPredicates:
            unaryPredicates[pred] = "RP" + str(predid)
            predid += 1
        newpred = unaryPredicates[pred]
    else:
        if pred not in binaryPredicates:
            binaryPredicates[pred] = "RP" + str(predid)
            predid += 1
        newpred = binaryPredicates[pred]
    return newpred, predid


def processAtom(atom, predid):
    # Get predicate
    idx = atom.find('(')
    pred = atom[:idx]
    # Get variables
    v = atom[idx + 1: -1]
    vs = v.split(',')
    unary = len(vs) == 1
    pred, predid = translatePredicate(pred, unary, predid)
    # Translate the variables
    vartext = ""
    for var in vs:
        vartext += var[1:] + ','
    vartext = vartext[:-1]
    return pred + '(' + vartext + ')', predid


for line in open(rulesin, 'rt'):
    line = line[:-1]
    if line.startswith("PREFIX"):
        line = line[7:]
        key = line[:line.find(':')]
        value = line[line.find(':')+1:]
        prefixes[key] = value
    else:
        # It's a rule
        if len(line) == 0:
            continue
        tkns = line.split(' :- ')
        head = tkns[0]
        body = tkns[1]
        if len(tkns) > 2:
            print(line)
        head, predid = processAtom(head, predid)
        body = body[:-2]
        body = body.split(', ')
        bodyAtoms = []
        for bodyAtom in body:
            bodyAtom, predid = processAtom(bodyAtom, predid)
            bodyAtoms.append(bodyAtom)
        rule = ''
        for b in bodyAtoms:
            rule += b + ','
        rule = rule[:-1]
        rule = head + ' :- ' + rule
        rules.append(rule)

fout = open(rulesout, 'wt')
# First print the unary predicate
for k,v in unaryPredicates.items():
    classname = k
    if ':' in classname:
        idpred = classname[:classname.find(':')]
        value = classname[classname.find(':')+1:]
        p = prefixes[idpred]
        p = p[1:-1]
        classname = p + value
    rule = v + '(X) :- TE(X,rdf:type,' + classname + '>)'
    fout.write(rule + '\n')
for k,v in binaryPredicates.items():
    predname = k
    if ':' in predname:
        idpred = predname[:predname.find(':')]
        value = predname[predname.find(':')+1:]
        p = prefixes[idpred]
        p = p[1:-1]
        predname = p + value
    rule = v + '(X,Y) :- TE(X,' + predname + '>,Y)'
    fout.write(rule + '\n')
#write the rules
for r in rules:
    fout.write(r + '\n')
fout.close()
