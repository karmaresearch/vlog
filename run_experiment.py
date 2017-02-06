#!/usr/bin/python

# Input file contains rules and data, and possibly prefixes.
# Format is same as RDFOX input, but Vlog-rules also work.
# TODO: query the result

import sys
import os
import cStringIO

kbPred = 'TE'

counter = 0
predicates = {}
prefixes = {}
edbRules = []
newRules = []
inputs = []

def rewritePred(pred, arity):
    global predicates
    global edbRules
    global counter

    if pred in predicates:
        return predicates[pred]
    else:
        # Get a new IDB predicate
        newpred = 'RP' + str(counter)
        counter += 1
        predicates[pred] = newpred
        # Add a rule in edbRules
        if arity == 2:
            newRule = newpred + '(A,B) :- TE(A,' + pred + ',B)'
            edbRules.append(newRule)
            return newpred
        else:
            newRule = newpred + '(A) :- TE(A,rdf:type,'+ pred + ')'
            edbRules.append(newRule)
            return newpred


def getURI(uri):
    global prefixes
    if not uri.startswith('<'):
        # Extract prefix
        index = uri.find(':')
        if index < 0:
            return uri
        prefx = uri[:uri.find(':')]
        if prefx not in prefixes:
            print 'Prefix not found: ' + prefx + ', URI = ' + uri
            raise Exception('Prefix not found: ')
        else:
            prefx = prefixes[prefx]
            uri = '<' + prefx + uri[uri.find(':') + 1:] + '>'
    else:
        if uri == "<int$false>":
            uri = '<http://ruliee/contradiction>'
    # Replace the URI of the ontology (RDFox version to VLog version)
    uri = uri.replace('http://swat.cse.lehigh.edu/onto/univ-bench.owl#','http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#')
    return uri


def processLiteral(lit):
    # Extract the predicate name
    predName = lit[0:lit.rfind('(')]

    # Parse the pred name using the prefixes
    if predName.startswith('RP'):
        pass
    elif predName == 'TE':
        pass
    else:
        predName = getURI(predName)

    # Process the body
    body = lit[lit.rfind('(') + 1:-1]
    arity = body.count(',') + 1
    terms = body.split(',')

    newbody = ''
    for term in terms:
        if term.startswith('?'):
            newbody +=  term[1:].upper() + ','
        else:
            if not term.startswith('<'):
                newbody += getURI(term) + ','
            else:
                newbody += getURI(term) + ','
    newbody = newbody[0:-1]
    if predName.startswith('RP'):
        newlit = predName + '(' + newbody + ')'
    elif predName.startswith('TE'):
        newlit = predName + '(' + newbody + ')'
    else:
        newlit = rewritePred(predName, arity) + '(' + newbody + ')'
    return newlit

def newSplit(value):
    items = []
    instream = cStringIO.StringIO(value)
    item=''
    escaped=False
    quoted=False
    while True:
        nextchar = instream.read(1)
        if nextchar == '':
            break
        if escaped:
            item += nextchar
            escaped=False
            continue
        if nextchar == ' ':
            if quoted:
                item += nextchar
                continue
            if len(item) > 0:
                items.append(item)
                item=''
        elif nextchar == '"':
            item += nextchar
            quoted = not quoted
        elif nextchar == '\\':
            item += nextchar
            escaped=True
        else:
            item += nextchar
    if len(item) > 0:
        items.append(item)
    return items

def parseInput(inputs, line):
    items = newSplit(line)
    if len(items) == 4:
        if items[3] == '.':
            del items[-1]
        else:
            raise Exception('Wrong syntax: ' + line)
    if len(items) == 3:
        newInput = '';
        for i in range(len(items)):
            if i > 0:
                newInput += ' '
            if items[i].startswith('"'):
                pass
            elif not items[i].startswith('<'):
                # Extract prefix
                prefx = items[i][:items[i].find(':')]
                if prefx not in prefixes:
                   pass
                else:
                    prefx = prefixes[prefx]
                    items[i] = '<' + prefx + items[i][items[i].find(':') + 1:] + '>'
            else:
                pass
            newInput += items[i]
        inputs.append(newInput + ' .')
    else:
        raise Exception('Wrong syntax: ' + line)


def parseRule(rules, rule):
    # Extract the head of the rule
    completeHead = rule[0:rule.find(':-')-1]
    completeHead = completeHead.strip()
    newRule = processLiteral(completeHead) + ' :- '

    body = rule[rule.find(':-')+2:]
    body = body.strip()
    while True:
        # Read predicate name
        predName = body[0:body.find('(')]
        body = body[body.find('(')+1:]
        litBody = body[0:body.find(')')]
        body = body[body.find(')')+1:]

        # Read body
        newRule += processLiteral(predName + '(' + litBody + ')')

        # Is body not finished? Add comma
        if len(body) <= 0:
            rules.append(newRule)
            break
        if body[0] != ',':
            rules.append(newRule)
            break
        else:
            newRule += ','
            body = body[1:].strip()


def parsePrefix(prefixes, prefix):
    # Remove the prefix header
    prefix = prefix[prefix.find(" ") + 1:]
    id = prefix[:prefix.find(" ")-1]
    uri = prefix[prefix.find(" ") + 2:-1]
    prefixes[id] = uri

print 'Extracting rules and input from ' + sys.argv[1]

for line in open(sys.argv[1], 'r'):
    line = line.strip()
    if line.startswith('PREFIX'):
        parsePrefix(prefixes, line)
    elif line.startswith('#'):
        pass
    elif len(line) <= 2:
        pass
    elif " :- " in line:
        parseRule(newRules, line)
    else:
        parseInput(inputs, line)

def ensure_dir(f):
    d = os.path.dirname(f)
    if not os.path.exists(d):
        os.makedirs(d)

def cleanup():
    os.system("rm -rf .data .edbconf .rules")

f1 = open("./.rules", 'w+')
for rule in edbRules:
    print >> f1, rule
for rule in newRules:
    print >> f1, rule
f1.close()

ensure_dir("./.data/input")
f1 = open("./.data/input", 'w+')
for item in inputs:
    print >> f1, item
f1.close()

print 'Generating database ...'
os.system("rm -rf ./.data/database")
os.system("./vlog load -i ./.data/input -o ./.data/database")

f1 = open("./.edbconf", 'w+')
print >> f1, "EDB0_predname=TE"
print >> f1, "EDB0_type=Trident"
print >> f1, "EDB0_param0=./.data/database"
f1.close()

print 'Materializing ...'
os.system("./vlog mat -e ./.edbconf --rules ./.rules -l info --storemat_format db --storemat_path ./.data/database")

cleanup()
