import sys
import os
import gzip

ntfile = sys.argv[1]
rulesfile = sys.argv[2]
outdir = sys.argv[3]
edbfile = sys.argv[4]

# First process the rule files to understand which binary predicates we need
binaryPredicates = {}
unaryPredicates = {}
for line in open(rulesfile, 'rt'):
    line = line[:-1]
    tkns = line.split(' :- ')
    predicateName = tkns[0][0:tkns[0].find('(')]
    body = tkns[1]
    if 'TE' in body:
        tkns = body.split(',')
        obj = tkns[2]
        if '<' in obj:
            # unary
            assert(tkns[1] == 'rdf:type')
            obj = obj[:-1]
            unaryPredicates[predicateName] = obj
        else:
            binaryPredicates[predicateName] = tkns[1]

invBinaryPredicates = {}
for key, value in binaryPredicates.items():
    invBinaryPredicates[value] = key
invUnaryPredicates = {}
for key, value in unaryPredicates.items():
    invUnaryPredicates[value] = key

# Now populate the relations
shortunarynames = {}
shortbinarynames = {}
allshortnames = {}

binaryRelations = {}
unaryRelations = {}
old2newpred = {}
count = 0
for line in open(ntfile, 'rt'):
    if count % 1000000 == 0:
        print(count)
    count += 1
    try:
        line = line[:-1]
        tkns = line.split(' ')
        subj = tkns[0]
        pred = tkns[1]
        obj = ''
        for i in range(2, len(tkns)):
            obj += tkns[i]
        obj = obj[:-1]
        if pred in invBinaryPredicates:
            if pred not in shortbinarynames:
                # Change relname is something meaninful
                idx = pred.rfind("#")
                if idx == -1:
                    idx = pred.rfind("/")
                relname = pred[idx + 1:-1]
                relname = relname.lower()
                # Check that relname is unique                
                idx = 0
                while relname in allshortnames:
                    relname = relname + str(idx)
                    idx += 1
                allshortnames[relname] = 0
                shortbinarynames[pred] = relname

                oldrelname = invBinaryPredicates[pred] # e.g., RP1
                old2newpred[oldrelname] = relname
            else:
                relname = shortbinarynames[pred]

            if relname not in binaryRelations:
                binaryRelations[relname] = []
            binaryRelations[relname].append((subj, obj))
        elif pred == 'rdf::type' or pred == '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>':
            if obj in invUnaryPredicates:
                if obj not in shortunarynames:
                    idx = obj.rfind("#")
                    if idx == -1:
                        idx = obj.rfind("/")
                    relname = obj[idx+1:-1] 
                    relname = relname.lower()
                    # Check that relname is unique                
                    idx = 0
                    while relname in allshortnames:
                        relname = relname + str(idx)
                        idx += 1
                        if idx > 30:
                            print(allshortnames)
                            print(relname)
                            break
                    allshortnames[relname] = 0
                    shortunarynames[obj] = relname

                    oldrelname = invUnaryPredicates[obj]
                    old2newpred[oldrelname] = relname
                else:
                    relname = shortunarynames[obj]
                if relname not in unaryRelations:
                    unaryRelations[relname] = []
                unaryRelations[relname].append(subj)
    except:
        print("Ignored line", line)

# Write the relations into files
if not os.path.exists(outdir):
    os.makedirs(outdir)

for key, value in binaryRelations.items():
    fout = gzip.open(outdir + '/e_' + key + '.csv.gz', 'wt')
    for p in value:
        fout.write(p[0] + '\t' + p[1] + '\n')
    fout.close()

for key, value in unaryRelations.items():
    fout = gzip.open(outdir + '/e_' + key + '.csv.gz', 'wt')
    for p in value:
        fout.write(p + '\n')
    fout.close()

# Create the EDB file
fedb = open(edbfile, 'wt')
i = 0
for key in binaryRelations:
    fedb.write('EDB' + str(i) + '_predname=e_' + key + '\n')
    fedb.write('EDB' + str(i) + '_type=INMEMORY\n')
    fedb.write('EDB' + str(i) + '_param0=' + outdir + '\n')
    fedb.write('EDB' + str(i) + '_param1=e_' + key + '\n')
    fedb.write('EDB' + str(i) + '_param2=t\n')
    fedb.write('\n')
    i += 1
for key in unaryRelations:
    fedb.write('EDB' + str(i) + '_predname=e_' + key + '\n')
    fedb.write('EDB' + str(i) + '_type=INMEMORY\n')
    fedb.write('EDB' + str(i) + '_param0=' + outdir + '\n')
    fedb.write('EDB' + str(i) + '_param1=e_' + key + '\n')
    fedb.write('EDB' + str(i) + '_param2=t\n')
    fedb.write('\n')
    i += 1
fedb.close()

# Create a new rulefile with the replaced predicates
text = ""
for line in open(rulesfile, 'rt'):
    if "TE" not in line:
        text += line
for key, value in old2newpred.items():
    text = text.replace(key + '(', value + '(')
frules = open(rulesfile + ".new", "wt")

invold2newpred = {}
for key, value in old2newpred.items():
    invold2newpred[value] = key

# Write the dependencies from edb to idb predicates
for key in unaryRelations:
    head = key
    frules.write(head + '(X) :- e_' + key + '(X)\n')

for key in binaryRelations:
    head = key
    frules.write(head + '(X,Y) :- e_' + key + '(X,Y)\n')
frules.write(text)
frules.close()
