import sys

rulesfile = sys.argv[1]

joins = {}

with open(rulesfile, 'rt') as f:
    for line in f:
        line = line[:-1]
        head, body = line.split(':-')
        literals = body.split('),')
        if len(literals) > 1:
            joinVars = {}
            for i, lit in enumerate(literals):
                # Extract vars
                if lit.endswith(')'):
                    lit = lit[:-1]
                vars = lit[lit.find('(')+1:]
                predname = lit[0:lit.find('(')]
                for j, var in enumerate(vars.split(',')):
                    if var in joinVars:
                        joinVars[var].append(predname + '-' + str(j))
                    else:
                        joinVars[var] = [ predname + '-' + str(j) ]
            for var, literals in joinVars.items():
                if len(literals) == 2:
                    literals.sort()
                    a = str(literals)
                    if a in joins:
                        joins[a].append('X')
                    else:
                        joins[a] = ['X']

                if len(literals) == 3:
                    literals.sort()
                    a = str(literals[0:2])
                    if a in joins:
                        joins[a].append('X')
                    else:
                        joins[a] = ['X']
                    a = str(literals[1:3])
                    if a in joins:
                        joins[a].append('X')
                    else:
                        joins[a] = ['X']
                    a = str([literals[0], literals[2]])
                    if a in joins:
                        joins[a].append('X')
                    else:
                        joins[a] = ['X']

print("# Joins", len(joins))
print(">1:")
for j, v in joins.items():
    if len(v) > 1:
        print(j, len(v))
