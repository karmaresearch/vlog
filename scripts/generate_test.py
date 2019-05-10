import sys

outputdir = sys.argv[1]

out1 = open(outputdir + '/triples.nt', 'wt')
size = 10000
for i in range(size):
    out1.write('<a'+ str(i) + '> <r> <b' + str(i) + '> .\n');
    out1.write('<c'+ str(i) + '> <s> <d' + str(i) + '> .\n');
out1.close()
