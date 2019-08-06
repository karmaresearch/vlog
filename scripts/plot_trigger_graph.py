from igraph import *
import sys

fnodes = sys.argv[1]
fedges = sys.argv[2]

g = Graph(directed=True)
for line in open(fnodes, 'rt'):
    if line.startswith('#'):
        continue
    line = line[:-1]
    tkns = line.split('\t')
    idnode = int(tkns[0])
    g.add_vertex(idnode, label=tkns[1] + " r" + tkns[2] + " " + tkns[3] + " " + tkns[4])

for line in open(fedges, 'rt'):
    line = line[:-1]
    tkns = line.split(' ')
    g.add_edge(int(tkns[0]), int(tkns[1]))

layout = g.layout("kk")
visual_style = {}
visual_style["vertex_size"] = 5
visual_style["layout"] = layout
visual_style["vertex_label"] = g.vs["label"]
visual_style["bbox"] = (3000, 3000)
plot(g, 'tg.pdf', **visual_style)
