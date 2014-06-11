#!/usr/bin/env python3

from indexer import Indexer
from collections import defaultdict

class Graph:
    """
    Implements a directed graph.
    self.nodes contains a map from node names to node index.
    self.links contains a map from node index to all linked nodes.
    self.redirects contains a map from transient node index to permanent node index.
    self.merged contains a map from node index to a list of all transient nodes redirecting to it.
    """
    def __init__(self):
        self.nodes = Indexer()
        self.links = defaultdict(list)
        self.redirects = {}
        self.merged = defaultdict(list)
    def AddLink(self,from_name,to_name):
        from_index = self.Index(from_name)
        to_index = self.Index(to_name)
        self.links[from_index].append(to_index)
    def Index(self,name):
        index = self.nodes[name]
    def MergeNodes(self,from_name,to_name):
        """
        Merges two nodes. to_name is kept.
        """
        from_index = self.Index(from_name)
        to_index = self.Index(to_name)
        self.redirects[from_index] = to_index
        self.merged[to_index].extend(self.merged[from_index])
        del self.merged[from_index]
        self.merged[to_index].append(from_index)
    def Links(self,from_index):
        if from_index in self.redirects:
            raise ValueError('Merged node')
        output = self.links[from_index][:]
        for redir in self.merged[from_index]:
            output.extend(self.links[redir])
        return output




def main():
    g = Graph()
    for i,line in enumerate(open('links.txt')):
        if i%int(1e6)==0:
            print(i,line.strip())
        from_name,command,to_name = line.split()
        if command=='->':
            g.AddLink(from_name,to_name)
        elif command=='=>':
            g.MergeNodes(from_name,to_name)
    import IPython; IPython.embed()

if __name__=='__main__':
    main()
