#!/usr/bin/env python3

import numpy
from indexer import Indexer
from collections import defaultdict
import random

def rank_converged(prev,curr):
    size = prev.size
    if size!=curr.size:
        raise ValueError('prev and curr must have same size')
    old_order = sorted(list(range(size)),key=lambda i:prev[i])
    new_order = sorted(list(range(size)),key=lambda i:curr[i])
    return old_order==new_order

class tol_converged:
    def __init__(self,tol):
        self.tol = tol
    def __call__(self,prev,curr):
        max_diff = abs(prev-curr).max()
        return max_diff if max_diff > self.tol else 0

class Graph:
    """
    Implements a directed graph.
    self.nodes contains a map from node names to node index.
    self.links contains a map from node index to all linked nodes.
    self.redirects contains a map from transient node index to permanent node index.
    """
    def __init__(self):
        self.nodes = Indexer()
        self.links = defaultdict(list)
        self.redirects = {}

    def __len__(self):
        return len(self.nodes)

    def AddLink(self,from_name,to_name):
        from_index = self.Index(from_name)
        to_index = self.Index(to_name)
        self.links[from_index].append(to_index)

    def AddRedirect(self,from_name,to_name):
        self.redirects[from_name] = to_name

    def Index(self,name):
        index = self.nodes[name]
        while index in self.redirects:
            index = self.redirects[index]
        return index

    def Links(self,from_index):
        if isinstance(from_index,int):
            return self.links[from_index]
        else:
            from_index = self.Index(from_index)
            links = self.links[from_index]
            link_names = [self.nodes.rev[link] for link in links]
            return link_names

    def PageRank(self, reset=0.15, steps_per_iteration=int(1e7), max_iter=100,
                 tol='rank'):
        """
        Computes the Page Rank of each node in the graph.
        @param reset The probability of making a random jump.
        @param steps_per_iteration The number of steps before checking convergence.
        @param max_iter The number of iterations before giving up.
        @param tol The convergence criteria.
          If 'rank', then it continues until the ordering of pages has stabilized.
          If the value is a float, then it continues until all values have changed less than the tolerance.
        """
        pure_pages = list(set(self.nodes.values()) - set(self.redirects))
        def random_jump():
            return random.choice(pure_pages)

        if tol=='rank':
            converged = rank_converged
        else:
            converted = tol_converged(tol)

        page_rank = numpy.zeros(len(self.nodes))
        total_steps = 0

        current = random_jump()
        for iter_num in range(max_iter):
            print('iter_num: ',iter_num)
            iteration_counts = numpy.zeros(len(self.nodes))
            for step_num in range(steps_per_iteration):
                if step_num%1000000==0:
                    print('step_num: ',step_num)
                options = self.Links(current)
                if not options or random.random()<reset:
                    current = random_jump()
                else:
                    current = random.choice(options)
                iteration_counts[current] += 1
            #Weighted average of new estimation and previous estimation
            prev = page_rank
            page_rank = (page_rank*total_steps + iteration_counts)/(total_steps + steps_per_iteration)
            total_steps += steps_per_iteration
            if converged(prev,page_rank):
                break
        return page_rank

    def PageRankMatrix(self, reset=0.15, max_iter=100, tol='rank'):
        """
        Computes the Page Rank of each node in the graph.
        Does so using matrix multiplication, rather than a random walk.
        """
        try:
            return self.page_rank
        except AttributeError:
            pass

        if tol=='rank':
            converged = rank_converged
        else:
            converged = tol_converged(tol)

        num_nodes = len(self.nodes)
        #Transpose self.links, so it can be used to find links to a page, not just from
        linked_from = defaultdict(list)
        for from_node,to_node_list in self.links.items():
            for to_node in to_node_list:
                linked_from[to_node].append((from_node,1/len(to_node_list)))
        #Find all dangling nodes
        dangling_nodes = set()
        for nodenum in range(num_nodes):
            if nodenum not in self.links or not self.links[nodenum]:
                dangling_nodes.add(nodenum)

        page_rank = numpy.ones(num_nodes)/num_nodes

        for iter_num in range(max_iter):
            print(iter_num)
            prev = page_rank
            page_rank = numpy.zeros(num_nodes)
            dangling_contrib = (1-reset)*sum(prev[d] for d in dangling_nodes)/num_nodes
            reset_contrib = reset/num_nodes
            for to_index in range(num_nodes):
                link_contrib = (1-reset)*sum(prev[from_index]*weight for from_index,weight in linked_from[to_index])
                page_rank[to_index] = link_contrib + dangling_contrib + reset_contrib
            if converged(prev,page_rank):
                break

        self.page_rank = page_rank
        return page_rank


    def TopNPages(self,n):
        ranking = self.PageRankMatrix()
        node_names = list(self.nodes)
        node_names.sort(key = lambda name:ranking[self.Index(name)],reverse=True)
        return node_names[:n]

    def WriteAllPageRanks(self,filename):
        ranking = self.PageRankMatrix()
        node_ranks = [(name,ranking[self.Index(name)]) for name in self.nodes]
        node_ranks.sort(key = lambda k:k[1],reverse=True)
        with open(filename,'w') as f:
            for name,rank in node_ranks:
                f.write('{}\t{}\n'.format(name,rank))

    def ExportCSV(self,filename,n):
        pages = set(self.TopNPages(n))
        with open(filename,'w') as f:
            f.write('Source,Target\n')
            for i,page in enumerate(pages):
                if i%1000==0:
                    print('Saving page',i)
                for link in self.Links(page):
                    if link in pages:
                        f.write('{},{}\n'.format(page,link))


def main():
    g = Graph()

    for i,line in enumerate(open('links.txt')):
        if i%int(1e6)==0:
            print('Reading redirects',i,line.strip())
        from_name,command,to_name = line.split()
        if command=='=>':
            g.AddRedirect(from_name,to_name)

    for i,line in enumerate(open('links.txt')):
        if i%int(1e6)==0:
            print('Reading links',i,line.strip())
        from_name,command,to_name = line.split()
        if command=='->':
            g.AddLink(from_name,to_name)

    print(g.TopNPages(100))
    import IPython; IPython.embed()

if __name__=='__main__':
    main()
