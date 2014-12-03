#ifndef _GRAPH_H_
#define _GRAPH_H_

#include <algorithm>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

class Graph{
public:
  Graph(std::string filename, bool verbose = false);
  std::vector<double> PageRank(bool verbose = false, int iter=100, double reset=0.15);
  void PrintTop(std::ostream& out, size_t n, bool verbose = false);

  template<typename Condition>
  void PrintIf(std::ostream& out, Condition cond, bool verbose = false, int iter=100){
    auto rank = PageRank(verbose, iter);

    typedef std::pair<std::string, double> nr;
    std::vector<nr> name_ranks;
    name_ranks.reserve(indices.size());
    for(const auto& elem : indices){
      name_ranks.push_back({elem.first,rank[elem.second]});
    }

    std::sort(name_ranks.begin(), name_ranks.end(),
              [](const nr& a, const nr& b){return a.second > b.second;} );

    for(int i=0; i<name_ranks.size(); i++){
      std::string name = name_ranks[i].first;
      double rank = name_ranks[i].second;
      if(cond(name,rank,i)){
        out.precision(15);
        out << name_ranks[i].first << "\t" << name_ranks[i].second << std::endl;
      }
    }
  }

private:
  void AddRedirect(std::string from, std::string to);
  void AddLink(std::string from, std::string to);
  int GetIndex(std::string name);

  std::unordered_map<std::string,std::string> redirects;
  std::unordered_map<std::string,int> indices;
  // links[i] contains a list of all pages linked to from i
  std::unordered_map<int, std::vector<int> > links;
};

#endif /* _GRAPH_H_ */
