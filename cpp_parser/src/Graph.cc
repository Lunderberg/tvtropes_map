#include "Graph.hh"

#include <fstream>
#include <stdexcept>
#include <utility>

Graph::Graph(std::string filename, bool verbose){
  std::ifstream in(filename);
  in.seekg(0, std::ios_base::end);
  auto size = in.tellg();

  std::string from, type, to;
  int lines_since_update = 0;

  // Read all the redirects
  in.seekg(0, std::ios_base::beg);
  while(in >> from >> type >> to){
    if(type=="=>"){
      AddRedirect(from,to);
    }
    if(verbose){
      if(lines_since_update++ > 1000){
        lines_since_update = 0;
        std::cout << "\rReading redirects: " << int(100*double(in.tellg())/double(size))
                  << "%" << std::flush;
      }
    }
  }
  if(verbose){
    std::cout << std::endl;
  }

  // Read all the links
  in.clear();
  in.seekg(0, std::ios_base::beg);
  while(in >> from >> type >> to){
    if(type=="->"){
      AddLink(from,to);
    }
    if(verbose){
      if(lines_since_update++ > 1000){
        lines_since_update = 0;
        std::cout << "\rReading links: " << int(100*double(in.tellg())/double(size))
                  << "%" << std::flush;
      }
    }
  }
  if(verbose){
    std::cout << std::endl;
  }
}

void Graph::AddRedirect(std::string from, std::string to){
  redirects[from] = to;
}

void Graph::AddLink(std::string from, std::string to){
  int from_index = GetIndex(from);
  int to_index = GetIndex(to);
  try{
    links.at(from_index).push_back(to_index);
  } catch (std::out_of_range& e){
    links[from_index] = std::vector<int>();
    links.at(from_index).push_back(to_index);
  }
}

int Graph::GetIndex(std::string name){
  while(redirects.count(name)){
    name = redirects[name];
  }

  try{
    return indices.at(name);
  } catch (std::out_of_range& e){
    int new_index = indices.size();
    indices[name] = new_index;
    return new_index;
  }
}

std::vector<double> Graph::PageRank(bool verbose, int iter, double reset){
  int num_nodes = indices.size();

  // Transpose links, so it can find all items that link to a given node.
  if(verbose){
    std::cout << "Transposing links" << std::endl;
  }

  std::unordered_map<int,std::vector<std::pair<int,double> > > linked_from;
  for(const auto& elem : links){
    int from = elem.first;
    double weight = 1.0/elem.second.size();
    for(auto to : elem.second){
      try{
        linked_from.at(to).push_back({from,weight});
      } catch (std::out_of_range& e){
        linked_from[to] = std::vector<std::pair<int,double> >();
        linked_from.at(to).push_back({from,weight});
      }
    }
  }

  if(verbose){
    std::cout << "Finding dangling pages" << std::endl;
  }

  // Find all pages without any links to them.
  std::vector<int> dangling_nodes;
  for(const auto& elem : links){
    int from = elem.first;
    if(!linked_from.count(from)){
      dangling_nodes.push_back(from);
    }
  }

  //Initialize with all nodes equal
  std::vector<double> rank;
  rank.resize(num_nodes, 1.0/num_nodes);

  for(int i=0; i<iter; i++){
    if(verbose){
      std::cout << "\rIteration: " << i << "/" << iter << std::flush;
    }

    std::vector<double> prev = rank;
    rank.clear();
    rank.resize(num_nodes, 0);

    // Pages with no outgoing links give their rank to everyone.
    double dangling_contrib = 0;
    for(auto d : dangling_nodes){
      dangling_contrib += prev[d];
    }
    dangling_contrib *= (1-reset)/num_nodes;

    // If the imaginary traveler gets bored, it goes to a random page.
    double reset_contrib = reset/num_nodes;

    for(int to=0; to<num_nodes; to++){
      double link_contrib = 0;
      for(const auto& elem : linked_from[to]){
        link_contrib += prev[elem.first]*elem.second;
      }
      link_contrib *= (1-reset);

      rank[to] = link_contrib + dangling_contrib + reset_contrib;
    }
  }
  std::cout << std::endl;

  return rank;
}

void Graph::PrintTop(std::ostream& out, size_t n, bool verbose){
  PrintIf(out,
          [n](std::string name, double rank, int i){return i<n;},
          verbose);
}
