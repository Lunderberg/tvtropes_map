#include <iostream>
using std::cout;
using std::endl;

#include "Graph.hh"

int main(){
  Graph g("../links.txt", true);
  g.PrintIf(std::cout,
            [](std::string name, double rank, int i){
              return name.find("Film/")==0;},
            false, 1000);
}
