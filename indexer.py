#!/usr/bin/env python3

class Indexer(dict):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.count = 0
        self.rev = {}
    def __getitem__(self,key):
        try:
            return super().__getitem__(key)
        except KeyError:
            val = self.count
            self.count += 1
            self[key] = val
            self.rev[val] = key
            return val
