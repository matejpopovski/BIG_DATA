import sys
from collections import OrderedDict

import grpc
import pandas as pd

from matchdb_pb2 import GetMatchCountReq
from matchdb_pb2_grpc import MatchCountStub


def simple_hash(country):
    out = 0
    for c in country:
        out += (out << 2) - out + ord(c)
    return out


class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, k):
        if k not in self.cache:
            return None
        self.cache.move_to_end(k)
        return self.cache[k]

    def put(self, k, v):
        if k in self.cache:
            self.cache.move_to_end(k)
        self.cache[k] = v
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)

def get_match_count(stub, winning_team, country):
    request = GetMatchCountReq(winning_team=winning_team, country=country)
    response = stub.GetMatchCount(request)
    return response.num_matches

def main():
    if len(sys.argv) != 4:
        print("Usage: python client.py <SERVER_0> <SERVER_1> <INPUT_FILE>")
        sys.exit(1)

    server_0, server_1, input_file = sys.argv[1:4]
    c0 = grpc.insecure_channel(server_0)
    c1 = grpc.insecure_channel(server_1)
    end0 = MatchCountStub(c0)
    end1 = MatchCountStub(c1)
    lru_cache = LRUCache(10)
    df_input = pd.read_csv(input_file)
    
    for _, row in df_input.iterrows():
        winning_team = row.get("winning_team", "")
        if winning_team != winning_team:
            winning_team = ""
        else:
            winning_team = str(winning_team).strip()
        country = row.get("country", "")
        if country != country:
            country = ""
        else:
            country = str(country).strip()

        k = "{},{}".format(winning_team, country)
        num = lru_cache.get(k)
        if num is None:
            if not country:
                num = (get_match_count(end0, winning_team, "") + get_match_count(end1, winning_team, ""))
            else:
                if simple_hash(country) % 2 == 0:
                    num = get_match_count(end0, winning_team, country)
                else:
                    num = get_match_count(end1, winning_team, country)
            lru_cache.put(k, num)
            print(num)
        else:
            print("{}*".format(num))
    c0.close()
    c1.close()

if __name__ == "__main__":
    main()
