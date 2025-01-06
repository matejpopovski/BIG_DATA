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

    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
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

    server_0, server_1, input_file = sys.argv[1], sys.argv[2], sys.argv[3]

    channel_0 = grpc.insecure_channel(server_0)
    channel_1 = grpc.insecure_channel(server_1)
    stub_0 = MatchCountStub(channel_0)
    stub_1 = MatchCountStub(channel_1)

    cache = LRUCache(10)

    df = pd.read_csv(input_file)

    for _, row in df.iterrows():
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

        key = "{},{}".format(winning_team, country)
        count = cache.get(key)

        if count is None:
            if not country:
                count = (get_match_count(stub_0, winning_team, "") + get_match_count(stub_1, winning_team, ""))
            else:
                if simple_hash(country) % 2 == 0:
                    count = get_match_count(stub_0, winning_team, country)
                else:
                    count = get_match_count(stub_1, winning_team, country)
            cache.put(key, count)
            print(count)
        else:
            print("{}*".format(count))

    channel_0.close()
    channel_1.close()

if __name__ == "__main__":
    main()
