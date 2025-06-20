import random
import threading
from typing import Any, List
import time
from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply, key2shard, shard_servers

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg

        # Your definitions here.
        self.client_id = nrand()
        self.sequence_num = 0
        self.nshards = max(cfg.nservers, 1)
        self.nreplicas = max(cfg.nreplicas, 1)
        # print(f"client_id: {self.client_id} nshards: {self.nshards} nreplicas: {self.nreplicas}")

    # to generate a unique sequence number for each request
    def next_sequence_num(self) -> int:
        self.sequence_num += 1
        return self.sequence_num

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        # You will have to modify this function.

        args = GetArgs(key)
        
        # get the shard and servers responsible for the key
        shard = key2shard(key, self.nshards)
        responsible_servers = shard_servers(shard, self.nshards, self.nreplicas)
        # print(f"client_id: {self.client_id} key: {key} responsible_servers: {responsible_servers} shard: {shard} nshards: {self.nshards} nreplicas: {self.nreplicas}")
        max_retries = 100  # 最大重試次數
        retry_count = 0
        
        while retry_count < max_retries:
            # try all the replicas
            for server_id in responsible_servers:
                try:
                    reply = self.servers[server_id].call("KVServer.Get", args)
                    if reply is not None and reply.wrong_leader == False:
                        return reply.value
                except:
                    continue
            
            retry_count += 1
            time.sleep(0.01)  # sleep for a short time and then retry
        
        # if the operation keeps failing
        print(f"Client {self.client_id} Failed to get key {key} after {max_retries} retries")
        raise TimeoutError()
    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        # You will have to modify this function.

        # assign a unique sequence number for this operation
        seq_num = self.next_sequence_num()
        args = PutAppendArgs(self.client_id, seq_num, key, value)
        
        # get the shard and servers responsible for the key
        shard = key2shard(key, self.nshards)
        responsible_servers = shard_servers(shard, self.nshards, self.nreplicas)

        # the primary server is the first server in the list
        primaryPreferred = responsible_servers[0]

        max_retries = 100  # 最大重試次數
        retry_count = 0
        
        while retry_count < max_retries:
            # only try the primary server
            try:
                reply = self.servers[primaryPreferred].call("KVServer." + op, args)
                if reply is not None and reply.wrong_leader == False:
                    return reply.value
            except:
                pass
            
            retry_count += 1
            time.sleep(0.01)  # sleep for a short time and then retry
        
        # if the operation keeps failing
        print(f"Client {self.client_id} Failed to {op} key {key} after {max_retries} retries")
        raise TimeoutError()
    
    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
