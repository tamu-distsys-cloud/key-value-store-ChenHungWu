import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

def key2shard(key: str, nshards: int) -> int:
    """map the key to the shard ID - using int(key) % nshards"""
    try:
        return int(key) % nshards
    except ValueError:
        return sum(ord(c) for c in key) % nshards

def shard_servers(shard: int, nshards: int, nreplicas: int) -> list:
    """return the list of servers responsible for the given shard"""
    servers = []
    for i in range(nreplicas):
        server_id = (shard + i) % nshards
        servers.append(server_id)
    return servers

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, client_id=None, sequence_num=None, key=None, value=None):
        self.client_id = client_id
        self.sequence_num = sequence_num
        self.key = key
        self.value = value

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value=None, wrong_leader=False):
        self.value = value
        self.wrong_leader = wrong_leader

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key=None):
        self.key = key

class GetReply:
    # Add definitions here if needed
    def __init__(self, value=None, wrong_leader=False):
        self.value = value
        self.wrong_leader = wrong_leader

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        # Your definitions here.
        self.data = {}  # key -> value
        self.client_sessions = {}  # client_id -> (last_sequence_num, last_reply)
        # self.nshards = max(cfg.nservers, 1)
        # self.nreplicas = max(cfg.nreplicas, 1)
        self.shard_id = len(cfg.running_servers)

    def is_duplicate(self, client_id: int, sequence_num: int) -> Tuple[bool, Any]:
        """check if this is a duplicate operation, return (is_duplicate, cached_reply)"""
        if client_id in self.client_sessions:
            last_seq, last_reply = self.get_last_response(client_id)
            if sequence_num == last_seq:
                # this is a duplicate request, just return the cached reply
                return True, last_reply
            elif sequence_num < last_seq:
                # should never happen
                print(f"duplicate request: client_id: {client_id} sequence_num: {sequence_num} last_seq: {last_seq}")
                return True, PutAppendReply("", wrong_leader=False)
        return False, None

    def is_responsible_for_key(self, key: str) -> bool:
        """check if the current server is responsible for handling this key"""
        shard = key2shard(key, self.cfg.nservers)
        responsible_servers = shard_servers(shard, self.cfg.nservers, self.cfg.nreplicas)
        return self.shard_id in responsible_servers

    def get_last_response(self, client_id: int):
        if client_id in self.client_sessions:
            last_seq, last_reply = self.client_sessions[client_id] # last_sequence_num, last_reply
        else:
            last_seq, last_reply = None, None        
        return last_seq, last_reply

    def set_last_response(self, client_id: int, sequence_num: int, reply: Any):
        self.client_sessions[client_id] = (sequence_num, reply)
        return

    def Get(self, args: GetArgs):
        if not self.is_responsible_for_key(args.key):
            return GetReply("", wrong_leader=True)
        
        with self.mu:
            value = self.data.get(args.key, "")
            return GetReply(value, wrong_leader=False)

    def Put(self, args: PutAppendArgs):
        if not self.is_responsible_for_key(args.key):
            return PutAppendReply("", wrong_leader=True)
        
        is_dup, cached_reply = self.is_duplicate(args.client_id, args.sequence_num)
        if is_dup:
            return cached_reply
        
        with self.mu:
            self.data[args.key] = args.value
            reply = PutAppendReply("", wrong_leader=False)
            
            # update the client session
            self.set_last_response(args.client_id, args.sequence_num, reply)
        
        # replicate to replicas after releasing the primary lock
        self.replicate_to_replicas(args.key, args.value, "Put")
        
        return reply

    def Append(self, args: PutAppendArgs):
        if not self.is_responsible_for_key(args.key):
            return PutAppendReply("", wrong_leader=True)
        
        is_dup, cached_reply = self.is_duplicate(args.client_id, args.sequence_num)
        if is_dup:
            return cached_reply
        
        with self.mu:      
            old_value = self.data.get(args.key, "")
            new_value = old_value + args.value
            self.data[args.key] = new_value
            reply = PutAppendReply(old_value, wrong_leader=False)
            
            # update the client session
            self.set_last_response(args.client_id, args.sequence_num, reply)
        
        # replicate to replicas after releasing the primary lock
        self.replicate_to_replicas(args.key, new_value, "Put")
        
        return reply

    def replicate_to_replicas(self, key: str, value: str, op: str):
        """replicate the data to the replicas"""
        shard = key2shard(key, self.cfg.nservers)
        responsible_servers = shard_servers(shard, self.cfg.nservers, self.cfg.nreplicas)
        
        # replicate to all replicas (except the primary server)
        for server_id in responsible_servers[1:]:  # skip the primary server
            try:
                replica_server = self.cfg.kvservers[server_id]
                # directly set the data on the replica
                with replica_server.mu:
                    replica_server.data[key] = value
            except Exception:
                continue
