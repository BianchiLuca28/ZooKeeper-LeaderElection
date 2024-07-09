from kazoo.client import KazooClient
import time
import os

ZK_HOSTS = '127.0.0.1:2181'
ELECTION_PATH = "/election"
current_znode_path = None
is_leader = False

def zk_client():
    zk = KazooClient(hosts=ZK_HOSTS)
    zk.start()
    return zk

def watch_node(zk, node_to_watch):
    @zk.DataWatch(f"{ELECTION_PATH}/{node_to_watch}")
    def watch_node(data, stat):
        if stat is None:
            print(f"Watched node {node_to_watch} went away, triggering re-election")
            elect_leader(zk)

def elect_leader(zk):
    global current_znode_path, is_leader
    # Create an ephemeral sequential node
    if current_znode_path is None:
        current_znode_path = zk.create(ELECTION_PATH + "/node_", ephemeral=True, sequence=True)
    nodes = zk.get_children(ELECTION_PATH)
    nodes.sort()

    # Check if the current node is the smallest one (i.e., the leader)
    if current_znode_path == f"{ELECTION_PATH}/{nodes[0]}":
        print(f"{current_znode_path} is the leader")
        is_leader = True
    else:
        next_node = nodes[nodes.index(current_znode_path.split('/')[-1]) - 1]
        watch_node(zk, next_node)
        print(f"{current_znode_path} is not the leader")
        is_leader = False

def run_election():
    global is_leader
    zk = zk_client()
    zk.ensure_path(ELECTION_PATH)
    elect_leader(zk)

    while True:
        if is_leader:
            print(f"Process {os.getpid()} is doing leader work...")
        else:
            print(f"Process {os.getpid()} is waiting or doing non-leader work...")
        
        # Wait for a short period before checking again
        time.sleep(5)

if __name__ == "__main__":
    run_election()
