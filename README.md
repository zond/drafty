drafty
======

```
        L  A    
     K        B 
    J          C
    I          D
     H        E 
        G  F     
```

`B` is responsible for keys `[A,B)`, `C` is responsble for keys `[B,C)` etc.

Each node also has backups of keys for `NBackups` of its predecessors. Ie `E`, while being responsible for `[D,E)` also has a backup of `[B,C)` and `[C,D)`.

Notes
======
* The cluster must always be fully functional as long as no `NBackups+1` nodes in a sequence disappear or join.
  * Thus, data must be present on `NBackups+1` nodes succeeding its key in the namespace.
  * To simplify this in a transaction context, the client itself will be responsible for including all `NBackups+1` nodes succeding the key in any transaction involving key.
  * The client has a copy of the cluster roster that isn't guaranteed to be up to date.
  * Since data consistency depend on at least one of the nodes responsible for a key is also involved in the transaction, nodes must refuse taking part in transactions involving keys they aren't responsible for.
  * [DONE] To enable this, all nodes must all times know exactly what keys they are responsible for.
  * [DONE] To ensure this, a cluster always has a raft layer with a leader.
  * [DONE] The leader is the only node that can accept or kick members.
  * [DONE] Before the leader accepts or kicks a node, it will stop the cluster.
  * [DONE] Stopping the cluster will, for each node in the cluster, block new data operations, and wait until all currently running data operations are finished.
  * [DONE] After the leader has accepted or kicked a node, it will restart the cluster.
  * [DONE] Restarting the cluster will, for each node in the cluster, update the cluster roster and then remove the block for new data operations.
* Transactions must not cause inconsistent data when any member, either client or node, of the transaction dies.
  * Synchronizing transaction context between nodes is hard, since contexts contain metadata for ranges split from original client requests, and node joins or disappearances would require merging or splitting those contexts.
  * Instead, the client will always try to push through the transaction, even if some nodes in the cluster don't respond, since as long as at least one of the nodes responsible for a key is still alive we won't get any inconsistencies, since it is enough that one of the `NBackups+1` nodes responsible for the key validates the transaction.
  * To ensure data consistency in the face of this behaviour, all nodes must voluntarily die if they note that `NBackups+1` nodes in a sequence disappear. Ie the cluster must suicide when it knows it can't keep data consistent any more.
  * To ensure that this also happens when a new node joins the cluster right before the last of an old sequence dies, and thus takes over a range that it hasn't as yet received a full copy of, this suicide watch must be aware of which nodes are fully synchronized and which are not.
* To ensure serializable snapshot isolation transactors will use http://www.vldb.org/pvldb/vol7/p329-mahmoud.pdf modified to let transactions collide over overlapping key ranges instead of just individual keys, which will (if it works) prohibit phantom reads.
  * This will require lots of legwork, such as:
    * Client transactions will need to cache the range lookups performed as well as their results.
      * To avoid phantom reads, ranges already looked up must never be looked up again against the cluster.
      * To avoid doing that, new range lookups will only perform the parts that don't overlap with already cached ranges.
      * To simplify returning consistent results with the above requirements, the final result of any range lookup will be fetched from the cache produced by the actual cluster range lookups.
      * To enable this, the cache will be sorted. 
    * All the parts http://www.vldb.org/pvldb/vol7/p329-mahmoud.pdf that mention `collects the transaction IDs of all transactions with soft read locks on y` and such need to be modified to `collects the transactionIDs of all transactions with soft read locks on ranges overlapping y`.
* [DONE] To make sure nodes joining the cluster get the data they need to be responsible for, or nodes that get new responsibilites as nodes leave the cluster have the data they need, the cluster will start clean and synchronize operations every time it restarts.
  * [DONE} A clean operation will go through the data of the node, from the node id and upwards (overflowing to start from the beginning if needed), until it hits the id of the `NBackups+2`'th predecessor of the node, and try to write the data it finds to each node the data actually belongs to.
  * [DONE] Whenever a write is successful, the data will be removed from the cleaning node.
  * [DONE] A synchronize operation will make sure that each `NBackups` node succeeding the synchronizing node in the cluster has the same data between the predecessor of the node and the node.
  * [DONE] To ensure this without going through and comparing each value between predecessor and synchronizing node all nodes keep a merkle tree for their data, which ensures a synchronization that is O(ln(n)+m) where n is the number of keys the synchronizing node owns and m is the number of keys that need to be synchronized.

