# wurmloch-crdt
Experimental implementations of conflict-free replicated data types (CRDTs) for the JVM

## What is a CRDT?

Definition of CRDTs from [Wikipedia][wikipedia crdt]:
> In distributed computing, a conflict-free replicated data type (abbreviated CRDT) is a type of specially-designed data structure used to achieve strong eventual consistency (SEC) and monotonicity (absence of rollbacks). As their name indicates, a CRDT instance is distributed into several replicas; each replica can be mutated promptly and concurrently; the potential divergence between replicas is however guaranteed to be eventually reconciled through downstream synchronisation (off the critical path); consequently CRDTs are known to be highly available.

A conflict-free replicated data type (abbreviated CRDT) is a special kind of data structure that can be used in distributed computing to easily share data between nodes.


## How can I use wurmloch-crdt?

_TBD_

## Examples

### Replica-Example

### GSet-Example

[wikipedia crdt]: https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type