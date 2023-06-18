# Solid Consensus

Protocol for achieving BFT consensus for a given state machine.


## Overview

Solid minimizes network communication. `accept` messages are sent only to the next designated leader (many to one). Once a leader accumulates `accept` messages from a majority of nodes (incl itself), it will then propose a new `proposal` to the network (one to many).

If the network detects that no proposal has been received from the network within a given timeout period, then the node will then send an `accept` to the next designated leader, with a `skip` of 1. This process is repeated until a valid proposal is sent from a node.

![Consensus Protocol Overview](docs/consensus.png)

Solid implements an async event stream interface where relevant events are omitted and it is the responsibility of the implementor to define the network and storage implementations.

Number of nodes should always be an odd number to prevent a deadlock situation where no majority can be reached.


### Accepts

Accepts are sent to the next designated leader in the following scenarios: 
 
  1. when a valid proposal is received from the previous leader (building on the last confirmed proposal)

  2. after a given timeout (in order to move on from nodes that are not online), these are referred to as `skips`

  3. if there are no pending proposals (usually on start up)

Accepts are either for a proposal:
    - confirmed height + 1 (when a valid proposal is received)
    - confirmed height (when no valid pending proposals exist)


## Todo
 
 - Add/remove peers from the protocol validators
