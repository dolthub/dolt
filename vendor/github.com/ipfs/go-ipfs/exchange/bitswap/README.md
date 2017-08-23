# Bitswap

## Protocol
Bitswap is the data trading module for ipfs, it manages requesting and sending
blocks to and from other peers in the network. Bitswap has two main jobs, the
first is to acquire blocks requested by the client from the network. The second
is to judiciously send blocks in its posession to other peers who want them.

Bitswap is a message based protocol, as opposed to response-reply. All messages
contain wantlists, or blocks. Upon receiving a wantlist, a node should consider
sending out wanted blocks if they have them. Upon receiving blocks, the node
should send out a notification called a 'Cancel' signifying that they no longer
want the block. At a protocol level, bitswap is very simple.

## go-ipfs Implementation
Internally, when a message with a wantlist is received, it is sent to the
decision engine to be considered, and blocks that we have that are wanted are
placed into the peer request queue. Any block we possess that is wanted by
another peer has a task in the peer request queue created for it. The peer
request queue is a priority queue that sorts available tasks by some metric,
currently, that metric is very simple and aims to fairly address the tasks
of each other peer. More advanced decision logic will be implemented in the
future. Task workers pull tasks to be done off of the queue, retreive the block
to be sent, and send it off. The number of task workers is limited by a constant
factor.

Client requests for new blocks are handled by the want manager, for every new
block (or set of blocks) wanted, the 'WantBlocks' method is invoked. The want
manager then ensures that connected peers are notified of the new block that we
want by sending the new entries to a message queue for each peer. The message
queue will loop while there is work available and do the following: 1) Ensure it
has a connection to its peer, 2) grab the message to be sent, and 3) send it.
If new messages are added while the loop is in steps 1 or 3, the messages are
combined into one to avoid having to keep an actual queue and send multiple
messages. The same process occurs when the client receives a block and sends a
cancel message for it.

