# Vision

*Noms enables developers to build rich decentralized applications.*

Decentralization promises to be the next great evolution of the Web. But developers today struggle to create even simple decentralized applications because there are no databases that work well in a decentralized environment.

Our vision is to provide a database native to the decentralized web that includes:

1. Efficient and correct multiparty sync and conflict resolution
1. Search across the entire network, including data that isn’t local
1. Paging of data from the network “lazily” as needed
1. Enforcement of business logic
1. Load-spreading to minimize hotspots
1. Data persistence: data should not lost when a node disconnects or goes away

This vision is achievable. Today Noms can do (1), (2), and (3). We
have ideas for how to solve (4).  Bittorrent and IPFS are existence
proofs for (5). We do not yet have a solution for (6), though there
are several promising efforts underway that may in the future provide
some of the tools we need (e.g., Filecoin).
