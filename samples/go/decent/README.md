# About

This directory contains two sample applications that demonstrate using Noms in a decentralized environment.

Both applications implement multiuser chat, using different strategies.

`p2p-chat` is the simplest possible example: a fully local noms replica is run on each node, and all nodes synchronize continuously with each other over HTTP.

`ipfs-chat` backs Noms with the [IPFS](https://ipfs.io/) network, so that nodes don't have to keep a full local replica of all data. However, because [Filecoin](http://filecoin.io/) doesn't yet exist, *some node* does have to keep a full replica, so ipfs-chat has a `daemon` mode so that you can run a persistent node somewhere to be the replica of last resort.
