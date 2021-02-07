Dolt’s feature set is quickly expanding.
Many of these features require new persistent data structures, 
which presents a compatibility challenge in supporting older repos. 
Compatibility issues come in two forms: forward and backward. 
For clarity: backward compatibility is new Dolt clients reading/writing old repositories; 
forward compatibility is old clients reading/writing new repos.

*Feature Version* is a tool to help guarantee data integrity and compatibility.
If a change needs to be made to persistent datastructures that will break forward or backward compatibility,
the feature version can be incremented to prevent older clients from interacting with newer repos.

A feature version is persisted with each RootValue, and is compiled with each Dolt binary.
While reading a RootValue, clients will error if the persisted version is greater than their own version.
Clients set each RootValue's version to their own while writing. 
Different versions can exist on various commits and branches within a database. 
