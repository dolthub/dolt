Really bad:
* We eagerly read refs recursively, which for many parts of the system results in ridiculous inefficency (e.g., inside chunks.Commit())
* Using the Value types is very frustrating because of all the wrapping and unwrapping Go types in Values. We need to either build helpers that (de)serialize to native Go types, or add codegen to build immutable structs, sets, and lists from definitions

A little bad:
* Seems like ChunkStore::Writer::Close() should also commit the write, like how Go's File does. Right now, only Ref() does, so if you don't need the ref, you won't actually commit
* MemoryStore should clear the buf rather than invalidating on close, to be like FileStore and S3Store
* S3Store should really be called AWSStore since it depends on Dynamo too
