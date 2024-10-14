# lsm-tree

- Implemented an LSM-Tree with SSTables for efficient writes and reads of key-value pairs.
• Designed and implemented a multi-level storage system with memtables and on-disk segments with merging and
compactions.
• Implemented efficient range queries and prefix scans using double-ended and bounded iterators, and bloom filters.
• Implemented a custom cache using LRU eviction policy, for both queries and disk blocks.
• Created a WAL and snapshots for crash recovery and data durability.
• Implemented serialization and deserialization methods for data structures, along with compression of disk blocks.- 

Overall TODO:

- do not export fields in structs which are not required, everything is fucking exported rn
- there are alot of pass by value instead of pass by refs, within structs, func args etc
- use things like `Bound[T]`, reduces memory usage :)
- many places uses values, pass by refs wherever possible
- someplaces, used []byte instead of value.Userkey and etc. fix it
- // iterate via channels@TODO:
- use path/filepath
