

Database <- Tx, Less
Tx <- Ver, B+Tree, Read, Write, Commit, Abort
B+Tree <- Read page, Alloc+Write page, Less
