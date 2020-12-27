# Non-Function resource Experiment:

Objectives:

To find the latency between writing serialized objects into File system and writing objects into IN-memory (H2-DB) with single thread and multi thread.

Procedures:

- Create a sample purchase orders of size **10000.**
- Write the objects into the File system with single thread.
- Persist the objects into H2-DB with single thread.
- Write the objects into the File system with two thread.
- Persist the objects into H2-DB with two thread

## Result:

## Single Thread:

### File System:

System took **5.6937485**** seconds** to write 10000 serialized objects into File Systems.

### IN-Memory (H2-DB):

System took **0.814038 seconds** to write 10000 objects into DB.

## Multi Thread:

### File System:

System took **0.2052302**** seconds** to write 10000 serialized objects into File Systems.

### IN-Memory (H2-DB):

System took **0.0654654**** seconds** to write 10000 objects into DB.

## Conclusion:

- IN-Memory DB writing is **6.9 times** faster than File System writing with single thread.
- IN-Memory DB writing is **3.1 times** faster than File System writing with two thread.