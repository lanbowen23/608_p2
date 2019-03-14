package sql608.Heap;

import storageManager.Tuple;

// when doing merging back, pull out a tuple
// we need to know its disk block id
// in order to get the "next tuple" in that disk block following it
// to be read into main memory and offer to Heap
public class TupleWithDiskId {
    public Tuple tuple;
    public int diskId;

    public TupleWithDiskId(Tuple tuple, int diskId) {
        this.tuple = tuple;
        this.diskId = diskId;
    }
}
