package sql608.heap;

import storageManager.Tuple;

// when doing merging back, pull out a tuple
// we need to know its disk block id
// in order to get the next tuple following it
// to be read into main memory and offer to heap
public class TupleWithDiskId {
    public Tuple tuple;
    public int diskId;

    public TupleWithDiskId(Tuple tuple, int diskId) {
        this.tuple = tuple;
        this.diskId = diskId;
    }
}
