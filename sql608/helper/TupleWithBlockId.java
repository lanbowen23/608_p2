package sql608.helper;

import storageManager.Tuple;

public class TupleWithBlockId {
    public Tuple tuple;
    public int blockId;

    public TupleWithBlockId(Tuple tuple, int blockId) {
        this.tuple = tuple;
        this.blockId = blockId;
    }
}
