package sql608.helper;

import storageManager.Block;
import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Tuple;

public class Write {

    /*
    Append the tuple in main memBlockId and set back to relation end in the disk
    The max fields of one block is 8, so tuple with fields
    more than four will consume one block
    */
    public static void tuple(Tuple tuple, Relation relation, MainMemory mem, int memBlockId) {
        Block block;
        /* if relation is empty in the disk, set at disk block 0 */
        if (relation.getNumOfBlocks() == 0) {
            block = mem.getBlock(memBlockId);
            block.clear();
            block.appendTuple(tuple);
            relation.setBlock(0, memBlockId);
        } else {
            /* read in the final block of the relation already exist in the disk */
            int finalDiskId = relation.getNumOfBlocks() - 1;
            relation.getBlock(finalDiskId, memBlockId);
            block = mem.getBlock(memBlockId);
            /*
            if current mem block is full, meaning the disk block is also full
            write to next block on the disk
            */
            if (block.isFull()) {
                block.clear();
                block.appendTuple(tuple);
                relation.setBlock(finalDiskId + 1, memBlockId);
            } else {
                block.appendTuple(tuple);
                relation.setBlock(finalDiskId, memBlockId);
            }
        }
    }
}
