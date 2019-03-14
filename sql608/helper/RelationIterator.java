package sql608.helper;

import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Tuple;

import java.util.LinkedList;
import java.util.Queue;

/*
read the tuples in the disk relation block by block to a queue
output the tuple one by one from the queue
*/
public class RelationIterator {
    private MainMemory mainMemory;
    private Relation relation;
    private int memBlock;
    private int curDiskId;
    private Queue<Tuple> tupleQueue;
    private int numRelationBlock;

    public RelationIterator(Relation relation, MainMemory mainMemory, int memBlock) {
        this.relation = relation;
        this.mainMemory = mainMemory;
        this.memBlock = memBlock;
        this.curDiskId = -1;  // the reading index of disk block
        this.tupleQueue = new LinkedList<>();
        this.numRelationBlock = relation.getNumOfBlocks();
    }

    private boolean hasNext() {
        if (!tupleQueue.isEmpty()) return true;
        else {
            while (tupleQueue.isEmpty()) {
                // read next block in the disk and offer to queue
                curDiskId++;
                if (curDiskId >= numRelationBlock) return false;
                mainMemory.getBlock(memBlock).clear();
                relation.getBlock(curDiskId, memBlock);  // get the block to the mem
                if (!mainMemory.getBlock(memBlock).isEmpty()) {
                    /* send all tuples in the block to the queue */
                    for (Tuple tuple : mainMemory.getBlock(memBlock).getTuples()) {
                        if (!tuple.isNull()) tupleQueue.offer(tuple);
                    }
                }
            }
            return true;
        }
    }

    public Tuple next() {
        if (!hasNext()) return null;
        return tupleQueue.poll();
    }
}
