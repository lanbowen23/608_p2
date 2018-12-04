package sql608.helper;

import storageManager.MainMemory;
import storageManager.Relation;
import storageManager.Tuple;

import java.util.LinkedList;
import java.util.Queue;

public class RelationIterator {
    private MainMemory mainMemory;
    private Relation relation;
    private int memTmpBlockId;
    private int curBlockId;
    private Queue<Tuple> tupleQueue;
    private int numBlocks;

    public RelationIterator(Relation relation, MainMemory mainMemory, int memTmpBlockId) {
        this.relation = relation;
        this.mainMemory = mainMemory;
        this.memTmpBlockId = memTmpBlockId;
        this.curBlockId = -1;
        this.tupleQueue = new LinkedList<>();
        this.numBlocks = relation.getNumOfBlocks();
    }

    public boolean hasNext() {
        if (!tupleQueue.isEmpty()) {
            return true;
        } else {
            while (tupleQueue.isEmpty()) {
                curBlockId++;
                if (curBlockId >= numBlocks) {
                    return false;
                }
                mainMemory.getBlock(memTmpBlockId).clear();
                relation.getBlock(curBlockId, memTmpBlockId);
                if (!mainMemory.getBlock(memTmpBlockId).isEmpty()) {
                    for (Tuple tuple : mainMemory.getBlock(memTmpBlockId).getTuples()) {
                        if (!tuple.isNull()) {
                            tupleQueue.offer(tuple);
                        }
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
