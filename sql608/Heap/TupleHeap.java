package sql608.Heap;

import storageManager.Tuple;

import java.util.Comparator;

// definition for Heap which returns Tuple
public class TupleHeap implements Heap<Tuple> {
    private HeapImpl heapImpl;
    private int count;

    public TupleHeap(Comparator<Tuple> comparator) {
        count = 0;
        this.heapImpl = new HeapImpl(10000, comparator);
    }

    @Override
    public void offer(Tuple tuple) {
        HeapNode<Tuple> heapNode = new HeapNode<>(count++, tuple);
        heapImpl.offer(heapNode);
    }

    @Override
    public Tuple poll() {
        HeapNode heapNode = heapImpl.poll();
        return (Tuple) heapNode.data;
    }

    @Override
    public boolean isEmpty() {
        return heapImpl.isEmpty();
    }

}
