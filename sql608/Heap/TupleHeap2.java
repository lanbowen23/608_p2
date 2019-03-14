package sql608.Heap;

import java.util.Comparator;

public class TupleHeap2 implements Heap<TupleWithDiskId> {
    private HeapImpl heapImpl;
    private int count;

    public TupleHeap2(Comparator<TupleWithDiskId> comparator) {
        count = 0;
        this.heapImpl = new HeapImpl(10000, comparator);
    }

    @Override
    //cannot handle duplicate tuple
    public void offer(TupleWithDiskId tuple) {
        HeapNode<TupleWithDiskId> heapNode = new HeapNode<>(count++, tuple);
        heapImpl.offer(heapNode);
    }

    @Override
    public TupleWithDiskId poll() {
        HeapNode heapNode = heapImpl.poll();
        return (TupleWithDiskId) heapNode.data;
    }

    @Override
    public boolean isEmpty() {
        return heapImpl.isEmpty();
    }
}
