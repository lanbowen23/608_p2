package sql608.helper;

import java.util.Comparator;

public class TupleWithBlockIdHeap implements Heap<TupleWithBlockId> {
    private MyHeapImpl myHeap;
    private int count;

    public TupleWithBlockIdHeap(Comparator<TupleWithBlockId> comparator) {
        count = 0;
        this.myHeap = new MyHeapImpl(10000, comparator);
    }

    @Override
    //cannot handle duplicate tuple
    public void offer(TupleWithBlockId tuple) {
        HeapNode<TupleWithBlockId> heapNode = new HeapNode<>(count++, tuple);
        myHeap.offer(heapNode);
    }

    @Override
    public boolean isEmpty() {
        return myHeap.isEmpty();
    }

    @Override
    public TupleWithBlockId poll() {
        HeapNode heapNode = myHeap.poll();
        return (TupleWithBlockId) heapNode.data;
    }
}
