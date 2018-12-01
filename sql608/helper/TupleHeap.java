package sql608.helper;

import storageManager.Tuple;

import java.util.Comparator;

// definition for heap which returns Tuple
public class TupleHeap implements Heap<Tuple> {
    private MyHeapImpl myHeap;
    private int count;

    public TupleHeap(Comparator<Tuple> comparator) {
        count = 0;
        this.myHeap = new MyHeapImpl(10000, comparator);
    }

    @Override
    //cannot handle duplicate tuple
    // push in
    public void offer(Tuple tuple) {
        HeapNode<Tuple> heapNode = new HeapNode<>(count++, tuple);
        myHeap.offer(heapNode);
    }

    @Override
    // pull out
    public Tuple poll() {
        HeapNode heapNode = myHeap.poll();
        return (Tuple) heapNode.data;
    }

    @Override
    public boolean isEmpty() {
        return myHeap.isEmpty();
    }

}
