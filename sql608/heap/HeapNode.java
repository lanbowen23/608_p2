package sql608.heap;

public class HeapNode<T> {
    // increasing count for each elements in the heap
    public int id;
    public T data;

    public HeapNode(int id, T data) {
        this.id = id;
        this.data = data;
    }
}
