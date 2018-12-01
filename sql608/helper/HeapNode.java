package sql608.helper;

public class HeapNode<T> {
    // unique id for each elements in the heap
    public int id;
    public T data;

    public HeapNode(int id, T data) {
        this.id = id;
        this.data = data;
    }
}
