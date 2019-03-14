package sql608.Heap;

// Heap storing different type of data structure
// need to implement several necessary operations
public interface Heap<T> {

    void offer(T node);

    T poll();

    boolean isEmpty();
}
