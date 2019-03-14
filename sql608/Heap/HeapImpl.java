package sql608.Heap;

import java.util.Arrays;
import java.util.Comparator;

public class HeapImpl {
    private int lastIndex;
    private int[] posArray;
    private HeapNode[] dataArray;
    private Comparator comparator;

    public HeapImpl(int size, Comparator comparator) {
        lastIndex = 0;
        posArray = new int[size];
        Arrays.fill(posArray, -1);

        dataArray = new HeapNode[size];
        this.comparator = comparator;
    }

    public boolean isEmpty() {
        return lastIndex == 0;
    }

    public boolean compareData (HeapNode node1, HeapNode node2, int mode) {
        switch(mode) {
            // fail the comparison
            case -1: return comparator.compare(node1.data, node2.data) < 0;
            // satisfy the comparator we want
            case 1: return comparator.compare(node1.data, node2.data) >= 0;
        }
        return false;
    }

    public void offer(HeapNode node) {
        dataArray[lastIndex] = node;
        posArray[node.id] = lastIndex;
        moveUp(lastIndex);
        lastIndex++;
    }

    public HeapNode poll() {
        if (isEmpty()) return null;
        HeapNode ans = dataArray[0];
        delete(ans);
        return ans;
    }

    private void moveUp(int pos) {
        if (pos == 0 || compareData(dataArray[pos], dataArray[(pos-1)/2],1)) {
            return;
        }
        swap(pos, (pos-1) / 2);
        moveUp((pos-1) / 2);
    }

    public void delete(HeapNode node) {
        int pos = posArray[node.id];
        if (pos < 0) return;
        // fix corner case, the one to delete is the last one
        if (pos == lastIndex - 1) {
            posArray[node.id] = -1;
            dataArray[pos] = null;
            lastIndex--;
            return;
        }
        dataArray[pos] = dataArray[lastIndex - 1];
        posArray[dataArray[pos].id] = pos;
        dataArray[lastIndex - 1] = null;
        lastIndex--;
        if (pos != 0 && compareData(dataArray[pos], dataArray[(pos-1)/2],-1)) {
            moveUp(pos);
        } else {
            moveDown(pos);
        }
    }

    private void moveDown(int pos) {
        int maxChildPos;
        if (2 * pos + 1 >= lastIndex) return;
        if (2 * pos + 2 >= lastIndex) {
            maxChildPos = 2 * pos + 1;
        } else {
            maxChildPos = compareData(dataArray[2 * pos + 1],dataArray[2 * pos + 2],-1)?
                          2 * pos + 1 : 2 * pos + 2;
        }
        if (compareData(dataArray[maxChildPos],dataArray[pos],1)) return;
        swap(pos, maxChildPos);
        moveDown(maxChildPos);
    }

    private void swap(int pos1, int pos2) {
        HeapNode tmp = dataArray[pos1];
        dataArray[pos1] = dataArray[pos2];
        posArray[dataArray[pos1].id] = pos1;
        dataArray[pos2] = tmp;
        posArray[dataArray[pos2].id] = pos2;
    }


}
