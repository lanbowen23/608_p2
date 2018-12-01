package sql608;

public class ExpressionTreeNode {
    private String value;
    private ExpressionTreeNode left, right;

    public ExpressionTreeNode(String string, ExpressionTreeNode left, ExpressionTreeNode right) {
        value = string;
        this.left = left;
        this.right = right;
    }

    public ExpressionTreeNode(String str) {
        value = str;
        setLeft(null);
        setRight(null);
    }

    public ExpressionTreeNode() {
        value = null;
        setLeft(null);
        setRight(null);
    }

    public ExpressionTreeNode getLeft() {
        return left;
    }

    public void setLeft(ExpressionTreeNode left) {
        this.left = left;
    }

    public ExpressionTreeNode getRight() {
        return right;
    }

    public void setRight(ExpressionTreeNode right) {
        this.right = right;
    }

    public String getValue() {
        return value;
    }

}
