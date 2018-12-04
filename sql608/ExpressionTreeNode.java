package sql608;

public class ExpressionTreeNode {
    // value can be operator and operands
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

    public String getString(ExpressionTreeNode root) {
        if (root == null) return " ";
        String left = getString(root.left);
        String cur = root.value;
        String right = getString(root.right);
        // left: " course.id "  cur: "="  right: " course2.id "
        return left + cur + right;
    }

}
