package sql608;

import storageManager.Tuple;

import java.util.Arrays;
import java.util.Stack;

public class ExpressionTree {
    private Stack<String> operator;
    // operand contains operator
    private Stack<ExpressionTreeNode> operand;
    private ExpressionTreeNode root;
    private String condition;
    static String relationName;

    public ExpressionTreeNode getRoot() {
        return root;
    }

    public ExpressionTree(String whereCondition, String relationName) {
        operand = new Stack<>();
        operator = new Stack<>();
        this.relationName = relationName;
        this.root = constructTree(whereCondition);
    }

    public ExpressionTreeNode constructTree(String whereCondition) {
        if (whereCondition == null || whereCondition.length() == 0) return null;
        String[] words = splitWords(whereCondition);
        for (String word : words) {
            if (isOperator(word)) {
                processOperator(word);
            } else if (word.equals("(")) {
                processLeftParentheses(word);
            } else if (word.equals(")")) {
                processRightParentheses(word);
            } else {
                processOperand(word);
            }
        }
        while (!operator.isEmpty()) {
            expandTree();
        }
        root = operand.pop();
        return root;
    }

    private void processOperator(String str) {
        int precedence = getPrecedence(str);
        while ((!operator.isEmpty()) && precedence <= getPrecedence(operator.peek())) {
            expandTree();
        }
        operator.push(str);
    }

    private void expandTree() {
        String op = operator.pop();
        ExpressionTreeNode right = operand.pop();
        ExpressionTreeNode left = operand.pop();
        ExpressionTreeNode tmp = new ExpressionTreeNode(op, left, right);
        operand.push(tmp);
    }

    private void processLeftParentheses(String str) {
        operator.push("(");
    }

    private void processRightParentheses(String str) {
        while (!operator.empty() && !operator.peek().equals("(")) {
            expandTree();
        }
        operator.pop();
    }

    private void processOperand(String str) {
        operand.push(new ExpressionTreeNode(str));
    }

    private int getPrecedence(String operator) {
        final String[] p1 = new String[] { "*", "/" };
        final String[] p2 = new String[] { "+", "-", ">", "<" };
        if (Arrays.asList(p1).contains(operator)) {
            return 3;
        } else if (Arrays.asList(p2).contains(operator)) {
            return 2;
        } else if (operator.equals("=")) {
            return 1;
        } else if (operator.equals("|")) {
            return -1;
        } else {
            return 0;
        }
    }

    public static boolean checkCondition(Tuple tuple, ExpressionTreeNode treeNode) {
        // string argument is not null and is equal, ignoring case, to the string "true".
        return Boolean.parseBoolean(evaluate(tuple, treeNode));
    }

    public static String evaluate(Tuple tuple, ExpressionTreeNode treeNode) {
        // evaluate the subCondition in WHERE
        // return "TRUE" or not
        String nodeOperand = treeNode.getValue();
        String leftOperand, rightOperand;
        boolean tempResult;
        switch (nodeOperand) {
            case "&":
                tempResult = Boolean.parseBoolean(evaluate(tuple, treeNode.getLeft()))
                        && Boolean.parseBoolean(evaluate(tuple, treeNode.getRight()));
                return String.valueOf(tempResult);
            case "|":
                tempResult = Boolean.parseBoolean(evaluate(tuple, treeNode.getLeft()))
                        || Boolean.parseBoolean(evaluate(tuple, treeNode.getRight()));
                return String.valueOf(tempResult);
            case "=":
                leftOperand = evaluate(tuple, treeNode.getLeft());
                rightOperand = evaluate(tuple, treeNode.getRight());
                if (isInteger(leftOperand)) {
                    return String.valueOf(Integer.parseInt(leftOperand) == Integer.parseInt(rightOperand));
                } else {
                    // leftOperand may be the tuple field of type STR
                    return String.valueOf(leftOperand.toLowerCase().equals(rightOperand.replaceAll("\"", "")));
                }
            case ">":
                leftOperand = evaluate(tuple, treeNode.getLeft());
                rightOperand = evaluate(tuple, treeNode.getRight());
                return String.valueOf(Integer.parseInt(leftOperand) > Integer.parseInt(rightOperand));
            case "<":
                leftOperand = evaluate(tuple, treeNode.getLeft());
                rightOperand = evaluate(tuple, treeNode.getRight());
                return String.valueOf(Integer.parseInt(leftOperand) < Integer.parseInt(rightOperand));
            case "+":
                leftOperand = evaluate(tuple, treeNode.getLeft());
                rightOperand = evaluate(tuple, treeNode.getRight());
                return String.valueOf(Integer.parseInt(leftOperand) + Integer.parseInt(rightOperand));
            case "-":
                leftOperand = evaluate(tuple, treeNode.getLeft());
                rightOperand = evaluate(tuple, treeNode.getRight());
                return String.valueOf(Integer.parseInt(leftOperand) - Integer.parseInt(rightOperand));
            case "*":
                leftOperand = evaluate(tuple, treeNode.getLeft());
                rightOperand = evaluate(tuple, treeNode.getRight());
                return String.valueOf(Integer.parseInt(leftOperand) * Integer.parseInt(rightOperand));
            case "/":
                leftOperand = evaluate(tuple, treeNode.getLeft());
                rightOperand = evaluate(tuple, treeNode.getRight());
                return String.valueOf(Integer.parseInt(leftOperand) / Integer.parseInt(rightOperand));
            default:
                if (isInteger(nodeOperand)) {
                    return nodeOperand;
                } else {
                    return getTupleField(tuple, nodeOperand);
                }
        }
    }

    public ExpressionTree(ExpressionTreeNode expressionTreeNode) {
        operand = new Stack<>();
        operator = new Stack<>();
        this.relationName = "";
        this.root = expressionTreeNode;
    }

    public ExpressionTree() {
        operand = new Stack<>();
        operator = new Stack<>();
        this.root = new ExpressionTreeNode();
    }

    private String[] splitWords(String str) {
        str = str.toLowerCase().replaceAll(" and ", " & ").replaceAll(" or ", " | ");
        str = str.replaceAll("\\+", " + ").replaceAll("\\*", " * ");
        final String[] operators = new String[] { "-", "/", "=", ">", "<" };
        for (String operator : operators) {
            if (str.contains(operator))
                str = str.replaceAll(operator, " " + operator + " ");
            if (str.contains("("))
                str = str.replaceAll("\\(", " ( ");
            if (str.contains(")"))
                str = str.replaceAll("\\)", " ) ");
        }
        return str.trim().split("[\\s]+");
    }

    private boolean isOperator(String str) {
        final String[] operators = new String[] { "-", "/", "=", ">", "<", "+", "*", "&", "|" };
        if (Arrays.asList(operators).contains(str)) {
            return true;
        }
        return false;
    }

    public static boolean isInteger(String str) {
        return str.matches("\\d+");
    }

    private static String getTupleField(Tuple tuple, String field) {
        // TODO why relationName will contain ?
        if (relationName != "" && !relationName.contains("naturalJoin")
            && !relationName.contains("CrossJoin") && field.contains(".")) {
            field = field.split("\\.")[1];
        }
        if (tuple.getSchema().getFieldNames().contains(field)) {
            return tuple.getField(field).toString();
        } else {
            return field;
        }
    }

}
