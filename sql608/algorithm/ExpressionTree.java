package sql608.algorithm;

import sql608.helper.ValueContainer;
import storageManager.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.Stack;

// expression tree is for condition check under WHERE clause
public class ExpressionTree {
    // each operator corresponds to ValueContainer operands
    private Stack<String> operator;
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

    // consider the parenthesis in the condition
    // by setting its precedence to be lowest
    public ExpressionTreeNode constructTree(String whereCondition) {
        condition = whereCondition;
        if (whereCondition == null || whereCondition.length() == 0) return null;
        String[] words = splitWords(whereCondition);
        for (String word : words) {
            // any if succeed, continue to next word
            if (isOperator(word)) {
                processOperator(word);
            } else if (word.equals("(")) {
                processLeftParentheses();
            } else if (word.equals(")")) {
                processRightParentheses();
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

    // pop out ValueContainer operand
    // form a binary node with corresponding operator

    private void processRightParentheses() {
        // right parentheses won't be pushed into heap
        // operate until meet left parentheses
        // expand tree again
        while (!operator.empty() &&
                !operator.peek().equals("(")) {
            expandTree();
        }
        String parentheses = operator.pop();
        // in case left parentheses is the last operator in the stack
        if (Objects.equals(parentheses, "(")) {
            if (!operator.empty()) expandTree();
        }
    }

    private void expandTree() {
        String op = operator.pop();
        ExpressionTreeNode right = operand.pop();
        ExpressionTreeNode left = operand.pop();
        ExpressionTreeNode tmp = new ExpressionTreeNode(op, left, right);
        operand.push(tmp);
    }

    private void processOperator(String str) {
        int precedence = getPrecedence(str);
        // if higher priority operator is in stack
        // pop out to form a tree nodeName
        while ((!operator.isEmpty()) &&
                precedence <= getPrecedence(operator.peek())) {
            expandTree();
        }
        operator.push(str);
    }

    private void processOperand(String str) {
        operand.push(new ExpressionTreeNode(str));
    }

    private void processLeftParentheses() {
        operator.push("(");
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
        } else if (operator.equals("(") || operator.equals(")")) {
            return -2;
        } else {  // &
            return 0;
        }
    }

    /* check the tuple satisfy the where condition or not */
    public static boolean checkCondition(Tuple tuple, ExpressionTreeNode treeNode) {
        /* string is equal, ignoring case, to the string "true" then return True */
        return Boolean.parseBoolean(evaluate(tuple, treeNode));
    }

    private static String evaluate(Tuple tuple, ExpressionTreeNode treeNode) {
        /*
        evaluate the subCondition in WHERE recursively follow the tree
        always return String like "true" or operand's value(INT, STR)
        */
        String nodeOperand = treeNode.getValue();
        String leftOperand, rightOperand;
        boolean tempResult;
        switch (nodeOperand) {
            default:
                if (isInteger(nodeOperand)) return nodeOperand;
                else return getTupleField(tuple, nodeOperand);
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
                        return String.valueOf(leftOperand.toLowerCase().
                                equals(rightOperand.replaceAll("\"", "")));
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
        }
    }

    // split by AND
    public static ValueContainer<ArrayList<ExpressionTreeNode>, ArrayList<String>> getSubTreeNodes(ExpressionTree tree) {
        if (tree.getRoot() == null || tree.getRoot() == null) {
            return new ValueContainer<>(null, null);
        }

        ArrayList<ExpressionTreeNode> nodes = new ArrayList<>();
        ArrayList<String> conditions = new ArrayList<>();

        if(!"&".contains(tree.getRoot().getValue())) {
            // nodeName not AND
            nodes.add(tree.getRoot());
            conditions.add(tree.condition);
        } else {
            // nodeName is AND
            ExpressionTree leftExpressionTree = new ExpressionTree(tree.getRoot().getLeft());
            nodes.addAll(getSubTreeNodes(leftExpressionTree).nodeName);
            conditions.addAll(getSubTreeNodes(leftExpressionTree).condition);
            ExpressionTree rightExpressionTree = new ExpressionTree(tree.getRoot().getRight());
            nodes.addAll(getSubTreeNodes(rightExpressionTree).nodeName);
            conditions.addAll(getSubTreeNodes(rightExpressionTree).condition);
        }
        return new ValueContainer<>(nodes, conditions);
    }

    public ExpressionTree(ExpressionTreeNode expressionTreeNode) {
        operand = new Stack<>();
        operator = new Stack<>();
        this.relationName = "";
        this.root = expressionTreeNode;
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
        return Arrays.asList(operators).contains(str);
    }

    public static boolean isInteger(String str) {
        return str.matches("\\d+");
    }

    private static String getTupleField(Tuple tuple, String field) {
        if (!Objects.equals(relationName, "")
            && !relationName.contains("naturalJoin")
            && !relationName.contains("Join")
            && field.contains(".")) {
            field = field.split("\\.")[1];
        }
        if (tuple.getSchema().getFieldNames().contains(field)) {
            return tuple.getField(field).toString();
        } else {
            return field;
        }
    }

}
