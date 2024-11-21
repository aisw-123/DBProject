
/* SpecialCondition class 

This class handles logic for where clause and checks the conditions */
public class SpecialCondition {
    String columnName; //Name of the columns
    private OperatorType operator; //Operator type ie grater than ,less than equal to etc
    String comparisonValue; 
    boolean negation; 
    public int columnOrdinal; 
    public DataTypes dataType;

    //Initialize the SpecialCondition constructors with the data Type 
    public SpecialCondition(DataTypes dataType) {
        this.dataType = dataType;
    }

    //List of supported operators as per the requirements
    public static String[] supportedOperators = { "<=", ">=", "<>", ">", "<", "=" };

    // Below function Converts the operator string from the user input to OperatorType
    public static OperatorType getOpType(String strOp) {
        switch (strOp) {
        case ">":
            return OperatorType.GREATERTHAN;
        case "<":
            return OperatorType.LESSTHAN;
        case "=":
            return OperatorType.EQUALTO;
        case ">=":
            return OperatorType.GREATERTHANOREQUAL;
        case "<=":
            return OperatorType.LESSTHANOREQUAL;
        case "<>":
            return OperatorType.NOTEQUAL;
        default:
            System.out.println("! Invalid operator \"" + strOp + "\"");
            return OperatorType.INVALID;
        }
    }

    //Compare functions compares accross the types of the dataType 
    //i.e for strings it checks lexo graphically and for others values 
    public static int compare(String one, String two, DataTypes dataType) {
        if (dataType == DataTypes.TEXT)
            return one.toLowerCase().compareTo(two);
        else if (dataType == DataTypes.NULL) {
            if (one == two)
                return 0;
            else if (one.toLowerCase().equals("null"))
                return 1;
            else
                return -1;
        } else {
            return Long.valueOf(Long.parseLong(one) - Long.parseLong(two)).intValue();
        }
    }

    //doOpOnDiff -means do a particular operation on some difference between values 
    private boolean doOpOnDiff(OperatorType op,int diff)
    {
        switch (op) {
            case LESSTHANOREQUAL:
            return diff <= 0;
        case GREATERTHANOREQUAL:
            return diff >= 0;
        case NOTEQUAL:
            return diff != 0;
        case LESSTHAN:
            return diff < 0;
        case GREATERTHAN:
            return diff > 0;
        case EQUALTO:
            return diff == 0;
        default:
            return false;
        }
    }

    //doStrCom does a string comparison based on the value and operator provided 
    private boolean doStrCom(String currVal, OperatorType op) {
        return doOpOnDiff(op,currVal.toLowerCase().compareTo(comparisonValue));
    }

    // Does comparison on currentvalue with the comparison value
    public boolean chkCondt(String currVal) {
        OperatorType operation = getOperation();

        if(currVal.toLowerCase().equals("null")
        || comparisonValue.toLowerCase().equals("null"))
            return doOpOnDiff(operation,compare(currVal,comparisonValue,DataTypes.NULL));

        if (dataType == DataTypes.TEXT || dataType == DataTypes.NULL)
            return doStrCom(currVal, operation);
        else {

            switch (operation) {
            case LESSTHANOREQUAL:
                return Long.parseLong(currVal) <= Long.parseLong(comparisonValue);
            case GREATERTHANOREQUAL:
                return Long.parseLong(currVal) >= Long.parseLong(comparisonValue);

            case NOTEQUAL:
                return Long.parseLong(currVal) != Long.parseLong(comparisonValue);
            case LESSTHAN:
                return Long.parseLong(currVal) < Long.parseLong(comparisonValue);

            case GREATERTHAN:
                return Long.parseLong(currVal) > Long.parseLong(comparisonValue);
            case EQUALTO:
                return Long.parseLong(currVal) == Long.parseLong(comparisonValue);

            default:
                return false;

            }

        }

    }

    //setConditionValue is the setter to set the class variables replacing the filler characters
    public void setConditionValue(String conditionValue) {
        this.comparisonValue = conditionValue;
        this.comparisonValue = comparisonValue.replace("'", "");
        this.comparisonValue = comparisonValue.replace("\"", "");

    }

    //Setter to set the columnName
    public void setColumName(String colName) {
        this.columnName = colName;
    }

    //Setter to set the operator 
    public void setOp(String op) {
        this.operator = getOpType(op);
    }

    //Sets the negation to the boolean negation provided
    public void setNegation(boolean negate) {
        this.negation = negate;
    }

    //Getter to get the operation type based on the negation provided
    public OperatorType getOperation() {
        if (!negation)
            return this.operator;
        else
            return negateOperator();
    }

    // In case of NOT operator, invert the operator
    private OperatorType negateOperator() {
        switch (this.operator) {
        case LESSTHANOREQUAL:
            return OperatorType.GREATERTHAN;
        case GREATERTHANOREQUAL:
            return OperatorType.LESSTHAN;
        case NOTEQUAL:
            return OperatorType.EQUALTO;
        case LESSTHAN:
            return OperatorType.GREATERTHANOREQUAL;
        case GREATERTHAN:
            return OperatorType.LESSTHANOREQUAL;
        case EQUALTO:
            return OperatorType.NOTEQUAL;
        default:
            System.out.println("! Invalid operator \"" + this.operator + "\"");
            return OperatorType.INVALID;
        }
    }
}
