
/* SpecialCondition class 

This class handles logic for where clause and checks the conditions */
public class SpecialCondition {
    String colHeader; //Name of the columns
    private OperatorType op; //Type of the operator (<, >, =, etc.)
    String compVal; // comparison values
    boolean neg; //Negation
    public int columnOrdinal; 
    public DataTypes dt ; //data types

    //Initialize the SpecialCondition constructors with the data Type 
    public SpecialCondition(DataTypes dataType) {
        this.dt = dataType;
    }

    //List of operators  
    public static String[] supportedOperators = { "<=", ">=", "<>", ">", "<", "=" };

    // Converts User inputted operator to OperatorType operator
    public static OperatorType getOpType(String stringOp) {
        if (stringOp.equals(">")) {
            return OperatorType.GREATERTHAN;
        } else if (stringOp.equals("<")) {
            return OperatorType.LESSTHAN;
        } else if (stringOp.equals("=")) {
            return OperatorType.EQUALTO;
        } else if (stringOp.equals(">=")) {
            return OperatorType.GREATERTHANOREQUAL;
        } else if (stringOp.equals("<=")) {
            return OperatorType.LESSTHANOREQUAL;
        } else if (stringOp.equals("<>")) {
            return OperatorType.NOTEQUAL;
        } else {
            System.out.println("! Operator \"" + stringOp + "\" is not supported.");
            return OperatorType.INVALID;
        }
    }

    //Compare functions compares accross the types of the dataType
    public static int compare(String str1, String str2, DataTypes dataType) {
        switch(dataType){
            case TEXT:
                return str1.compareTo(str2);
            case NULL:
                return compareNullVals(str1, str2);
            default:
                return compareNumVals(str1, str2);
        }
    }
    
    // compare null values
    private static int compareNullVals(String str1, String str2) {
        if (str1.equals(str2)) {
            return 0;
        } else if (str1.equals("null")) {
            return 1;
        } else {
            return -1;
        }
    }

    // compare numeric values
    private static int compareNumVals(String one, String two) {
        return Long.valueOf(Long.parseLong(one) - Long.parseLong(two)).intValue();
    }

    //doOpOnDiff -means do a particular operation on some difference between values 
    private boolean doOpOnDiff(OperatorType op, int diff)
    {  
        if (op == OperatorType.LESSTHANOREQUAL) {
            return diff <= 0;
        } else if (op == OperatorType.GREATERTHANOREQUAL) {
            return diff >= 0;
        } else if (op == OperatorType.NOTEQUAL) {
            return diff != 0;
        } else if (op == OperatorType.LESSTHAN) {
            return diff < 0;
        } else if (op == OperatorType.GREATERTHAN) {
            return diff > 0;
        } else if (op == OperatorType.EQUALTO) {
            return diff == 0;
        } else {
            return false;
        }
    }

    //doStrCom does a string comparison based on the value and operator provided 
    private boolean doStrCom(String currentVal, OperatorType op) {
        return doOpOnDiff(op,currentVal.toLowerCase().compareTo(compVal));
    }

    // Does comparison on currentvalue with the comparison value
    public boolean chkCondt(String currentVal) {
        OperatorType operation = getOperation();

        // checks and handles null
        if (isNull(currentVal) || isNull(compVal)) 
            return doOpOnDiff(op, compare(currentVal, compVal, DataTypes.NULL));

        if (dt == DataTypes.TEXT || dt == DataTypes.NULL)
            return doStrCom(currentVal, op);
        else {
            if(op == OperatorType.LESSTHANOREQUAL) {
                return Long.parseLong(currentVal) <= Long.parseLong(compVal);
            } else if (op == OperatorType.GREATERTHANOREQUAL) {
                return Long.parseLong(currentVal) >= Long.parseLong(compVal);
            } else if (op == OperatorType.NOTEQUAL) {
                return Long.parseLong(currentVal) != Long.parseLong(compVal);
            } else if (op == OperatorType.LESSTHAN){
                return Long.parseLong(currentVal) < Long.parseLong(compVal);
            } else if (op == OperatorType.GREATERTHAN) {
                return Long.parseLong(currentVal) > Long.parseLong(compVal);
            } else if (op == OperatorType.EQUALTO) {
                return Long.parseLong(currentVal) == Long.parseLong(compVal);
            } else {
                return false;
            }
        }
    }

    // Checks if a string is null
    private boolean isNull(String val){
        return val == null || val.toLowerCase().equals("null");
    }

    //setConditionValue is the setter to set the class variables replacing the filler characters
    public void setConditionValue(String condVal) {
        this.compVal = condVal;
        this.compVal = compVal.replace("'", "");
        this.compVal = compVal.replace("\"", "");
    }

    //Setter to set the colHeader
    public void setColumName(String colName) {
        this.colHeader = colName;
    }

    //Setter to set the operator 
    public void setOp(String op) {
        this.op = getOpType(op);
    }

    //Sets the negation to the boolean negation provided
    public void setNegation(boolean negate) {
        this.neg = negate;
    }

    //Getter to get the operation type based on the negation provided
    public OperatorType getOperation() {
        if (!neg)
            return this.op;
        else
            return negateOperator();
    }

    // In case of NOT operator, invert the operator
    private OperatorType negateOperator() {
        if (this.op == OperatorType.GREATERTHAN) {
            return OperatorType.LESSTHANOREQUAL;
        } else if (this.op == OperatorType.LESSTHAN) {
            return OperatorType.GREATERTHANOREQUAL;
        } else if (this.op == OperatorType.EQUALTO) {
            return OperatorType.NOTEQUAL;
        } else if (this.op == OperatorType.NOTEQUAL) {
            return OperatorType.EQUALTO;
        } else if (this.op == OperatorType.GREATERTHANOREQUAL) {
            return OperatorType.LESSTHAN;
        } else if (this.op == OperatorType.LESSTHANOREQUAL) {
            return OperatorType.GREATERTHAN;
        } else {
            System.out.println("! Operator \"" + this.op + "\" is invalid");
            return OperatorType.INVALID;
        }
    }
}
