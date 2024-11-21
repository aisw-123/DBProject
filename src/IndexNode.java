import java.util.List;

public class IndexNode{
    public TableAttribute indexValue; //index  value of row
    public List<Integer> rowids; // row IDS
    public boolean isInteriorNode; // is the node interior
    public int leftPageNo; // left page no of node

    // parameterized constructor of index node
    public IndexNode(TableAttribute indexVal,List<Integer> rowIds)
    {
        this.indexValue = indexVal;
        this.rowids = rowIds;
    }

}