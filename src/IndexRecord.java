import java.util.List;

/*This file stores the index records of the page 
The header and the cell body so 
Index header has -- rowId (1)| DataTypes of row (int ,long double etc)|Array of index Values|
List of rowIds the index is dependent on |The page header or the cell no | The offset from the 
begining of the page | page no of the left child | right childs pageNO| index node ds to store the structure of 
every index i.e attributes ,and so on*/
public class IndexRecord{
    //Records made private so as to enable security and use getters and setters for sensitive page data
    public Byte numRowIds;
    public DataTypes dtType;
    public Byte[] indValue;
    public List<Integer> rowId;
    public short pgHeaderIndex;
    public short pgOffset;
    int leftPgNo;
    int rightPgNo;
    int pgNo;
    private IndexNode indexNode;

//Constructor adds an index Record based on the query for the attribute provided through the splashTerminal 
    IndexRecord(short pgHdrIndx,DataTypes dTyp,Byte NRowIds, byte[] inxVal, List<Integer> rowId
    ,int lPgNo,int rPgNo,int pNo,short pgOff){
      
        this.pgOffset = pgOff;
        this.pgHeaderIndex = pgHdrIndx;
        this.numRowIds = NRowIds;
        this.dtType = dTyp;
        this.indValue = ByteConvertor.byteToBytes(inxVal);
        this.rowId = rowId;

        indexNode = new IndexNode(new TableAttribute(this.dtType, inxVal),rowId);
        this.leftPgNo = lPgNo;
        this.rightPgNo = rPgNo;
        this.pgNo = pNo;
    }

    //Getter to get the particular index Node
    public IndexNode getIndxNd()
    {
        return indexNode;
    }


}
