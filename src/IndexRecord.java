import java.util.List;

/*This file stores the index records of the page 
The header and the cell body so 
Index header has -- rowId (1)| DataTypes of row (int ,long double etc)|Array of index Values|
List of rowIds the index is dependent on |The page header or the cell no | The offset from the 
begining of the page | page no of the left child | right childs pageNO| index node ds to store the structure of 
every index i.e attributes ,and so on*/
public class IndexRecord{
    //Records made private so as to enable security and use getters and setters for sensitive page data
    public Byte noOfRowIds;
    public DataTypes dataType;
    public Byte[] indexValue;
    public List<Integer> rowIds;
    public short pageHeaderIndex;
    public short pageOffset;
    int leftPageNo;
    int rightPageNo;
    int pageNo;
    private IndexNode indexNode;

//Constructor adds an index Record based on the query for the attribute provided through the splashTerminal 
    IndexRecord(short pgHeaderIndx,DataTypes dtType,Byte NoOfRowIds, byte[] indxVal, List<Integer> rowIds
    ,int lftPgNo,int rtPgNo,int pgNo,short pgOffset){
      
        this.pageOffset = pgOffset;
        this.pageHeaderIndex = pgHeaderIndx;
        this.noOfRowIds = NoOfRowIds;
        this.dataType = dtType;
        this.indexValue = ByteConvertor.byteToBytes(indxVal);
        this.rowIds = rowIds;

        indexNode = new IndexNode(new TableAttribute(this.dataType, indxVal),rowIds);
        this.leftPageNo = lftPgNo;
        this.rightPageNo = rtPgNo;
        this.pageNo = pgNo;
    }

    //Getter to get the particular index Node
    public IndexNode getIndxNd()
    {
        return indexNode;
    }


}