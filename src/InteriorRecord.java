public class InteriorRecord
{
    public int rowId; //row id 
    public int leftChildPageNo; // left child page number

    //constructor to find record of table when class variable declared
    public InteriorRecord(int rowid, int leftChildno){
        this.rowId = rowid;
        this.leftChildPageNo = leftChildno;  
    }
}
