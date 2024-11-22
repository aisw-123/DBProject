public class InteriorRecord
{
    public int rId; //row id 
    public int leftChildPgNo; // left child page number

    //constructor to find record of table when class variable declared
    public InteriorRecord(int rid, int lChildno){
        this.rId = rid;
        this.leftChildPgNo = lChildno;  
    }
}
