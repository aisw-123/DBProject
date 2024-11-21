import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BTree {
    Page root;
    RandomAccessFile bFile;

    public BTree(RandomAccessFile file) {
        this.bFile = file;
        this.root = new Page(bFile, DavisBaseBinaryFile.getRootPage(bFile));
    }

    /**
     * This method does binary search recursively using the given value and find the right pageNo to insert the index value
     * @param pg This is the page for which a nearest page number is to be found
     * @param val The index value of the page pg
     */
    private int getNearestPageNo(Page pg, String val) {
        if (pg.pageType == PageType.LEAFINDEX) {
            return pg.pageNo;
        } else {
            if (SpecialCondition.compare(val , pg.getIdxVals().get(0),pg.indexValueDataType) < 0)
                return getNearestPageNo
                    (new Page(bFile,pg.indexValuePointer.get(pg.getIdxVals().get(0)).leftPageNo),
                        val);
            else if(SpecialCondition.compare(val,pg.getIdxVals().get(pg.getIdxVals().size()-1),pg.indexValueDataType) > 0)
                return getNearestPageNo(
                    new Page(bFile,pg.rightPage),
                        val);
            else{
                //perform binary search 
                String closestValue = binarySearch(pg.getIdxVals().toArray(new String[pg.getIdxVals().size()]),val,0,pg.getIdxVals().size() -1,pg.indexValueDataType);
                int i = pg.getIdxVals().indexOf(closestValue);
                List<String> indexValues = pg.getIdxVals();
                if(closestValue.compareTo(val) < 0 && i+1 < indexValues.size())
                {
                    return pg.indexValuePointer.get(indexValues.get(i+1)).leftPageNo;
                }
                else if(closestValue.compareTo(val) > 0)
                {
                    return pg.indexValuePointer.get(closestValue).leftPageNo;
                }
                else{
                    return pg.pageNo;
                }
            }
        }
    }

    /**
     * This method is used to get the row Ids for the left of given node
     * @param pageNo
     * @param idxVal
     * @return rowIds
     */
    private List<Integer> getRowIdsLeftOf(int pageNo, String idxVal)
    {
        List<Integer> rowIds = new ArrayList<>();
        if(pageNo == -1)
            return rowIds;
        Page page = new Page(this.bFile,pageNo);
        List<String> indexValues = Arrays.asList(page.getIdxVals().toArray(new String[page.getIdxVals().size()]));

        for(int i=0;i< indexValues.size() && SpecialCondition.compare(indexValues.get(i), idxVal, page.indexValueDataType) < 0 ;i++)
        {
            rowIds.addAll(page.indexValuePointer.get(indexValues.get(i)).getIndxNd().rowids);
            addChildRowIds(page.indexValuePointer.get(indexValues.get(i)).leftPageNo, rowIds);
        }

        if(page.indexValuePointer.get(idxVal)!= null)
            addChildRowIds(page.indexValuePointer.get(idxVal).leftPageNo, rowIds);

        return rowIds;
    }

    /**
     * This method is used to get the row Ids which are satisfying a given condition
     * @param condition
     * @return rowIds
     */
    public List<Integer> getRowIds(SpecialCondition condition)
    {
        List<Integer> rowIds = new ArrayList<>();

        //get to the closest page number satisfying the condition
        Page page = new Page(bFile,getNearestPageNo(root, condition.comparisonValue));
    
        //get the index values for that page
        String[] indexValues= page.getIdxVals().toArray(new String[page.getIdxVals().size()]);
        
        OperatorType operationType = condition.getOperation();
        
        //store the rowids if the indexvalue is equal to the closest value
        for(int i=0;i < indexValues.length;i++)
        {
            if(condition.chkCondt(page.indexValuePointer.get(indexValues[i]).getIndxNd().indexValue.fieldValue))
                rowIds.addAll(page.indexValuePointer.get(indexValues[i]).rowIds);
        }    

        //to store all the rowids from the left side of the node recursivesly
        if(operationType == OperatorType.LESSTHAN || operationType == OperatorType.LESSTHANOREQUAL)
        {
           if(page.pageType == PageType.LEAFINDEX)
               rowIds.addAll(getRowIdsLeftOf(page.parentPageNo,indexValues[0]));
           else 
                rowIds.addAll(getRowIdsLeftOf(page.pageNo,condition.comparisonValue));
        }

         //to store all the rowids from the right side of the node recursively
        if(operationType == OperatorType.GREATERTHAN || operationType == OperatorType.GREATERTHANOREQUAL)
        {
         if(page.pageType == PageType.LEAFINDEX)
            rowIds.addAll(getRowIdsRightOf(page.parentPageNo,indexValues[indexValues.length - 1]));
            else 
              rowIds.addAll(getRowIdsRightOf(page.pageNo,condition.comparisonValue));
        }
        return rowIds;
    }

    /**
     * This method is used to get the rowids that are right to given node
     * @param pgNo
     * @param idxVal
     * @return rowIds
     */
    private List<Integer> getRowIdsRightOf(int pgNo, String idxVal)
    {
        List<Integer> rowIds = new ArrayList<>();

        if(pgNo == -1)
            return rowIds;
        Page page = new Page(this.bFile,pgNo);
        List<String> indexValues = Arrays.asList(page.getIdxVals().toArray(new String[page.getIdxVals().size()]));
        for(int i=indexValues.size() - 1; i >= 0 && SpecialCondition.compare(indexValues.get(i), idxVal, page.indexValueDataType) > 0; i--)
        {
               rowIds.addAll(page.indexValuePointer.get(indexValues.get(i)).getIndxNd().rowids);
                addChildRowIds(page.rightPage, rowIds);
         }

        if(page.indexValuePointer.get(idxVal)!= null)
           addChildRowIds(page.indexValuePointer.get(idxVal).rightPageNo, rowIds);

        return rowIds;
    }

    /**
     * This method is used to add childRows for a given pageNo
     * @param pageNo
     * @param rowId
     */
    private void addChildRowIds(int pageNo,List<Integer> rowId)
    {
        if(pageNo == -1)
            return;
        Page page = new Page(this.bFile, pageNo);
            for (IndexRecord record :page.indexValuePointer.values())
            {
                rowId.addAll(record.rowIds);
                if(page.pageType == PageType.INTERIORINDEX)
                 {
                    addChildRowIds(record.leftPageNo, rowId);
                    addChildRowIds(record.rightPageNo, rowId);
                 }
            }  
    }

    /**
     * Inserts index value into the index page
     * @param attr
     * @param rowId
     */
    public void insertRow(TableAttribute attr,int rowId)
    {
        insertRow(attr,Arrays.asList(rowId));
    }

    /**
     * This method is used to insert rows for a given list of rowIds and attribute into the index page
     * @param attr
     * @param rowId
     */
    public void insertRow(TableAttribute attr,List<Integer> rowId)
    {
        try{
            int pageNo = getNearestPageNo(root, attr.fieldValue) ;
            Page page = new Page(bFile, pageNo);
            page.addIdx(new IndexNode(attr,rowId));
            }
            catch(IOException e)
            {
                 System.out.println("! Error while insering " + attr.fieldValue +" into index file");
            }
    }

    /**
     * This method is used to delete a row
     * @param attr
     * @param rowId
     */
    public void deleteRow(TableAttribute attr, int rowId)
    {
        try{
            int pageNo = getNearestPageNo(root, attr.fieldValue) ;
            Page page = new Page(bFile, pageNo);
            IndexNode tempNode = page.indexValuePointer.get(attr.fieldValue).getIndxNd();
            //remove the rowid from the index value
            tempNode.rowids.remove(tempNode.rowids.indexOf(rowId));
            page.deleteIdx(tempNode);
            if(tempNode.rowids.size() !=0)
               page.addIdx(tempNode);

            }
            catch(IOException e)
            {
                 System.out.println("! Error while deleting " + attr.fieldValue +" from index file");
            }
    }

    /**
     * This method is used to search for a value among the given string array using binary search
     * @param vals
     * @param searchVal
     * @param start
     * @param end
     * @param dType
     * @return
     */
    private String binarySearch(String[] vals,String searchVal,int start, int end , DataTypes dType)
    {
        if(end - start <= 3)
        {
            int i =start;
            for(i=start;i <end;i++){
                if(SpecialCondition.compare(vals[i], searchVal, dType) < 0)
                    continue;
                else
                    break;
            }
            return vals[i];
        }
        else{
            
                int mid = (end - start) / 2 + start;
                if(vals[mid].equals(searchVal))
                    return vals[mid];

                    if(SpecialCondition.compare(vals[mid], searchVal, dType) < 0)
                    return binarySearch(vals,searchVal,mid + 1,end,dType);
                else 
                    return binarySearch(vals,searchVal,start,mid - 1,dType);
            
        }
    }
}