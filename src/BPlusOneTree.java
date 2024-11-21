import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;

//B + 1 tree implementation for traversing table files
public class BPlusOneTree {

    RandomAccessFile bnFile;
    int rtPgNo;
    String tblName;

    public BPlusOneTree(RandomAccessFile file, int rtPgNo, String tblNm) {
        this.bnFile = file;
        this.rtPgNo = rtPgNo;
        this.tblName = tblNm;
    }

    // This method does a traversal on the B+1 tree and returns the leaf pages in seqquential order
    public List<Integer> getAllLeaves() throws IOException {

        List<Integer> leafPgs = new ArrayList<>();
        bnFile.seek(rtPgNo * DavisBaseBinaryFile.pageSize);
        // if root is a leaf page, read directly and return. No traversal is required
        PageType rtPgType = PageType.get(bnFile.readByte());
        if (rtPgType == PageType.LEAF) {
            if (!leafPgs.contains(rtPgNo))
                leafPgs.add(rtPgNo);
        } else {
            addLeaves(rtPgNo, leafPgs);
        }

        return leafPgs;

    }

    // recursively adds leaves
    private void addLeaves(int interiorPgNo, List<Integer> leafPg) throws IOException {
        Page interiorPage = new Page(bnFile, interiorPgNo);
        for (InteriorRecord leftPage : interiorPage.leftChildren) {
            if (Page.getPageType(bnFile, leftPage.leftChildPageNo) == PageType.LEAF) {
                if (!leafPg.contains(leftPage.leftChildPageNo))
                leafPg.add(leftPage.leftChildPageNo);
            } else {
                addLeaves(leftPage.leftChildPageNo, leafPg);
            }
        }

        if (Page.getPageType(bnFile, interiorPage.rightPage) == PageType.LEAF) {
            if (!leafPg.contains(interiorPage.rightPage))
            leafPg.add(interiorPage.rightPage);
        } else {
            addLeaves(interiorPage.rightPage, leafPg);
        }

    }

    public List<Integer> getAllLeaves(SpecialCondition condition) throws IOException {

        if (condition == null || condition.getOperation() == OperatorType.NOTEQUAL
                || !(new File(TableUtils.getIndexFilePath(tblName, condition.columnName)).exists())) {
            // Since there is no index, use brute force algorithm to trverse through all leaves
            return getAllLeaves();
        } else {

            RandomAccessFile indexFile = new RandomAccessFile(
                    TableUtils.getIndexFilePath(tblName, condition.columnName), "r");
            BTree bTree = new BTree(indexFile);

            // Binary search on the btree
            List<Integer> rowIds = bTree.getRowIds(condition);
            Set<Integer> hash_Set = new HashSet<>();
           
            for (int rowId : rowIds) {
                hash_Set.add(gtPgNo(rowId, new Page(bnFile, rtPgNo)));
            }

            
            System.out.print(" count : " + rowIds.size() + " ---> ");
            for (int rowId : rowIds) {
                System.out.print(" " + rowId + " ");
            }

            System.out.println();
            System.out.println(" leaves: " + hash_Set);
            System.out.println();

            indexFile.close();

            return Arrays.asList(hash_Set.toArray(new Integer[hash_Set.size()]));
        }

    }

    // Returns the page(right most) for inserting new records
    public static int getPgNoForInsert(RandomAccessFile file, int rtPgNo) {
        Page rootPage = new Page(file, rtPgNo);
        if (rootPage.pageType != PageType.LEAF && rootPage.pageType != PageType.LEAFINDEX)
            return getPgNoForInsert(file, rootPage.rightPage);
        else
            return rtPgNo;

    }

    // perform binary search on Bplus one tree and find the rowids
    public int gtPgNo(int rowId, Page page) {
        if (page.pageType == PageType.LEAF)
            return page.pageNo;

        int index = binarySearch(page.leftChildren, rowId, 0, page.noOfCells - 1);

        if (rowId < page.leftChildren.get(index).rowId) {   //Recursion
            return gtPgNo(rowId, new Page(bnFile, page.leftChildren.get(index).leftChildPageNo));
        } else {
        if( index+1 < page.leftChildren.size())
            return gtPgNo(rowId, new Page(bnFile, page.leftChildren.get(index+1).leftChildPageNo));
        else
           return gtPgNo(rowId, new Page(bnFile, page.rightPage));


        }
    }

    private int binarySearch(List<InteriorRecord> vals, int searchVal, int start, int end) {   //Binary search algo

        if(end - start <= 2)
        {
            int i =start;
            for(i=start;i <end;i++){
                if(vals.get(i).rowId < searchVal)
                    continue;
                else
                    break;
            }
            return i;
        }
        else{
            
                int mid = (end - start) / 2 + start;
                if (vals.get(mid).rowId == searchVal)
                    return mid;

                if (vals.get(mid).rowId < searchVal)
                    return binarySearch(vals, searchVal, mid + 1, end);
                else
                    return binarySearch(vals, searchVal, start, mid - 1);
            
        }

    }

}