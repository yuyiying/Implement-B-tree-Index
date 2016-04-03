/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"



using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
    // pid0存储当前root的pid设为1， pid1的前四位初始化为0
}




/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
    RC rc;
    if((rc=pf.open(indexname, 'r'))<0 && (mode == 'w'))
    {
        close();
        pf.open(indexname, 'w');
        char rootbuffer[PageFile::PAGE_SIZE];
        rootPid = 1;
        treeHeight = 1;
        // pid0存储当前root的pid设为1， treeheight设为1，pid1的前四位初始化为0
        memcpy(rootbuffer, &rootPid, sizeof(PageId));
        memcpy(rootbuffer+sizeof(PageId), &treeHeight, sizeof(int));
        pf.write(0, rootbuffer);
        char firstnode[PageFile::PAGE_SIZE];
        int keycount = 0;
        memcpy(firstnode, &keycount, sizeof(int));
        pf.write(1, firstnode);
    }
    else
    {
        close();
        if((rc=pf.open(indexname,mode))<0) return rc;
        pf.open(indexname, mode);
        char rootpidbuffer[PageFile::PAGE_SIZE];
        pf.read(0, rootpidbuffer);
        memcpy(&rootPid, rootpidbuffer, sizeof(PageId));
        memcpy(&treeHeight, rootpidbuffer+sizeof(PageId), sizeof(int));
    }
    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    pf.close();
    return 0;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
    char buffer[PageFile::PAGE_SIZE];
    pf.read(0, buffer);
    PageId LfEpid = 1;
    if(rootPid == 1)
    {
        BTLeafNode leaf;
        leaf.read(rootPid, pf);
        int num = leaf.getKeyCount();
        if(num < leaf.leaftotal)
        {
            leaf.insert(key, rid);
            leaf.write(rootPid, pf);
            
            memcpy(buffer+sizeof(PageId)+sizeof(int),&LfEpid, sizeof(PageId));
            pf.write(0, buffer);
        }
        else
        {
            BTLeafNode sibling;
            int siblingkey;
            leaf.insertAndSplit(key, rid, sibling, siblingkey);
            PageId next_pid = rootPid+1;
            leaf.setNextNodePtr(next_pid);
            leaf.write(rootPid, pf);
            sibling.write(next_pid, pf);
            
            LfEpid = 2;
            memcpy(buffer+sizeof(PageId)+sizeof(int),&LfEpid, sizeof(PageId));
            
            BTNonLeafNode newroot;
            newroot.initializeRoot(rootPid, siblingkey, next_pid);
            rootPid = pf.endPid();
            newroot.write(rootPid, pf);
            
            memcpy(buffer, &rootPid, sizeof(PageId));
            treeHeight = 2;
            memcpy(buffer+sizeof(PageId), &treeHeight, sizeof(int));
            pf.write(0, buffer);
        }
    }
    else
    {
        PageId traverse[treeHeight];//记录经过的pid
        IndexCursor cursor;
        cursor = recursor(rootPid, key, traverse, 0);

        BTLeafNode leaf;
        leaf.read(cursor.pid, pf);
        int keycount = leaf.getKeyCount();
        if(keycount < leaf.leaftotal)
        {
            leaf.insert(key, rid);
            leaf.write(cursor.pid, pf);
        }
        else
        {
            BTLeafNode sibling;
            int siblingkey;
            PageId next_pid;
            next_pid = leaf.getNextNodePtr();
            leaf.insertAndSplit(key, rid, sibling, siblingkey);
            PageId siblingpid = pf.endPid();
            leaf.setNextNodePtr(siblingpid);
            sibling.setNextNodePtr(next_pid);
            
            if(next_pid == 0)
            {
                LfEpid = siblingpid;
                memcpy(buffer+sizeof(PageId)+sizeof(int),&LfEpid, sizeof(PageId));
                pf.write(0, buffer);
            }
            
            leaf.write(cursor.pid, pf);
            sibling.write(siblingpid, pf);
            
            int level = treeHeight-1;
            Treerecursor(traverse, level,siblingkey, siblingpid);
        }
    }
    return 0;
}

RC BTreeIndex::Treerecursor(PageId traverse[],int level, int siblingkey, PageId siblingpid)
{
    if(level == 1)
    {
        BTNonLeafNode nonleaf;
        nonleaf.read(traverse[0], pf);
        int countkey = nonleaf.getKeyCount();
        if(countkey < nonleaf.nonleaftotal)
        {
            nonleaf.insert(siblingkey,siblingpid);
            nonleaf.write(traverse[0], pf);
        }
        else
        {
            int midkey;
            BTNonLeafNode middle;
            nonleaf.insertAndSplit(siblingkey, siblingpid, middle, midkey);
            nonleaf.write(traverse[0], pf);
            PageId next_pid = pf.endPid();
            middle.write(next_pid, pf);
            BTNonLeafNode newroot;
            newroot.initializeRoot(traverse[0], midkey, next_pid);
            rootPid = next_pid+1;
            newroot.write(rootPid, pf);
            char buffer[PageFile::PAGE_SIZE];
            memcpy(buffer,&rootPid, sizeof(PageId));
            treeHeight++;
            memcpy(buffer+sizeof(PageId), &treeHeight, sizeof(int));
            pf.write(0, buffer);
        }
    }
    else
    {
        BTNonLeafNode nonleaf;
        nonleaf.read(traverse[level-1],pf);
        int countkey = nonleaf.getKeyCount();
        if(countkey<nonleaf.nonleaftotal)
        {
            nonleaf.insert(siblingkey, siblingpid);
            nonleaf.write(traverse[level-1], pf);
        }
        else
        {
            BTNonLeafNode middle;
            int midkey;
            nonleaf.insertAndSplit(siblingkey, siblingpid, middle, midkey);
            nonleaf.write(traverse[level-1], pf);
            PageId midpid = pf.endPid();
            middle.write(midpid,pf);
            level--;
            return Treerecursor(traverse, level, midkey, midpid);
        }
    }
    return 0;
}
/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
    //得到树的高度
    char buffer[PageFile::PAGE_SIZE];
    pf.read(0, buffer);
    memcpy(&treeHeight, buffer+sizeof(PageId), sizeof(int));
    //建立一个pageid的数组
    PageId traverse[treeHeight];
    int num = 0;
    cursor = recursor(rootPid, searchKey,traverse,num);
    int key;
    RecordId rid;
    IndexCursor tempcursor = cursor;
    readForward(tempcursor, key, rid);
    if(key != searchKey)
        return RC_NO_SUCH_RECORD;
    return 0;
}

IndexCursor BTreeIndex::recursor(PageId pid,int searchkey,PageId traverse[],int num)
{
    char buffer[PageFile::PAGE_SIZE];
    IndexCursor cursor;
    pf.read(pid, buffer);
    int flag;
    memcpy(&flag, buffer+1016, 4);
    if(flag == 0)
    {
        BTLeafNode leaf;
        leaf.read(pid, pf);
        int eid;
        leaf.locate(searchkey, eid);
        cursor.eid = eid;
        cursor.pid = pid;
        return cursor;
    }
    else
    {
        BTNonLeafNode nonleaf;
        nonleaf.read(pid,pf);
        traverse[num] = pid;// 记录当前traverse的nonleaf的pid
        PageId childpid;
        nonleaf.locateChildPtr(searchkey, childpid);
        return recursor(childpid,searchkey,traverse,++num);
    }
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
    BTLeafNode leaf;
    leaf.read(cursor.pid, pf);
    leaf.readEntry(cursor.eid,key,rid);
    cursor.eid++;
    return 0;
}

RC BTreeIndex::BTLprint(PageId pid){
    BTLeafNode bl;
    bl.read(pid, pf);
    int key;
    RecordId rid;
    printf("pid:%d, size:%d\n", pid, bl.getKeyCount());
    for (int i=1; i<=bl.getKeyCount(); ++i) {
        bl.readEntry(i, key, rid);
        printf("%d,%d %d\t", key, rid.pid, rid.sid);
    }
    printf("\n");
    return 0;
}

RC BTreeIndex::BTNLprint(PageId pid){
    BTNonLeafNode bn;
    bn.read(pid, pf);
    int key;
    PageId rid;
    printf("pid:%d, size:%d\n", pid, bn.getKeyCount());
    for (int i=1; i<=bn.getKeyCount(); ++i) {
        bn.readentry(i, key, rid);
        printf("%d,%d\t", key, rid);
    }
    printf("\n");
    return 0;
}


PageId BTreeIndex::GetNextpid(PageId pid)
{
    BTLeafNode leafnode;
    leafnode.read(pid, pf);
    PageId next_pid;
    next_pid = leafnode.getNextNodePtr();
    return next_pid;
}

int BTreeIndex::GetKeycount(PageId pid)
{
    BTLeafNode leafnode;
    leafnode.read(pid, pf);
    int keycount;
    keycount = leafnode.getKeyCount();
    return keycount;
}

PageId  BTreeIndex::GetLfEpid()
{
    PageId epid;
    char buffer[1024];
    pf.read(0,buffer);
    memcpy(&epid, buffer+sizeof(PageId)+sizeof(int), sizeof(PageId));
    return epid;
}

