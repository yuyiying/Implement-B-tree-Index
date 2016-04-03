#include "BTreeNode.h"


using namespace std;

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
    RC rc;
    if (pid < 0 || pid >= pf.endPid())
        return RC_INVALID_PID;
    if ((rc = pf.read(pid, buffer)) < 0)
        return rc;
    return 0;
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{
    RC rc;
    if (pid < 0)
        return RC_INVALID_PID;
    int flag = 0;
    memcpy(buffer+1016,&flag,4);
    if((rc=pf.write(pid, buffer))<0)
        return rc;
    return 0;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
    int count;
    memcpy(&count, buffer, sizeof(int));
    return count;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
    int num;
    num = getKeyCount();
    RC rc = -1;
    if(num >= leaftotal)
        return rc;
    int i;
    for(i = 0; i < num; i++)
    {
        int cur_key;
        memcpy(&cur_key,buffer+sizeof(int)+sizeof(RecordId)+(sizeof(key)+sizeof(RecordId))*i,sizeof(int));
        if(cur_key > key)
            break;
    }
    memmove(buffer+sizeof(int)+(sizeof(key)+sizeof(RecordId))*(i+1),buffer+sizeof(int)
            +(sizeof(key)+sizeof(RecordId))*i,(sizeof(RecordId)+sizeof(int))*(num-i));
    memcpy(buffer+sizeof(int)+(sizeof(key)+sizeof(RecordId))*i,&rid,sizeof(RecordId));
    memcpy(buffer+sizeof(int)+sizeof(RecordId)+(sizeof(key)+sizeof(RecordId))*i,&key,sizeof(int));
    
    num +=1;
    memcpy(buffer,&num,sizeof(int));
    return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{
    int total;
    total = (sizeof(buffer)-sizeof(int))/(sizeof(RecordId)+sizeof(int));
    int num;
    num = getKeyCount();
    RC rc = -1;
    if(num < leaftotal)
        return rc;
    char tempNode[PageFile::PAGE_SIZE+sizeof(RecordId)+sizeof(int)];
    memcpy(tempNode,buffer,sizeof(buffer));
    int i;
    for(i = 0; i < num; i++)
    {
        int cur_key;
        memcpy(&cur_key,tempNode+sizeof(int)+sizeof(RecordId)+(sizeof(key)+sizeof(RecordId))*i,sizeof(int));
        if(cur_key > key)
            break;
    }
    memmove(tempNode+sizeof(int)+(sizeof(key)+sizeof(RecordId))*(i+1),tempNode+sizeof(int)+(sizeof(key)+sizeof(RecordId))*i,(sizeof(RecordId)+sizeof(int))*(num-i));
    memcpy(tempNode+sizeof(int)+(sizeof(key)+sizeof(RecordId))*i,&rid,sizeof(RecordId));
    memcpy(tempNode+sizeof(int)+sizeof(RecordId)+(sizeof(key)+sizeof(RecordId))*i,&key,sizeof(int));
    
    num +=1;
    int num_left= (num/2)+1;
    int num_right = num-num_left;
    memset(buffer,0,sizeof(buffer));
    memcpy(buffer,&num_left,sizeof(int));
    memcpy(buffer+sizeof(int),tempNode+sizeof(int),(sizeof(RecordId)+sizeof(int))*num_left);
    
    memcpy(sibling.buffer,&num_right,sizeof(int));
    memcpy(sibling.buffer+sizeof(int),tempNode+sizeof(int)+(sizeof(RecordId)+sizeof(int))*num_left,(sizeof(RecordId)+sizeof(int))*num_right);
    
    memcpy(&siblingKey,sibling.buffer+sizeof(int)+sizeof(RecordId),sizeof(int));
    return 0;
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
    int num = getKeyCount();
    int i;
    for(i = 0; i < num; i++)
    {
        int key;
        memcpy(&key,buffer+sizeof(int)+sizeof(RecordId)+(sizeof(key)+sizeof(RecordId))*i,sizeof(int));
        if(key == searchKey)
        {
            eid = i+1;
            return 0;
        }
        else if(key > searchKey)
        {
            eid = i+1;
            return RC_NO_SUCH_RECORD;
        }
    }
    eid = i+1;
    return 0;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
    int num;
    memcpy(&num, buffer, sizeof(int));
    RC rc;
    if(eid < 1 || eid > num)
        return rc;
    memcpy(&key,buffer+sizeof(int)+sizeof(RecordId)+(sizeof(key)+sizeof(RecordId))*(eid-1),sizeof(int));
    memcpy(&rid, buffer+sizeof(int)+(sizeof(RecordId)+sizeof(key))*(eid-1),sizeof(rid));
    return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{
    PageId next_pid;
    memcpy(&next_pid,buffer+sizeof(buffer)-sizeof(PageId),sizeof(PageId));
    return next_pid;
    
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
    memcpy(buffer+sizeof(buffer)-sizeof(PageId),&pid,sizeof(pid));
    return 0;
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
    RC rc;
    if (pid < 0 || pid >= pf.endPid())
        return RC_INVALID_PID;
    if ((rc = pf.read(pid, buffer)) < 0)
        return rc;
    return 0;
 }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
    RC rc;
    if (pid < 0)
        return RC_INVALID_PID;
    int flag = 1;
    memcpy(buffer+1016,&flag,4);
    if((rc=pf.write(pid, buffer))<0)
        return rc;
    return 0;
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
    int count;
    memcpy(&count,buffer,sizeof(int));
    return count;
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
    
    int num;
    num = getKeyCount();
    RC rc=-1;
    if(num >= nonleaftotal)
        return rc;
    int i;
    for(i = 0; i< num;i++)
    {
        int cur_key;
        memcpy(&cur_key,buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i,sizeof(int));
        if(cur_key > key)
            break;
    }
    memmove(buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*(i+1),buffer+sizeof(int)+sizeof(PageId)+(sizeof(PageId)+sizeof(int))*i,(sizeof(int)+sizeof(PageId))*(num-i));
    memcpy(buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i,&key,sizeof(int));
    memcpy(buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i+sizeof(int),&pid,sizeof(PageId));
    
    num += 1;
    memcpy(buffer,&num,sizeof(int));
    return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{
    int num;
    num = getKeyCount();
    RC rc=-1;
    if(num < nonleaftotal)
        return rc;
    char tempNode[PageFile::PAGE_SIZE+sizeof(int)+sizeof(PageId)];
    memcpy(tempNode,buffer,PageFile::PAGE_SIZE);
    int i;
    for(i = 0; i < num; i++)
    {
        int cur_key;
        memcpy(&cur_key,tempNode+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i,sizeof(int));
        if(cur_key > key)
            break;
    }
    memmove(tempNode+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*(i+1),buffer+sizeof(int)+sizeof(PageId)+(sizeof(PageId)+sizeof(int))*i,(sizeof(int)+sizeof(PageId))*(num-i));
    memcpy(tempNode+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i,&key,sizeof(int));
    memcpy(tempNode+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i+sizeof(int),&pid,sizeof(PageId));
    
    num += 1;
    int num_left = num/2;
    int num_right = num -num_left-1;
    memset(buffer,0,PageFile::PAGE_SIZE);
    memcpy(buffer,&num_left,sizeof(int));
    memcpy(buffer+sizeof(int),tempNode+sizeof(int),sizeof(PageId)+(sizeof(int)+sizeof(PageId))*num_left);
    
    memcpy(&midKey,tempNode+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*num_left,sizeof(int));
    memcpy(sibling.buffer,&num_right,sizeof(int));
    memcpy(sibling.buffer+sizeof(int),tempNode+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*num_left+sizeof(int),sizeof(PageId)+(sizeof(int)+sizeof(PageId))*num_right);
    return 0;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
    int num;
    num = getKeyCount();
    RC rc=-1;
    if(num < 1)
        return rc;
    for( int i = 0; i< num ; i++)
    {
        int cur_key;
        memcpy(&cur_key,buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i,sizeof(int));
        if(cur_key>searchKey)
        {
            memcpy(&pid,buffer+sizeof(int)+(sizeof(PageId)+sizeof(int))*i,sizeof(PageId));
            break;
        }
        else if(i == num-1)
        {
            memcpy(&pid,buffer+sizeof(int)+(sizeof(PageId)+sizeof(int))*num,sizeof(PageId));
                    
        }
    }
    return 0;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
    int count;
    RC rc=-1;
    count = getKeyCount();
    if(count != 0)
        return rc;
    count = 1;
    memcpy(buffer,&count,sizeof(int));
    memcpy(buffer+sizeof(int),&pid1,sizeof(PageId));
    memcpy(buffer+sizeof(int)+sizeof(PageId),&key,sizeof(PageId));
    memcpy(buffer+sizeof(int)+sizeof(PageId)+sizeof(int),&pid2,sizeof(PageId));
    int flag = 1;
    memcpy(buffer+PageFile::PAGE_SIZE-sizeof(PageId)-sizeof(int), &flag, sizeof(int));
    return 0;
}


RC BTNonLeafNode::readentry(int eid, int& key, PageId& pid)
{
    memcpy(&key, buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*(eid-1), sizeof(int));
    memcpy(&pid, buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*(eid-1)+sizeof(int), sizeof(PageId));
    return 0;
}





