/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 * 
 * CS564 PP3
 * Shihan Cheng   Student ID: 9079915733
 * Yimiao Cao     Student ID: 9080624373
 * Jiayuan Huang  Student ID: 9075663824
 * 
 * Description: Implement a buffer manager as a B+ tree constructor, provides scanning, insertion, removing function.
 * 				The tree is with a given leaf size which contains sepctific number of keys in a node.
 * 				Once the leaf is full, slipt the leaf and create a higher level of non-leaf node as the index pointer poiting
 * 				to the leaf nodes.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include <algorithm>
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_pinned_exception.h"

//#define DEBUG
using namespace std;

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
/**
 * BTreeIndex Constructor. 
 * Check to see if the corresponding index file exists. If so, open the file.
 * If not, create it and insert entries for every tuple in the base relation using FileScan class.
 *
 * @param relationName        Name of file.
 * @param outIndexName        Return the name of index file.
 * @param bufMgrIn						Buffer Manager Instance
 * @param attrByteOffset			Offset of attribute, over which index is to be built, in the record
 * @param attrType						Datatype of attribute over which index is built
 * @throws  BadIndexInfoException     If the index file already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.
 **/

BTreeIndex::BTreeIndex(const std::string &relationName,
					   std::string &outIndexName,
					   BufMgr *bufMgrIn,
					   const int attrByteOffset,
					   const Datatype attrType)
{
	// Construct from the global
	this->bufMgr = bufMgrIn;
	this->attrByteOffset = attrByteOffset;
	this->attributeType = attrType;

	leafOccupancy = INTARRAYLEAFSIZE;	 // Number of key slots in B+Tree leaf for INTEGER key.
	nodeOccupancy = INTARRAYNONLEAFSIZE; // Number of key slots in B+Tree non-leaf for INTEGER key.

	scanExecuting = false; // initialize the scan variable

	// Retrieve the Index Name
	std::ostringstream idxStr;
	idxStr << relationName << "." << attrByteOffset;
	outIndexName = idxStr.str();

	IndexMetaInfo *metadata;
	Page *headerPage;
	Page *rootPage;
	try
	{
		// if file exists, open the file
		file = new BlobFile(outIndexName, false);
		// read meta info, it should be the first page
		headerPageNum = file->getFirstPageNo();
		// read file by calling the read page in the buffer manager
		bufMgr->readPage(file, headerPageNum, headerPage);
		// Metadata of the header page
		metadata = (IndexMetaInfo *)headerPage;
		rootPageNum = metadata->rootPageNo;

		// check if index info matches
		if (strcmp(metadata->relationName, relationName.c_str()) != 0 || attrType != metadata->attrType || metadata->attrByteOffset != attrByteOffset)
		{
			throw BadIndexInfoException(outIndexName);
		}
		bufMgr->unPinPage(file, headerPageNum, false);
	}
	// create a new file if not found
	catch (FileNotFoundException e)
	{
		file = new BlobFile(outIndexName, true);
		// allocate root and header page
		bufMgr->allocPage(file, headerPageNum, headerPage);
		bufMgr->allocPage(file, rootPageNum, rootPage);

		// fill in meta info
		metadata = (IndexMetaInfo *)headerPage;
		strncpy((char *)(&(metadata->relationName)), relationName.c_str(), 20);
		metadata->relationName[19] = 0;
		metadata->attrByteOffset = attrByteOffset;
		metadata->attrType = attrType;
		metadata->rootPageNo = rootPageNum;

		// initialize root
		initialRootPageNum = rootPageNum;
		LeafNodeInt *root = (LeafNodeInt *)rootPage;
		root->rightSibPageNo = 0;
		// Unpin as soon as we can
		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, true);
		
		try
		{
			FileScan fscan(relationName, bufMgr);			
			RecordId scanRid;
			while (1)
			{
				fscan.scanNext(scanRid);
				std::string recordStr = fscan.getRecord(); // store the record in a string
				const char *record = recordStr.c_str();
				int key = *((int *)(record + attrByteOffset));
				insertEntry(&key, scanRid);
			}
		} catch (EndOfFileException e) {
				// save btree index file to disk
				bufMgr->flushFile(file);
			
		}
		
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------
/**
 * BTreeIndex Destructor. 
 * End any initialized scan, flush index file, after unpinning any pinned pages, from the buffer manager
 * and delete file instance thereby closing the index file.
 * Destructor should not throw any exceptions. All exceptions should be caught in here itself. 
 **/
BTreeIndex::~BTreeIndex()
{
	// reset state variable and unpin the B+ tree pages that are pinned
	if (scanExecuting)
		endScan();
	bufMgr->flushFile(BTreeIndex ::file);
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------
/**
 * Insert a new entry using the pair <value,rid>. 
 * Start from root to recursively find out the leaf to insert the entry in. The insertion may cause splitting of leaf node.
 * This splitting will require addition of new leaf page number entry into the parent non-leaf, which may in-turn get split.
 * This may continue all the way upto the root causing the root to get split. If root gets split, metapage needs to be changed accordingly.
 * Make sure to unpin pages as soon as you can.
 * @param key			Key to insert, pointer to integer/double/char string
 * @param rid			Record ID of a record whose entry is getting inserted into the index.
 **/
const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
	// entry setup
	RIDKeyPair<int> entry;
	entry.set(rid, *((int *)key));
	// root
	Page *root;
	// read rootPageNum
	bufMgr->readPage(file, rootPageNum, root);
	// new child entry setup
	PageKeyPair<int> *newchildEntry = nullptr;
	// once we have the root node, we can traverse down the tree
	insertion(root, rootPageNum, entry, newchildEntry, initialRootPageNum == rootPageNum ? true : false);
}

/**
  * Recursively perform insertion with different cases
  * 
  * @param current             The current Page given
  * @param curPageNum          The current PageID given
  * @param dataEntry           Index entry that needs to be inserted
  * @param newEntry            The newEntry which is a page key pair pushed up to after splitting
  * @param isLeaf              whether the current page is a leaf node or not
  **/
const void BTreeIndex::insertion(Page *current, PageId currPageNum, const RIDKeyPair<int> entry, PageKeyPair<int> *&newEntry, bool isLeaf)
{
	// base case: leaf node
	if (isLeaf)
	{
		LeafNodeInt *node = (LeafNodeInt *)current;
		// if not full, directly insert the key and record id to the node
		if (node->ridArray[leafOccupancy - 1].page_number == 0)
		{
			insertToLeafNode(node, entry);
			//unpin as soon as we can
			bufMgr->unPinPage(file, currPageNum, true);
			newEntry = nullptr;
		}
		else
		{
			// redistribute entries evenly
			splitLeaf(node, currPageNum, newEntry, entry);
		}
	}
	else
	{
		// non leaf node case
		NonLeafNodeInt *node = (NonLeafNodeInt *)current;
		// keep track of the next page
		Page *nextPage;
		PageId nextNode;

		findNextNonLeafNode(node, nextNode, entry.key);
		bufMgr->readPage(file, nextNode, nextPage);
		// if the level of current node is just above the leafnode
		if (node->level == 1)
		{
			isLeaf = 1;
		}
		// recursively insert the element
		insertion(nextPage, nextNode, entry, newEntry, isLeaf);

		if (newEntry == nullptr)
		{
			bufMgr->unPinPage(file, currPageNum, false);
			// if current node is not full, insertToNonLeafNode
		}
		else if (node->pageNoArray[nodeOccupancy] == 0)
		{
			insertToNonLeafNode(node, newEntry);
			newEntry = nullptr;
			bufMgr->unPinPage(file, currPageNum, true);
			// if current node is full, we need to split it immediately
		}
		else if (node->pageNoArray[nodeOccupancy] != 0)
		{
			// redistribute entries evenly, but pushing up the middle key
			splitNonLeaf(node, currPageNum, newEntry);
		}
	}
}

/**
 * Helper to split a leaf node
 * 
 * @param node                the original given we split from
 * @param pageId              the pageID of the splitting leaf
 * @param newEntry            the new entry which is the page key pair pushed up to after splitting
 * @param entry               the data entry given to perform insertion
 **/

const void BTreeIndex::splitLeaf(LeafNodeInt *node, PageId leafPageId, PageKeyPair<int> *&newEntry,
								 const RIDKeyPair<int> entry)
{
	Page *newPage;
	PageId newPageNum;
	bufMgr->allocPage(file, newPageNum, newPage);
	LeafNodeInt *newNode = (LeafNodeInt *)newPage;

	// set up the midpoint and adjust it if LeafOccupancy is odd
	int midPt = leafOccupancy / 2 + leafOccupancy % 2;;
	
	// copy elements from old node to new node
	const size_t len = leafOccupancy - midPt;
	memcpy(&newNode->keyArray, &node->keyArray[midPt], len * sizeof(int));
	memcpy(&newNode->ridArray, &node->ridArray[midPt], len * sizeof(RecordId));

	// remove elements from old node
	memset(&node->keyArray[midPt], 0, len * sizeof(int));
	memset(&node->ridArray[midPt], 0, len * sizeof(RecordId));
  
	// Perform leaf insertion
	if (entry.key > node->keyArray[midPt - 1])
	{
		// right side
		insertToLeafNode(newNode, entry);
	}
	else
	{
		// left side
		insertToLeafNode(node, entry);
	}

	// set the next page id
  	newNode->rightSibPageNo = node->rightSibPageNo;
	node->rightSibPageNo = newPageNum;
   
	// copy the middle key after insertion
	newEntry = new PageKeyPair<int>();
	PageKeyPair<int> newKeyPair;
	// set the middle key to the key we want to move up
	newKeyPair.set(newPageNum, newNode->keyArray[0]);
	newEntry = &newKeyPair;
	// write the page back
	bufMgr->unPinPage(file, leafPageId, true);
	bufMgr->unPinPage(file, newPageNum, true);

	// if the current page is root
	if (leafPageId == rootPageNum)
	{
		moveKeyUp(leafPageId, newEntry);
	}
}
/**
 * Helper to split a non-leaf node
 * 
 * @param node                the original given we split from
 * @param pageId              the pageID of the splitting non-leaf
 * @param newEntry            the new entry which is the page key pair pushed up to after splitting
 **/
const void BTreeIndex::splitNonLeaf(NonLeafNodeInt *node, PageId pageId, PageKeyPair<int> *&newEntry) 
{
	// Allocate a new leaf page
	Page *newPage;
	PageId newPageId;
	bufMgr->allocPage(file, newPageId, newPage);
	NonLeafNodeInt *newNode = (NonLeafNodeInt *) newPage;
	
	// set up the midpoint
	int midPt = nodeOccupancy / 2;
	int pushIndex = midPt;
	PageKeyPair<int> pushEntry;
	
	// copy key from old node to pushupKey
	if (nodeOccupancy % 2 == 0) {
        pushIndex = newEntry->key < node->keyArray[midPt] ? midPt - 1 : midPt;
	}
    pushEntry.set(newPageId, node->keyArray[pushIndex]);
	// copy values from old node to new node
	// from key after middle one
	
	midPt = pushIndex + 1;
	size_t len = nodeOccupancy - midPt;
	memcpy(&newNode->pageNoArray, &node->pageNoArray[midPt+1],(len - 1) * sizeof(PageId));
	memcpy(&newNode->keyArray, &node->keyArray[midPt],len * sizeof(int));
	
	// remove elements from old node
 	memset(&node->keyArray[midPt + 1], 0, (len - 1) * sizeof(int));
  	memset(&node->pageNoArray[midPt + 1], 0, (len - 1) * sizeof(PageId));

	// Back to the original node for performing insertion
    newNode->level = node->level;
    node->keyArray[pushIndex] = 0;
    node->pageNoArray[pushIndex] = (PageId) 0;
    if(newEntry->key < newNode->keyArray[0])
        insertToNonLeafNode(node, newEntry);
    else
        insertToNonLeafNode( newNode, newEntry);

    // Updating root after insertion
    newEntry = &pushEntry;
    bufMgr->unPinPage(file, pageId, true);
    bufMgr->unPinPage(file, newPageId, true);
    if (pageId == rootPageNum) {
        moveKeyUp(pageId, newEntry);
    }
	
}

/**
 * Create a new root, insert the new entry and update the new header page
 * 
 * @param firstPageInRoot   The pageId of the first pointer in the root page
 * @param newchildEntry     The keyPair that is pushed up after splitting
 **/
const void BTreeIndex::moveKeyUp(PageId firstPid, PageKeyPair<int> *newEntry) 
{
	// Alloc a new page for root
        PageId newRootPageId;
        Page *root;
        bufMgr->allocPage(file, newRootPageId, root);
        NonLeafNodeInt *newRoot = (NonLeafNodeInt *) root;

        // Set up the key and page numbers
        newRoot->level = initialRootPageNum == rootPageNum ? 1 : 0;
        newRoot->pageNoArray[0] = firstPid;
        newRoot->pageNoArray[1] = newEntry->pageNo;
        newRoot->keyArray[0] = newEntry->key;

        // Updating the index meta infromation
        Page *metaData;
        bufMgr->readPage(file, headerPageNum, metaData);
        IndexMetaInfo *metaPage = (IndexMetaInfo *) metaData;
        metaPage->rootPageNo = newRootPageId;
        rootPageNum = newRootPageId;

        // Unpin unused page
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, newRootPageId, true);
}

/**
 * Return the appropriate index of records in the internal node
 * 
 * @param node an internal node
 * @return int 
 */
int BTreeIndex::findNonLeafLen(NonLeafNodeInt *node) {
	//static auto comp = [](const PageId &p1,const PageId &p2) { return p1 >= p2; };
	//if the child page is null, go left
	int j = nodeOccupancy;
	while (j >= 0 && (node->pageNoArray[j] == 0))
	{
		j--;
	}
	// PageId *start = node->pageNoArray;
  	// PageId *end = &node->pageNoArray[INTARRAYNONLEAFSIZE];
	return j;
}

/**
 * Return the apropriate index of records in the leaf node
 * 
 * @param node leafnode
 * @return int 
 */
int BTreeIndex::findLeafLen(LeafNodeInt *node) {
	
 	int i = leafOccupancy - 1;
    //find the end
    while(i >= 0 && (node->ridArray[i].page_number == 0))
    {
      i--;
    }
	
	return i;
}

/**
 * Return the index of the first integer that is large than the given key
 * 
 * @param node  	an internal node
 * @param index 	the target index
 * @param key 		the target key
 * @return int 
 */
int BTreeIndex::findArrayIndex(NonLeafNodeInt *node, int index, int key) {
  // compare the key values in the node with the parameter one, if greater go left
	while (index > 0 && (node->keyArray[index - 1] >= key))
	{
		index--;
	}
	return index;

}


/**
 * To find pageID of node that the key value should be at the next level
 * @param node                the given current node
 * @param nextNodeNum         value for the pageID at the next level
 * @param val                 the given key value
 **/
const void BTreeIndex::findNextNonLeafNode(NonLeafNodeInt *node, PageId &nextNodeNum, int val)
{
	
	int j = findNonLeafLen(node);
	j = findArrayIndex(node, j, val);
	// return the pageID
	nextNodeNum = node->pageNoArray[j];
}

/**
 * Insert the given recordId keypair intot the leaf node 
 * 
 * @param node               The leaf node needed to be inserted
 * @param entry              The entry of the record ID key pair given for insertion
 * 
 **/
const void BTreeIndex::insertToLeafNode(LeafNodeInt *node, RIDKeyPair<int> entry)
{
	// if the leaf page is empty. set the key and record id
	if (node->ridArray[0].page_number == 0)
	{
		node->ridArray[0] = entry.rid;
		node->keyArray[0] = entry.key;
	}
	else
	{
	int i = findLeafLen(node);
	
    while (i >= 0 && (node->keyArray[i] > entry.key)) {
      i--;
    }
    int len = leafOccupancy - i - 1;
		//shift items to add space for the new element
		memmove(&node->keyArray[i + 1], &node->keyArray[i], len * sizeof(int));
		memmove(&node->ridArray[i + 1], &node->ridArray[i], len * sizeof(RecordId));

		// save the key and record id to the leaf node
		node->keyArray[i + 1] = entry.key;
		node->ridArray[i + 1] = entry.rid;
	}
}

/**
 * Helper to insert entry into a non leaf node
 * @param node                Nonleaf node that need to be inserted into
 * @param PageId              the pageID of the non-leaf
 * @param entry               Then entry needed to be inserted
 **/
const void BTreeIndex::insertToNonLeafNode(NonLeafNodeInt *node, PageKeyPair<int> *newEntry)
{
	
	int i = findNonLeafLen(node);
	int val = newEntry->key;
	i = findArrayIndex(node, i, val);
	int len = nodeOccupancy - i - 1;
	memmove(&node->keyArray[i], &node->keyArray[i - 1], len * sizeof(int));
	memmove(&node->pageNoArray[i + 1], &node->pageNoArray[i], len * sizeof(PageId));

	// store the key and page number to the node
	node->keyArray[i] = newEntry->key;
	node->pageNoArray[i + 1] = newEntry->pageNo;
}

/**
 * Check if the record ID is satisfied within the range 
 * 
 * @param lowVal             Low value of range, pointer to integer/ double / char string
 * @param lowOp              Low operator(GT/GTE)
 * @param highVal            High value of range, pointer to integer/ double / char string
 * @param highOp             High operator(LT/LTE)
 * @param key                val of the rid
 * @return true 
 * @return false 
 **/
const bool BTreeIndex::checkKeyInRange(int lowVal, const Operator lowOp, int highVal, const Operator highOp, int key)
{
	// If low found inclusively
	if (lowOp == GTE)
	{
		// If high found exclusively
		if (highOp == LT)
		{ // Check if key is in the range with the same case
			return key < highVal && key >= lowVal;
		}
		// If high found inclusively
		else
		{
			// Check if key is in the range with the same case
			return key <= highVal && key >= lowVal;
		}
	}

	// If low found exclusively
	if (lowOp == GT)
	{
		// If high found exclusively
		if (highOp == LT)
		{
			// Check if key is in the range with the same case
			return key > lowVal && key < highVal;
		}
		// If high found inclusively
		else
		{
			// Check if key is in the range with the same case
			return key >= lowVal && key < highVal;
		}
	}
	return false;
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
/**
 * Begin a filtered scan of the index.  For instance, if the method is called 
 * using ("a",GT,"d",LTE) then we should seek all entries with a value 
 * greater than "a" and less than or equal to "d".
 * If another scan is already executing, that needs to be ended here.
 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
 * @param lowVal	Low value of range, pointer to integer / double / char string
 * @param lowOp		Low operator (GT/GTE)
 * @param highVal	High value of range, pointer to integer / double / char string
 * @param highOp	High operator (LT/LTE)
 * @throw BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
 * @throw BadScanrangeException If lowVal > highval
 * @throw NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
 **/
const void BTreeIndex::startScan(const void *lowValParm,
								 const Operator lowOpParm,
								 const void *highValParm,
								 const Operator highOpParm)
{
	// If scanning found, end it
	if (scanExecuting) endScan();

	// Get the low and high value
	lowValInt = *((int *)lowValParm);
	highValInt = *((int *)highValParm);

	// Get the operators
	lowOp = lowOpParm;
	highOp = highOpParm;

	// If the low range is higher than the high range, throw exception
	if (lowValInt > highValInt) throw BadScanrangeException();

	// If the operators are not in the range currently, throw exception
	if (!((lowOp == GT or lowOp == GTE) && (highOp == LT or highOp == LTE)))
	{
		throw BadOpcodesException();
	}

	// Get root's page first
	currentPageNum = rootPageNum;
	// Then read root's page in the buffer pool, start to scan
	bufMgr -> readPage(file, currentPageNum, currentPageData);
	bool found = false;

	// Check if root is leaf
	// If not, we continue to find the leaf
	if (initialRootPageNum != rootPageNum)
	{	
		
		// Intialize a temp to store current page's being scanned
		NonLeafNodeInt * cur = (NonLeafNodeInt *) currentPageData;
		
		while (!found)
		{	
			// Get the page is being scanned
			cur = (NonLeafNodeInt *) currentPageData;
			// Check if current page is at the level above leaf
			// If so, the next level will be leaf
			if (cur -> level == 1) found = true;

			// Get next page
			PageId nextPage;
			findNextNonLeafNode(cur, nextPage, lowValInt);
			// Upin current page
			try
			{
				bufMgr -> unPinPage(file, currentPageNum, false);
				currentPageNum = nextPage;
			}
			catch (PageNotPinnedException& e) 
			{
    		} 
			catch (HashNotFoundException& e) 
			{
    		}
			// Read next page
			bufMgr -> readPage(file, currentPageNum, currentPageData);
		}
	}

	// Reset bool varibale found to continue using
	found = false;
	// We now have the leaf node, then we have to find the entry which is 
	// the smallest one and within the range of OPs
	while (!found)
	{
		// Get the page
		LeafNodeInt * cur = (LeafNodeInt *) currentPageData;
		// Make sure the page is not null
		// Otherwise unpin it and throw exception
		int size = cur -> ridArray[0].page_number;
		if (size == 0)
		{
			try
			{
				bufMgr -> unPinPage(file, currentPageNum, false);
				throw NoSuchKeyFoundException();
			}
			catch (PageNotPinnedException& e) 
			{
    		} 
			catch (HashNotFoundException& e) 
			{
    		}
		}

		// Search the pages from left to right
		// Get the best one
		for (int i = 0; i < leafOccupancy; i++)
		{
			// If current key is in the range，we get it and end the while loop
			if (checkKeyInRange(lowValInt, lowOp, highValInt, highOp, cur -> keyArray[i]))
			{	
				scanExecuting = true;
				found = true;
				nextEntry = i;
				break;
			}
			// Else if current key is above current OPs, unpin the page and throw exception
			else if ((cur -> keyArray[i] >= highValInt && highOp == LT) || (cur -> keyArray[i] > highValInt && highOp == LTE))
			{
				try
				{
					bufMgr -> unPinPage(file, currentPageNum, false);
					throw NoSuchKeyFoundException();
				}
				catch (PageNotPinnedException& e) 
				{
    			} 
				catch (HashNotFoundException& e) 
				{
    			}	
			}

			// Check if cur does not reach the last key and next key is null
			bool getNull = cur -> ridArray[i + 1].page_number == 0 && i < leafOccupancy ? true : false;
			
			// Now check if we reach the last key in this leaf
			if (getNull || i == leafOccupancy - 1)
			{
				// Unpin current page
				try
				{
					bufMgr -> unPinPage(file, currentPageNum, false);
				}
				catch (PageNotPinnedException& e) 
				{
    			} 
				catch (HashNotFoundException& e) 
				{
    			}

				// If current page has right sibling
				// Get its right sibling
				if (cur -> rightSibPageNo != 0)
				{
					currentPageNum = cur -> rightSibPageNo;
					bufMgr -> readPage(file, currentPageNum, currentPageData);
				}
				// Otherwise, throw exception
				else 
				{
					throw NoSuchKeyFoundException();
				}
			}
			// If we reach the last key, quit the for oop
			if (getNull) break;
		}
	}
}

/**
 * Fetch the record id of the next index entry that matches the scan.
 * Return the next record from current page being scanned. If current page has been scanned to its entirety, 
 * move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin 
 * any pages that are no longer required.
 * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
 * @throws ScanNotInitializedException If no scan has been initialized.
 * @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
 **/
const void BTreeIndex::scanNext(RecordId &outRid)
{
	// If no scan has been intitialized, we cannot do scanning for next
	// Throw exception
	if (!scanExecuting) throw ScanNotInitializedException();
	
	// Otherwise, cast to the node
	LeafNodeInt * cur = (LeafNodeInt *) currentPageData;

	// When next entry's index is out of the range which has maximum leafOccupancy - 1
	// or next entry's page_number is 0
	if ((nextEntry == leafOccupancy) || (cur->ridArray[nextEntry].page_number == 0))
	{
		// Unpin the page
		try
		{
			bufMgr->unPinPage(file, currentPageNum, false);
		}
		catch (PageNotPinnedException& e) 
		{
    	} 
		catch (HashNotFoundException& e) 
		{
    	}

		// If current node has no next leaf sibling, throw exeption
		if (cur->rightSibPageNo == 0) throw IndexScanCompletedException();

		// Go to the of next leaf-sibling
		currentPageNum = cur->rightSibPageNo;
		// Read the page
		bufMgr->readPage(file, currentPageNum, currentPageData);
		// Get the data
		cur = (LeafNodeInt *)currentPageData;
		// Reset nextEntry as 0
		nextEntry = 0;
	}

	// If rid is not in range, throw exception
	if (!checkKeyInRange(lowValInt, lowOp, highValInt, highOp, cur->keyArray[nextEntry]))
	{
		throw IndexScanCompletedException();
	}
	// Otherwise we increase next entry
	else
	{
		outRid = cur->ridArray[nextEntry];
		nextEntry += 1;
	}
}

/**
 * This method terminates the current scan and unpins all the pages that have
 * been pinned for the purpose of the scan.
 *
 * It throws ScanNotInitializedException when called before a successful
 * startScan call.
 */
const void BTreeIndex::endScan()
{
	// If no scan has been intitialized
	if (!scanExecuting)
		throw ScanNotInitializedException();

	// Set the status of scanning as false
	scanExecuting = false;
	// Unpin the pages that are currently pinned
    try 
	{
        bufMgr->unPinPage(file, currentPageNum, false);
    } 
	catch (PageNotPinnedException& e) 
	{
    } 
	catch (HashNotFoundException& e) 
	{
    }
}

} // namespace badgerdb
