/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgr,
		const int attrByteOffset,
		const Datatype attrType)
{

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

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
 * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values 
 * @throws  BadScanrangeException If lowVal > highval
 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
**/

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	
	lowValInt = *((int *)lowValParm);
	highValInt = *((int *)highValParm);

	if((lowOpParm == GT or lowOpParm == GTE) and (highOpParm == LT or highOpParm == LTE))
	{
		throw BadOpcodesException();
	}
	if(lowValInt > highValInt)
	{
		throw BadScanrangeException();
	}

	lowOp = lowOpParm;
	highOp = highOpParm;

	// Scan is already started
	if(scanExecuting)
	{
		endScan();
	}

	currentPageNum = rootPageNum;
	// Start scanning by reading rootpage into the buffer pool
	bufMgr->readPage(file, currentPageNum, currentPageData);

	// Cast
	NonLeafNodeInt* currentNode = (NonLeafNodeInt *) currentPageData;
	while(currentNode->level == 1)
	{
		// Cast page to node 
		currentNode = (NonLeafNodeInt *) currentPageData;
		// Check the key array in each level
		bool nullVal = false;
		for(int i = 0; i < INTARRAYNONLEAFSIZE && !nullVal; i++) //what if the slot have null value??????
		{
			// Check if the next one in the key is not inserted
			if(i < INTARRAYNONLEAFSIZE - 1 && currentNode->keyArray[i + 1] <= currentNode->keyArray[i])
			{
				nullVal = true;
			}
			// Next page is on the left of the key that is bigger then the value
			if(currentNode->keyArray[i] > lowValInt)
			{
				//unpin unused page
				bufMgr->unPinPage(file, currentPageNum, false);
				currentPageNum = currentNode->pageNoArray[i];
				//read next page into bufferpoll
				bufMgr->readPage(file, currentPageNum, currentPageData);
				break;
			}
			/* If we did not find any key that is bigger then the value, then it is on the right side
			   of the biggest value */
			if(i == INTARRAYNONLEAFSIZE - 1 or nullVal)
			{
				//unpin unused page
				bufMgr->unPinPage(file, currentPageNum, false);
				currentPageNum = currentNode->pageNoArray[i + 1];
				//read next page into bufferpoll
				bufMgr->readPage(file, currentPageNum, currentPageData);
			}
		}
	}

	// Now the curNode is lefe node try to find the smallest one that satisefy the OP
	bool found = false;
	while(!found){
		// Cast page to node
		LeafNodeInt* currentNode = (LeafNodeInt *) currentPageData;
		// Search from the left leaf page to the right to find the fit
		bool nullVal = false;
		for(int i = 0; i < INTARRAYLEAFSIZE and !nullVal; i++)
		{
			int key = currentNode->keyArray[i];
			// Check if the next one in the key is not inserted
			if(i < INTARRAYNONLEAFSIZE - 1 and currentNode->keyArray[i + 1] <= key)
			{
				nullVal = true;
			}

			if(_satisfies(lowValInt, lowOp, highValInt, highOp, key))
			{
				// select
				nextEntry = i;
				found = true;
				scanExecuting = true;
				break;	
			}
			else if((highOp == LT and key >= highValInt) or (highOp == LTE and key > highValInt))
			{
				throw NoSuchKeyFoundException();
			}
			
			// Did not find any matching key in this leaf, go to next leaf
			if(i == INTARRAYLEAFSIZE - 1 or nullVal){
				//unpin page
				bufMgr->unPinPage(file, currentPageNum, false);
				//did not find the matching one in the more right leaf
				if(currentNode->rightSibPageNo == 0)
				{
					throw NoSuchKeyFoundException();
				}
				currentPageNum = currentNode->rightSibPageNo;
				bufMgr->readPage(file, currentPageNum, currentPageData);
			}
		}
	}
}

/**
	* Fetch the record id of the next index entry that matches the scan.
	* Return the next record from current page being scanned. If current page has been scanned to its entirety, move on to the right sibling of current page, if any exists, to start scanning that page. Make sure to unpin any pages that are no longer required.
  * @param outRid	RecordId of next record found that satisfies the scan criteria returned in this
	* @throws ScanNotInitializedException If no scan has been initialized.
	* @throws IndexScanCompletedException If no more records, satisfying the scan criteria, are left to be scanned.
**/
const void BTreeIndex::scanNext(RecordId& outRid) 
{
	if(!scanExecuting)
	{
		throw ScanNotInitializedException();
	}
	// Cast page to node
	LeafNodeInt* currentNode = (LeafNodeInt *) currentPageData;

	// Check  if rid satisfy 
	int key = currentNode->keyArray[nextEntry];
	if(_satisfies(lowValInt, lowOp, highValInt, highOp, key))
	{
		outRid = currentNode->ridArray[nextEntry];
		// Incrment nextEntry
		nextEntry++;
		// If current page has been scanned to its entirety
		if((nextEntry < INTARRAYNONLEAFSIZE - 1 and currentNode->keyArray[nextEntry + 1] <= key) or nextEntry == INTARRAYNONLEAFSIZE)
		{
			//unpin page and read papge
			bufMgr->unPinPage(file, currentPageNum, false);
			//no more next leaf
			if(currentNode->rightSibPageNo == 0)
			{
				throw IndexScanCompletedException();
			}
			currentPageNum = currentNode->rightSibPageNo;
			bufMgr->readPage(file, currentPageNum, currentPageData);
			// Reset nextEntry
			nextEntry = 0;
		}
	}
	else
	{
		throw IndexScanCompletedException();
	}
}

/**
	* Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
	* @throws ScanNotInitializedException If no scan has been initialized.
**/
const void BTreeIndex::endScan() 
{
	if(!scanExecuting)
	{
		throw ScanNotInitializedException();
	}
	scanExecuting = false;
	// Unpin page
	bufMgr->unPinPage(file, currentPageNum, false);
	// Reset variable
	currentPageData = nullptr;
	currentPageNum = -1;
	nextEntry = -1;
}

/**
	* Helper function to check if the key is satisfies
	* @param lowVal	  Low value of range, pointer to integer / double / char string
  * @param lowOp		Low operator (GT/GTE)
  * @param highVal	High value of range, pointer to integer / double / char string
  * @param highOp	  High operator (LT/LTE)
	* @param val      Value of the key
	* @return True if satisfies False if not
	*
**/
const bool BTreeIndex::_satisfies(int lowVal, const Operator lowOp, int highVal, const Operator highOp, int val)
{
	if(lowOp == GTE && highOp == LTE) 
	{
		return val >= lowVal && val <= highVal;
	} 
	else if(lowOp == GT && highOp == LTE) 
	{
		return val > lowVal && val <= highVal;
	} 
	else if(lowOp == GTE && highOp == LT) 
	{
		return val >= lowVal && val < highVal;
	} 
	else
	{
		return val > lowVal && val < highVal;
	}
}

}
