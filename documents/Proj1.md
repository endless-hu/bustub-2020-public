### Parallel Tests For LRU Replacer

#### 1. `ParallelUnpinAndVictimTest1`

##### Test Purpose

1. Repeatedly unpin a same page ID should **NOT** affect the correctness of the replacer;
2. The replacer should be able to handle parallel unpin operations on the **same** page IDs.

##### Test Process

1. Create a set of page IDs that are going to be unpinned;
2. Use 4 threads to iterate through the same set and unpin every page ID in it;
3. Use 4 threads to parallelly get victims from the replacer until the replacer is empty.

##### Expected Results

- After *step 2*, the replacer should be full;
- After *step 3*, the victim page IDs should be the same as the page IDs unpinned in the *step 1*. Their orders are allowed to differ.

#### 2. `ParallelUnpinAndVictimTest2`

##### Test Purpose

The replacer should be able to handle parallel unpin operations on **separate** page IDs.

##### Test Process

1. Create a set of page IDs that are going to be unpinned;
2. Split the page IDs into 4 sets and use 4 threads to call `Unpin`, each for one set;
3. Use 4 threads to parallelly get victims from the replacer until the replacer is empty.

##### Expected Results

- After *step 2*, the replacer should be **full**;
- After *step 3*, the victim page IDs should be **the same as the page IDs unpinned** in the *step 1*. Their orders are allowed to differ.

#### 3. `MixedParallelTest1` & `MixedParallelTest2`

##### Test Purpose

The replacer should be able to handle parallel `Unpin()` **AND `Pin()`** operations on **the same OR separate** page IDs.

##### Test Process

1. Create a set of 100 page IDs that are going to be unpinned;
2. Split the page IDs into 4 sets and use 4 threads to call `Unpin()`, each for one set;
3. Create a set of 40 page IDs that are going to be pinned;
4. Split the page IDs into 3 sets and use 3 threads to call `Pin()`, each for one set;
5. Use 4 threads to parallelly get victims from the replacer until the replacer is empty.

##### Expected Results

- After *step 2*, the replacer should be **full**;
- After *step 4*, there should be **60 pages remaining** in the replacer;
- After *step 5*, the **#victim page IDs** should equal to **60** and these page IDs should **NOT** repeat.

### Parallel Tests For Buffer Pool Manger

#### Test Purpose

The buffer pool manager should correctly perform `FetchPage()`, `UnpinPage()`, `NewPage()`, etc in parallel situation, *especially when there's race conditions.*

#### Test Process

1. Use two threads to parallelly create new pages and write different contents to these pages until the buffer pool is full;
2. Use 3 threads to unpin all pages in the buffer pool manager. Each page will be unpinned three times.
3. Create new pages until full, then unpin one page to leave room for the subsequent `FetchPage()`;
4. Use two threads to fetch pages they previously created, check the contents of these pages and unpin them. Because there's ONLY ONE available frame to fetch and pin pages, the two threads will race at the frame and we can test the correctness of the race condition.

#### Expected Results

- After *step 1*, the **pool size** should equal to **max size**, the **#page IDs** created by the two threads should sum to **max size**, and we *cannot* create new pages any more. 
- After *step 2*, the buffer pool should be empty. We can verify this by unpin page IDs. Any `UnpinPage()` should return false.
- In *step 4*, the two threads should retrieve previous pages and their contents are correct.
