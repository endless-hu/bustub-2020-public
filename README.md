# FOREWORD

This is **my project report** for [CMU's 15-445 Introduction to Database Systems, Fall 2020](https://15445.courses.cs.cmu.edu/fall2020/). 

The original `README.md` file can be found [here](https://github.com/cmu-db/bustub).

**PLEASE NOTE: The hyperlinks to my source code in this repo are INVALID!!! This is a public version of my project. I don't open my source code because it is a course project and I believe I'm obliged to help protect academic integrity.**

# Declaration

All the five projects are **my own effort**, including the first two projects though they are committed by anonymous source. That's because it was my first time to use `git` and I was not familiar with `git` operation and command, and I misconfigured the commit setting.

You MAY NOT plagiarize my code.

# Build and Test Guide

### Environment Setup

To ensure that you have the proper packages on your machine, run the following script to automatically install them:

```bash
$ sudo bash build_support/packages.sh
```

### Build The System

First, make a `build` directory and `cd` into it. All binary files are generated in it:

```bash
$ mkdir build
$ cd build
```

Configure `cmake` and build the whole system:

```bash
$ cmake ..
$ make -j 4
```

### Test Files

Relevant 10 test files are:

- starter_test
- lru_replacer_test
- buffer_pool_manager_test
- b_plus_tree_insert_test
- b_plus_tree_delete_test
- b_plus_tree_concurrent_test
- executor_test
- catalog_test
- lock_manager_test
- transaction_test

Besides, there is a `b_plus_tree_print_test`, which you can manually insert and delete records in B+tree and make the tree into graph.

(P.s. I provide a random number generation program in `./document/random_num_gen.cpp`, which you can compile it using C++ compiler and generate arbitrary number of random numbers to test insert and delete functions of my B+ tree, and draw the result into `.png` file by following the instructions on the [course website](https://15445.courses.cs.cmu.edu/fall2020/project2/) - TREE VISUALIZATION)

### Run Tests

**Ensure that you are under `${PROJECT_ROOT}/build` directory!**

To run single test, for example, `starter_test`:

```bash
$ make starter_test
$ ./test/starter_test
```

To run all tests, run:

```bash
$ make build-tests
$ make check-tests
```

### If Above Steps Fail:

Please refer to the [course website](https://15445.courses.cs.cmu.edu/fall2020/project0/) for build instructions.

# Project Overview

## Homework 1 - SQL

**Detailed specification can be found [here](https://15445.courses.cs.cmu.edu/fall2020/homework1/)**.

Write SQL to produce desired results. The report can be found in [`./documents/hw1.md`](./documents/hw1.md).

## Project 0 - Starter Project

This project has nothing to do with database system. Instead it is a preparation project to demonstrate that the participants in the course have the skill of C++ programming and help set up work environment for future projects.

**Detailed specification can be found [here](https://15445.courses.cs.cmu.edu/fall2020/project0/)**.

### Task

To implement methods in `Matrix` class so that the class can support matrix addition, multiplication and simplified [General Matrix Multiply](https://en.wikipedia.org/wiki/General_matrix_multiply) (GEMM) operation. 

### Relevant Files

- [`./src/include/primer/p0_starter.h`](src/include/primer/p0_starter.h)

### Development Log and Test

Detailed log can be found in [`./documents/Proj0.md`](./documents/Proj0.md).

## Project 1 - Buffer Pool Manager

**Detailed specification can be found [here](https://15445.courses.cs.cmu.edu/fall2020/project1/)**.

### Tasks

#### Task 1 - LRU Replacer

To implement a replacer in buffer pool manager so that Least Recently Used(LRU) policy can be applied to fan out surplus pages in buffer pool. 

#### Task 2 - Buffer Pool Manager

To implement a buffer pool manager so that it can support following operations:

- Fetch pages in the disk
- Flush dirty(modified) pages into the disk
- Unpin pages when they are no longer needed
- Delete pages both in main memory and disk
- Request new pages from `disk manager` and allocate them in main memory

### Relevant Files

- [`src/include/buffer/lru_replacer.h`](./src/include/buffer/lru_replacer.h)
- [`src/buffer/lru_replacer.cpp`](./src/buffer/lru_replacer.cpp)
- [`src/include/buffer/buffer_pool_manager.h`](./src/include/buffer/buffer_pool_manager.h)
- [`src/buffer/buffer_pool_manager.cpp`](./src/buffer/buffer_pool_manager.cpp)

### Update On 2022/02/17: Add Parallel Tests For LRU Replacer And Buffer Pool Manager

**Claim:** By the parallel tests, I'm sure that *both LRU replacer and buffer pool manager* are **thread-safe**.

#### Parallel Tests For LRU Replacer

##### 1. `ParallelUnpinAndVictimTest1`

###### Test Purpose

1. Repeatedly unpin a same page ID should **NOT** affect the correctness of the replacer;
2. The replacer should be able to handle parallel unpin operations on the **same** page IDs.

###### Test Process

1. Create a set of page IDs that are going to be unpinned;
2. Use 4 threads to iterate through the same set and unpin every page ID in it;
3. Use 4 threads to parallelly get victims from the replacer until the replacer is empty.

###### Expected Results

- After *step 2*, the replacer should be full;
- After *step 3*, the victim page IDs should be the same as the page IDs unpinned in the *step 1*. Their orders are allowed to differ.

##### 2. `ParallelUnpinAndVictimTest2`

###### Test Purpose

The replacer should be able to handle parallel unpin operations on **separate** page IDs.

###### Test Process

1. Create a set of page IDs that are going to be unpinned;
2. Split the page IDs into 4 sets and use 4 threads to call `Unpin`, each for one set;
3. Use 4 threads to parallelly get victims from the replacer until the replacer is empty.

###### Expected Results

- After *step 2*, the replacer should be **full**;
- After *step 3*, the victim page IDs should be **the same as the page IDs unpinned** in the *step 1*. Their orders are allowed to differ.

##### 3. `MixedParallelTest1` & `MixedParallelTest2`

###### Test Purpose

The replacer should be able to handle parallel `Unpin()` **AND `Pin()`** operations on **the same OR separate** page IDs.

###### Test Process

1. Create a set of 100 page IDs that are going to be unpinned;
2. Split the page IDs into 4 sets and use 4 threads to call `Unpin()`, each for one set;
3. Create a set of 40 page IDs that are going to be pinned;
4. Split the page IDs into 3 sets and use 3 threads to call `Pin()`, each for one set;
5. Use 4 threads to parallelly get victims from the replacer until the replacer is empty.

###### Expected Results

- After *step 2*, the replacer should be **full**;
- After *step 4*, there should be **60 pages remaining** in the replacer;
- After *step 5*, the **#victim page IDs** should equal to **60** and these page IDs should **NOT** repeat.

#### Parallel Tests For Buffer Pool Manger

##### Test Purpose

The buffer pool manager should correctly perform `FetchPage()`, `UnpinPage()`, `NewPage()`, etc in parallel situation, *especially when there's race conditions.*

##### Test Process

1. Use two threads to parallelly create new pages and write different contents to these pages until the buffer pool is full;
2. Use 3 threads to unpin all pages in the buffer pool manager. Each page will be unpinned three times.
3. Create new pages until full, then unpin one page to leave room for the subsequent `FetchPage()`;
4. Use two threads to fetch pages they previously created, check the contents of these pages and unpin them. Because there's ONLY ONE available frame to fetch and pin pages, the two threads will race at the frame and we can test the correctness of the race condition.

##### Expected Results

- After *step 1*, the **pool size** should equal to **max size**, the **#page IDs** created by the two threads should sum to **max size**, and we *cannot* create new pages any more. 
- After *step 2*, the buffer pool should be empty. We can verify this by unpin page IDs. Any `UnpinPage()` should return false.
- In *step 4*, the two threads should retrieve previous pages and their contents are correct.

### <del>Potential Bugs and Future Improvements</del>

<del>The project reqests that I should ensure the thread safety of both `buffer pool manager` and `LRU replacer`, but I don't know what specific measures I should take to achieve it.</del>

<del>Obviously I can just `std::lock_guard` the `latch_` in the class when method starts, but that will certainly limit the concurrent operation.</del>

<del>Finally I just use page latches to ensure that every time a page is accessed, it will be protected under appropriate page latch. But I don't know if it will ensure "thread safety" required in the project specification.</del>

### Development Log and Test

Detailed log can be found [`./documents/Proj1.md`](./documents/Proj1.md).

## Project 2 - B+Tree Index

**Detailed specification can be found [here](https://15445.courses.cs.cmu.edu/fall2020/project2/)**.

### Tasks

To implement a B+ tree data structure to support indexing. The B+ tree should support following operations:

- Insertion with correct split
- Deletion with correct redistribute or coalesce
- Point query with iterator support

Besides, the B+ tree should support concurrent operations and take measures to ensure thread safety.

### Relevant Files

- [`src/include/storage/page/b_plus_tree_page.h`](./src/include/storage/page/b_plus_tree_page.h)
- [`src/page/b_plus_tree_page.cpp`](./src/page/b_plus_tree_page.cpp)
- [`src/include/storage/page/b_plus_tree_internal_page.h`](./src/include/storage/page/b_plus_tree_internal_page.h)
- [`src/page/b_plus_tree_internal_page.cpp`](./src/page/b_plus_tree_internal_page.cpp)
- [`src/include/storage/page/b_plus_tree_leaf_page.h`](./src/include/storage/page/b_plus_tree_leaf_page.h)
- [`src/storage/page/b_plus_tree_leaf_page.cpp`](./src/storage/page/b_plus_tree_leaf_page.cpp)
- [`src/include/storage/index/b_plus_tree.h`](./src/include/storage/index/b_plus_tree.h)
- [`src/storage/index/b_plus_tree.cpp`](./src/storage/index/b_plus_tree.cpp)
- [`src/include/storage/index/index_iterator.h`](./src/include/storage/index/index_iterator.h)
- [`src/storage/index/index_iterator.cpp`](./src/storage/index/index_iterator.cpp)

### Potential Bugs and Future Improvements

I passed all relevant tests in `./test` folder, and I'm sure I implemented `insert` operation with concurrency support correctly. However, I'm not sure if I achieved correct `delete` operation with concurrency(But the `delete` operation under single thread is still **correct** ). The `b_plus_tree_concurrent_test`is simple and may need more rigorous tests to ensure accuracy. 

### Development Log and Test

Detailed log can be found in [`./documents/Proj2.md`](./documents/Proj2.md).

## Project 3 - Query Execution

**Detailed specification can be found [here](https://15445.courses.cs.cmu.edu/fall2020/project3/)**.

### Tasks

#### Catalog Manager

Implement it to allow the DBMS to add new **tables and indexes** to the database and retrieve them.

#### Executor

To implement a series of executions, including:

- Sequential Scan
- Limit
- Index Scan
- Update
- Insert
- Delete
- Aggregate (Should also support group by)
- Nested Index Join
- Nested Loop Join

### Relevant Files

- `src/include/catalog/catalog.h`
- `src/include/execution/executors/seq_scan_executor.h`
- `src/include/execution/executors/index_scan_executor.h`
- `src/include/execution/executors/insert_executor.h`
- `src/include/execution/executors/update_executor.h`
- `src/include/execution/executors/delete_executor.h`
- `src/include/execution/executors/nested_loop_join_executor.h`
- `src/include/execution/executors/nested_index_join_executor.h`
- `src/include/execution/executors/limit_executor.h`
- `src/include/execution/executors/aggregation_executor.h`
- `src/execution/seq_scan_executor.cpp`
- `src/execution/index_scan_executor.cpp`
- `src/execution/insert_executor.cpp`
- `src/execution/update_executor.cpp`
- `src/execution/delete_executor.cpp`
- `src/execution/nested_loop_join_executor.cpp`
- `src/execution/nested_index_join_executor.cpp`
- `src/execution/limit_executor.cpp`
- `src/execution/aggregation_executor.cpp`

### Future Improvements

The `index_scan_executor` is just equal to sequential scan because I don't know how to utilize the `predicate`. The B+ tree only supports exact key query, if I want a range scan, which means the begin condition is not accurate with keys in the index. 

### Additional Tests

I passed all tests in `./test` folder. Besides, I added several additional tests to verify operations like: 

- Limit
- Update
- Index Scan
- Nested Index Join

To verify it, clone my repository and build the test by 

```
make executor_test
./test/executor_test
```

## Project 4 - Concurrency Control

**Detailed specification can be found [here](https://15445.courses.cs.cmu.edu/fall2020/project4/)**.

### Tasks

#### Task 1 - Lock Manager

To implement a lock manager which expose correct lock grant behaviors on `Record ID(RID)` level (tuple level) according to Isolation Level in the following methods:

- `LockShared(Transaction, RID)`
- `LockExclusive(Transaction, RID)`
- `LockUpgrade(Transaction, RID)`
- `Unlock(Transaction, RID)`

#### Task 2 - Deadlock Detection

Add implementation to methods in `LockManger` class so that it can support deadlock detection and abort the newer transactions to break cycles using DFS algorithm:

- `AddEdge(txn_id_t t1, txn_id_t t2)`: Adds an edge in your graph from t1 to t2. If the edge already exists, you don't have to do anything.
- `RemoveEdge(txn_id_t t1, txn_id_t t2)`: Removes edge t1 to t2 from your graph. If no such edge exists, you don't have to do anything.
- `HasCycle(txn_id_t& txn_id)`: Looks for a cycle by using the **Depth First Search (DFS)** algorithm. If it finds a cycle, `HasCycle` should store the transaction id of the **youngest** transaction in the cycle in `txn_id` and return true. Your function should return the first cycle it finds. If your graph has no cycles, `HasCycle` should return false.
- `GetEdgeList()`: Returns a list of tuples representing the edges in your graph. We will use this to test correctness of your graph. A pair (t1,t2) corresponds to an edge from t1 to t2.
- `RunCycleDetection()`: Contains skeleton code for running cycle detection in the background. You should implement your cycle detection logic in here.

#### Task 3 - Concurrent Query Execution

Modify executors in **Project 3** so that these executors can be executed in concurrent contexts without getting incorrect results.

### Relevant Files

- [`src/include/concurrency/lock_manager.h`](./src/include/concurrency/lock_manager.h)
- [`src/concurrency/lock_manager.cpp`](./src/concurrency/lock_manager.cpp)


### Test

Clone my repository and build the tests by 

```
make lock_manager_test
./test/lock_manager_test
make transaction_test
./test/transaction_test
make executor_test
./test/executor_test
```

### <del>Potential Bugs</del>

<del>When I first run the `lock_manager_test`, it got stuck in the first test. Besides, sometimes it would throw error in the first test that **a thread is using some space which was previously allocated and destroyed by another thread**. But both of the situations are undebuggable because when I type `run` in GDB, the test was passed. And I run it again and again but the error never occurred again.</del>

<del>Sometimes the error will occur when I make some changes and run `make`. But still it cannot be caught in GDB.</del>

### <del>Future Improvements</del>

<del>According to the error, **a thread is using some space which was previously allocated and destroyed by another thread**, I can locate the code which causes the error:</del>

<del>In `lock_manager.cpp:212`(Unlock):</del>

```
if (!lock_table_[rid].request_queue_.empty()) 
  lock_table_[rid].request_queue_.remove(dummy_req);
```

<del>I guess that when a thread *(say thread 1)* have removed the desired `Request` *(say the request for RID0)*, another thread *(say thread 2)* at the same time got the iterator of the removed `Request`. Since the `RID` which the iterator is pointing to has been removed, when `operator++` is invoked, the error is throwed.</del>

<del>Therefore, to fix the bug, I shall add `std::lock_guard` in the beginning of the `Unlock` method. But when I tried it, the test had the problem of joining the threads in the end of the test body and the test got stuck. So I give up the try and let the bug remains.</del>

### Update On 2022/02/19: Fix Concurrency Bugs

The striked-through sections above, which were written nearly one year ago, reveal the concurrency issues with my implementation about `LockManager`. 

Now I figured out the problem is that *the `stl` containers, such as `list`, `unordered_map`, etc are NOT thread safe and they will cause problems if operating under multi-thread situation.* 

To **expose** these problems, I should make the thread number larger in the first `SampleTest`, so that concurrency issues will escalate. 

To **address** these problems, use the `std::mutex` to protect these data structures in the `LockManager`, especially the `lock_table_`.

Besides, I added several tests to ensure the correctness of `LockManager`. See [`test/concurrency/lock_manager_test.cpp`](test/concurrency/lock_manager_test.cpp) for more details.

### Update On 2022/02/20: Improve Logics of LockUpgrade and Some Executors

The `LockManager::LockUpgrade()` was only *loosely* tested before, and it did not work under
complex situations. 

The problem was exposed when I was testing transactions under a typical situation mentioned 
in the course:

1. *A* has 1000 in her account, *B* has 500 in his account; <- Initial state
2. *A* wants to transfer 250 to *B*; <- Here's Transaction 1
3. But at the same time, the bank decides to pay 10% interest to both *A* and *B*. <- Here's Transaction 2

Transaction 1 and 2 are run in parallel, but the bank system should ensure the consistency of the data after the transactions are executed. 
