/**
 * b_plus_tree_test.cpp
 */

#include <chrono>  // NOLINT
#include <cstdio>
#include <functional>
#include <random>
#include <thread>                   // NOLINT
#include "b_plus_tree_test_util.h"  // NOLINT

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  Transaction *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

// helper function to seperate query
void QueryHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                      int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      rids.clear();
      index_key.SetFromInteger(key);
      tree->GetValue(index_key, &rids);
      ASSERT_EQ(rids.size(), 1);

      int64_t value = key & 0xFFFFFFFF;
      ASSERT_EQ(rids[0].GetSlotNum(), value);
    }
  }
}

TEST(BPlusTreeConcurrentTest, InsertTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    EXPECT_TRUE(tree.GetValue(index_key, &rids));
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

// Additional test.
TEST(BPlusTreeConcurrentTest, InsertTest1_MultiLayer) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 4, 4);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  std::cout << "Addtional Test: Insert 10000 records into a 4-4 tree" << std::endl;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);
  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    EXPECT_TRUE(tree.GetValue(index_key, &rids));
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

// Additional test.
TEST(BPlusTreeConcurrentTest, InsertTest1_Massive) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 4, 4);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 1000;
  std::cout << "Addtional Test: Insert " << scale_factor << " records into a 4-4 tree" << std::endl;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelper, &tree, keys);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    ASSERT_TRUE(tree.GetValue(index_key, &rids));
    ASSERT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    ASSERT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    ASSERT_EQ(location.GetPageId(), 0);
    ASSERT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  ASSERT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, InsertTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 100;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  LaunchParallelTest(2, InsertHelperSplit, &tree, keys, 2);

  std::vector<RID> rids;
  GenericKey<8> index_key;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

// Additional test. Also for benchmark.
TEST(BPlusTreeConcurrentTest, InsertTest2_Massive_Sequential) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 10000;
  int total_thread = 2;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }

  int64_t millisec, query_rate, ins_rate;
  auto start = std::chrono::steady_clock::now();
  LaunchParallelTest(total_thread, InsertHelperSplit, &tree, keys, total_thread);
  auto end = std::chrono::steady_clock::now();
  millisec = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  ins_rate = scale_factor * 1000 / millisec;
  std::cout << "Inserting " << scale_factor << " records with " << total_thread << " threads takes " << millisec
            << " ms" << std::endl;

  start = std::chrono::steady_clock::now();
  LaunchParallelTest(total_thread, QueryHelperSplit, &tree, keys, total_thread);
  end = std::chrono::steady_clock::now();
  millisec = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  query_rate = scale_factor * 1000 / millisec;
  std::cout << "Querying " << scale_factor << " records with " << total_thread << " threads takes " << millisec << " ms"
            << std::endl;

  GenericKey<8> index_key;
  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
  std::cout << "====== Benchmark Report ======" << std::endl
            << "Insert: " << ins_rate << " records/s" << std::endl
            << "Query: " << query_rate << " records/s" << std::endl;
}

// Additional test. Also for benchmark.
TEST(BPlusTreeConcurrentTest, InsertTest2_Massive_Shuffled) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // keys to Insert
  std::vector<int64_t> keys;
  int64_t scale_factor = 10000;
  int total_thread = 4;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }
  // shuffle keys
  std::random_shuffle(keys.begin(), keys.end());

  int64_t millisec, query_rate, ins_rate;
  auto start = std::chrono::steady_clock::now();
  LaunchParallelTest(total_thread, InsertHelperSplit, &tree, keys, total_thread);
  auto end = std::chrono::steady_clock::now();
  millisec = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  ins_rate = scale_factor * 1000 / millisec;
  std::cout << "Inserting " << scale_factor << " records with " << total_thread << " threads takes " << millisec
            << " ms" << std::endl;

  start = std::chrono::steady_clock::now();
  LaunchParallelTest(total_thread, QueryHelperSplit, &tree, keys, total_thread);
  end = std::chrono::steady_clock::now();
  millisec = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  query_rate = scale_factor * 1000 / millisec;
  std::cout << "Querying " << scale_factor << " records with " << total_thread << " threads takes " << millisec << " ms"
            << std::endl;

  GenericKey<8> index_key;
  int64_t start_key = 1;
  int64_t current_key = start_key;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
  }

  EXPECT_EQ(current_key, keys.size() + 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
  std::cout << "====== Benchmark Report ======" << std::endl
            << "Insert: " << ins_rate << " records/s" << std::endl
            << "Query: " << query_rate << " records/s" << std::endl;
}

TEST(BPlusTreeConcurrentTest, DeleteTest1) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, DeleteTest2) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // sequential insert
  std::vector<int64_t> keys = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  InsertHelper(&tree, keys);

  std::vector<int64_t> remove_keys = {1, 4, 3, 2, 5, 6};
  LaunchParallelTest(2, DeleteHelperSplit, &tree, remove_keys, 2);

  int64_t start_key = 7;
  int64_t current_key = start_key;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);

  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    auto location = (*iterator).second;
    EXPECT_EQ(location.GetPageId(), 0);
    EXPECT_EQ(location.GetSlotNum(), current_key);
    current_key = current_key + 1;
    size = size + 1;
  }

  EXPECT_EQ(size, 4);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/**
 * Insert many keys, then parallelly delete all
 */
TEST(BPlusTreeConcurrentTest, MassiveDeleteTest1_Additional) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int64_t scale_factor = 10000;
  std::cout << "Addtional Test: Insert " << scale_factor << " keys,  then parallelly delete all" << std::endl;
  std::vector<int64_t> keys;
  for (int64_t key = 1; key <= scale_factor; key++) {
    keys.push_back(key);
  }

  for (int iter = 0; iter < 10; iter++) {
    // sequential insert
    std::random_shuffle(keys.begin(), keys.end());
    InsertHelper(&tree, keys);
    // verify insertion
    std::vector<RID> rids;
    GenericKey<8> index_key;
    for (auto key : keys) {
      rids.clear();
      index_key.SetFromInteger(key);
      ASSERT_TRUE(tree.GetValue(index_key, &rids));
      ASSERT_EQ(rids.size(), 1);

      int64_t value = key & 0xFFFFFFFF;
      ASSERT_EQ(rids[0].GetSlotNum(), value);
    }
    int64_t start_key = 1;
    int64_t current_key = start_key;
    index_key.SetFromInteger(start_key);
    for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
      auto location = (*iterator).second;
      ASSERT_EQ(location.GetPageId(), 0);
      ASSERT_EQ(location.GetSlotNum(), current_key);
      current_key = current_key + 1;
    }
    ASSERT_EQ(current_key, keys.size() + 1);

    // start removing
    std::vector<int64_t> remove_keys(keys);
    std::random_shuffle(remove_keys.begin(), remove_keys.end());
    LaunchParallelTest(2, DeleteHelper, &tree, remove_keys);
    // verify insertion
    EXPECT_TRUE(tree.IsEmpty());
    for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
      // Since there's no key in the tree, no commands in this body should be executed
      EXPECT_TRUE(false);
    }
  }
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/**
 * Same as above, but use splitter
 */
TEST(BPlusTreeConcurrentTest, MassiveDeleteTest2_Additional) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  int num_threads = 4;
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  int64_t scale_factor = 10000;
  std::cout << "Addtional Test: Insert " << scale_factor << " keys,  then parallelly delete all using splitter" << std::endl;
  std::vector<int64_t> keys;
  for (int64_t key = 1; key <= scale_factor; key++) {
    keys.push_back(key);
  }
  for (int iter = 0; iter < 10; iter++) {
    // sequential insert
    std::random_shuffle(keys.begin(), keys.end());
    InsertHelper(&tree, keys);

    std::vector<int64_t> remove_keys(keys);
    std::random_shuffle(remove_keys.begin(), remove_keys.end());
    LaunchParallelTest(num_threads, DeleteHelperSplit, &tree, remove_keys, num_threads);

    EXPECT_TRUE(tree.IsEmpty());
    for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
      EXPECT_TRUE(false);
    }
    for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
      std::cout << (*iterator).first.ToString() << std::endl;
      EXPECT_TRUE(false);
    }
  }
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(BPlusTreeConcurrentTest, MixTest) {
  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;
  // first, populate index
  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  InsertHelper(&tree, keys);

  // concurrent insert
  keys.clear();
  for (int i = 6; i <= 10; i++) {
    keys.push_back(i);
  }
  LaunchParallelTest(1, InsertHelper, &tree, keys);
  // concurrent delete
  std::vector<int64_t> remove_keys = {1, 4, 3, 5, 6};
  LaunchParallelTest(1, DeleteHelper, &tree, remove_keys);

  int64_t start_key = 2;
  int64_t size = 0;
  index_key.SetFromInteger(start_key);
  for (auto iterator = tree.Begin(index_key); iterator != tree.end(); ++iterator) {
    size = size + 1;
  }

  EXPECT_EQ(size, 5);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

/**
 * Launch some threads to insert keys in [1, 1000]
 * Launch some threads to remove keys in [1, 1000]
 * Keep running for 5 seconds, then check the shape of the tree
 */
TEST(BPlusTreeConcurrentTest, MassiveMixTest) {
  std::cout << "Addtional Test: insert and delete happen at the same time" << std::endl;

  // create KeyComparator and index schema
  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator);
  GenericKey<8> index_key;

  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  std::vector<std::thread> insert_thread_group;
  std::vector<std::thread> remove_thread_group;
  int num_threads = 4;
  std::atomic<bool> done{false};
  for (int i = 0; i < num_threads; i++) {
    insert_thread_group.push_back(std::thread([&, i] {  // `i` captured by copy
      // create transaction
      Transaction *transaction = new Transaction(i);
      GenericKey<8> index_key;
      RID rid;
      // Initialize random number generator
      std::random_device rd;   // Will be used to obtain a seed for the random number engine
      std::mt19937 gen(rd());  // Standard mersenne_twister_engine seeded with rd()
      std::uniform_int_distribution<long> distrib(1, 1000);
      while (!done.load()) {
        int64_t key = distrib(gen);
        int64_t value = key & 0xFFFFFFFF;
        rid.Set(static_cast<int32_t>(key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Insert(index_key, rid, transaction);
      }
      delete transaction;
    }));
    remove_thread_group.push_back(std::thread([&, i] {  // `i` captured by copy
      // create transaction
      Transaction *transaction = new Transaction(2 * i + 1);
      GenericKey<8> index_key;
      RID rid;
      // Initialize random number generator
      std::random_device rd;   // Will be used to obtain a seed for the random number engine
      std::mt19937 gen(rd());  // Standard mersenne_twister_engine seeded with rd()
      std::uniform_int_distribution<long> distrib(1, 1000);
      while (!done.load()) {
        int64_t key = distrib(gen);
        int64_t value = key & 0xFFFFFFFF;
        rid.Set(static_cast<int32_t>(key >> 32), value);
        index_key.SetFromInteger(key);
        tree.Remove(index_key, transaction);
      }
      delete transaction;
    }));
  }
  std::this_thread::sleep_for(std::chrono::seconds(5));
  done.store(true);
  // join all threads
  for (int i = 0; i < num_threads; i++) {
    insert_thread_group[i].join();
    remove_thread_group[i].join();
  }

  // Verify the shape
  // first get all keys
  std::vector<int64_t> keys;
  for (auto iterator = tree.begin(); iterator != tree.end(); ++iterator) {
    keys.push_back((*iterator).first.ToString());
  }
  // query them one by one
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    EXPECT_TRUE(tree.GetValue(index_key, &rids));
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete key_schema;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

}  // namespace bustub
