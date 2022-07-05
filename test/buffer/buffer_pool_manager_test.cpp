//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <thread>
#include "gtest/gtest.h"

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
// helper function to unpin all pages
void UnpinHelper(BufferPoolManager *bpm, __attribute__((unused)) uint64_t thread_itr = 0) {
  for (size_t i = 0; i < bpm->GetPoolSize(); i++) bpm->UnpinPage(i, true);
}

// Since `snprintf()` and vectors are not thread safe, use the mutex to protect them
std::mutex lk;

void thread_newpages1(BufferPoolManager *bpm, std::vector<page_id_t> *new_pages) {
  page_id_t page_id_tmp;
  // Scenario: The buffer pool is empty. We should be able to create a new page.
  auto *pg = bpm->NewPage(&page_id_tmp);
  {
    const std::lock_guard<std::mutex> guard(lk);
    EXPECT_NE(pg, nullptr);
    new_pages->push_back(page_id_tmp);
    // Scenario: Once we have a page, we should be able to read and write content.
    snprintf(pg->GetData(), PAGE_SIZE, "Hello from thread 1");
    EXPECT_EQ(0, strcmp(pg->GetData(), "Hello from thread 1"));
  }
  // Delay for some time to allow thread 2 to create some new pages
  std::this_thread::sleep_for(std::chrono::microseconds(5));
  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  while ((pg = bpm->NewPage(&page_id_tmp)) != nullptr) {
    {
      const std::lock_guard<std::mutex> guard(lk);
      snprintf(pg->GetData(), PAGE_SIZE, "Hello from thread 1");
      EXPECT_EQ(0, strcmp(pg->GetData(), "Hello from thread 1"));
      new_pages->push_back(page_id_tmp);
    }
  }
}

void thread_newpages2(BufferPoolManager *bpm, std::vector<page_id_t> *new_pages) {
  page_id_t page_id_tmp;
  // Scenario: The buffer pool is empty. We should be able to create a new page.
  auto *pg = bpm->NewPage(&page_id_tmp);
  {
    const std::lock_guard<std::mutex> guard(lk);
    EXPECT_NE(pg, nullptr);
    // Scenario: Once we have a page, we should be able to read and write content.
    snprintf(pg->GetData(), PAGE_SIZE, "Hello from thread 2");
    EXPECT_EQ(0, strcmp(pg->GetData(), "Hello from thread 2"));
    new_pages->push_back(page_id_tmp);
  }
  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  while ((pg = bpm->NewPage(&page_id_tmp)) != nullptr) {
    {
      const std::lock_guard<std::mutex> guard(lk);
      snprintf(pg->GetData(), PAGE_SIZE, "Hello from thread 2");
      EXPECT_EQ(0, strcmp(pg->GetData(), "Hello from thread 2"));
      new_pages->push_back(page_id_tmp);
    }
  }
}

void thread_fetchpages1(BufferPoolManager *bpm, const std::vector<page_id_t> &t1_pages) {
  Page *pg;
  int cnt = 0;  // Contentions with the only frame
  for (auto &page_id : t1_pages) {
    // Because then the function is called, there is only one available frame
    // It's very likely that FetchPage() cannot get the page desired
    // So we loop until it gets the page
    while ((pg = bpm->FetchPage(page_id)) == nullptr) cnt++;
    EXPECT_EQ(0, strcmp(pg->GetData(), "Hello from thread 1"));
    EXPECT_EQ(bpm->UnpinPage(page_id, false), true);
    bpm->UnpinPage(page_id, false);
    // Randomly sleep for a short time for the other thread to fetch pages
    if (page_id % 7 < 6) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  // report the contentions
  std::cout << "Thread 1 fetch page contentions = " << cnt << std::endl;
}
void thread_fetchpages2(BufferPoolManager *bpm, const std::vector<page_id_t> &t2_pages) {
  Page *pg;
  int cnt = 0;
  for (auto &page_id : t2_pages) {
    // Because then the function is called, there is only one available frame
    // It's very likely that FetchPage() cannot get the page desired
    // So we loop until it gets the page
    while ((pg = bpm->FetchPage(page_id)) == nullptr) cnt++;
    EXPECT_EQ(0, strcmp(bpm->FetchPage(page_id)->GetData(), "Hello from thread 2"));
    EXPECT_EQ(bpm->UnpinPage(page_id, false), true);
    bpm->UnpinPage(page_id, false);
    // Randomly sleep for a short time for the other thread to fetch pages
    if (page_id % 7 < 1) std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  // report the contentions
  std::cout << "Thread 2 fetch page contentions = " << cnt << std::endl;
}

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;

  std::random_device r;
  std::default_random_engine rng(r());
  std::uniform_int_distribution<char> uniform_dist(0);

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[PAGE_SIZE];
  // Generate random binary data
  for (char &i : random_binary_data) {
    i = uniform_dist(rng);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[PAGE_SIZE / 2] = '\0';
  random_binary_data[PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::memcpy(page0->GetData(), random_binary_data, PAGE_SIZE);
  EXPECT_EQ(0, std::memcmp(page0->GetData(), random_binary_data, PAGE_SIZE));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one cache frame left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, memcmp(page0->GetData(), random_binary_data, PAGE_SIZE));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, ParallelTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 100;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);
  page_id_t page_id_temp;

  // Scenario: Use two threads to create new pages until bpm is full
  std::vector<page_id_t> t1_pages;
  std::vector<page_id_t> t2_pages;
  std::thread t_newpages1(thread_newpages1, bpm, &t1_pages);
  std::thread t_newpages2(thread_newpages2, bpm, &t2_pages);
  t_newpages1.join();
  t_newpages2.join();
  // Report the size of t1_pages and t2_pages, respectively
  std::cout << "t1_pages.size() = " << t1_pages.size() << ", t2_pages.size() = " << t2_pages.size() << std::endl;
  EXPECT_EQ(t1_pages.size() + t2_pages.size(), buffer_pool_size);

  // The new pages should range in [0, 100) and they should appear only once
  bool appeared[buffer_pool_size] = {false};
  for (auto &pgid : t1_pages) {
    ASSERT_GE(pgid, 0);
    ASSERT_LT(pgid, 100);
    EXPECT_EQ(false, appeared[pgid]);
    appeared[pgid] = true;
  }
  for (auto &pgid : t2_pages) {
    ASSERT_GE(pgid, 0);
    ASSERT_LT(pgid, 100);
    EXPECT_EQ(false, appeared[pgid]);
    appeared[pgid] = true;
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Parallelly unpin all pages.
  LaunchParallelTest(3, UnpinHelper, bpm);
  // Scenario: Try unpinning them. It should return false
  for (size_t i = 0; i < buffer_pool_size; i++) EXPECT_EQ(false, bpm->UnpinPage(i, true));
  // Scenario: Create new pages until full
  for (size_t i = 0; i < buffer_pool_size; i++) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    EXPECT_GE(page_id_temp, buffer_pool_size);
  }
  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  // Here we deliberately create thread contentions by parallelly fetch pages
  // when the bpm has ONLY one available frame
  bpm->UnpinPage(page_id_temp, true);
  std::thread t_fetch_pages1(thread_fetchpages1, bpm, t1_pages);
  std::thread t_fetch_pages2(thread_fetchpages2, bpm, t2_pages);
  t_fetch_pages1.join();
  t_fetch_pages2.join();

  // Fill the last available frame
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_GE(page_id_temp, buffer_pool_size);
  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  if (bpm->FetchPage(0) != nullptr) {  // Ensure page 0 is in the bpm
    bpm->UnpinPage(0, false);
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    EXPECT_EQ(nullptr, bpm->FetchPage(0));
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
