//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer_test.cpp
//
// Identification: test/buffer/lru_replacer_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sys/stat.h>
#include <algorithm>
#include <chrono>  // NOLINT
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
#include <thread>  // NOLINT

#include "buffer/lru_replacer.h"
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

void PinHelper(LRUReplacer *replacer, const std::vector<frame_id_t> &frame_ids,
               __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto frame_id : frame_ids) replacer->Pin(frame_id);
}

void PinHelperSplit(LRUReplacer *replacer, const std::vector<frame_id_t> &frame_ids, int total_threads,
                    __attribute__((unused)) uint64_t thread_itr) {
  for (size_t i = thread_itr; i < frame_ids.size(); i += total_threads) {
    replacer->Pin(frame_ids[i]);
  }
}

void UnpinHelper(LRUReplacer *replacer, const std::vector<frame_id_t> &frame_ids,
                 __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto frame_id : frame_ids) replacer->Unpin(frame_id);
}

void UnpinHelperSplit(LRUReplacer *replacer, const std::vector<frame_id_t> &frame_ids, int total_threads,
                      __attribute__((unused)) uint64_t thread_itr) {
  for (size_t i = thread_itr; i < frame_ids.size(); i += total_threads) {
    replacer->Unpin(frame_ids[i]);
  }
}

// use the lock to protect the vector in VictimHelper()
std::mutex lk;
void VictimHelper(LRUReplacer *replacer, std::vector<frame_id_t> *values,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  int value;
  while (replacer->Victim(&value)) {
    {
      const std::lock_guard<std::mutex> guard(lk);
      values->push_back(value);
    }
  }
}

TEST(LRUReplacerTest, SampleTest) {
  LRUReplacer lru_replacer(7);
  // Scenario: unpin six elements, i.e. add them to the replacer.
  lru_replacer.Unpin(1);
  lru_replacer.Unpin(2);
  lru_replacer.Unpin(3);
  lru_replacer.Unpin(4);
  lru_replacer.Unpin(5);
  lru_replacer.Unpin(6);
  lru_replacer.Unpin(1);
  EXPECT_EQ(6, lru_replacer.Size());

  // Scenario: get three victims from the lru.
  int value;
  lru_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(3, value);

  // Scenario: pin elements in the replacer.
  // Note that 3 has already been victimized, so pinning 3 should have no effect.
  lru_replacer.Pin(3);
  lru_replacer.Pin(4);
  EXPECT_EQ(2, lru_replacer.Size());

  // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
  lru_replacer.Unpin(4);

  // Scenario: continue looking for victims. We expect these victims.
  lru_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(6, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(4, value);
}

TEST(LRUReplacerTest, EmptyTest) {
  /**
   * Based on SampleTest.
   * Tests if LRUReplacer behave correctly when Victim() is called with no frames in it.
   */
  LRUReplacer lru_replacer(7);
  int value;
  EXPECT_EQ(lru_replacer.Victim(&value), false);

  // ------ below is the same as SampleTest-----
  lru_replacer.Unpin(1);
  lru_replacer.Unpin(2);
  lru_replacer.Unpin(3);
  lru_replacer.Unpin(4);
  lru_replacer.Unpin(5);
  lru_replacer.Unpin(6);
  lru_replacer.Unpin(1);
  EXPECT_EQ(6, lru_replacer.Size());
  lru_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(3, value);
  lru_replacer.Pin(3);
  lru_replacer.Pin(4);
  EXPECT_EQ(2, lru_replacer.Size());
  lru_replacer.Unpin(4);
  lru_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(6, value);
  lru_replacer.Victim(&value);
  EXPECT_EQ(4, value);
  // ------  SampleTest section ends--------

  EXPECT_EQ(lru_replacer.Victim(&value), false);
}

TEST(LRUReplacerTest, ParallelUnpinAndVictimTest1) {
  /** Repeatedly unpin */
  const frame_id_t total_pages = 100;
  LRUReplacer lru_replacer(total_pages);
  std::vector<frame_id_t> frame_ids;
  // Parallelly unpin same frame ids
  for (frame_id_t i = 1; i <= total_pages; i++) {
    frame_ids.push_back(i);
  }
  LaunchParallelTest(4, UnpinHelper, &lru_replacer, frame_ids);
  EXPECT_EQ(lru_replacer.Size(), total_pages);

  // Get all victims from the replacer parallelly
  frame_ids.clear();
  LaunchParallelTest(4, VictimHelper, &lru_replacer, &frame_ids);
  EXPECT_EQ(lru_replacer.Size(), 0);
  EXPECT_EQ(frame_ids.size(), total_pages);
  // examine the elements in frame_ids, they shouldn't repeat
  std::vector<bool> appeared(frame_ids.size(), false);
  for (size_t i = 0; i < frame_ids.size(); i++) {
    EXPECT_EQ(false, appeared[i]);
    appeared[i] = true;
  }
}

TEST(LRUReplacerTest, ParallelUnpinAndVictimTest2) {
  /** Separately unpin */
  const frame_id_t total_pages = 100;
  LRUReplacer lru_replacer(total_pages);
  std::vector<frame_id_t> frame_ids;
  // Parallelly unpin frame ids
  for (frame_id_t i = 1; i <= total_pages; i++) {
    frame_ids.push_back(i);
  }
  LaunchParallelTest(4, UnpinHelperSplit, &lru_replacer, frame_ids, 4);
  EXPECT_EQ(lru_replacer.Size(), total_pages);

  // Get all victims from the replacer parallelly
  frame_ids.clear();
  LaunchParallelTest(4, VictimHelper, &lru_replacer, &frame_ids);
  EXPECT_EQ(lru_replacer.Size(), 0);
  EXPECT_EQ(frame_ids.size(), total_pages);
  // examine the elements in frame_ids, they shouldn't repeat
  std::vector<bool> appeared(frame_ids.size(), false);
  for (size_t i = 0; i < frame_ids.size(); i++) {
    EXPECT_EQ(false, appeared[i]);
    appeared[i] = true;
  }
}

TEST(LRUReplacerTest, MixedParallelTest1) {
  /** Seperately unpin and pin */
  const frame_id_t total_pages = 100;
  LRUReplacer lru_replacer(total_pages);
  std::vector<frame_id_t> frame_ids;
  // Parallelly unpin same frame ids
  for (frame_id_t i = 1; i <= total_pages; i++) {
    frame_ids.push_back(i);
  }
  LaunchParallelTest(4, UnpinHelperSplit, &lru_replacer, frame_ids, 4);
  EXPECT_EQ(lru_replacer.Size(), total_pages);

  // Parallelly pin 40 frames
  frame_ids.clear();
  for (size_t i = 1; i <= 40; i++) {
    frame_ids.push_back(i);
  }
  LaunchParallelTest(3, PinHelperSplit, &lru_replacer, frame_ids, 3);
  EXPECT_EQ(lru_replacer.Size(), total_pages - 40);

  // Finally, get all victims from the replacer parallelly
  frame_ids.clear();
  LaunchParallelTest(4, VictimHelper, &lru_replacer, &frame_ids);
  EXPECT_EQ(lru_replacer.Size(), 0);
  EXPECT_EQ(frame_ids.size(), total_pages - 40);
  // examine the elements in frame_ids, they shouldn't repeat
  std::vector<bool> appeared(frame_ids.size(), false);
  for (size_t i = 0; i < frame_ids.size(); i++) {
    EXPECT_EQ(false, appeared[i]);
    appeared[i] = true;
  }
}

TEST(LRUReplacerTest, MixedParallelTest2) {
  /** Seperately unpin, repeatedly pin */
  const frame_id_t total_pages = 100;
  LRUReplacer lru_replacer(total_pages);
  std::vector<frame_id_t> frame_ids;
  // Parallelly unpin same frame ids
  for (frame_id_t i = 1; i <= total_pages; i++) {
    frame_ids.push_back(i);
  }
  LaunchParallelTest(4, UnpinHelperSplit, &lru_replacer, frame_ids, 4);
  EXPECT_EQ(lru_replacer.Size(), total_pages);

  // Parallelly pin 40 frames
  frame_ids.clear();
  for (size_t i = 1; i <= 40; i++) {
    frame_ids.push_back(i);
  }
  LaunchParallelTest(3, PinHelper, &lru_replacer, frame_ids);
  EXPECT_EQ(lru_replacer.Size(), total_pages - 40);

  // Finally, get all victims from the replacer parallelly
  frame_ids.clear();
  LaunchParallelTest(4, VictimHelper, &lru_replacer, &frame_ids);
  EXPECT_EQ(lru_replacer.Size(), 0);
  EXPECT_EQ(frame_ids.size(), total_pages - 40);
  // examine the elements in frame_ids, they shouldn't repeat
  std::vector<bool> appeared(frame_ids.size(), false);
  for (size_t i = 0; i < frame_ids.size(); i++) {
    EXPECT_EQ(false, appeared[i]);
    appeared[i] = true;
  }
}

}  // namespace bustub
