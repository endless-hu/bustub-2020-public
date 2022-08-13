/**
 * lock_manager_test.cpp
 */

#include <random>
#include <thread>  // NOLINT

#include "common/config.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace bustub {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}

// Basic shared lock test under REPEATABLE_READ
void BasicTest1() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};

  std::vector<RID> rids;
  std::vector<Transaction *> txns;
  int num_rids = 100;  // make this larger so that thread safety issues will appear
  for (int i = 0; i < num_rids; i++) {
    RID rid{i, static_cast<uint32_t>(i)};
    rids.push_back(rid);
    txns.push_back(txn_mgr.Begin());
    EXPECT_EQ(i, txns[i]->GetTransactionId());
  }
  // test

  auto task = [&](int txn_id) {
    bool res;
    for (const RID &rid : rids) {
      res = lock_mgr.LockShared(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckGrowing(txns[txn_id]);
    }
    for (const RID &rid : rids) {
      res = lock_mgr.Unlock(txns[txn_id], rid);
      EXPECT_TRUE(res);
      CheckShrinking(txns[txn_id]);
    }
    txn_mgr.Commit(txns[txn_id]);
    CheckCommitted(txns[txn_id]);
  };
  std::vector<std::thread> threads;
  threads.reserve(num_rids);

  for (int i = 0; i < num_rids; i++) {
    threads.emplace_back(std::thread{task, i});
  }

  for (int i = 0; i < num_rids; i++) {
    threads[i].join();
  }

  for (int i = 0; i < num_rids; i++) {
    delete txns[i];
  }
}
TEST(LockManagerTest, BasicTest) { BasicTest1(); }

void TwoPLTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid0{0, 0};
  RID rid1{0, 1};

  auto txn = txn_mgr.Begin();
  EXPECT_EQ(0, txn->GetTransactionId());

  bool res;
  res = lock_mgr.LockShared(txn, rid0);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 0);

  res = lock_mgr.LockExclusive(txn, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 1);

  res = lock_mgr.Unlock(txn, rid0);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnLockSize(txn, 0, 1);

  try {
    lock_mgr.LockShared(txn, rid0);
    CheckAborted(txn);
    // Size shouldn't change here
    CheckTxnLockSize(txn, 0, 1);
  } catch (TransactionAbortException &e) {
    // std::cout << e.GetInfo() << std::endl;
    CheckAborted(txn);
    // Size shouldn't change here
    CheckTxnLockSize(txn, 0, 1);
  }
  // Need to call txn_mgr's abort
  txn_mgr.Abort(txn);
  CheckAborted(txn);
  CheckTxnLockSize(txn, 0, 0);

  delete txn;
}
TEST(LockManagerTest, TwoPLTest) { TwoPLTest(); }

TEST(LockManagerTest, MixedSXLockTest_2PL) {
  /** Test the correctness after multiple, mixed S-lock and X-lock under 2PL */
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  std::vector<Transaction *> txns;

  for (int i = 0; i < 4; i++) {
    auto txn = txn_mgr.Begin();
    ASSERT_EQ(txn->GetTransactionId(), i);
    txns.push_back(txn);
  }

  // Thread 0 gets an S-lock on rid
  std::thread t0([&] {
    bool res = lock_mgr.LockShared(txns[0], rid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[0]);
    CheckTxnLockSize(txns[0], 1, 0);
    // block t2 for 100 ms
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    res = lock_mgr.Unlock(txns[0], rid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[0]);
    CheckTxnLockSize(txns[0], 0, 0);
  });
  // Thread 1 gets an S-lock on rid, too
  std::thread t1([&] {
    bool res = lock_mgr.LockShared(txns[1], rid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[1]);
    CheckTxnLockSize(txns[1], 1, 0);
    // block t2 for 100 ms
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    res = lock_mgr.Unlock(txns[1], rid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[1]);
    CheckTxnLockSize(txns[1], 0, 0);
  });
  // Thread 2 wants to get an X-lock on rid after T0 and T1 got S-lock.
  // So it should be blocked for 100 ms
  std::thread t2([&] {
    // Put aside some time for txn1 and txn2 to grab S-lock
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    const std::chrono::time_point<std::chrono::steady_clock> start = std::chrono::steady_clock::now();
    // This step should be blocked on wait for at least 80 ms.
    bool res = lock_mgr.LockExclusive(txns[2], rid);
    const auto end = std::chrono::steady_clock::now();
    EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(), 80);
    EXPECT_TRUE(res);
    CheckGrowing(txns[2]);
    CheckTxnLockSize(txns[2], 0, 1);
    // block t3 for another 100 ms
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    res = lock_mgr.Unlock(txns[2], rid);
    CheckShrinking(txns[2]);
    CheckTxnLockSize(txns[2], 0, 0);
  });
  // Thread 3 wants to get an S-lock after T2 issued X-lock request.
  // It should NOT get the S-lock until T2 releases its X-lock, otherwise T2 may suffer starvation
  std::thread t3([&] {
    // Put aside some time for txn3 to request for X-lock
    std::this_thread::sleep_for(std::chrono::milliseconds(6));
    const std::chrono::time_point<std::chrono::steady_clock> start = std::chrono::steady_clock::now();
    bool res = lock_mgr.LockShared(txns[3], rid);
    const auto end = std::chrono::steady_clock::now();
    // This step should be blocked on wait for at least 160 ms.
    // In fact, t3 should wait for 100+100 = 200 ms
    EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(), 160);
    EXPECT_TRUE(res);
    CheckGrowing(txns[3]);
    CheckTxnLockSize(txns[3], 1, 0);
    res = lock_mgr.Unlock(txns[3], rid);
    CheckShrinking(txns[3]);
    CheckTxnLockSize(txns[3], 0, 0);
  });

  t0.join();
  t1.join();
  t2.join();
  t3.join();

  // Need to call txn_mgr's abort
  for (auto txn : txns) {
    txn_mgr.Abort(txn);
    CheckAborted(txn);
    CheckTxnLockSize(txn, 0, 0);
    delete txn;
  }
}

TEST(LockManagerTest, MixedSXLockTest_RC) {
  /** Test the correctness after multiple, mixed S-lock and X-lock under READ_COMMITTED
   *
   *                                Test Procedure
   *                        ------------------------------
   *          T0           +             T1            +         T2          +         T3
   *         BEGIN         |            BEGIN          |        BEGIN        |        BEGIN
   *        S-Lock(A)      |          S-Lock(A)        |      X-Lock(A) ...  |
   *      sleep 100ms ..   |        sleep 100ms ..     |                     |
   *        release(A)     |         release(A)        |                     |        S-Lock(A)
   *                       |                           |                     |      sleep 100ms ..
   *                       |                           |   Get X-Lock here   |        release(A)
   *
   */

  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  std::vector<Transaction *> txns;

  for (int i = 0; i < 4; i++) {
    auto txn = txn_mgr.Begin(nullptr, IsolationLevel::READ_COMMITTED);
    ASSERT_EQ(txn->GetTransactionId(), i);
    txns.push_back(txn);
  }

  // Thread 0 gets an S-lock on rid
  std::thread t0([&] {
    bool res = lock_mgr.LockShared(txns[0], rid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[0]);
    CheckTxnLockSize(txns[0], 1, 0);
    // block t2 for 100 ms
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    res = lock_mgr.Unlock(txns[0], rid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[0]);
    CheckTxnLockSize(txns[0], 0, 0);
  });
  // Thread 1 gets an S-lock on rid, too
  std::thread t1([&] {
    bool res = lock_mgr.LockShared(txns[1], rid);
    EXPECT_TRUE(res);
    CheckGrowing(txns[1]);
    CheckTxnLockSize(txns[1], 1, 0);
    // block t2 for 100 ms
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    res = lock_mgr.Unlock(txns[1], rid);
    EXPECT_TRUE(res);
    CheckShrinking(txns[1]);
    CheckTxnLockSize(txns[1], 0, 0);
  });
  /** Thread 2 wants to get an X-lock on rid after T0 and T1 got S-lock.
   * So it should be blocked for 100 ms
   * At the end of waiting, T3 will wake up and grab another S-lock, then sleep for another 100 ms
   * Finally, T2 should sleep for at least 180 ms
   * */
  std::thread t2([&] {
    // Put aside some time for txn1 and txn2 to grab S-lock
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    const std::chrono::time_point<std::chrono::steady_clock> start = std::chrono::steady_clock::now();
    // This step should be blocked on wait for at least 180 ms.
    bool res = lock_mgr.LockExclusive(txns[2], rid);
    const auto end = std::chrono::steady_clock::now();
    EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(), 180);
    EXPECT_TRUE(res);
    CheckGrowing(txns[2]);
    CheckTxnLockSize(txns[2], 0, 1);
  });
  // Thread 3 wants to get an S-lock after T2 issued X-lock request.
  // Because it is READ_COMMITTED, the S-lock should be granted immediately
  std::thread t3([&] {
    // Put aside some time for txn3 to request for X-lock
    std::this_thread::sleep_for(std::chrono::milliseconds(90));
    const std::chrono::time_point<std::chrono::steady_clock> start = std::chrono::steady_clock::now();
    bool res = lock_mgr.LockShared(txns[3], rid);
    const auto end = std::chrono::steady_clock::now();
    // should immediately get S-lock
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count(), 2);
    EXPECT_TRUE(res);
    CheckGrowing(txns[3]);
    CheckTxnLockSize(txns[3], 1, 0);
    // block T2 for another 100 ms
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    res = lock_mgr.Unlock(txns[3], rid);
    CheckShrinking(txns[3]);
    CheckTxnLockSize(txns[3], 0, 0);
  });

  t0.join();
  t1.join();
  t2.join();
  t3.join();

  // Need to call txn_mgr's abort
  for (auto txn : txns) {
    txn_mgr.Abort(txn);
    CheckAborted(txn);
    CheckTxnLockSize(txn, 0, 0);
    delete txn;
  }
}

TEST(LockManagerTest, READ_CMMITTEDTest) {
  /** Test the correctness of READ_COMMITTED level*/
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  RID rid1{0, 1};
  auto txn = txn_mgr.Begin(nullptr, IsolationLevel::READ_COMMITTED);
  ASSERT_EQ(txn->GetTransactionId(), 0);
  // Take S-lock on rid1
  bool res = lock_mgr.LockShared(txn, rid1);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 0);
  // Take X-lock on rid
  res = lock_mgr.LockExclusive(txn, rid);
  EXPECT_TRUE(res);
  CheckGrowing(txn);
  CheckTxnLockSize(txn, 1, 1);

  // Unlock rid1
  res = lock_mgr.Unlock(txn, rid1);
  EXPECT_TRUE(res);
  CheckShrinking(txn);
  CheckTxnLockSize(txn, 0, 1);

  // Unlock rid. This is NOT allowed!!!
  try {
    res = lock_mgr.Unlock(txn, rid);
    EXPECT_EQ(TransactionState::ABORTED, txn->GetState());
    txn_mgr.Abort(txn);
  } catch (TransactionAbortException &e) {
    EXPECT_EQ(TransactionState::ABORTED, txn->GetState());
    txn_mgr.Abort(txn);
  }

  CheckAborted(txn);

  delete txn;
}

void UpgradeTest() {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  Transaction txn(0);
  txn_mgr.Begin(&txn);

  bool res = lock_mgr.LockShared(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 1, 0);
  CheckGrowing(&txn);

  res = lock_mgr.LockUpgrade(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 0, 1);
  CheckGrowing(&txn);

  res = lock_mgr.Unlock(&txn, rid);
  EXPECT_TRUE(res);
  CheckTxnLockSize(&txn, 0, 0);
  CheckShrinking(&txn);

  txn_mgr.Commit(&txn);
  CheckCommitted(&txn);
}
TEST(LockManagerTest, UpgradeLockTest) { UpgradeTest(); }

TEST(LockManagerTest, GraphEdgeTest) {
  LockManager lock_mgr{};
  TransactionManager txn_mgr{&lock_mgr};
  const int num_nodes = 100;
  const int num_edges = num_nodes / 2;
  const int seed = 15445;
  std::srand(seed);

  // Create txn ids and shuffle
  std::vector<txn_id_t> txn_ids;
  txn_ids.reserve(num_nodes);
  for (int i = 0; i < num_nodes; i++) {
    txn_ids.emplace_back(i);
  }
  EXPECT_EQ(num_nodes, txn_ids.size());
  auto rng = std::default_random_engine{};
  std::shuffle(std::begin(txn_ids), std::end(txn_ids), rng);
  EXPECT_EQ(num_nodes, txn_ids.size());

  // Create edges by pairing adjacent txn_ids
  std::vector<std::pair<txn_id_t, txn_id_t>> edges;
  for (int i = 0; i < num_nodes; i += 2) {
    EXPECT_EQ(i / 2, lock_mgr.GetEdgeList().size());
    auto t1 = txn_ids[i];
    auto t2 = txn_ids[i + 1];
    lock_mgr.AddEdge(t1, t2);
    edges.emplace_back(t1, t2);
    EXPECT_EQ((i / 2) + 1, lock_mgr.GetEdgeList().size());
  }

  auto lock_mgr_edges = lock_mgr.GetEdgeList();
  EXPECT_EQ(num_edges, lock_mgr_edges.size());
  EXPECT_EQ(num_edges, edges.size());

  std::sort(lock_mgr_edges.begin(), lock_mgr_edges.end());
  std::sort(edges.begin(), edges.end());

  for (int i = 0; i < num_edges; i++) {
    EXPECT_EQ(edges[i], lock_mgr_edges[i]);
  }
}

TEST(LockManagerTest, BasicCycleTest) {
  LockManager lock_mgr{}; /* Use Deadlock detection */
  TransactionManager txn_mgr{&lock_mgr};

  /*** Create 0->1->0 cycle ***/
  lock_mgr.AddEdge(0, 1);
  lock_mgr.AddEdge(1, 0);
  EXPECT_EQ(2, lock_mgr.GetEdgeList().size());

  txn_id_t txn;
  EXPECT_EQ(true, lock_mgr.HasCycle(&txn));
  EXPECT_EQ(1, txn);

  lock_mgr.RemoveEdge(1, 0);
  EXPECT_EQ(false, lock_mgr.HasCycle(&txn));
}

TEST(LockManagerTest, BasicDeadlockDetectionTest) {
  LockManager lock_mgr{};
  cycle_detection_interval = std::chrono::milliseconds(500);
  TransactionManager txn_mgr{&lock_mgr};
  RID rid0{0, 0};
  RID rid1{1, 1};
  auto *txn0 = txn_mgr.Begin();
  auto *txn1 = txn_mgr.Begin();
  EXPECT_EQ(0, txn0->GetTransactionId());
  EXPECT_EQ(1, txn1->GetTransactionId());

  std::thread t0([&] {
    // Lock and sleep
    bool res = lock_mgr.LockExclusive(txn0, rid0);
    EXPECT_EQ(true, res);
    EXPECT_EQ(TransactionState::GROWING, txn0->GetState());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // This will block
    lock_mgr.LockExclusive(txn0, rid1);

    lock_mgr.Unlock(txn0, rid0);
    lock_mgr.Unlock(txn0, rid1);

    txn_mgr.Commit(txn0);
    EXPECT_EQ(TransactionState::COMMITTED, txn0->GetState());
  });

  std::thread t1([&] {
    // Sleep so T0 can take necessary locks
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bool res = lock_mgr.LockExclusive(txn1, rid1);
    EXPECT_EQ(res, true);
    EXPECT_EQ(TransactionState::GROWING, txn1->GetState());

    // This will block
    try {
      res = lock_mgr.LockExclusive(txn1, rid0);
      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
      txn_mgr.Abort(txn1);
    } catch (TransactionAbortException &e) {
      // std::cout << e.GetInfo() << std::endl;
      EXPECT_EQ(TransactionState::ABORTED, txn1->GetState());
      txn_mgr.Abort(txn1);
    }
  });

  // Sleep for enough time to break cycle
  std::this_thread::sleep_for(cycle_detection_interval * 2);

  t0.join();
  t1.join();

  delete txn0;
  delete txn1;
}
}  // namespace bustub
