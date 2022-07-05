/**
 * transaction_test.cpp
 */

#include <atomic>
#include <cstdio>
#include <memory>
#include <random>
#include <string>
#include <utility>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/table_generator.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/executor_context.h"
#include "execution/executors/insert_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/update_plan.h"
#include "gtest/gtest.h"
#include "storage/b_plus_tree_test_util.h"  // NOLINT
#include "type/value_factory.h"

#define TEST_TIMEOUT_BEGIN                           \
  std::promise<bool> promisedFinished;               \
  auto futureResult = promisedFinished.get_future(); \
                              std::thread([](std::promise<bool>& finished) {
#define TEST_TIMEOUT_FAIL_END(X)                                                                  \
  finished.set_value(true);                                                                       \
  }, std::ref(promisedFinished)).detach();                                                        \
  EXPECT_TRUE(futureResult.wait_for(std::chrono::milliseconds(X)) != std::future_status::timeout) \
      << "Test Failed Due to Time Out";

namespace bustub {

class TransactionTest : public ::testing::Test {
 public:
  // This function is called before every test.
  void SetUp() override {
    ::testing::Test::SetUp();
    // For each test, we create a new DiskManager, BufferPoolManager, TransactionManager, and Catalog.
    disk_manager_ = std::make_unique<DiskManager>("executor_test.db");
    bpm_ = std::make_unique<BufferPoolManager>(2560, disk_manager_.get());
    page_id_t page_id;
    bpm_->NewPage(&page_id);
    lock_manager_ = std::make_unique<LockManager>();
    txn_mgr_ = std::make_unique<TransactionManager>(lock_manager_.get(), log_manager_.get());
    catalog_ = std::make_unique<Catalog>(bpm_.get(), lock_manager_.get(), log_manager_.get());
    // Begin a new transaction, along with its executor context.
    txn_ = txn_mgr_->Begin();
    exec_ctx_ =
        std::make_unique<ExecutorContext>(txn_, catalog_.get(), bpm_.get(), txn_mgr_.get(), lock_manager_.get());
    // Generate some test tables.
    TableGenerator gen{exec_ctx_.get()};
    gen.GenerateTestTables();

    execution_engine_ = std::make_unique<ExecutionEngine>(bpm_.get(), txn_mgr_.get(), catalog_.get());
  }

  // This function is called after every test.
  void TearDown() override {
    // Commit our transaction.
    txn_mgr_->Commit(txn_);
    // Shut down the disk manager and clean up the transaction.
    disk_manager_->ShutDown();
    remove("executor_test.db");
    delete txn_;
  };

  /** @return the executor context in our test class */
  ExecutorContext *GetExecutorContext() { return exec_ctx_.get(); }
  ExecutionEngine *GetExecutionEngine() { return execution_engine_.get(); }
  Transaction *GetTxn() { return txn_; }
  TransactionManager *GetTxnManager() { return txn_mgr_.get(); }
  Catalog *GetCatalog() { return catalog_.get(); }
  BufferPoolManager *GetBPM() { return bpm_.get(); }
  LockManager *GetLockManager() { return lock_manager_.get(); }

  // The below helper functions are useful for testing.

  const AbstractExpression *MakeColumnValueExpression(const Schema &schema, uint32_t tuple_idx,
                                                      const std::string &col_name) {
    uint32_t col_idx = schema.GetColIdx(col_name);
    auto col_type = schema.GetColumn(col_idx).GetType();
    allocated_exprs_.emplace_back(std::make_unique<ColumnValueExpression>(tuple_idx, col_idx, col_type));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeConstantValueExpression(const Value &val) {
    allocated_exprs_.emplace_back(std::make_unique<ConstantValueExpression>(val));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeComparisonExpression(const AbstractExpression *lhs, const AbstractExpression *rhs,
                                                     ComparisonType comp_type) {
    allocated_exprs_.emplace_back(std::make_unique<ComparisonExpression>(lhs, rhs, comp_type));
    return allocated_exprs_.back().get();
  }

  const AbstractExpression *MakeAggregateValueExpression(bool is_group_by_term, uint32_t term_idx) {
    allocated_exprs_.emplace_back(
        std::make_unique<AggregateValueExpression>(is_group_by_term, term_idx, TypeId::INTEGER));
    return allocated_exprs_.back().get();
  }

  const Schema *MakeOutputSchema(const std::vector<std::pair<std::string, const AbstractExpression *>> &exprs) {
    std::vector<Column> cols;
    cols.reserve(exprs.size());
    for (const auto &input : exprs) {
      if (input.second->GetReturnType() != TypeId::VARCHAR) {
        cols.emplace_back(input.first, input.second->GetReturnType(), input.second);
      } else {
        cols.emplace_back(input.first, input.second->GetReturnType(), MAX_VARCHAR_SIZE, input.second);
      }
    }
    allocated_output_schemas_.emplace_back(std::make_unique<Schema>(cols));
    return allocated_output_schemas_.back().get();
  }

 private:
  std::unique_ptr<TransactionManager> txn_mgr_;
  Transaction *txn_{nullptr};
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<LogManager> log_manager_ = nullptr;
  std::unique_ptr<LockManager> lock_manager_;
  std::unique_ptr<BufferPoolManager> bpm_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<ExecutorContext> exec_ctx_;
  std::unique_ptr<ExecutionEngine> execution_engine_;
  std::vector<std::unique_ptr<AbstractExpression>> allocated_exprs_;
  std::vector<std::unique_ptr<Schema>> allocated_output_schemas_;
  static constexpr uint32_t MAX_VARCHAR_SIZE = 128;
};

// --- Helper functions ---
void CheckGrowing(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::GROWING); }

void CheckShrinking(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::SHRINKING); }

void CheckAborted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::ABORTED); }

void CheckCommitted(Transaction *txn) { EXPECT_EQ(txn->GetState(), TransactionState::COMMITTED); }

void CheckTxnLockSize(Transaction *txn, size_t shared_size, size_t exclusive_size) {
  EXPECT_EQ(txn->GetSharedLockSet()->size(), shared_size);
  EXPECT_EQ(txn->GetExclusiveLockSet()->size(), exclusive_size);
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, SimpleInsertRollbackTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn1: abort
  // txn2: SELECT * FROM empty_table2;
  auto txn1 = GetTxnManager()->Begin();
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
  // Create insert plan node
  auto table_info = exec_ctx1->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());
  GetTxnManager()->Abort(txn1);
  delete txn1;

  // Iterate through table make sure that values were not inserted.
  auto txn2 = GetTxnManager()->Begin();
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto colB = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

  // Size
  ASSERT_EQ(result_set.size(), 0);
  std::vector<RID> rids;

  GetTxnManager()->Commit(txn2);
  delete txn2;
}

// NOLINTNEXTLINE
TEST_F(TransactionTest, DirtyReadsTest) {
  // txn1: INSERT INTO empty_table2 VALUES (200, 20), (201, 21), (202, 22)
  // txn2: SELECT * FROM empty_table2;
  // txn1: abort
  auto txn1 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(20)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(21)};
  std::vector<Value> val3{ValueFactory::GetIntegerValue(202), ValueFactory::GetIntegerValue(22)};
  std::vector<std::vector<Value>> raw_vals{val1, val2, val3};
  // Create insert plan node
  auto table_info = exec_ctx1->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  //  auto index_info = exec_ctx1->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
  //      txn1, "index1", "empty_table2", table_info->schema_, *key_schema, {0}, 8);

  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());

  // Iterate through table to read the tuples.
  auto txn2 = GetTxnManager()->Begin(nullptr, IsolationLevel::READ_UNCOMMITTED);
  auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto colB = MakeColumnValueExpression(schema, 0, "colB");
  auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});
  SeqScanPlanNode scan_plan{out_schema, nullptr, table_info->oid_};

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_plan, &result_set, txn2, exec_ctx2.get());

  GetTxnManager()->Abort(txn1);
  delete txn1;

  // First value
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
  ASSERT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 20);

  // Second value
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
  ASSERT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 21);

  // Third value
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 202);
  ASSERT_EQ(result_set[2].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 22);

  // Size
  ASSERT_EQ(result_set.size(), 3);

  GetTxnManager()->Commit(txn2);
  delete txn2;
  delete key_schema;
}

/** txn1: INSERT INTO empty_table2 VALUES (200, 1000), (201, 500)
 *  "200" and "201" can be considered as UserID, "1000" and "500" can be considered as money
 * txn1: commit
 * | txn2: Tansfer 250 from "200"'s account to "201"'s
 * | txn3: Multiply 1.05 to both "200"'s and "201"'s accounts
 * | txn2: SELECT * FROM empty_table2
 * | txn3: SELECT * FROM empty_table2
 * | txn2: commit
 * | txn3: commit
 * txn4: SELECT * FROM empty_table2
 * txn4: commit
 * */
TEST_F(TransactionTest, RepeatableTest) {
  // ------- txn1 -------
  auto txn1 = GetTxnManager()->Begin();
  auto exec_ctx1 = std::make_unique<ExecutorContext>(txn1, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());
  // Create Values to insert
  std::vector<Value> val1{ValueFactory::GetIntegerValue(200), ValueFactory::GetIntegerValue(1000)};
  std::vector<Value> val2{ValueFactory::GetIntegerValue(201), ValueFactory::GetIntegerValue(500)};
  std::vector<std::vector<Value>> raw_vals{val1, val2};
  // Create insert plan node
  auto table_info = exec_ctx1->GetCatalog()->GetTable("empty_table2");
  InsertPlanNode insert_plan{std::move(raw_vals), table_info->oid_};

  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  //  auto index_info = exec_ctx1->GetCatalog()->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
  //      txn1, "index1", "empty_table2", table_info->schema_, *key_schema, {0}, 8);

  GetExecutionEngine()->Execute(&insert_plan, nullptr, txn1, exec_ctx1.get());
  GetTxnManager()->Commit(txn1);
  delete txn1;
  delete key_schema;

  // -------- txn2 ---------
  std::thread t2([&] {
    auto txn2 = GetTxnManager()->Begin();
    auto exec_ctx2 = std::make_unique<ExecutorContext>(txn2, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

    auto &schema = table_info->schema_;
    auto colA = MakeColumnValueExpression(schema, 0, "colA");
    auto colB = MakeColumnValueExpression(schema, 0, "colB");

    auto const200 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(200));
    auto const201 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(201));
    auto predicate_200 = MakeComparisonExpression(colA, const200, ComparisonType::Equal);
    auto predicate_201 = MakeComparisonExpression(colA, const201, ComparisonType::Equal);
    auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});

    SeqScanPlanNode scan_200_plan{out_schema, predicate_200, table_info->oid_};
    SeqScanPlanNode scan_201_plan{out_schema, predicate_201, table_info->oid_};

    std::vector<Tuple> result_set_200;
    std::vector<Tuple> result_set_201;
    GetExecutionEngine()->Execute(&scan_200_plan, &result_set_200, txn2, exec_ctx2.get());
    GetExecutionEngine()->Execute(&scan_201_plan, &result_set_201, txn2, exec_ctx2.get());
    // First value
    EXPECT_EQ(result_set_200[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
    EXPECT_EQ(result_set_200[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 1000);
    // Second value
    EXPECT_EQ(result_set_201[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
    EXPECT_EQ(result_set_201[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 500);
    // Size
    EXPECT_EQ(result_set_200.size(), 1);
    EXPECT_EQ(result_set_201.size(), 1);

    // Update "200" and "201"
    UpdateInfo add250(UpdateType::Add, 250);
    UpdateInfo sub250(UpdateType::Add, -250);
    std::unordered_map<uint32_t, UpdateInfo> sub_200_attr{{1, sub250}};
    std::unordered_map<uint32_t, UpdateInfo> add_201_attr{{1, add250}};

    UpdatePlanNode sub_200(&scan_200_plan, table_info->oid_, sub_200_attr);
    UpdatePlanNode add_201(&scan_201_plan, table_info->oid_, add_201_attr);

    // Before update, sleep for sometime to verify that T3 cannot get any X-locks
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    GetExecutionEngine()->Execute(&sub_200, nullptr, txn2, exec_ctx2.get());
    GetExecutionEngine()->Execute(&add_201, nullptr, txn2, exec_ctx2.get());

    // Read again, test repeatable read
    result_set_200.clear();
    result_set_201.clear();
    GetExecutionEngine()->Execute(&scan_200_plan, &result_set_200, txn2, exec_ctx2.get());
    GetExecutionEngine()->Execute(&scan_201_plan, &result_set_201, txn2, exec_ctx2.get());
    // First value
    EXPECT_EQ(result_set_200[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
    EXPECT_EQ(result_set_200[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 750);
    // Second value
    EXPECT_EQ(result_set_201[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
    EXPECT_EQ(result_set_201[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>(), 750);
    // Size
    EXPECT_EQ(result_set_200.size(), 1);
    EXPECT_EQ(result_set_201.size(), 1);

    GetTxnManager()->Commit(txn2);
    delete txn2;
  });

  // -------- txn3 ---------
  std::thread t3([&] {
    auto txn3 = GetTxnManager()->Begin();
    auto exec_ctx3 = std::make_unique<ExecutorContext>(txn3, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

    // Manually grab X-locks over rids in the table
    // We CANNOT grab S-locks frist then upgrade because It will cause dead lock with T2
    // which results in the abortion of this txn.
    // Sleep for some time so that T2 could get all locks
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    for (auto it = table_info->table_->Begin(txn3); it != table_info->table_->End(); it++)
      exec_ctx3->GetLockManager()->LockExclusive(txn3, it->GetRid());

    auto &schema = table_info->schema_;
    auto colA = MakeColumnValueExpression(schema, 0, "colA");
    auto colB = MakeColumnValueExpression(schema, 0, "colB");

    auto const200 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(200));
    auto const201 = MakeConstantValueExpression(ValueFactory::GetIntegerValue(201));
    auto predicate_200 = MakeComparisonExpression(colA, const200, ComparisonType::Equal);
    auto predicate_201 = MakeComparisonExpression(colA, const201, ComparisonType::Equal);
    auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});

    SeqScanPlanNode scan_200_plan{out_schema, predicate_200, table_info->oid_};
    SeqScanPlanNode scan_201_plan{out_schema, predicate_201, table_info->oid_};

    std::vector<Tuple> result_set_200;
    std::vector<Tuple> result_set_201;
    GetExecutionEngine()->Execute(&scan_200_plan, &result_set_200, txn3, exec_ctx3.get());
    GetExecutionEngine()->Execute(&scan_201_plan, &result_set_201, txn3, exec_ctx3.get());
    // First value
    EXPECT_EQ(result_set_200[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
    // Second value
    EXPECT_EQ(result_set_201[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
    // colB should sum to 1500
    int32_t colB200 = result_set_200[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>();
    int32_t colB201 = result_set_201[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>();
    EXPECT_EQ(colB200 + colB201, 1500);
    // Size
    EXPECT_EQ(result_set_200.size(), 1);
    EXPECT_EQ(result_set_201.size(), 1);
    std::cout << "txn 3 finished scan\n";

    // Multiply colB by 1.1 in "200" and "201"
    UpdateInfo mul200(UpdateType::Set, colB200 * 1.1);
    UpdateInfo mul201(UpdateType::Set, colB201 * 1.1);
    std::unordered_map<uint32_t, UpdateInfo> mul_200_attr{{1, mul200}};
    std::unordered_map<uint32_t, UpdateInfo> mul_201_attr{{1, mul201}};

    UpdatePlanNode mul_200(&scan_200_plan, table_info->oid_, mul_200_attr);
    UpdatePlanNode mul_201(&scan_201_plan, table_info->oid_, mul_201_attr);

    GetExecutionEngine()->Execute(&mul_200, nullptr, txn3, exec_ctx3.get());
    GetExecutionEngine()->Execute(&mul_201, nullptr, txn3, exec_ctx3.get());

    // Read again, test repeatable read
    result_set_200.clear();
    result_set_201.clear();
    GetExecutionEngine()->Execute(&scan_200_plan, &result_set_200, txn3, exec_ctx3.get());
    GetExecutionEngine()->Execute(&scan_201_plan, &result_set_201, txn3, exec_ctx3.get());
    // First value
    EXPECT_EQ(result_set_200[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
    // Second value
    EXPECT_EQ(result_set_201[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
    // They should sum to 1500*1.1 = 1650
    colB200 = result_set_200[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>();
    colB201 = result_set_201[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>();
    EXPECT_EQ(colB200 + colB201, 1650);
    // Size
    EXPECT_EQ(result_set_200.size(), 1);
    EXPECT_EQ(result_set_201.size(), 1);

    GetTxnManager()->Commit(txn3);
    delete txn3;
  });

  t2.join();
  t3.join();

  // ------- txn4 --------
  // iterate through the table to ensure consistency
  auto txn4 = GetTxnManager()->Begin();
  auto exec_ctx4 = std::make_unique<ExecutorContext>(txn4, GetCatalog(), GetBPM(), GetTxnManager(), GetLockManager());

  auto &schema = table_info->schema_;
  auto colA = MakeColumnValueExpression(schema, 0, "colA");
  auto colB = MakeColumnValueExpression(schema, 0, "colB");

  auto out_schema = MakeOutputSchema({{"colA", colA}, {"colB", colB}});

  SeqScanPlanNode scan_all_plan{out_schema, nullptr, table_info->oid_};

  std::vector<Tuple> result_set;
  GetExecutionEngine()->Execute(&scan_all_plan, &result_set, txn4, exec_ctx4.get());

  EXPECT_EQ(result_set.size(), 2);
  // First value
  EXPECT_EQ(result_set[0].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 200);
  // Second value
  EXPECT_EQ(result_set[1].GetValue(out_schema, out_schema->GetColIdx("colA")).GetAs<int32_t>(), 201);
  // They should sum to 1500*1.1 = 1650
  int32_t colB200 = result_set[0].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>();
  int32_t colB201 = result_set[1].GetValue(out_schema, out_schema->GetColIdx("colB")).GetAs<int32_t>();
  EXPECT_EQ(colB200 + colB201, 1650);
  // report the values
  std::cout << "colA = 200, colB = " << colB200 << "; colA = 201, colB = " << colB201 << std::endl;

  delete txn4;
}

}  // namespace bustub
