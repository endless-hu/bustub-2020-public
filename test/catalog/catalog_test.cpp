//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "catalog/table_generator.h"
#include "execution/executor_context.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

TEST(CatalogTest, CreateTable1) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);

  const std::string table_name{"foobar"};

  // The table shouldn't exist in the catalog yet
  EXPECT_EQ(catalog->GetTable(table_name), nullptr);

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  // Table creation should succeed
  Schema schema{columns};
  auto *table_info = catalog->CreateTable(nullptr, table_name, schema);
  // check the created table info
  EXPECT_EQ(table_info->name_, "foobar");
  columns.clear();
  EXPECT_EQ(table_info->schema_.GetColumnCount(), 2);
  columns = table_info->schema_.GetColumns();
  EXPECT_EQ(columns[0].GetName(), "A");
  EXPECT_EQ(columns[0].GetType(), TypeId::INTEGER);
  EXPECT_EQ(columns[1].GetName(), "B");
  EXPECT_EQ(columns[1].GetType(), TypeId::BOOLEAN);

  // Querying the table name should now succeed
  EXPECT_EQ(catalog->GetTable(table_name), table_info);

  // Querying the table OID should also succeed
  const auto table_oid = table_info->oid_;
  EXPECT_EQ(catalog->GetTable(table_oid), table_info);

  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, CreateTable2) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);

  const std::string table_name{"foobar"};

  // The table shouldn't exist in the catalog yet
  EXPECT_EQ(catalog->GetTable(table_name), nullptr);

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  // Table creation should succeed
  Schema schema{columns};
  auto *table_info = catalog->CreateTable(nullptr, table_name, schema);
  // check the created table info
  EXPECT_EQ(table_info->name_, "foobar");
  columns.clear();
  EXPECT_EQ(table_info->schema_.GetColumnCount(), 2);
  columns = table_info->schema_.GetColumns();
  EXPECT_EQ(columns[0].GetName(), "A");
  EXPECT_EQ(columns[0].GetType(), TypeId::INTEGER);
  EXPECT_EQ(columns[1].GetName(), "B");
  EXPECT_EQ(columns[1].GetType(), TypeId::BOOLEAN);

  // Querying the table name should now succeed
  EXPECT_EQ(catalog->GetTable(table_name), table_info);

  // Subsequent attempt to create table with the same name should fail
  EXPECT_EQ(catalog->CreateTable(nullptr, table_name, schema), nullptr);

  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, CreateTable3) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);

  const std::string table_name{"foobar"};

  // The table shouldn't exist in the catalog yet
  EXPECT_EQ(catalog->GetTable(table_name), nullptr);

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema{columns};
  auto *table_info_0 = catalog->CreateTable(nullptr, table_name, schema);

  // Querying the table name should now succeed
  auto *table_info_1 = catalog->GetTable(table_name);

  // The metdata returned by GetTable() should be equivalent
  // to the metadata returned on table construction
  EXPECT_EQ(table_info_0, table_info_1);
  EXPECT_EQ(table_info_0->oid_, table_info_1->oid_);
  EXPECT_EQ(table_info_0->name_, table_info_1->name_);

  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);

  const std::string table_name{"foobar"};

  // The table shouldn't exist in the catalog yet.
  EXPECT_EQ(catalog->GetTable(table_name), nullptr);

  // Put the table into the catalog.
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema{columns};
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);

  // Catalog lookups should succeed
  {
    EXPECT_EQ(table_metadata, catalog->GetTable(table_metadata->oid_));
    EXPECT_EQ(table_metadata, catalog->GetTable(table_name));
  }

  // Basic empty table attributes
  {
    EXPECT_EQ(table_metadata->table_->GetFirstPageId(), 0);
    EXPECT_EQ(table_metadata->name_, table_name);
    EXPECT_EQ(table_metadata->schema_.GetColumnCount(), columns.size());
    for (std::size_t i = 0; i < columns.size(); ++i) {
      EXPECT_EQ(table_metadata->schema_.GetColumns()[i].GetName(), columns[i].GetName());
      EXPECT_EQ(table_metadata->schema_.GetColumns()[i].GetType(), columns[i].GetType());
    }
  }

  // Try inserting a tuple and checking that the catalog lookup gives us the right table
  {
    std::vector<Value> values{};
    values.emplace_back(ValueFactory::GetIntegerValue(15445));
    values.emplace_back(ValueFactory::GetBooleanValue(false));
    Tuple tuple(values, &schema);

    Transaction txn(0);
    RID rid;
    table_metadata->table_->InsertTuple(tuple, &rid, &txn);

    auto table_iter = catalog->GetTable(table_name)->table_->Begin(&txn);
    EXPECT_EQ((*table_iter).GetValue(&schema, 0).CompareEquals(tuple.GetValue(&schema, 0)), CmpBool::CmpTrue);
    EXPECT_EQ((*table_iter).GetValue(&schema, 1).CompareEquals(tuple.GetValue(&schema, 1)), CmpBool::CmpTrue);
    EXPECT_EQ(++table_iter, catalog->GetTable(table_name)->table_->End());
  }
}

// Vanilla index creation for valid table
TEST(CatalogTest, CreateIndex1) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const std::string table_name{"foobar"};
  const std::string index_name{"index1"};

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::BIGINT);
  columns.emplace_back("B", TypeId::BOOLEAN);
  Schema schema{columns};
  EXPECT_NE(catalog->CreateTable(txn.get(), table_name, schema), nullptr);

  // No indexes should exist for the table
  const auto table_indexes1 = catalog->GetTableIndexes(table_name);
  EXPECT_TRUE(table_indexes1.empty());

  // Construction of an index for the table should succeed
  std::vector<Column> key_columns{};
  std::vector<uint32_t> key_attrs{};
  key_columns.emplace_back("A", TypeId::BIGINT);
  key_attrs.emplace_back(0);
  Schema key_schema{key_columns};
  auto *index = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(txn.get(), index_name, table_name,
                                                                               schema, key_schema, key_attrs, 8);
  EXPECT_NE(index, nullptr);

  // Querying the table indexes should return our index
  const auto table_indexes2 = catalog->GetTableIndexes(table_name);
  EXPECT_EQ(table_indexes2.size(), 1);
  EXPECT_EQ(table_indexes2[0], index);

  remove("catalog_test.db");
  remove("catalog_test.log");
}

// Attempts to create an index with duplicate name should fail
TEST(CatalogTest, CreateIndex2) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const std::string table_name{"foobar"};
  const std::string index_name{"index1"};

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::BIGINT);
  columns.emplace_back("B", TypeId::BOOLEAN);
  Schema schema{columns};
  catalog->CreateTable(nullptr, table_name, schema);

  // No indexes should exist for the table
  const auto table_indexes1 = catalog->GetTableIndexes(table_name);
  EXPECT_TRUE(table_indexes1.empty());

  // Construct an index for the table
  std::vector<Column> key_columns{};
  std::vector<uint32_t> key_attrs{};
  key_columns.emplace_back("A", TypeId::BIGINT);
  key_attrs.emplace_back(0);

  Schema key_schema{key_columns};

  // Index construction should succeed
  auto *index = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(txn.get(), index_name, table_name,
                                                                               schema, key_schema, key_attrs, 8);

  // Querying the table indexes should return our index
  const auto table_indexes2 = catalog->GetTableIndexes(table_name);
  EXPECT_EQ(table_indexes2.size(), 1);
  EXPECT_EQ(table_indexes2[0], index);

  // Subsequent attempt to create an index with the same name should fail
  auto create_index_f = [&]() -> IndexInfo * {
    return catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(txn.get(), index_name, table_name, schema,
                                                                          key_schema, key_attrs, 8);
  };
  EXPECT_EQ(create_index_f(), nullptr);

  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, CreateIndex3) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);

  Transaction txn{0};

  auto exec_ctx = std::make_unique<ExecutorContext>(&txn, catalog.get(), bpm.get(), nullptr, nullptr);

  TableGenerator gen{exec_ctx.get()};
  gen.GenerateTestTables();

  auto *table_info = exec_ctx->GetCatalog()->GetTable("test_1");

  Schema &schema = table_info->schema_;
  auto itr = table_info->table_->Begin(&txn);
  auto tuple = *itr;

  std::vector<Column> key_columns{Column{"A", TypeId::BIGINT}};
  Schema key_schema{key_columns};

  auto *index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(&txn, "index1", "test_1", schema,
                                                                                    key_schema, {0}, 8);

  std::vector<RID> index_rid{};
  index_info->index_->ScanKey(tuple.KeyFromTuple(schema, key_schema, index_info->index_->GetKeyAttrs()), &index_rid,
                              &txn);
  EXPECT_EQ(tuple.GetRid().Get(), index_rid[0].Get());
}

// Vanilla index queries by name
TEST(CatalogTest, QueryIndex1) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const std::string table_name{"foobar"};
  const std::string index_name{"index1"};

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::BIGINT);
  columns.emplace_back("B", TypeId::BOOLEAN);
  Schema schema{columns};
  catalog->CreateTable(nullptr, table_name, schema);

  // Querying for the index should fail
  EXPECT_EQ(catalog->GetIndex(index_name, table_name), nullptr);

  // Construct an index for the table
  std::vector<Column> key_columns{};
  std::vector<uint32_t> key_attrs{};
  key_columns.emplace_back("A", TypeId::BIGINT);
  key_attrs.emplace_back(0);

  Schema key_schema{key_columns};

  // Index construction should succeed
  auto create_index_f = [&]() -> IndexInfo * {
    return catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(txn.get(), index_name, table_name, schema,
                                                                          key_schema, key_attrs, 8);
  };
  IndexInfo *index_info;
  EXPECT_NO_THROW(index_info = create_index_f());

  // Querying the table indexes should return our index
  EXPECT_EQ(index_info, catalog->GetIndex(index_name, table_name));
  EXPECT_EQ(index_info, catalog->GetIndex(index_info->index_oid_));

  remove("catalog_test.db");
  remove("catalog_test.log");
}

// Vanilla index queries by index OID
TEST(CatalogTest, QueryIndex2) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const std::string table_name{"foobar"};
  const std::string index_name{"index1"};

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::BIGINT);
  columns.emplace_back("B", TypeId::BOOLEAN);
  Schema schema{columns};
  auto *table_info = catalog->CreateTable(txn.get(), table_name, schema);
  EXPECT_NE(table_info, nullptr);

  // Querying for the index should fail
  EXPECT_EQ(catalog->GetIndex(index_name, table_name), nullptr);

  // Construct an index for the table
  std::vector<Column> key_columns{};
  std::vector<uint32_t> key_attrs{};
  key_columns.emplace_back("A", TypeId::BIGINT);
  key_attrs.emplace_back(0);

  Schema key_schema{key_columns};

  // Index construction should succeed
  auto create_index_f = [&]() -> IndexInfo * {
    return catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(txn.get(), index_name, table_name, schema,
                                                                          key_schema, key_attrs, 8);
  };
  EXPECT_NE(create_index_f(), nullptr);

  // Querying the table indexes should return our index
  auto *index_info1 = catalog->GetIndex(index_name, table_name);

  const auto oid = index_info1->index_oid_;
  auto *index_info2 = catalog->GetIndex(oid);

  // Information retrieved from the two queries should match
  EXPECT_EQ(index_info1->index_oid_, index_info2->index_oid_);

  remove("catalog_test.db");
  remove("catalog_test.log");
}

// Query for nonexistent index on table should fail
TEST(CatalogTest, FailedQuery1) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const std::string table_name{"foobar"};

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::BIGINT);
  columns.emplace_back("B", TypeId::BOOLEAN);
  Schema schema{columns};
  EXPECT_NE(catalog->CreateTable(txn.get(), table_name, schema), nullptr);

  EXPECT_EQ(nullptr, catalog->GetIndex("index1", table_name));

  remove("catalog_test.db");
  remove("catalog_test.log");
}

// Query for index on nonexistent table should fail
TEST(CatalogTest, FailedQuery2) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  EXPECT_EQ(nullptr, catalog->GetIndex("index1", "invalid_table"));

  remove("catalog_test.db");
  remove("catalog_test.log");
}

// Query for nonexistent index OID should throw
TEST(CatalogTest, FailedQuery3) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const index_oid_t bad_oid = 1337;
  EXPECT_EQ(catalog->GetIndex(bad_oid), nullptr);

  remove("catalog_test.db");
  remove("catalog_test.log");
}

// Query for all indexes on nonexistent table should give empty collection
TEST(CatalogTest, FailedQuery4) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const auto indexes = catalog->GetTableIndexes("invalid_table");
  EXPECT_TRUE(indexes.empty());

  remove("catalog_test.db");
  remove("catalog_test.log");
}

// Query for all indexes on existing table with no
// indexes defined should return empty collection
TEST(CatalogTest, FailedQuery5) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const std::string table_name{"foobar"};

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{};
  columns.emplace_back("A", TypeId::BIGINT);
  columns.emplace_back("B", TypeId::BOOLEAN);
  Schema schema{columns};
  EXPECT_NE(nullptr, catalog->CreateTable(nullptr, table_name, schema));

  const auto indexes = catalog->GetTableIndexes(table_name);
  EXPECT_TRUE(indexes.empty());

  remove("catalog_test.db");
  remove("catalog_test.log");
}

// Should be able to create and interact with an index with a single BIGINT key
TEST(CatalogTest, IndexInteraction0) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const std::string table_name{"foobar"};
  const std::string index_name{"index1"};

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{{"A", TypeId::BIGINT}};
  Schema table_schema{columns};
  auto *table_info = catalog->CreateTable(nullptr, table_name, table_schema);

  // Construct an index for the table
  std::vector<Column> key_columns{{"A", TypeId::BIGINT}};
  std::vector<uint32_t> key_attrs{0};
  Schema key_schema{key_columns};

  // Index construction should succeed
  auto *index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      txn.get(), index_name, table_name, table_schema, key_schema, key_attrs, 8);
  auto *index = index_info->index_.get();

  // We should now be able to interect with the index
  Tuple tuple{std::vector<Value>{ValueFactory::GetBigIntValue(100)}, &table_schema};

  // Insert an entry
  RID rid{};
  const Tuple index_key = tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
  index->InsertEntry(index_key, rid, txn.get());

  // Scan should provide 1 result
  std::vector<RID> results{};
  index->ScanKey(index_key, &results, txn.get());
  ASSERT_EQ(1, results.size());

  // Delete the entry
  index->DeleteEntry(index_key, rid, txn.get());

  // Scan should now provide 0 results
  results.clear();
  index->ScanKey(index_key, &results, txn.get());
  ASSERT_TRUE(results.empty());

  remove("catalog_test.db");
  remove("catalog_test.log");
}

// Should be able to create and interact with an index that is keyed by two INTEGER values
TEST(CatalogTest, IndexInteraction1) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const std::string table_name{"foobar"};
  const std::string index_name{"index1"};

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{{"A", TypeId::INTEGER}, {"B", TypeId::INTEGER}};
  Schema table_schema{columns};
  auto *table_info = catalog->CreateTable(nullptr, table_name, table_schema);

  // Construct an index for the table
  std::vector<Column> key_columns{{"A", TypeId::INTEGER}, {"B", TypeId::INTEGER}};
  std::vector<uint32_t> key_attrs{0, 1};
  Schema key_schema{key_columns};

  // Index construction should succeed
  auto *index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      txn.get(), index_name, table_name, table_schema, key_schema, key_attrs, 8);
  auto *index = index_info->index_.get();

  // We should now be able to interect with the index
  Tuple tuple{std::vector<Value>{ValueFactory::GetBigIntValue(100), ValueFactory::GetIntegerValue(101)}, &table_schema};

  // Insert an entry
  RID rid{};
  const Tuple index_key = tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
  index->InsertEntry(index_key, rid, txn.get());

  // Scan should provide 1 result
  std::vector<RID> results{};
  index->ScanKey(index_key, &results, txn.get());
  ASSERT_EQ(1, results.size());

  // Delete the entry
  index->DeleteEntry(index_key, rid, txn.get());

  // Scan should now provide 0 results
  results.clear();
  index->ScanKey(index_key, &results, txn.get());
  ASSERT_TRUE(results.empty());

  remove("catalog_test.db");
  remove("catalog_test.log");
}

// Should be able to create and interact with an index that is keyed by a single INTEGER column
TEST(CatalogTest, IndexInteraction2) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const std::string table_name{"foobar"};
  const std::string index_name{"index1"};

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{{"A", TypeId::INTEGER}};
  Schema table_schema{columns};
  auto *table_info = catalog->CreateTable(nullptr, table_name, table_schema);

  // Construct an index for the table
  std::vector<Column> key_columns{{"A", TypeId::INTEGER}};
  std::vector<uint32_t> key_attrs{0};
  Schema key_schema{key_columns};

  // Index construction should succeed
  auto *index_info = catalog->CreateIndex<GenericKey<4>, RID, GenericComparator<4>>(
      txn.get(), index_name, table_name, table_schema, key_schema, key_attrs, 4);
  auto *index = index_info->index_.get();

  // We should now be able to interect with the index
  Tuple tuple{std::vector<Value>{ValueFactory::GetIntegerValue(100)}, &table_schema};

  // Insert an entry
  RID rid{};
  const Tuple index_key = tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
  index->InsertEntry(index_key, rid, txn.get());

  // Scan should provide 1 result
  std::vector<RID> results{};
  index->ScanKey(index_key, &results, txn.get());
  ASSERT_EQ(1, results.size());

  // Delete the entry
  index->DeleteEntry(index_key, rid, txn.get());

  // Scan should now provide 0 results
  results.clear();
  index->ScanKey(index_key, &results, txn.get());
  ASSERT_TRUE(results.empty());

  remove("catalog_test.db");
  remove("catalog_test.log");
}

TEST(CatalogTest, IndexInteraction3) {
  auto disk_manager = std::make_unique<DiskManager>("catalog_test.db");
  auto bpm = std::make_unique<BufferPoolManager>(32, disk_manager.get());
  auto catalog = std::make_unique<Catalog>(bpm.get(), nullptr, nullptr);
  auto txn = std::make_unique<Transaction>(0);

  const std::string table_name{"foobar"};
  const std::string index_name{"index1"};

  // Construct a new table and add it to the catalog
  std::vector<Column> columns{
      {"A", TypeId::SMALLINT}, {"B", TypeId::SMALLINT}, {"C", TypeId::SMALLINT}, {"D", TypeId::SMALLINT}};
  Schema table_schema{columns};
  auto *table_info = catalog->CreateTable(nullptr, table_name, table_schema);

  // Construct an index for the table
  std::vector<Column> key_columns{
      {"A", TypeId::SMALLINT}, {"B", TypeId::SMALLINT}, {"C", TypeId::SMALLINT}, {"D", TypeId::SMALLINT}};
  std::vector<uint32_t> key_attrs{0, 1, 2, 3};
  Schema key_schema{key_columns};

  // Index construction should succeed
  auto *index_info = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(
      txn.get(), index_name, table_name, table_schema, key_schema, key_attrs, 8);
  auto *index = index_info->index_.get();

  // We should now be able to interect with the index
  Tuple tuple{std::vector<Value>{ValueFactory::GetSmallIntValue(100), ValueFactory::GetSmallIntValue(101),
                                 ValueFactory::GetSmallIntValue(102), ValueFactory::GetSmallIntValue(103)},
              &table_schema};

  // Insert an entry
  RID rid{};
  const Tuple index_key = tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), index->GetKeyAttrs());
  index->InsertEntry(index_key, rid, txn.get());

  // Scan should provide 1 result
  std::vector<RID> results{};
  index->ScanKey(index_key, &results, txn.get());
  ASSERT_EQ(1, results.size());

  // Delete the entry
  index->DeleteEntry(index_key, rid, txn.get());

  // Scan should now provide 0 results
  results.clear();
  index->ScanKey(index_key, &results, txn.get());
  ASSERT_TRUE(results.empty());

  remove("catalog_test.db");
  remove("catalog_test.log");
}

}  // namespace bustub
