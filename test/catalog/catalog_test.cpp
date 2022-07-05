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
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  (void)table_metadata;

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it

  // check the created table info
  EXPECT_EQ(table_metadata->name_, "potato");
  columns.clear();
  EXPECT_EQ(table_metadata->schema_.GetColumnCount(), 2);
  columns = table_metadata->schema_.GetColumns();
  EXPECT_EQ(columns[0].GetName(), "A");
  EXPECT_EQ(columns[0].GetType(), TypeId::INTEGER);
  EXPECT_EQ(columns[1].GetName(), "B");
  EXPECT_EQ(columns[1].GetType(), TypeId::BOOLEAN);

  delete catalog;
  delete bpm;
  delete disk_manager;
}

TEST(CatalogTest, CreateIndexTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  (void)table_metadata;
  // codes above are the same as the first test

  columns.clear();
  columns.emplace_back("A", TypeId::INTEGER);
  Schema key_schema(columns);

  // now test create index function
  std::string index_name = "bp_index";
  

  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
