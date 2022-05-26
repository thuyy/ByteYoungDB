#pragma once

#include "sql/CreateStatement.h"
#include "sql/Table.h"
#include "storage.h"

#include <string.h>
#include <unordered_map>

using namespace hsql;

inline uint64_t BKDRHash(const char* str, int len) {
  uint64_t seed = 131313;
  uint64_t hash = 0;
  while (0 != len--) {
    hash = hash * seed + (*str++);
  }
  return hash;
}

namespace std {
template <>
struct hash<TableName> {
 public:
  size_t operator()(const TableName& T) const {
    std::size_t hash_val = BKDRHash(T.schema, strlen(T.schema));
    hash_val ^= BKDRHash(T.name, strlen(T.name));
    return hash_val;
  }
};

template <>
struct equal_to<TableName> {
 public:
  bool operator()(const TableName& T1, const TableName& T2) const {
    return (strcmp(T1.schema, T2.schema) == 0 && strcmp(T1.name, T2.name) == 0);
  }
};
}  // namespace std

namespace bydb {

struct Index {
  char* name;
  std::vector<ColumnDefinition*> columns;
};

class Table {
 public:
  Table(char* schema, char* name, std::vector<ColumnDefinition*>* columns);
  ~Table();

  ColumnDefinition* getColumn(char* name);
  Index* getIndex(char* name);
  char* schema() { return schema_; };
  char* name() { return name_; };
  std::vector<ColumnDefinition*>* columns() { return &columns_; };
  std::vector<Index*>* indexes() { return &indexes_; };
  void addIndex(Index* index) { indexes_.push_back(index); };
  TableStore* getTableStore() { return tableStore_; };

 private:
  char* schema_;
  char* name_;
  std::vector<ColumnDefinition*> columns_;
  std::vector<Index*> indexes_;
  TableStore* tableStore_;
};

class MetaData {
 public:
  MetaData(){};
  ~MetaData(){};

  bool insertTable(Table* table);
  bool dropIndex(char* schema, char* name, char* indexName);
  bool dropTable(char* schema, char* name);
  bool dropSchema(char* schema);
  void getAllTables(std::vector<Table*>* tables);

  bool findSchema(char* schema);
  Table* getTable(char* schema, char* name);
  Index* getIndex(char* schema, char* name, char* index_name);

 private:
  std::unordered_map<TableName, Table*> table_map_;
};

extern MetaData g_meta_data;

}  // namespace bydb
