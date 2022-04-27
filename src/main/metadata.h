#pragma once

#include "sql/CreateStatement.h"
#include "sql/Table.h"

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
  std::vector<char*> columns;
};

struct Table {
  Table() {
    name.schema = nullptr;
    name.name = nullptr;
    ref = nullptr;
  }

  TableName name;
  TableRef* ref;
  std::vector<ColumnDefinition*> columns;
  std::vector<Index*> indexes;
};

class MetaData {
 public:
  MetaData(){};
  ~MetaData(){};

  bool insertTable(Table* table);
  bool dropTable(TableName& table_name);
  Table* getTable(TableName& table_name);
  void dropSchema(char* schema);

 private:
  std::unordered_map<TableName, Table*> table_map_;
};

extern MetaData g_meta_data;

}  // namespace bydb