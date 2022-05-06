#pragma once

#include "sql/ColumnType.h"
#include "sql/Table.h"
#include "sql/statements.h"

#include <string>

using namespace hsql;

namespace bydb {
inline bool IsDataTypeSupport(DataType type) {
  return (type == DataType::INT || type == DataType::LONG ||
          type == DataType::CHAR || type == DataType::VARCHAR);
}

inline std::string TableNameToString(char* schema, char* name) {
  std::string str =
      (schema == nullptr) ? name : schema + std::string("/") + name;
  return str;
}

inline void SetTableName(TableName& table_name, char* schema, char* name) {
  table_name.schema = schema;
  table_name.name = name;
}

const char* StmtTypeToString(StatementType type);
const char* DataTypeToString(DataType type);
const char* DropTypeToString(DropType type);

}  // namespace bydb