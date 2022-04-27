#include "sql/statements.h"
#include "sql/ColumnType.h"
#include "sql/Table.h"

#include <string>

using namespace hsql;

namespace bydb {
inline bool IsDataTypeSupport(DataType type) {
  return (type == DataType::INT || type == DataType::LONG||
          type == DataType::CHAR || type == DataType::VARCHAR);
}

inline std::string TableNameToString(TableName& table_name) {
  std::string name =
      (table_name.schema == nullptr)
          ? table_name.name
          : table_name.schema + std::string("/") + table_name.name;
  return name;
}

inline void SetTableName(TableName& table_name, char* schema, char* name) {
  table_name.schema = schema;
  table_name.name = name;
}

const char* StmtTypeToString(StatementType type);
const char* DataTypeToString(DataType type);
const char* DropTypeToString(DropType type);

}  // namespace bydb