#include "util.h"

namespace bydb {
const char* StmtTypeToString(StatementType type) {
  switch (type) {
    case kStmtError:
      return "ERROR";
    case kStmtSelect:
      return "SELECT";
    case kStmtImport:
      return "IMPORT";
    case kStmtInsert:
      return "INSERT";
    case kStmtUpdate:
      return "UPDATE";
    case kStmtDelete:
      return "DELETE";
    case kStmtCreate:
      return "CREATE";
    case kStmtDrop:
      return "DROP";
    case kStmtPrepare:
      return "PREPARE";
    case kStmtExecute:
      return "EXECUTE";
    case kStmtExport:
      return "EXPORT";
    case kStmtRename:
      return "RENAME";
    case kStmtAlter:
      return "ALTER";
    case kStmtShow:
      return "SHOW";
    case kStmtTransaction:
      return "TRANSACTION";
    default:
      return "UNKOUWN";
  }
}

const char* DataTypeToString(DataType type) {
  switch (type) {
    case DataType::CHAR:
      return "CHAR";
    case DataType::DATE:
      return "DATE";
    case DataType::DATETIME:
      return "DATETIME";
    case DataType::DECIMAL:
      return "DECIMAL";
    case DataType::DOUBLE:
      return "DOUBLE";
    case DataType::FLOAT:
      return "FLOAT";
    case DataType::INT:
      return "INT";
    case DataType::LONG:
      return "LONG";
    case DataType::REAL:
      return "REAL";
    case DataType::SMALLINT:
      return "SMALLINT";
    case DataType::TEXT:
      return "TEXT";
    case DataType::TIME:
      return "TIME";
    case DataType::VARCHAR:
      return "VARCHAR";
    default:
      return "UNKNOWN";
  }
}

const char* DropTypeToString(DropType type) {
  switch (type) {
    case kDropTable:
      return "DropTable";
    case kDropSchema:
      return "DropSchema";
    case kDropIndex:
      return "DropIndex";
    case kDropView:
      return "DropView";
    case kDropPreparedStatement:
      return "DropPreparedStatement";
    default:
      return "UNKNOWN";
  }
}

}  // namespace bydb