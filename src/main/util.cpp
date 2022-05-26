#include "util.h"

#include <cstdint>
#include <cstring>
#include <iostream>

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

const char* ExprTypeToString(ExprType type) {
  switch (type) {
    case kExprLiteralFloat:
      return "ExprLiteralFloat";
    case kExprLiteralString:
      return "ExprLiteralString";
    case kExprLiteralInt:
      return "ExprLiteralInt";
    case kExprLiteralNull:
      return "ExprLiteralNull";
    case kExprLiteralDate:
      return "ExprLiteralDate";
    case kExprLiteralInterval:
      return "ExprLiteralInterval";
    case kExprStar:
      return "ExprStar";
    case kExprParameter:
      return "ExprParameter";
    case kExprColumnRef:
      return "ExprColumnRef";
    case kExprFunctionRef:
      return "ExprFunctionRef";
    case kExprOperator:
      return "ExprOperator";
    case kExprSelect:
      return "ExprSelect";
    case kExprHint:
      return "ExprHint";
    case kExprArray:
      return "ExprArray";
    case kExprArrayIndex:
      return "ExprArrayIndex";
    case kExprExtract:
      return "ExprExtract";
    case kExprCast:
      return "ExprCast";
    default:
      return "UNKNOWN";
  }
}

size_t ColumnTypeSize(ColumnType& type) {
  switch (type.data_type) {
    case DataType::INT:
      return sizeof(int32_t);
    case DataType::LONG:
      return sizeof(int64_t);
    case DataType::CHAR:
      return type.length + 1;
    case DataType::VARCHAR:
      return type.length + 1;
    default:
      return -1;
  }
}

/* 
INT32_MAX: 2,147,483,647
INT64_MAX: 9,223,372,036,854,775,807
add one more byte for the symbol
*/
#define MAX_INT32_LEN 11
#define MAX_INT64_LEN 20

void PrintTuples(std::vector<ColumnDefinition*>& columns,
                 std::vector<std::vector<Expr*>>& tuples) {
  /* Calculate offset and length for each column */
  size_t total_len = 0;
  std::vector<size_t> col_lens;
  for (auto col : columns) {
    size_t len = col->type.length;
    len = (strlen(col->name) > len) ? strlen(col->name) : len;
    if (col->type.data_type == DataType::INT) {
      len = (MAX_INT32_LEN > len) ? MAX_INT32_LEN : len;
    } else if (col->type.data_type == DataType::LONG) {
      len = (MAX_INT64_LEN > len) ? MAX_INT64_LEN : len;
    }
    len += 2; // reserve some space
    col_lens.push_back(len);
    total_len += len;
  }

  /* Print column names */
  for (size_t i = 0; i < columns.size(); i++) {
    std::cout.width(col_lens[i]);
    std::cout << columns[i]->name;
  }
  std::cout << std::endl;

  /* Print separators */
  std::cout << std::string(total_len, '-') << std::endl;

  /* Print each tuple */
  for (auto tup : tuples) {
    int i = 0;
    for (auto expr : tup) {
      std::cout.width(col_lens[i]);
      i++;
      switch (expr->type) {
        case kExprLiteralString:
          std::cout << expr->name;
          break;
        case kExprLiteralInt:
          std::cout << expr->ival;
          break;
        case kExprLiteralNull:
          std::cout << "NULL";
          break;
        default:
          break;
      }
    }
    std::cout << std::endl;
  }
}

}  // namespace bydb