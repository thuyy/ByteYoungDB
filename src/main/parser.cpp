#include "parser.h"
#include "metadata.h"
#include "util.h"

#include <cstdint>
#include <iostream>

using namespace hsql;

namespace bydb {

Parser::Parser() { result_ = nullptr; }

Parser::~Parser() {
  delete result_;
  result_ = nullptr;
}

bool Parser::parseStatement(std::string query) {
  result_ = new SQLParserResult;
  SQLParser::parse(query, result_);

  if (result_->isValid()) {
    return checkStmtsMeta();
  }

  return true;
}

bool Parser::checkStmtsMeta() {
  for (size_t i = 0; i < result_->size(); ++i) {
    const SQLStatement* stmt = result_->getStatement(i);
    if (checkMeta(stmt)) {
      return true;
    }
  }

  return false;
}

bool Parser::checkMeta(const SQLStatement* stmt) {
  switch (stmt->type()) {
    case kStmtSelect:
      return checkSelectStmt(static_cast<const SelectStatement*>(stmt));
    case kStmtInsert:
      return checkInsertStmt(static_cast<const InsertStatement*>(stmt));
    case kStmtUpdate:
      return checkUpdateStmt(static_cast<const UpdateStatement*>(stmt));
    case kStmtDelete:
      return checkDeleteStmt(static_cast<const DeleteStatement*>(stmt));
    case kStmtCreate:
      return checkCreateStmt(static_cast<const CreateStatement*>(stmt));
    case kStmtDrop:
      return checkDropStmt(static_cast<const DropStatement*>(stmt));
    case kStmtTransaction:
    case kStmtShow:
      return false;
    default:
      std::cout << "# ERROR: Statement type " << StmtTypeToString(stmt->type())
                << " is not supported now." << std::endl;
  }

  return true;
}

bool Parser::checkSelectStmt(const SelectStatement* stmt) {
  TableRef* table_ref = stmt->fromTable;
  Table* table = getTable(table_ref);
  if (table == nullptr) {
    std::cout << "# ERROR: Can not find table "
              << TableNameToString(table_ref->schema, table_ref->name)
              << std::endl;
    return true;
  }

  if (stmt->groupBy != nullptr) {
    std::cout << "# ERROR: Do not support 'Group By' clause" << std::endl;
    return true;
  }

  if (stmt->setOperations != nullptr) {
    std::cout << "# ERROR: Do not support Set Operation like 'UNION', "
                 "'Intersect', ect."
              << std::endl;
    return true;
  }

  if (stmt->withDescriptions != nullptr) {
    std::cout << "# ERROR: Do not support 'with' clause." << std::endl;
    return true;
  }

  if (stmt->lockings != nullptr) {
    std::cout << "# ERROR: Do not support 'lock' clause." << std::endl;
    return true;
  }

  if (stmt->selectList != nullptr) {
    for (auto expr : *stmt->selectList) {
      if (checkExpr(table, expr)) {
        return true;
      }
    }
  }

  if (stmt->whereClause != nullptr) {
    if (checkExpr(table, stmt->whereClause)) {
      return true;
    }
  }

  if (stmt->order != nullptr) {
    for (auto order : *stmt->order) {
      if (checkExpr(table, order->expr)) {
        return true;
      }
    }
  }

  if (stmt->limit != nullptr) {
    if (checkExpr(table, stmt->limit->limit)) {
      return true;
    }
    if (checkExpr(table, stmt->limit->offset)) {
      return true;
    }
  }

  return false;
}

bool Parser::checkInsertStmt(const InsertStatement* stmt) {
  if (stmt->type == kInsertSelect) {
    std::cout << "# ERROR: Do not support 'INSERT INTO ... SELECT ...'."
              << std::endl;
  }

  Table* table = g_meta_data.getTable(stmt->schema, stmt->tableName);
  if (table == nullptr) {
    std::cout << "# ERROR: Can not find table "
              << TableNameToString(stmt->schema, stmt->tableName) << std::endl;
    return true;
  }

  if (stmt->columns != nullptr) {
    for (auto col_name : *stmt->columns) {
      if (checkColumn(table, col_name)) {
        return true;
      }
    }
  }

  /* Prepare values for each columns in the table.
  If value was not provided for some columns, add NULL expr for then. */
  std::vector<Expr*> new_values;
  for (size_t i = 0; i < table->columns()->size(); i++) {
    auto col_def = (*table->columns())[i];
    if (stmt->columns != nullptr) {
      size_t j;
      for (j = 0; j < stmt->columns->size(); j++) {
        if (strcmp(col_def->name, (*stmt->columns)[j])) {
          break;
        }
      }
      if (j < stmt->columns->size()) {
        new_values.push_back((*stmt->values)[j]);
      } else {
        Expr* e = new Expr(kExprLiteralNull);
        new_values.push_back(e);
      }
    } else {
      if (i < stmt->values->size()) {
        new_values.push_back((*stmt->values)[i]);
      } else {
        Expr* e = new Expr(kExprLiteralNull);
        new_values.push_back(e);
      }
    }
  }

  stmt->values->assign(new_values.begin(), new_values.end());

  if (checkValues(table->columns(), stmt->values)) {
    return true;
  }

  return false;
}

bool Parser::checkUpdateStmt(const UpdateStatement* stmt) {
  TableRef* table_ref = stmt->table;
  Table* table = getTable(table_ref);
  if (table == nullptr) {
    std::cout << "# ERROR: Can not find table "
              << TableNameToString(table_ref->schema, table_ref->name)
              << std::endl;
    return true;
  }

  if (stmt->updates != nullptr) {
    for (auto update : *stmt->updates) {
      if (checkColumn(table, update->column)) {
        return true;
      }
      if (checkExpr(table, update->value)) {
        return true;
      }
    }
  }

  if (checkExpr(table, stmt->where)) {
    return true;
  }

  return false;
}

bool Parser::checkDeleteStmt(const DeleteStatement* stmt) {
  Table* table = g_meta_data.getTable(stmt->schema, stmt->tableName);
  if (table == nullptr) {
    std::cout << "# ERROR: Can not find table "
              << TableNameToString(table->schema(), table->name()) << std::endl;
    return true;
  }

  if (checkExpr(table, stmt->expr)) {
    return true;
  }

  return false;
}

Table* Parser::getTable(TableRef* table_ref) {
  if (table_ref->type != kTableName) {
    std::cout << "# ERROR: Only support ordinary table." << std::endl;
    return nullptr;
  }

  Table* table = g_meta_data.getTable(table_ref->schema, table_ref->name);
  if (table == nullptr) {
    std::cout << "# ERROR: Table "
              << TableNameToString(table_ref->schema, table_ref->name)
              << " did not exist!" << std::endl;
    return nullptr;
  }

  return table;
}

bool Parser::checkColumn(Table* table, char* col_name) {
  for (auto col_def : *table->columns()) {
    if (strcmp(col_name, col_def->name) == 0) {
      return false;
    }
  }

  std::cout << "# ERROR: Can not find column " << col_name << " in table "
            << TableNameToString(table->schema(), table->name()) << std::endl;
  return true;
}

bool Parser::checkExpr(Table* table, Expr* expr) {
  switch (expr->type) {
    case kExprLiteralFloat:
    case kExprLiteralString:
    case kExprLiteralInt:
      return false;
    case kExprSelect:
      return checkExpr(table, expr->expr);
    case kExprOperator: {
      if (expr->expr != nullptr && checkExpr(table, expr->expr)) {
        return true;
      }
      if (expr->expr2 != nullptr && checkExpr(table, expr->expr2)) {
        return true;
      }
      break;
    }
    case kExprColumnRef: {
      if (checkColumn(table, expr->name)) {
        return true;
      }
      break;
    }
    default:
      std::cout << "# ERROR: Unsupport opertation " << std::endl;
      return true;
  }

  return false;
}

bool Parser::checkValues(std::vector<ColumnDefinition*>* columns, std::vector<Expr*>* values) {
  for (size_t i = 0; i < columns->size(); i++) {
    auto col_def = (*columns)[i];
    auto expr = (*values)[i];

    switch (col_def->type.data_type) {
      case DataType::INT:
      case DataType::LONG:
        if (expr->type != kExprLiteralInt) {
          std::cout << "# ERROR: Invalid insert value type " << ExprTypeToString(expr->type)
                    << " for column " << col_def->name << std::endl;
          return true;
        }
        if (col_def->type.data_type == DataType::INT && expr->ival > INT32_MAX) {
          std::cout << "# ERROR: The value " << expr->ival << " exceed the limitation of INT32_MAX" << std::endl;
          return true;
        }
        break;
      case DataType::CHAR:
      case DataType::VARCHAR:
        if (expr->type != kExprLiteralString) {
          std::cout << "# ERROR: Invalid insert value type " << ExprTypeToString(expr->type)
                    << " for column " << col_def->name << std::endl;
          return true;
        }
        if (strlen(expr->name) > static_cast<size_t>(col_def->type.length)) {
          std::cout << "# ERROR: The value '" << expr->name << "' is too long for column " << col_def->name << std::endl;
          return true;
        }
        break;
      default:
        return true;
        break;
    }
  }

  return false;
}

bool Parser::checkCreateStmt(const CreateStatement* stmt) {
  switch (stmt->type) {
    case kCreateTable:
      if (checkCreateTableStmt(stmt)) {
        return true;
      }
      break;
    case kCreateIndex:
      if (checkCreateIndexStmt(stmt)) {
        return true;
      }
      break;
    default:
      std::cout << "# ERROR: Only support 'Create Table/Index'." << std::endl;
      return true;
  }

  return false;
}

bool Parser::checkCreateTableStmt(const CreateStatement* stmt) {
  // Check if the table already existed.
  if (g_meta_data.getTable(stmt->schema, stmt->tableName) != nullptr &&
      !stmt->ifNotExists) {
    std::cout << "# ERROR: Table "
              << TableNameToString(stmt->schema, stmt->tableName)
              << " already existed!" << std::endl;
    return true;
  }

  // Check each columns
  if (stmt->columns == nullptr || stmt->columns->size() == 0) {
    std::cout << "# ERROR: Valid column should be spicified in 'Create "
                 "Table' statement."
              << std::endl;
    return true;
  }

  for (auto col_def : *stmt->columns) {
    if (col_def == nullptr || col_def->name == nullptr) {
      std::cout << "# ERROR: Valid column should be spicified in 'Create "
                   "Table' statement."
                << std::endl;
      return true;
    }

    if (!IsDataTypeSupport(col_def->type.data_type)) {
      std::cout << "# ERROR: Unsupport data type "
                << DataTypeToString(col_def->type.data_type) << std::endl;
      return true;
    }
  }

  return false;
}

bool Parser::checkCreateIndexStmt(const CreateStatement* stmt) {
  if (g_meta_data.getIndex(stmt->schema, stmt->tableName, stmt->indexName) !=
          nullptr &&
      !stmt->ifNotExists) {
    std::cout << "# ERROR: Index " << stmt->indexName << "of "
              << TableNameToString(stmt->schema, stmt->tableName)
              << " already existed!" << std::endl;
    return true;
  }

  // Check if each column of this index existed.
  Table* table = g_meta_data.getTable(stmt->schema, stmt->tableName);
  for (auto idx_col : *stmt->indexColumns) {
    if (checkColumn(table, idx_col)) {
      return true;
    }
  }

  return false;
}

bool Parser::checkDropStmt(const DropStatement* stmt) {
  switch (stmt->type) {
    case kDropTable: {
      if (g_meta_data.getTable(stmt->schema, stmt->name) == nullptr &&
          !stmt->ifExists) {
        std::cout << "# ERROR: Table "
                  << TableNameToString(stmt->schema, stmt->name)
                  << " did not exist!" << std::endl;
        return true;
      }
      break;
    }
    case kDropSchema: {
      if (!g_meta_data.findSchema(stmt->schema) && !stmt->ifExists) {
        std::cout << "# ERROR: Schema " << stmt->schema << " did not exist"
                  << std::endl;
        return true;
      }
      break;
    }
    case kDropIndex: {
      if (g_meta_data.getIndex(stmt->schema, stmt->name, stmt->indexName) ==
              nullptr &&
          !stmt->ifExists) {
        std::cout << "# ERROR: Index " << stmt->indexName << " of "
                  << TableNameToString(stmt->schema, stmt->name)
                  << " did not exist!" << std::endl;
        return true;
      }
      break;
    }
    default:
      std::cout << "# ERROR: Not support drop statement "
                << DropTypeToString(stmt->type) << std::endl;
      return true;
  }

  return false;
}

}  // namespace bydb
