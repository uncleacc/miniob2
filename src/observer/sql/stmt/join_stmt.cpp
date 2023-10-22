#include "join_stmt.h"

JoinStmt::~JoinStmt() 
{
  if (filter_ != nullptr) {
    delete filter_;
  }
}

RC JoinStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *table_map,
    const JoinSqlNode &sql_node, JoinStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt = nullptr;

  // 检查比较连接条件
  if (sql_node.conditions.size() == 0) {
    return RC::INVALID_ARGUMENT;
  }
  JoinStmt *tmp_stmt = new JoinStmt();
  // 创建过滤语句
  rc = FilterStmt::create(db, default_table, table_map, sql_node.conditions.data(), 
    sql_node.conditions.size(), tmp_stmt->filter_);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }
  // 设置表名
  tmp_stmt->t_name_ = sql_node.right_rel;
  // 设置table
  if (table_map->count(tmp_stmt->t_name_) == 0) {
    return RC::INTERNAL;
  }
  auto iter = table_map->find(tmp_stmt->t_name_);
  tmp_stmt->table_ = iter->second;
  // OK
  stmt = tmp_stmt;
  return rc;
}