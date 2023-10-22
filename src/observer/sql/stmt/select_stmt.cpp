/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/join_stmt.h"
#include "sql/stmt/aggregation_stmt.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
  for (auto p : join_stmts_) {
    if (p != nullptr) {
      delete p;
    }
  }
}

static void wildcard_fields(Table *table, std::vector<Field> &field_metas)
{
  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    field_metas.push_back(Field(table, table_meta.field(i)));
  }
}

static void wildcard_query_exprs(Table *table, std::vector<Expression *> &expressions)
{
  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    expressions.emplace_back(new FieldExpr(Field(table, table_meta.field(i))));
  }
}

RC SelectStmt::create(Db *db, const SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  // 判断逗号和join是否同时存在
  vector<JoinStmt*> join_stmts;
  if (select_sql.relations.size() > 1 && select_sql.joins.size() > 0) {
    return RC::INTERNAL;  // 既有逗号又有join，语法不支持
  }

  // collect tables in `from` statement
  // 找到'from'后面的逗号连接的表
  std::vector<Table *> tables;
  std::unordered_map<std::string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    DEBUG_PRINT("debug: 找到表：%s\n", table_name);
    tables.push_back(table);
    table_map.insert(std::pair<std::string, Table *>(table_name, table));
  }

  // 找到'from'后面的join连接的表
  for (size_t i = 0; i < select_sql.joins.size(); i++) { // 从左向右
    const std::string &table_name = select_sql.joins[i].right_rel;
    if (table_name.empty()) {
      LOG_WARN("invalid argument. relation name is null. join index=%d", select_sql.joins.size() - i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name.c_str());
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name.c_str());
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    DEBUG_PRINT("debug: 找到join表：%s\n",table_name.c_str());
    tables.push_back(table);
    table_map.insert(std::pair<std::string, Table *>(table_name, table));
  }

  // collect query fields in `select` statement
  std::vector<Expression *> query_exprs; // new
    // TODO, 先简单使用数量判断，做group by时再来修改
  int num_attr = 0; // 属性数
  int num_aggr = 0; // 聚合数
  for (int i = static_cast<int>(select_sql.select_exprs.size()) - 1; i >= 0; i--) {
    // 检查是否有聚合操作
    const SelectExprNode &s_expr_node = select_sql.select_exprs[i];

    // =============聚合============= //
    if (s_expr_node.type == AGGR_FUNC_SELECT_T) {
      //DEBUG_PRINT("debug: 进行聚合函数解析: %d\n",i);
      
      Table *default_table = nullptr;
      if (tables.size() == 1) {
        default_table = tables[0];
      }

      AggrUnit *aggr_unit = nullptr;

      RC rc = AggrStmt::create_aggr_unit(db,
        default_table,
        &table_map,
        *s_expr_node.aggrfunc,
        aggr_unit);
      if (rc != RC::SUCCESS) {
        // DEBUG_PRINT("debug: 创建聚合失败\n");
        return rc;
      }
      // DEBUG_PRINT("debug: 创建聚合成功\n");
      query_exprs.emplace_back(aggr_unit->get_aggr_expr()); // modify

      num_aggr++;
      continue;
    }
    num_attr++;
    // =============聚合============= //

    const RelAttrSqlNode &relation_attr = *s_expr_node.attribute;

    // 表名为空，列属性为"*"，添加所有列
    if (common::is_blank(relation_attr.relation_name.c_str()) &&
        0 == strcmp(relation_attr.attribute_name.c_str(), "*")) {
      for (Table *table : tables) {
        wildcard_query_exprs(table, query_exprs);   // new
      }
    }
    // 表名不为空
    else if (!common::is_blank(relation_attr.relation_name.c_str())) {
      const char *table_name = relation_attr.relation_name.c_str();
      const char *field_name = relation_attr.attribute_name.c_str();
      // 表名为"*"
      if (0 == strcmp(table_name, "*")) {
        // 属性不为"*"
        if (0 != strcmp(field_name, "*")) {
          LOG_WARN("invalid field name while table is *. attr=%s", field_name);
          return RC::SCHEMA_FIELD_MISSING;
        }
        for (Table *table : tables) {
          wildcard_query_exprs(table, query_exprs);   // new
        }
      } else {
        auto iter = table_map.find(table_name);
        if (iter == table_map.end()) {
          LOG_WARN("no such table in from list: %s", table_name);
          return RC::SCHEMA_FIELD_MISSING;
        }

        Table *table = iter->second;
        if (0 == strcmp(field_name, "*")) {
          wildcard_query_exprs(table, query_exprs);   // new
        } else {
          const FieldMeta *field_meta = table->table_meta().field(field_name);
          if (nullptr == field_meta) {
            LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name);
            return RC::SCHEMA_FIELD_MISSING;
          }

          query_exprs.emplace_back(new FieldExpr(Field(table, field_meta)));    // modify
        }
      }
    }
    // 表名为空则默认为第一个表，有多个表则错误
    else {
      if (tables.size() != 1) {
        LOG_WARN("invalid. I do not know the attr's table. attr=%s", relation_attr.attribute_name.c_str());
        return RC::SCHEMA_FIELD_MISSING;
      }

      Table *table = tables[0];
      const FieldMeta *field_meta = table->table_meta().field(relation_attr.attribute_name.c_str());
      if (nullptr == field_meta) {
        LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), relation_attr.attribute_name.c_str());
        return RC::SCHEMA_FIELD_MISSING;
      }

      query_exprs.emplace_back(new FieldExpr(Field(table, field_meta)));   // new
    }
  }

  // 检查聚合
  if (select_sql.grourp_by_rels.empty() && num_aggr && num_attr) {
    DEBUG_PRINT("debug: 聚合语法错误\n");
    return RC::INVALID_ARGUMENT;
  }

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // 创建join stmt
  for (size_t i = 0; i < select_sql.joins.size(); i++) {
    RC rc = RC::SUCCESS;
    JoinStmt* join_stmt = nullptr;
    rc = JoinStmt::create(db, default_table, &table_map, select_sql.joins[i], join_stmt);
    // TODO
    join_stmts.emplace_back(join_stmt);
  }
  


  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db,
      default_table,
      &table_map,
      select_sql.conditions.data(),
      static_cast<int>(select_sql.conditions.size()),
      filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();
  // TODO add expression copy
  select_stmt->tables_.swap(tables);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->query_exprs_.swap(query_exprs);
  select_stmt->join_stmts_.swap(join_stmts);
  stmt = select_stmt;
  return RC::SUCCESS;
}
