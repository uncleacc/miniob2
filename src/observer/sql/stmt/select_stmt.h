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
// Created by Wangyunlai on 2022/6/5.
//

#pragma once

#include <vector>
#include <memory>

#include "common/rc.h"
#include "sql/stmt/stmt.h"
#include "storage/field/field.h"

class FieldMeta;
class FilterStmt;
class AggrStmt;
class Db;
class Table;
class JoinStmt;

/**
 * @brief 表示select语句
 * @ingroup Statement
 */
class SelectStmt : public Stmt 
{
public:
  SelectStmt() = default;
  ~SelectStmt() override;

  StmtType type() const override
  {
    return StmtType::SELECT;
  }

public:
  static RC create(Db *db, const SelectSqlNode &select_sql, Stmt *&stmt);

public:
  const std::vector<Table *> &tables() const
  {
    return tables_;
  }
  FilterStmt *filter_stmt() const
  {
    return filter_stmt_;
  }
  // new
  std::vector<Expression*> &query_exprs() {
    return query_exprs_;
  }
  // new
  std::vector<JoinStmt*> &join_stmts() {
    return join_stmts_;
  }

private:
  std::vector<Table *> tables_;
  FilterStmt *filter_stmt_ = nullptr;
  std::vector<Expression *> query_exprs_; // new
  // join: 表名，对应的表，join条件
  std::vector<JoinStmt*> join_stmts_;  // new
  // GOURP BY
};
