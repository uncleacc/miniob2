/* Copyright (c) 2021OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

InsertStmt::InsertStmt(Table *table, const Value *values, int value_amount)
    : table_(table), values_(values), value_amount_(value_amount)
{}

RC InsertStmt::create(Db *db,/*delete*/ InsertSqlNode &inserts, Stmt *&stmt)
{
  const char *table_name = inserts.relation_name.c_str();
  if (nullptr == db || nullptr == table_name || inserts.values.empty()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, static_cast<int>(inserts.values.size()));
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check the fields number
  Value *values = inserts.values.data();  // modify
  const int value_num = static_cast<int>(inserts.values.size());
  const TableMeta &table_meta = table->table_meta();
  const int field_num = table_meta.field_num() - table_meta.sys_field_num();
  if (field_num != value_num) {
    LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
    return RC::SCHEMA_FIELD_MISSING;
  }

  // check fields type
  const int sys_field_num = table_meta.sys_field_num();
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field_meta = table_meta.field(i + sys_field_num);
    const AttrType field_type = field_meta->type();
    const AttrType value_type = values[i].attr_type();
    // 检查不同类型是否可以类型转换
    if (field_type != value_type) {  // TODO try to convert the value type to field type
      LOG_WARN("field type mismatch. table=%s, field=%s, field type=%d, value_type=%d",
          table_name, field_meta->name(), field_type, value_type);
      // TODO，感觉代码有点乱，有点冗余，看看怎么优化
      // 使用value的get是否能排除0的情况
      if (field_type == INTS) {     // 转INT
        switch (value_type)
        {
        case FLOATS:{
          int convert_data = (int)values[i].get_float();
          values[i].set_int(convert_data);
        } break;
        case CHARS: {
          int convert_data = atoi(values[i].get_string().c_str());
          values[i].set_int(convert_data);
        } break;
        default:
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
      } else if (field_type == FLOATS) {  // 转FLOAT
        switch (value_type)
        {
        case INTS:{
          float convert_data = (float)values[i].get_int();
          values[i].set_float(convert_data);
        } break;
        case CHARS: {
          float convert_data = atof(values[i].get_string().c_str());
          values[i].set_float(convert_data);
        } break;
        default:
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
      } else if (field_type == CHARS) {
        const char * s = values[i].get_string().c_str();
        values[i].set_string(s);
      }
    }
    // 检查是否是非法日期
    if (value_type == DATES && values[i].get_date() == 0) {
      return RC::VARIABLE_NOT_VALID;
    }
    // 检查字符串是否超长
    if (value_type == CHARS && values[i].length() > field_meta->len()) {
      return RC::VARIABLE_NOT_VALID;
    }
  }

  // everything alright
  stmt = new InsertStmt(table, values, value_num);
  return RC::SUCCESS;
}
