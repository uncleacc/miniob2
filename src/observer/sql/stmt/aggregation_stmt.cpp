// created by NieYang on 2023/20/19

#include "common/rc.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "aggregation_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

RC get_table_and_field2(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const RelAttrSqlNode &attr, Table *&table, const FieldMeta *&field);

RC AggrUnit::add_field(Field *&field) 
{
    // DEBUG_PRINT("debug: AggrUnit::add_field\n");

    if (star_) {          // 已有*，再加任何列都错
      return RC::INTERNAL;
    } else if (aggr_field_ == nullptr) {  // 没有
      if (is_star(field)) {   // 加的是*，只有count能用
        if (type_ != COUNT_AGGR_T)
          return RC::INTERNAL;
        star_ = true;
      }
      aggr_field_ = field;
      return RC::SUCCESS;
    } else {  // 有
      bool field_exist = exist(field);
      if (field_exist && (type_ == SUM_AGGR_T || type_ == AVG_AGGR_T))
        return RC::SUCCESS;
      else 
        return RC::INTERNAL;
    }
}

bool AggrUnit::is_star(Field *field) 
{ 
  return 0==strcmp(field->meta()->name(), "*"); 
}

RC AggrStmt::create_aggr_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    AggrFuncNode &aggr_func_node, AggrUnit *&aggr_unit)
{
    RC rc = RC::SUCCESS;

    AggrUnit *new_aggr_unit = new AggrUnit();
    new_aggr_unit->setType(aggr_func_node.type);
    // 获取字段
    Table *table = nullptr;
    const FieldMeta *field_meta = nullptr;
    for (RelAttrSqlNode &attr : aggr_func_node.attributes) {
        rc = get_table_and_field2(db, default_table, tables, attr, table, field_meta);
        if (rc != RC::SUCCESS) {
            DEBUG_PRINT("cannot find attr\n");
            return rc;
        }
        Field *new_field = new Field(table, field_meta);
        rc = new_aggr_unit->add_field(new_field);
        if (rc != RC::SUCCESS) {
            DEBUG_PRINT("aggregation func wrong\n");
            return rc;
        }
    }

    aggr_unit = new_aggr_unit;

    return rc;
}






// 直接全部复制
RC get_table_and_field2(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const RelAttrSqlNode &attr, Table *&table, const FieldMeta *&field)
{
  if (common::is_blank(attr.relation_name.c_str())) {
    table = default_table;
  } else if (nullptr != tables) {
    auto iter = tables->find(attr.relation_name);
    if (iter != tables->end()) {
      table = iter->second;
    }
  } else {
    table = db->find_table(attr.relation_name.c_str());
  }
  
  if (nullptr == table) {
    LOG_WARN("No such table: attr.relation_name: %s", attr.relation_name.c_str());
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  if (strcmp(attr.attribute_name.c_str(), "*") == 0) {
    field = new FieldMeta("*", (AttrType)1, 1, 1, 1);

    return RC::SUCCESS;
  }

  field = table->table_meta().field(attr.attribute_name.c_str());
  if (nullptr == field) {
    LOG_WARN("no such field in table: table %s, field %s", table->name(), attr.attribute_name.c_str());
    table = nullptr;
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  return RC::SUCCESS;
}