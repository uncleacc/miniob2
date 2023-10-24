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
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "json/json.h"
#include "index_meta.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_names");

RC IndexMeta::init(const char *name, std::vector<const FieldMeta *> &fields)
{
  DEBUG_PRINT("debug: 初始化索引元数据..\n");
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  name_ = name;
  for (auto &iter : fields) {
    fields_.emplace_back(iter->name());
  }
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME] = name_;
  Json::Value filed_names(Json::arrayValue);
  for (const auto &str : fields_) {
    filed_names.append(str);
  }
  json_value[FIELD_FIELD_NAME] = filed_names;
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value = json_value[FIELD_NAME];
  const Json::Value &field_value = json_value[FIELD_FIELD_NAME];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  if (!field_value.isArray()) {
    LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
        name_value.asCString(),
        field_value.toStyledString().c_str());
    return RC::INTERNAL;
  }
  
  std::vector<const FieldMeta *> field_metas;
  for (const Json::Value &element : field_value) {
    if (element.isString()) {
        std::string fieldName = element.asString();
        // 在这里处理 fieldName，可以将它添加到向量或执行其他操作
        const FieldMeta *field = table.field(fieldName.c_str());
        if (nullptr == field) {
          LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), fieldName.c_str());
          return RC::SCHEMA_FIELD_MISSING;
        }
        field_metas.emplace_back(field);
    } else {
      return RC::INTERNAL;
    }
  }

  return index.init(name_value.asCString(), field_metas);
}

const char *IndexMeta::name() const
{
  return name_.c_str();
}

const char* IndexMeta::field(int i) const
{
  if (i < 0 || i >= fields_.size()) {
    return nullptr;
  }
  return fields_[i].c_str();
}

const char *IndexMeta::fields() const 
{ 
  std::string field_names;
  for (int i = 0; i < fields_.size(); i++) {
    field_names += fields_[i];
    if (i != fields_.size() - 1) {
      field_names += ", ";
    }
  }
  return field_names.c_str();
}

int const IndexMeta::field_num() const 
{ 
  return fields_.size();
}
void IndexMeta::desc(std::ostream &os) const
{
  // os << "index name=" << name_ << ", field=" << field_;
  os << "index name=" << name_ << ", field=";
  for (int i = 0; i < fields_.size(); i++) {
    os << fields_[i];
    if (i != fields_.size() - 1) {
      os << ", ";
    }
  }
}