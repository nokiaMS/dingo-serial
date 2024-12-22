// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#include <memory>
#include <optional>
#include <string>

#include "serial/record/record_decoder.h"
#include "serial/schema/V2/base_schema.h"
#include "serial/schema/V2/boolean_list_schema.h"
#include "serial/schema/V2/boolean_schema.h"
#include "serial/schema/V2/dingo_schema.h"
#include "serial/schema/V2/double_list_schema.h"
#include "serial/schema/V2/double_schema.h"
#include "serial/schema/V2/float_list_schema.h"
#include "serial/schema/V2/float_schema.h"
#include "serial/schema/V2/integer_list_schema.h"
#include "serial/schema/V2/integer_schema.h"
#include "serial/schema/V2/long_list_schema.h"
#include "serial/schema/V2/long_schema.h"
#include "serial/schema/V2/string_list_schema.h"
#include "serial/schema/V2/string_schema.h"
#include "serial/schema/base_schema.h"

namespace dingodb {

std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> ConvertSchemasV1(
    const std::vector<serialV2::BaseSchemaPtr>& schemas) {
  std::vector<std::shared_ptr<BaseSchema>> schemas_v1;
  for (auto item : schemas) {
    // build per item for v1.
    auto item_type = item->GetType();
    switch (item_type) {
      case BaseSchema::kBool: {
        auto item_v1 =
            std::make_shared<dingodb::DingoSchema<std::optional<bool>>>();
        item_v1->SetIndex(item->GetIndex());
        item_v1->SetAllowNull(item->AllowNull());
        item_v1->SetIsKey(item->IsKey());
        item_v1->SetName(item->GetName());
        // no need to set le.
        schemas_v1.push_back(item_v1);
        break;
      }

      case BaseSchema::kInteger: {
        auto item_v1 =
            std::make_shared<dingodb::DingoSchema<std::optional<int32_t>>>();
        item_v1->SetIndex(item->GetIndex());
        item_v1->SetAllowNull(item->AllowNull());
        item_v1->SetIsKey(item->IsKey());
        item_v1->SetName(item->GetName());
        item_v1->SetIsLe(item->IsLe());
        schemas_v1.push_back(item_v1);
        break;
      }

      case BaseSchema::kIntegerList: {
        auto item_v1 = std::make_shared<DingoSchema<
            std::optional<std::shared_ptr<std::vector<int32_t>>>>>();
        item_v1->SetIndex(item->GetIndex());
        item_v1->SetAllowNull(item->AllowNull());
        item_v1->SetIsKey(item->IsKey());
        item_v1->SetName(item->GetName());
        schemas_v1.push_back(item_v1);
        break;
      }

      case BaseSchema::kLong: {
        auto item_v1 =
            std::make_shared<dingodb::DingoSchema<std::optional<int64_t>>>();
        item_v1->SetIndex(item->GetIndex());
        item_v1->SetAllowNull(item->AllowNull());
        item_v1->SetIsKey(item->IsKey());
        item_v1->SetName(item->GetName());
        item_v1->SetIsLe(item->IsLe());
        schemas_v1.push_back(item_v1);
        break;
      }

      case BaseSchema::kLongList: {
        auto item_v1 = std::make_shared<DingoSchema<
            std::optional<std::shared_ptr<std::vector<int64_t>>>>>();
        item_v1->SetIndex(item->GetIndex());
        item_v1->SetAllowNull(item->AllowNull());
        item_v1->SetIsKey(item->IsKey());
        item_v1->SetName(item->GetName());
        schemas_v1.push_back(item_v1);
        break;
      }

      case BaseSchema::kFloat: {
        auto item_v1 =
            std::make_shared<dingodb::DingoSchema<std::optional<float>>>();
        item_v1->SetIndex(item->GetIndex());
        item_v1->SetAllowNull(item->AllowNull());
        item_v1->SetIsKey(item->IsKey());
        item_v1->SetName(item->GetName());
        item_v1->SetIsLe(item->IsLe());
        schemas_v1.push_back(item_v1);
        break;
      }

      case BaseSchema::kFloatList: {
        auto item_v1 = std::make_shared<
            DingoSchema<std::optional<std::shared_ptr<std::vector<bool>>>>>();
        item_v1->SetIndex(item->GetIndex());
        item_v1->SetAllowNull(item->AllowNull());
        item_v1->SetIsKey(item->IsKey());
        item_v1->SetName(item->GetName());
        schemas_v1.push_back(item_v1);
        break;
      }

      case BaseSchema::kDouble: {
        auto item_v1 =
            std::make_shared<dingodb::DingoSchema<std::optional<double>>>();
        item_v1->SetIndex(item->GetIndex());
        item_v1->SetAllowNull(item->AllowNull());
        item_v1->SetIsKey(item->IsKey());
        item_v1->SetName(item->GetName());
        item_v1->SetIsLe(item->IsLe());
        schemas_v1.push_back(item_v1);
        break;
      }

      case BaseSchema::kDoubleList: {
        auto item_v1 = std::make_shared<
            DingoSchema<std::optional<std::shared_ptr<std::vector<double>>>>>();
        item_v1->SetIndex(item->GetIndex());
        item_v1->SetAllowNull(item->AllowNull());
        item_v1->SetIsKey(item->IsKey());
        item_v1->SetName(item->GetName());
        schemas_v1.push_back(item_v1);
        break;
      }
      case BaseSchema::kString: {
        auto item_v1 = std::make_shared<
            DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
        item_v1->SetIndex(item->GetIndex());
        item_v1->SetAllowNull(item->AllowNull());
        item_v1->SetIsKey(item->IsKey());
        item_v1->SetName(item->GetName());
        schemas_v1.push_back(item_v1);
        break;
      }
      case BaseSchema::kStringList: {
        auto item_v1 = std::make_shared<DingoSchema<
            std::optional<std::shared_ptr<std::vector<std::string>>>>>();
        item_v1->SetIndex(item->GetIndex());
        item_v1->SetAllowNull(item->AllowNull());
        item_v1->SetIsKey(item->IsKey());
        item_v1->SetName(item->GetName());
        schemas_v1.push_back(item_v1);
        break;
      }
    }
  }

  return std::move(
      std::make_shared<std::vector<std::shared_ptr<BaseSchema>>>(schemas_v1));
}

template <class T>
static void FillSchemaV2(std::shared_ptr<T>& item_v2,
                         std::shared_ptr<dingodb::BaseSchema>& item_v1) {
  item_v2->SetIndex(item_v1->GetIndex());
  item_v2->SetAllowNull(item_v1->AllowNull());
  item_v2->SetIsKey(item_v1->IsKey());
  item_v2->SetName(item_v1->GetName());
}

template <class T>
static void SetLe(std::shared_ptr<T>& item_v2,
                  std::shared_ptr<dingodb::BaseSchema>& item_v1,
                  dingodb::BaseSchema::Type type) {
  switch (type) {
    case BaseSchema::kInteger: {
      auto it = std::dynamic_pointer_cast<DingoSchema<std::optional<int32_t>>>(
          item_v1);
      item_v2->SetIsLe(it->GetIsLe());
      break;
    }
    case BaseSchema::kLong: {
      auto it = std::dynamic_pointer_cast<DingoSchema<std::optional<int64_t>>>(
          item_v1);
      item_v2->SetIsLe(it->GetIsLe());
      break;
    }
    case BaseSchema::kFloat: {
      auto it =
          std::dynamic_pointer_cast<DingoSchema<std::optional<float>>>(item_v1);
      item_v2->SetIsLe(it->GetIsLe());
      break;
    }
    case BaseSchema::kDouble: {
      auto it = std::dynamic_pointer_cast<DingoSchema<std::optional<double>>>(
          item_v1);
      item_v2->SetIsLe(it->GetIsLe());
      break;
    }
    default: {
      // Only int, long, float and double care le.
      throw std::runtime_error("bool list unsupport length");
    }
  }
}

std::vector<serialV2::BaseSchemaPtr> ConvertSchemasV2(
    const std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas) {
  std::vector<serialV2::BaseSchemaPtr> schemas_v2;
  for (auto item : *schemas) {
    // build per item for v2.
    auto item_type = item->GetType();
    switch (item_type) {
      case BaseSchema::kBool: {
        auto item_v2 = std::make_shared<serialV2::DingoSchema<bool>>();
        FillSchemaV2(item_v2, item);
        schemas_v2.push_back(item_v2);
        break;
      }
      case BaseSchema::kInteger: {
        auto item_v2 = std::make_shared<serialV2::DingoSchema<int32_t>>();
        FillSchemaV2(item_v2, item);
        SetLe(item_v2, item, BaseSchema::kInteger);
        schemas_v2.push_back(item_v2);
        break;
      }
      case BaseSchema::kLong: {
        auto item_v2 = std::make_shared<serialV2::DingoSchema<int64_t>>();
        FillSchemaV2(item_v2, item);
        SetLe(item_v2, item, BaseSchema::kLong);
        schemas_v2.push_back(item_v2);
        break;
      }
      case BaseSchema::kFloat: {
        auto item_v2 = std::make_shared<serialV2::DingoSchema<float>>();
        FillSchemaV2(item_v2, item);
        SetLe(item_v2, item, BaseSchema::kFloat);
        schemas_v2.push_back(item_v2);
        break;
      }
      case BaseSchema::kDouble: {
        auto item_v2 = std::make_shared<serialV2::DingoSchema<double>>();
        FillSchemaV2(item_v2, item);
        SetLe(item_v2, item, BaseSchema::kDouble);
        schemas_v2.push_back(item_v2);
        break;
      }
      case BaseSchema::kBoolList: {
        auto item_v2 =
            std::make_shared<serialV2::DingoSchema<std::vector<bool>>>();
        FillSchemaV2(item_v2, item);
        schemas_v2.push_back(item_v2);
        break;
      }
      case BaseSchema::kIntegerList: {
        auto item_v2 =
            std::make_shared<serialV2::DingoSchema<std::vector<int32_t>>>();
        FillSchemaV2(item_v2, item);
        schemas_v2.push_back(item_v2);
        break;
      }
      case BaseSchema::kLongList: {
        auto item_v2 =
            std::make_shared<serialV2::DingoSchema<std::vector<int64_t>>>();
        FillSchemaV2(item_v2, item);
        schemas_v2.push_back(item_v2);
        break;
      }
      case BaseSchema::kFloatList: {
        auto item_v2 =
            std::make_shared<serialV2::DingoSchema<std::vector<float>>>();
        FillSchemaV2(item_v2, item);
        schemas_v2.push_back(item_v2);
        break;
      }
      case BaseSchema::kDoubleList: {
        auto item_v2 =
            std::make_shared<serialV2::DingoSchema<std::vector<double>>>();
        FillSchemaV2(item_v2, item);
        schemas_v2.push_back(item_v2);
        break;
      }
      case BaseSchema::kString: {
        auto item_v2 = std::make_shared<serialV2::DingoSchema<std::string>>();
        FillSchemaV2(item_v2, item);
        schemas_v2.push_back(item_v2);
        break;
      }
      case BaseSchema::kStringList: {
        auto item_v2 =
            std::make_shared<serialV2::DingoSchema<std::vector<std::string>>>();
        FillSchemaV2(item_v2, item);
        schemas_v2.push_back(item_v2);
        break;
      }
    }
  }

  return std::move(schemas_v2);
}

}  // namespace dingodb