#include <memory>
#include <optional>
#include <string>

#include "../record/record_decoder.h"
#include "../recordV2/record_decoder.h"
#include "../record_decoder_wrapper.h"
#include "../schema/base_schema.h"
#include "../schemaV2/base_schema.h"

namespace dingodb {

std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> ConvertSchemas2V1(
    const std::vector<V2::BaseSchemaPtr>& schemas) {
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
        auto item_v1 = std::make_shared<DingoSchema<std::optional<std::shared_ptr<std::string>>>>();
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

}