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
#include "record_encoder.h"

#include <vector>

#include "serial/record/record_encoder.h"
#include "serial/record/V2/common.h"
#include "serial/record/V2/record_encoder.h"
#include "serial/utils/V2/schema_converter.h"

namespace dingodb {

RecordEncoder::RecordEncoder(
    int schema_version,
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
    long common_id) {
  this->codec_version_ = serialV2::CODEC_VERSION_V2;
  schemas_v1_ = schemas;
  schemas_v2_ = ConvertSchemasV2(schemas);
  this->re_v1_ = new RecordEncoderV1(schema_version, schemas, common_id);
  this->re_v2_ = new serialV2::RecordEncoderV2(schema_version, schemas_v2_, common_id);
}

RecordEncoder::RecordEncoder(
    int schema_version,
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
    long common_id, bool le) {
  this->codec_version_ = serialV2::CODEC_VERSION_V2;
  schemas_v1_ = schemas;
  schemas_v2_ = ConvertSchemasV2(schemas);
  this->re_v1_ = new RecordEncoderV1(schema_version, schemas, common_id, le);
  this->re_v2_ = new serialV2::RecordEncoderV2(schema_version, schemas_v2_, common_id, le);
}

RecordEncoder::RecordEncoder(int schema_version,
                             const std::vector<serialV2::BaseSchemaPtr>& schemas,
                             long common_id) {
  this->codec_version_ = serialV2::CODEC_VERSION_V2;
  this->schemas_v1_ = ConvertSchemasV1(schemas);
  this->schemas_v2_ = schemas;
  this->re_v1_ = new RecordEncoderV1(schema_version, schemas_v1_, common_id);
  this->re_v2_ = new serialV2::RecordEncoderV2(schema_version, schemas, common_id);
}
RecordEncoder::RecordEncoder(int schema_version,
                             const std::vector<serialV2::BaseSchemaPtr>& schemas,
                             long common_id, bool le) {
  this->codec_version_ = serialV2::CODEC_VERSION_V2;
  this->schemas_v1_ = ConvertSchemasV1(schemas);
  this->schemas_v2_ = schemas;
  this->re_v1_ = new RecordEncoderV1(schema_version, schemas_v1_, common_id, le);
  this->re_v2_ = new serialV2::RecordEncoderV2(schema_version, schemas, common_id, le);
}

inline void RecordEncoder::Init(
    int schema_version,
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
    long common_id) {
  re_v1_->Init(schema_version, schemas, common_id);
}

}  // namespace dingodb
