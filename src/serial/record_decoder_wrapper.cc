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
#include "record_decoder_wrapper.h"

#include <memory>
#include <optional>
#include <string>

#include "serial/record/record_decoder.h"
#include "serial/recordV2/record_decoder.h"
#include "serial/schema/base_schema.h"
#include "serial/schemaV2/base_schema.h"
#include "serial/utilsV2/schema_converter.h"

namespace dingodb {

// constructor for v1.
RecordDecoder::RecordDecoder(
    int schema_version,
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
    long common_id) {
  codec_version_ = V2::CODEC_VERSION_V1;
  schemas_v1_ = schemas;
  re_v1_ = new RecordDecoderV1(schema_version, schemas, common_id);
  re_v2_ = nullptr;
}

// constructor for v1.
RecordDecoder::RecordDecoder(
    int schema_version,
    std::shared_ptr<std::vector<std::shared_ptr<BaseSchema>>> schemas,
    long common_id, bool le) {
  codec_version_ = V2::CODEC_VERSION_V1;
  schemas_v1_ = schemas;
  re_v1_ = new RecordDecoderV1(schema_version, schemas, common_id, le);
  re_v2_ = nullptr;
}

// constructor for v2.
RecordDecoder::RecordDecoder(int schema_version,
                             const std::vector<V2::BaseSchemaPtr>& schemas,
                             long common_id) {
  codec_version_ = V2::CODEC_VERSION_V2;
  schemas_v1_ = ConvertSchemas2V1(schemas);
  re_v1_ =
      new RecordDecoderV1(schema_version, schemas_v1_, common_id);
  re_v2_ = new V2::RecordDecoderV2(schema_version, schemas, common_id);
}

// constructor for v2.
RecordDecoder::RecordDecoder(int schema_version,
                             const std::vector<V2::BaseSchemaPtr>& schemas,
                             long common_id, bool le) {
  codec_version_ = V2::CODEC_VERSION_V2;
  schemas_v1_ = ConvertSchemas2V1(schemas);
  re_v1_ =
      new RecordDecoderV1(schema_version, schemas_v1_, common_id, le);
  re_v2_ = new V2::RecordDecoderV2(schema_version, schemas, common_id, le);
}



}  // namespace dingodb
