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

#ifndef DINGO_SERIAL_RECORD_ENCODER_V2_H_
#define DINGO_SERIAL_RECORD_ENCODER_V2_H_

#include <map>
#include <memory>
#include <string>

#include "any"
#include "common.h"
#include "functional"  // IWYU pragma: keep
#include "optional"    // IWYU pragma: keep
#include "serial/schema/V2/boolean_list_schema.h" // IWYU pragma: keep
#include "serial/schema/V2/boolean_schema.h"  // IWYU pragma: keep
#include "serial/schema/V2/double_list_schema.h" // IWYU pragma: keep
#include "serial/schema/V2/double_schema.h"  // IWYU pragma: keep
#include "serial/schema/V2/float_list_schema.h" // IWYU pragma: keep
#include "serial/schema/V2/float_schema.h"  // IWYU pragma: keep
#include "serial/schema/V2/integer_list_schema.h" // IWYU pragma: keep
#include "serial/schema/V2/integer_schema.h"  // IWYU pragma: keep
#include "serial/schema/V2/long_list_schema.h" // IWYU pragma: keep
#include "serial/schema/V2/long_schema.h"  // IWYU pragma: keep
#include "serial/schema/V2/string_list_schema.h" // IWYU pragma: keep
#include "serial/schema/V2/string_schema.h"  // IWYU pragma: keep
#include "serial/utils/V2/keyvalue.h"        // IWYU pragma: keep
#include "serial/utils/V2/utils.h" // IWYU pragma: keep
#include "serial/utils/V2/utils.h"  // IWYU pragma: keep

namespace dingodb {
namespace serialV2 {

class RecordEncoderV2;
using RecordEncoderPtr = std::shared_ptr<RecordEncoderV2>;

class RecordEncoderV2 {
 public:
  RecordEncoderV2(int schema_version, const std::vector<BaseSchemaPtr>& schemas,
                  long common_id);
  RecordEncoderV2(int schema_version, const std::vector<BaseSchemaPtr>& schemas,
                  long common_id, bool le);

  static RecordEncoderPtr New(int schema_version,
                              const std::vector<BaseSchemaPtr>& schemas,
                              long common_id) {
    return std::make_shared<RecordEncoderV2>(schema_version, schemas,
                                             common_id);
  }

  int Encode(char prefix, const std::vector<std::any>& record, std::string& key,
             std::string& value);

  int EncodeKey(char prefix, const std::vector<std::any>& record,
                std::string& output);
  int EncodeValue(const std::vector<std::any>& record, std::string& output);

  int EncodeMaxKeyPrefix(char prefix, std::string& output) const;
  int EncodeMinKeyPrefix(char prefix, std::string& output) const;
  void Refresh();

 private:
  void EncodePrefix(Buf& buf, char prefix) const;
  void EncodeSchemaVersion(Buf& buf) const;
  void EncodeCodecVersion(Buf& buf) const;

  // Flag for little end or not.
  bool le_;

  // codec version.
  uint8_t codec_version_{CODEC_VERSION_V2};

  int schema_version_{0x01};
  long common_id_;

  std::vector<BaseSchemaPtr> schemas_;
};

}  // namespace serialV2
}  // namespace dingodb

#endif
