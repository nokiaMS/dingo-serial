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

#include "boolean_list_schema.h"

#include <any>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include "serial/utils/V2/compiler.h"

namespace dingodb {
namespace serialV2 {

int DingoSchema<std::vector<bool>>::GetLengthForKey() {
  throw std::runtime_error("bool list unsupport length");
  return -1;
}

int DingoSchema<std::vector<bool>>::GetLengthForValue() {
  throw std::runtime_error("bool list unsupport length");
  return -1;
}

int DingoSchema<std::vector<bool>>::SkipKey(Buf&) {
  throw std::runtime_error("Unsupport encoding key list type");
  return -1;
}

int DingoSchema<std::vector<bool>>::SkipValue(Buf& buf) {
  const int32_t size = buf.ReadInt();
  buf.Skip(size);

  return size + 4;
}

int DingoSchema<std::vector<bool>>::EncodeKey(const std::any&, Buf&) {
  throw std::runtime_error("Unsupport encoding key list type");
  return -1;
}

// {n:4byte} | {value: 1byte}*n
int DingoSchema<std::vector<bool>>::EncodeValue(const std::any& data,
                                                Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but no value in data.");
  }

  if (DINGO_LIKELY(data.has_value())) {
    const auto& ref_data = std::any_cast<const std::vector<bool>&>(data);

    // if (!ref_data.empty()) {
    buf.WriteInt(ref_data.size());
    for (const bool& value : ref_data) {
      buf.Write(value ? 0x1 : 0x0);
    }

    return ref_data.size() + 4;
    //}
  }

  return 0;
}

std::any DingoSchema<std::vector<bool>>::DecodeKey(Buf&) {
  throw std::runtime_error("Unsupport decoding key list type");
}

std::any DingoSchema<std::vector<bool>>::DecodeValue(Buf& buf) {
  int size = buf.ReadInt();

  std::vector<bool> data(size, false);
  for (int i = 0; i < size; ++i) {
    data[i] = buf.Read();
  }

  return std::move(std::any(std::move(data)));
}

}  // namespace serialV2
}  // namespace dingodb