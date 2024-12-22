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

#include "integer_list_schema.h"

#include <cstdint>
#include <utility>

#include "serial/utils/V2/compiler.h"

namespace dingodb {
namespace serialV2 {

constexpr int kDataLengthForValue = 4;
constexpr int kDataLengthForKey = kDataLengthForValue + 1;

void DingoSchema<std::vector<int32_t>>::EncodeIntList(
    const std::vector<int32_t>& data, Buf& buf) {
  buf.WriteInt(data.size());

  if (DINGO_LIKELY(IsLe())) {
    for (const int32_t& value : data) {
      uint32_t* v = (uint32_t*)&value;
      buf.Write(*v >> 24);
      buf.Write(*v >> 16);
      buf.Write(*v >> 8);
      buf.Write(*v);
    }
  } else {
    for (const int32_t& value : data) {
      uint32_t* v = (uint32_t*)&value;
      buf.Write(*v);
      buf.Write(*v >> 8);
      buf.Write(*v >> 16);
      buf.Write(*v >> 24);
    }
  }
}

void DingoSchema<std::vector<int32_t>>::DecodeIntList(
    Buf& buf, std::vector<int32_t>& data) {
  int size = buf.ReadInt();
  data.resize(size);

  if (DINGO_LIKELY(IsLe())) {
    for (int i = 0; i < size; ++i) {
      uint32_t value = ((buf.Read() & 0xFF) << 24) |
                       ((buf.Read() & 0xFF) << 16) |
                       ((buf.Read() & 0xFF) << 8) | (buf.Read() & 0xFF);
      data[i] = static_cast<int32_t>(value);
    }

  } else {
    for (int i = 0; i < size; ++i) {
      uint32_t value = (buf.Read() & 0xFF) | ((buf.Read() & 0xFF) << 8) |
                       ((buf.Read() & 0xFF) << 16) |
                       ((buf.Read() & 0xFF) << 24);
      data[i] = static_cast<int32_t>(value);
    }
  }
}

int DingoSchema<std::vector<int32_t>>::GetLengthForKey() {
  throw std::runtime_error("int list unsupport length");
  return -1;
}

int DingoSchema<std::vector<int32_t>>::GetLengthForValue() {
  throw std::runtime_error("int list unsupport length");
  return -1;
}

int DingoSchema<std::vector<int32_t>>::SkipKey(Buf&) {
  throw std::runtime_error("Unsupport encoding key list type");
  return -1;
}

int DingoSchema<std::vector<int32_t>>::SkipValue(Buf& buf) {
  int32_t size = buf.ReadInt() * 4;
  buf.Skip(size);

  return size + 4;
}

int DingoSchema<std::vector<int32_t>>::EncodeKey(const std::any&, Buf&) {
  throw std::runtime_error("Unsupport encoding key list type");
  return -1;
}

// {n:4byte}|{value: 4byte}*n
int DingoSchema<std::vector<int32_t>>::EncodeValue(const std::any& data,
                                                   Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but no data in value.");
  }

  if (data.has_value()) {
    const auto& ref_data = std::any_cast<const std::vector<int32_t>&>(data);

    EncodeIntList(ref_data, buf);
    return ref_data.size() * 4 + 4;
  }

  return 0;
}

std::any DingoSchema<std::vector<int32_t>>::DecodeKey(Buf&) {
  throw std::runtime_error("Unsupport encoding key list type");
}

std::any DingoSchema<std::vector<int32_t>>::DecodeValue(Buf& buf) {
  std::vector<int32_t> data;
  DecodeIntList(buf, data);

  return std::move(std::any(std::move(data)));
}

}  // namespace serialV2
}  // namespace dingodb