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

#include "double_schema.h"

#include <cstdint>
#include <cstring>
#include <utility>
#include <stdexcept>
#include <any>

#include "serial/utilsV2/compiler.h"

namespace dingodb {
namespace V2 {

constexpr int kDataLengthInValue = 8;
constexpr int kDataLengthInKey = kDataLengthInValue + 1;

void DingoSchema<double>::EncodeDoubleComparable(double data, Buf& buf) {
  uint64_t bits;
  memcpy(&bits, &data, 8);

  if (IsLe() && data >= 0) {
    buf.Write(bits >> 56 ^ 0x80);
    buf.Write(bits >> 48);
    buf.Write(bits >> 40);
    buf.Write(bits >> 32);
    buf.Write(bits >> 24);
    buf.Write(bits >> 16);
    buf.Write(bits >> 8);
    buf.Write(bits);
  } else if (IsLe() && data < 0) {
    buf.Write(~bits >> 56);
    buf.Write(~bits >> 48);
    buf.Write(~bits >> 40);
    buf.Write(~bits >> 32);
    buf.Write(~bits >> 24);
    buf.Write(~bits >> 16);
    buf.Write(~bits >> 8);
    buf.Write(~bits);
  } else if (!IsLe() && data >= 0) {
    buf.Write(bits ^ 0x80);
    buf.Write(bits >> 8);
    buf.Write(bits >> 16);
    buf.Write(bits >> 24);
    buf.Write(bits >> 32);
    buf.Write(bits >> 40);
    buf.Write(bits >> 48);
    buf.Write(bits >> 56);
  } else {
    buf.Write(~bits);
    buf.Write(~bits >> 8);
    buf.Write(~bits >> 16);
    buf.Write(~bits >> 24);
    buf.Write(~bits >> 32);
    buf.Write(~bits >> 40);
    buf.Write(~bits >> 48);
    buf.Write(~bits >> 56);
  }
}

double DingoSchema<double>::DecodeDoubleComparable(Buf& buf) {
  uint64_t l = buf.Read() & 0xFF;
  if (IsLe()) {
    if (l >= 0x80) {
      l = l ^ 0x80;
      for (int i = 0; i < 7; ++i) {
        l <<= 8;
        l |= buf.Read() & 0xFF;
      }
    } else {
      l = ~l;
      for (int i = 0; i < 7; ++i) {
        l <<= 8;
        l |= ~buf.Read() & 0xFF;
      }
    }
  } else {
    if (l >= 0x80) {
      l = l ^ 0x80;
      for (int i = 1; i < 8; ++i) {
        l |= (((uint64_t)buf.Read() & 0xFF) << (8 * i));
      }
    } else {
      for (int i = 1; i < 8; ++i) {
        l |= (((uint64_t)buf.Read() & 0xFF) << (8 * i));
      }
      l = ~l;
    }
  }

  void* v = &l;
  return *reinterpret_cast<double*>(v);
}

void DingoSchema<double>::EncodeDoubleNotComparable(double data, Buf& buf) {
  uint64_t bits;
  memcpy(&bits, &data, 8);
  if (IsLe()) {
    buf.Write(bits >> 56);
    buf.Write(bits >> 48);
    buf.Write(bits >> 40);
    buf.Write(bits >> 32);
    buf.Write(bits >> 24);
    buf.Write(bits >> 16);
    buf.Write(bits >> 8);
    buf.Write(bits);
  } else {
    buf.Write(bits);
    buf.Write(bits >> 8);
    buf.Write(bits >> 16);
    buf.Write(bits >> 24);
    buf.Write(bits >> 32);
    buf.Write(bits >> 40);
    buf.Write(bits >> 48);
    buf.Write(bits >> 56);
  }
}

double DingoSchema<double>::DecodeDoubleNotComparable(Buf& buf) {
  uint64_t data = buf.Read() & 0xFF;
  if (IsLe()) {
    for (int i = 0; i < 7; ++i) {
      data <<= 8;
      data |= buf.Read() & 0xFF;
    }
  } else {
    for (int i = 1; i < 8; ++i) {
      data |= (((uint64_t)buf.Read() & 0xFF) << (8 * i));
    }
  }

  void* v = &data;
  return *reinterpret_cast<double*>(v);
}

int DingoSchema<double>::GetLengthForKey() { return kDataLengthInKey; }
int DingoSchema<double>::GetLengthForValue() { return kDataLengthInValue; }

int DingoSchema<double>::SkipKey(Buf& buf) {
  buf.Skip(kDataLengthInKey);
  return kDataLengthInKey;
}

int DingoSchema<double>::SkipValue(Buf& buf) {
  buf.Skip(kDataLengthInValue);
  return kDataLengthInValue;
}

// {is_null: 1byte}|{value: 8byte}
int DingoSchema<double>::EncodeKey(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    buf.Write(k_not_null);
    const auto& ref_data = std::any_cast<const double&>(data);
    EncodeDoubleComparable(ref_data, buf);
  } else {
    buf.Write(k_null);
    buf.WriteLong(0);
  }

  return kDataLengthInKey;
}

// {value: 8byte}
int DingoSchema<double>::EncodeValue(const std::any& data, Buf& buf) {
  if (DINGO_UNLIKELY(!AllowNull() && !data.has_value())) {
    throw std::runtime_error("Not allow null, but data not has value.");
  }

  if (data.has_value()) {
    const auto& ref_data = std::any_cast<const double&>(data);
    EncodeDoubleNotComparable(ref_data, buf);
    return kDataLengthInValue;
  }

  return 0;
}

std::any DingoSchema<double>::DecodeKey(Buf& buf) {
  if (buf.Read() == k_null) {
    buf.Skip(kDataLengthInKey - 1);
    return std::any();
  }

  return std::move(std::any(DecodeDoubleComparable(buf)));
}

inline std::any DingoSchema<double>::DecodeValue(Buf& buf) {
  return std::move(std::any(DecodeDoubleNotComparable(buf)));
}

}  // namespace V2
}  // namespace dingodb