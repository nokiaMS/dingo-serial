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

#ifndef DINGO_SERIAL_VALUE_HEADER_H_
#define DINGO_SERIAL_VALUE_HEADER_H_

#include "common.h"
#include "serial/utils/V2/buf.h"

namespace dingodb {
namespace serialV2 {

class ValueHeader {
  public:
  int cnt_not_null_col;
  int cnt_null_col;
  int total_col_cnt;

  int ids_pos;
  int offset_pos;
  int data_pos;

  std::map<int, int> col_id_offset_map = {};

  ValueHeader() = default;

  ValueHeader(Buf& value_buf) {
    cnt_not_null_col = value_buf.ReadShort();
    cnt_null_col = value_buf.ReadShort();
    total_col_cnt = cnt_not_null_col + cnt_null_col;

    // schema_version(4 bytes) + col_cnt (2 bytes + 2bytes) = 8 bytes.
    ids_pos = 8;
    offset_pos = ids_pos + ID_2_BYTE * total_col_cnt;
    data_pos = offset_pos + OFFSET_4_BYTE * total_col_cnt;

    for (int i = 0; i < total_col_cnt; ++i) {
      col_id_offset_map[value_buf.ReadShort(ids_pos)] =
          value_buf.ReadInt(offset_pos);
      ids_pos += ID_2_BYTE;
      offset_pos += OFFSET_4_BYTE;
    }
  }

  bool allNullColumns() {
    return total_col_cnt == cnt_null_col;
  }

};

}  // namespace serialV2
}  // namespace dingodb

#endif
