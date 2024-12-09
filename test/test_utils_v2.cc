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
#include <gtest/gtest.h>
#include <serial/utils/utils.h>
#include <serial/utils/V2/utils.h>

#include <iostream>
#include <string>

class BufferTest : public testing::Test {};

std::any func_a() {
  return std::any();
}

TEST_F(BufferTest, utilsV2) {
  {
    EXPECT_EQ(true, dingodb::serialV2::IsLE());
    EXPECT_EQ(dingodb::IsLE(), dingodb::serialV2::IsLE());

    std::any a = std::make_any<bool>(nullptr);
    bool b = a.has_value();

    std::any c = std::make_any<bool>(true);
    bool d = c.has_value();

    std::any e = std::make_any<bool>();
    bool f = e.has_value();

    std::any m;
    bool n = m.has_value();

    auto a1 = std::any();
    bool a2 = a1.has_value();

    bool k = std::any_cast<bool>(a);
    EXPECT_EQ(true, true);

    std::any ret;
    auto b1 = std::move(ret);
    bool is = b1.has_value();
    EXPECT_EQ(true, true);

    EXPECT_EQ(func_a().has_value(), false);
  }
}
