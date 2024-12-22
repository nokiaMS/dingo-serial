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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <memory>
#include <string>
#include <utility>

#include "serial/utils/V2/buf.h"

// using namespace dingodb::serialV2;

void Print(const unsigned char* addr, uint32_t size) {
  for (uint32_t i = 0; i < size; ++i) {
    std::cout << std::setw(2) << std::setfill('0') << std::hex
              << static_cast<int>(*(addr + i)) << " ";
  }

  std::cout << std::endl;
}

class BufTest : public testing::Test {};

TEST_F(BufTest, CastType) {
  {
    int32_t a = -111111;
    uint32_t b = static_cast<uint32_t>(a);
    int32_t c = static_cast<int32_t>(b);
    EXPECT_EQ(a, c);
  }

  {
    int32_t a = 111111;
    uint32_t b = static_cast<uint32_t>(a);
    EXPECT_EQ(a, b);
  }

  {
    uint32_t a = 111111;
    int32_t b = static_cast<int32_t>(a);
    EXPECT_EQ(a, b);
  }
}

TEST_F(BufTest, Build) {
  {
    dingodb::serialV2::Buf buf(64, true);

    ASSERT_EQ(0, buf.Size());
    ASSERT_EQ(true, buf.IsLe());
  }

  {
    std::string s = "hello world";
    dingodb::serialV2::Buf buf(s, true);

    ASSERT_EQ(s.size(), buf.Size());
    ASSERT_EQ(true, buf.IsLe());
  }

  {
    std::string s = "hello world";
    size_t size = s.size();
    dingodb::serialV2::Buf buf(std::move(s), true);

    ASSERT_TRUE(s.empty());

    ASSERT_EQ(size, buf.Size());
    ASSERT_EQ(true, buf.IsLe());
  }
}

TEST_F(BufTest, WriteAndRead) {
  dingodb::serialV2::Buf buf(64, true);

  buf.Write(11);
  ASSERT_EQ(11, buf.Read());
  buf.WriteInt(13);
  ASSERT_EQ(13, buf.ReadInt());
  ASSERT_EQ(13, buf.Read(4));
  buf.WriteLong(15);
  ASSERT_EQ(15, buf.ReadLong());
}

TEST_F(BufTest, WriteAndPeek) {
  dingodb::serialV2::Buf buf(64, true);

  buf.Write(0x11);
  buf.Write(0x12);
  buf.Write(0x13);
  buf.Write(0x14);
  buf.Write(0x15);
  buf.Write(0x16);
  buf.Write(0x17);
  buf.Write(0x18);

  ASSERT_EQ(0x11, buf.Peek());
  ASSERT_EQ(0x11121314, buf.PeekInt());
  ASSERT_EQ(0x1112131415161718, buf.PeekLong());
}

TEST_F(BufTest, ByteTest) {
  dingodb::serialV2::Buf buf(3, true);

  buf.Write((char)0x01);
  buf.Write((char)0x02);
  buf.Write((char)0x03);

  ASSERT_EQ(0x1, buf.Peek());
  ASSERT_EQ(0x1, buf.Peek());

  ASSERT_EQ(0x1, buf.Read());
  ASSERT_EQ(0x2, buf.Read());
  ASSERT_EQ(0x3, buf.Read());

  ASSERT_EQ(0x1, buf.Read(0));
  ASSERT_EQ(0x2, buf.Read(1));
  ASSERT_EQ(0x3, buf.Read(2));

  buf.WriteByte(1, 0x04);
  ASSERT_EQ(0x1, buf.Read(0));
  ASSERT_EQ(0x4, buf.Read(1));
  ASSERT_EQ(0x3, buf.Read(2));
}

TEST_F(BufTest, ShortTest) {
  dingodb::serialV2::Buf buf(6, true);
  buf.ReSize(6);

  buf.WriteShort(0, (short)0x3132);
  buf.WriteShort(2, (short)0x3133);
  buf.WriteShort(4, (short)0x3134);

  ASSERT_EQ(0x3132, buf.ReadShort());
  ASSERT_EQ(0x3133, buf.ReadShort());
  ASSERT_EQ(0x3134, buf.ReadShort());

  ASSERT_EQ(0x31, buf.Read(0));
  ASSERT_EQ(0x32, buf.Read(1));
  ASSERT_EQ(0x31, buf.Read(2));

  buf.WriteShort(2, (short)0x04);
  ASSERT_EQ(0x3132, buf.ReadShort(0));
  ASSERT_EQ(0x04, buf.ReadShort(2));
  ASSERT_EQ(0x3134, buf.ReadShort(4));
}

TEST_F(BufTest, IntTest) {
  dingodb::serialV2::Buf buf(12, true);
  buf.ReSize(12);

  buf.WriteInt(0, (int)0x31323334);
  buf.WriteInt(4, (int)0x31323334);
  buf.WriteInt(8, (int)0x31323334);
  buf.WriteInt((int)0xaabbccdd);

  ASSERT_EQ(0x31323334, buf.ReadInt());
  ASSERT_EQ(0x31323334, buf.ReadInt());
  ASSERT_EQ(0x31323334, buf.ReadInt());
  ASSERT_EQ(0xaabbccdd, buf.ReadInt());

  ASSERT_EQ(0x31, buf.Read(0));
  ASSERT_EQ(0x32, buf.Read(1));
  ASSERT_EQ(0x33, buf.Read(2));
  ASSERT_EQ(0x34, buf.Read(3));
  ASSERT_EQ(0x31, buf.Read(4));
  ASSERT_EQ(0xaa, buf.Read(12));

  buf.WriteInt(4, (short)0x04);
  ASSERT_EQ(0x31323334, buf.ReadInt(0));
  ASSERT_EQ(0x04, buf.ReadInt(4));
  ASSERT_EQ(0x31323334, buf.ReadInt(8));

  ASSERT_EQ(16, buf.Size());
}

TEST_F(BufTest, LongTest) {
  dingodb::serialV2::Buf buf(24, true);

  buf.WriteLong((long)0x31323334aabbccdd);
  buf.WriteLong((long)0x31323336aabbccdd);

  ASSERT_EQ(16, buf.Size());

  ASSERT_EQ(0x31323334aabbccdd, buf.ReadLong());
  ASSERT_EQ(0x31323336aabbccdd, buf.PeekLong());
  ASSERT_EQ(0x31323336aabbccdd, buf.ReadLong());

  ASSERT_EQ(0x31, buf.Read(0));
  ASSERT_EQ(0x32, buf.Read(1));
  ASSERT_EQ(0x33, buf.Read(2));
  ASSERT_EQ(0x34, buf.Read(3));
  ASSERT_EQ(0xaa, buf.Read(4));
  ASSERT_EQ(0xbb, buf.Read(5));
  ASSERT_EQ(0xcc, buf.Read(6));
  ASSERT_EQ(0xdd, buf.Read(7));
  ASSERT_EQ(0x31, buf.Read(8));

  buf.WriteLong((long)0x04);
  ASSERT_EQ(0x31323334aabbccdd, buf.ReadLong(0));
  ASSERT_EQ(0x31323336aabbccdd, buf.ReadLong(8));
  ASSERT_EQ(0x04, buf.ReadLong(16));
}

TEST_F(BufTest, StringTest) {
  dingodb::serialV2::Buf buf(100, true);

  buf.WriteString("abcde12345");
  buf.WriteString("abcde12345");

  ASSERT_EQ(20, buf.Size());

  ASSERT_EQ("abcde12345abcde12345", buf.GetString());

  std::string str = "";
  buf.GetString(str);
  ASSERT_EQ("abcde12345abcde12345", str);
  ASSERT_EQ(0, buf.Size());
}
