// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include <boost/utility.hpp>

#include <iostream>

#include "parquet/util/bit-stream-utils.inline.h"

namespace parquet {

TEST(BitStreamUtil, SkipBatch_and_GetBatch) {
  int max_num_bits = 32;
  int max_offset = 64;
  int max_skip = 128;
  int values_to_read = 8;
  int num_values = max_offset + max_skip + values_to_read;
  int buffer_len = num_values * max_num_bits / 8;
  uint8_t buffer[buffer_len];
  uint32_t read_buffer[buffer_len];

  for (int num_bits = 1; num_bits <= max_num_bits; ++num_bits) {
    BitWriter writer(buffer, buffer_len);
    uint64_t mask = (1L << num_bits) - 1;

    for (int i = 0; i <= num_values; ++i) {
      writer.PutValue(i & mask, num_bits);
    }
    writer.Flush();

    for (int offset = 0; offset <= max_offset; ++offset) {
      for (int skip = 0; skip <= max_skip; ++skip) {
        BitReader reader(buffer, buffer_len);

        uint32_t values_read = reader.GetBatch(num_bits, read_buffer, offset);
        ASSERT_EQ(offset, values_read);
        for (int i = 0; i < offset; ++i) {
          ASSERT_EQ(i & mask, read_buffer[i]) << "num_bits=" << num_bits
                                              << " offset=" << offset;
        }

        values_read = reader.SkipBatch<uint32_t>(num_bits, skip);
        ASSERT_EQ(skip, values_read);

        values_read = reader.GetBatch(num_bits, read_buffer, values_to_read);
        ASSERT_EQ(values_to_read, values_read);
        for (int i = 0; i < values_to_read; ++i) {
          ASSERT_EQ((offset + skip + i) & mask, read_buffer[i])
              << "num_bits=" << num_bits << " offset=" << offset << " skip=" << skip
              << " i=" << i;
        }
      }
    }
  }
}

}  // namespace parquet
