// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestEmptyQuery(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE TEMPORARY STREAM test_empty_query (
			  Col1 uint8
			, Col2 array(uint8)
			, Col3 low_cardinality(string)
			, NestedCol  nested (
				  First  uint32
				, Second uint32
			)
		) ENGINE = Memory
		`
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
			defer cancel()
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_empty_query"); assert.NoError(t, err) {
				assert.NoError(t, batch.Send())
			}
		}
	}
}
