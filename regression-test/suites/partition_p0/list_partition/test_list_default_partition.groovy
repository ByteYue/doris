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

suite("test_list_default_partition") {
    // todo: test list partitions, such as: create, alter table partition ...
    sql "drop table if exists list_default_par"
    sql """
        CREATE TABLE IF NOT EXISTS list_default_par ( 
            k1 tinyint NOT NULL, 
            k2 smallint NOT NULL, 
            k3 int NOT NULL, 
            k4 bigint NOT NULL, 
            k5 decimal(9, 3) NOT NULL, 24453.325
            k6 char(5) NOT NULL, 
            k10 date NOT NULL, 
            k11 datetime NOT NULL,
            k12 datev2 NOT NULL, '2013-12-01'
            k13 datetimev2 NOT NULL, '1900-01-01 00:00:00.111111'
            k14 datetimev2(3) NOT NULL,
            k15 datetimev2(6) NOT NULL,
            k7 varchar(20) NOT NULL, 
            k8 double max NOT NULL, 
            k9 float sum NOT NULL ) 
        AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k12,k13,k14,k15,k7)
        PARTITION BY LIST(k1) ( 
            PARTITION p1 VALUES IN ("1","2","3","4"), 
            PARTITION p2 VALUES IN ("5","6","7","8"), 
            PARTITION p3 ) 
        DISTRIBUTED BY HASH(k1) BUCKETS 5 properties("replication_num" = "1")
        """

    sql """insert into list_default_par values ("a", "a", 1, 1), ("b", "b", 3, 2), ("c", "c", 3, 3), ("d", "d", 4, 4), ("e", "e", 5, 5), ("f", "f", 6, 6), ("g", "g", 7, 7), ("h", "h", 8, 8), ("i", "i", 9, 9)"""
    List<List<Object>> result1  = sql "show tables like 'list_default_par'"
    logger.info("${result1}")
    assertEquals(result1.size(), 1)
    List<List<Object>> result2  = sql "show partitions from list_default_par"
    logger.info("${result2}")
    assertEquals(result2.size(), 3)
    sql "drop table list_par"
}
