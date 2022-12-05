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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.FieldChecker;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AnalyzeSubQueryTest extends TestWithFeService implements PatternMatchSupported {

    private final NereidsParser parser = new NereidsParser();

    private final List<String> testSql = ImmutableList.of(
            "select\n"
                    + "    /*+ SET_VAR(query_timeout = 600) */ ref_0.`quantity` as c0,\n"
                    + "    coalesce(ref_0.`standard_type`,\n"
                    + "     null) as c1,\n"
                    + "    ref_0.`is_takeaway` as c2,\n"
                    + "    BITMAP_XOR(\n"
                    + "            cast(coalesce(case when true then null else null end\n"
                    + "            ,\n"
                    + "            SUB_BITMAP(\n"
                    + "            cast(BITMAP_EMPTY() as bitmap),\n"
                    + "    cast(ref_0.`order_time` as bigint),\n"
                    + "    cast(ref_0.`order_time` as bigint))) as bitmap),\n"
                    + "    cast(BITMAP_EMPTY() as bitmap)) as c4\n"
                    + "    from\n"
                    + "    dwd_pos_dish_list_detail_di_bh as ref_0\n"
                    + "    where BITMAP_EMPTY() is NULL order by ref_0.`oper_type`,ref_0.`operator`"
    );

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createTables(
                "CREATE TABLE `dwd_pos_dish_list_detail_di_bh` (\n"
                        + "  `rq` datetime NULL COMMENT '订单时间',\n"
                        + "  `id` varchar(32) NOT NULL COMMENT 'pos的id',\n"
                        + "  `shop_code` varchar(10) NULL COMMENT '门店编码',\n"
                        + "  `dish_code` varchar(10) NULL COMMENT '菜品编码',\n"
                        + "  `dish_type` smallint(6) NULL COMMENT '类型 ： 1-菜品 2-火锅 3-底料',\n"
                        + "  `standard_name` varchar(200) NULL COMMENT '规格名称',\n"
                        + "  `taste_name` varchar(600) NULL COMMENT '口味名称',\n"
                        + "  `region_name` varchar(200) NULL COMMENT '区域名称',\n"
                        + "  `dish_short_code` varchar(30) NULL COMMENT '菜品短编码',\n"
                        + "  `dish_name` varchar(500) NULL COMMENT '菜品名称',\n"
                        + "  `standard_code` varchar(30) NULL COMMENT '[A]规格编码',\n"
                        + "  `shop_name` varchar(150) NULL COMMENT '门店名称',\n"
                        + "  `practice` varchar(600) NULL COMMENT '菜品做法',\n"
                        + "  `standard_type` varchar(50) NULL COMMENT '[A]规格类型（引用菜品类型1普通菜品、3底料）',\n"
                        + "  `dish_type_code` varchar(20) NULL COMMENT '菜品分类编码',\n"
                        + "  `big_group_name` varchar(150) NULL COMMENT '大类名称',\n"
                        + "  `small_group_name` varchar(150) NULL COMMENT '小类名称',\n"
                        + "  `table_bill_id` varchar(32) NULL COMMENT '菜单编号',\n"
                        + "  `taste_type_id` varchar(500) NULL COMMENT '口味类型编号',\n"
                        + "  `dish_status` varchar(100) NULL COMMENT '菜品状态: 原菜品详细状态',\n"
                        + "  `dish_abnormal_status` varchar(30) NULL COMMENT '[A]菜品异常状态:免单、折扣、奉送、退菜之一，互相覆盖，以最后为准',\n"
                        + "  `dish_type_name` varchar(10) NULL COMMENT '类型 ： 1-菜品 2-火锅 3-底料',\n"
                        + "  `dish_price` decimal(10, 2) NULL COMMENT '菜品单价',\n"
                        + "  `unit` varchar(20) NULL COMMENT '菜品单位',\n"
                        + "  `quantity` int(11) NULL COMMENT '数量',\n"
                        + "  `actual_price` decimal(20, 8) NULL COMMENT '实际价格',\n"
                        + "  `member_price` decimal(20, 8) NULL COMMENT '会员价',\n"
                        + "  `cost_price` decimal(20, 8) NULL COMMENT '实际成本价格',\n"
                        + "  `served_quantity` int(11) NULL COMMENT '已上数量',\n"
                        + "  `ignore_money` decimal(20, 8) NULL COMMENT '自动抹零分摊金额',\n"
                        + "  `gift_card_money` decimal(20, 8) NULL COMMENT '赠卡金额',\n"
                        + "  `service_charge_money` decimal(20, 8) NULL COMMENT '服务费分摊金额',\n"
                        + "  `dish_discount_money` decimal(20, 8) NULL COMMENT '折扣金额',\n"
                        + "  `is_gift` char(1) NULL COMMENT '是否赠品',\n"
                        + "  `is_discount` char(1) NULL COMMENT '是否折扣',\n"
                        + "  `is_remark` char(1) NULL COMMENT '是否备注',\n"
                        + "  `is_practice` char(1) NULL COMMENT '是否做法',\n"
                        + "  `is_taste` char(1) NULL COMMENT '是否口味',\n"
                        + "  `is_weigh` char(1) NULL COMMENT '是否称重',\n"
                        + "  `start_num` int(11) NULL DEFAULT \"1\" COMMENT '起点份数',\n"
                        + "  `is_dine` char(1) NULL DEFAULT \"Y\" COMMENT '堂食',\n"
                        + "  `is_takeout` char(1) NULL DEFAULT \"Y\" COMMENT '外带',\n"
                        + "  `is_takeaway` char(1) NULL DEFAULT \"Y\" COMMENT '外卖',\n"
                        + "  `display` char(1) NULL COMMENT '是否展示估清菜品',\n"
                        + "  `consumption_rate` decimal(20, 8) NULL COMMENT '消费税率',\n"
                        + "  `order_time` bigint(20) NULL COMMENT '点菜时间',\n"
                        + "  `remark` varchar(600) NULL,\n"
                        + "  `oper_type` varchar(10) NULL COMMENT '操作类型:只记录“加菜”、“转台”等操作类型（即只记录在菜单中新增菜品时的操作）',\n"
                        + "  `operator` varchar(64) NULL COMMENT '操作人',\n"
                        + "  `deliver_id` varchar(32) NULL COMMENT '上菜员',\n"
                        + "  `cook_id` varchar(32) NULL COMMENT '厨师',\n"
                        + "  `waiter_id` varchar(32) NULL COMMENT '服务员',\n"
                        + "  `ts` bigint(20) NULL COMMENT '下单时间戳',\n"
                        + "  `ts_format` datetime NULL COMMENT '下单时间',\n"
                        + "  `create_time_offset` datetime NULL COMMENT '落库时间'\n"
                        + ") ENGINE=OLAP\n"
                        + "UNIQUE KEY(`rq`, `id`, `shop_code`)\n"
                        + "COMMENT 'POS事实菜品明细数据表'\n"
                        + "DISTRIBUTED BY HASH(`shop_code`) BUCKETS 20\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"in_memory\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"disable_auto_compaction\" = \"false\"\n"
                        + ");"
        );
    }

    @Override
    protected void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    @Test
    public void testTranslateCase() throws Exception {
        for (String sql : testSql) {
            NamedExpressionUtil.clear();
            StatementContext statementContext = MemoTestUtils.createStatementContext(connectContext, sql);
            NereidsPlanner planner = new NereidsPlanner(statementContext);
            PhysicalPlan plan = planner.plan(
                    parser.parseSingle(sql),
                    PhysicalProperties.ANY
            );
            // Just to check whether translate will throw exception
            new PhysicalPlanTranslator().translatePlan(plan, new PlanTranslatorContext(planner.getCascadesContext()));
        }
    }

    @Test
    public void testCaseSubQuery() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(0))
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .matchesFromRoot(
                    logicalProject(
                        logicalProject(
                            logicalProject(
                                logicalProject(
                                    logicalOlapScan().when(o -> true)
                                )
                            )
                        ).when(FieldChecker.check("projects", ImmutableList.of(
                            new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("T")),
                            new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("T"))))
                        )
                    ).when(FieldChecker.check("projects", ImmutableList.of(
                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("T2")),
                        new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("T2"))))
                    )
                );
    }

    @Test
    public void testCaseMixed() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(1))
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .matchesFromRoot(
                    logicalProject(
                        logicalJoin(
                            logicalProject(
                                logicalOlapScan()
                            ),
                            logicalProject(
                                logicalProject(
                                    logicalProject(
                                        logicalOlapScan()
                                    )
                                )
                            ).when(FieldChecker.check("projects", ImmutableList.of(
                                new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("TT2")),
                                new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("TT2"))))
                            )
                        )
                        .when(FieldChecker.check("joinType", JoinType.INNER_JOIN))
                        .when(FieldChecker.check("otherJoinConjuncts",
                                ImmutableList.of(new EqualTo(
                                        new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("TT1")),
                                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("T")))))
                        )
                    ).when(FieldChecker.check("projects", ImmutableList.of(
                        new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("TT1")),
                        new SlotReference(new ExprId(3), "score", BigIntType.INSTANCE, true, ImmutableList.of("TT1")),
                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("T")),
                        new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("T"))))
                    )
                );
    }

    @Test
    public void testCaseJoinSameTable() {
        PlanChecker.from(connectContext)
                .analyze(testSql.get(5))
                .applyTopDown(new LogicalSubQueryAliasToLogicalProject())
                .matchesFromRoot(
                    logicalProject(
                        logicalJoin(
                            logicalOlapScan(),
                            logicalProject(
                                logicalOlapScan()
                            )
                        )
                        .when(FieldChecker.check("joinType", JoinType.INNER_JOIN))
                        .when(FieldChecker.check("otherJoinConjuncts", ImmutableList.of(new EqualTo(
                                new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("default_cluster:test", "T1")),
                                new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("T2")))))
                        )
                    ).when(FieldChecker.check("projects", ImmutableList.of(
                        new SlotReference(new ExprId(0), "id", BigIntType.INSTANCE, true, ImmutableList.of("default_cluster:test", "T1")),
                        new SlotReference(new ExprId(1), "score", BigIntType.INSTANCE, true, ImmutableList.of("default_cluster:test", "T1")),
                        new SlotReference(new ExprId(2), "id", BigIntType.INSTANCE, true, ImmutableList.of("T2")),
                        new SlotReference(new ExprId(3), "score", BigIntType.INSTANCE, true, ImmutableList.of("T2"))))
                    )
                );
    }
}

