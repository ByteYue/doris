select /*+ common_hint(1) */ 1;
select /*+ common_hint(1.1) */ 1;
select /*+ common_hint("string") */ 1;
select /*+ common_hint(k = "v") */ 1;
select /*+ common_hint(1, 1.1, "string") */ 1;
select /*+ common_hint(1) another_hint(2) */ 1;
