
def log_table_structure(df_table, rules_module=None):
    print("---------------------------------------------------")
    for _, col in df_table.iterrows():
        col_name = col["COLUMN_NAME"]
        dtype = col["TYPE"]
        length = int(col["LENGTH"])
        rule = col.get("FAKE_RULE", "")
        origin = "base"

        if isinstance(rule, str):
            rule = rule.strip()
            if rule.startswith("faker."):
                origin = "faker"
            elif rules_module and "(" in rule and ")" in rule:
                origin = "custom"

        print(f"[GEN] {col_name:<13} | {dtype}({length}) | {rule} [{origin}]")
