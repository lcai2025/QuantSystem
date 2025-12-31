import yaml
from WindPy import w

def validate_real_config():
    # 1. 启动 Wind
    if not w.isconnected():
        w.start()
        
    print("🔍 读取 config.yaml 配置...")
    try:
        with open("config.yaml", "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        print(f"❌ 读取配置文件失败: {e}")
        return

    # 2. 提取将要发送给 Wind 的字段 (提取 Keys)
    mkt_keys = list(cfg['fields']['market_map'].keys())
    fin_keys = list(cfg['fields']['financial_map'].keys())
    all_fields = mkt_keys + fin_keys
    
    print(f"📋 检测到 {len(all_fields)} 个字段将发送给 Wind API:")
    print(all_fields)
    print("-" * 50)

    # 3. 逐个击破
    failed_fields = []
    for field in all_fields:
        # 测试：拉取一天数据，使用 config 里的参数
        # 注意：这里我们测试字段名的有效性
        error_code, _ = w.wsd("000001.SZ", field, "2023-12-01", "2023-12-01", "priceAdj=U", usedf=True)
        
        if error_code == 0:
            print(f"✅ PASS: {field}")
        else:
            print(f"❌ FAIL: {field} (Error: {error_code})")
            failed_fields.append(field)
            
    print("-" * 50)
    if failed_fields:
        print(f"🚫 发现 {len(failed_fields)} 个非法字段，正是它们导致了 -40522007 错误:")
        for f in failed_fields:
            print(f"   - {f}")
        print("\n请在 config.yaml 中将上述 Key (冒号左边的词) 修改为正确代码。")
    else:
        print("🎉 所有字段验证通过！现在运行 wind_fetcher.py 应该没问题了。")

if __name__ == "__main__":
    validate_real_config()