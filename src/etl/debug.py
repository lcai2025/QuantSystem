from WindPy import w
import yaml
import pandas as pd

# 1. 模拟你的配置 (或者直接读取你的 config.yaml)
# 这里我列出了之前提供的所有字段，用于排查
fields_to_test = [
    # --- 行情字段 ---
    "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "AMT",
    "ADJFACTOR", "VWAP", "TURN", "MKT_CAP_ARD",
    
    # --- 财务字段 (嫌疑最大) ---
    "NP_BELONGTO_PARCOMSH",            # 归母净利润
    "OPER_REV",                     # 营业总收入
    "TOT_ASSETS",                   # 总资产
    "eqy_belongto_parcomsh", # 股东权益(不含少数股东权益)
    "NET_CASH_FLOWS_OPER_ACT" ,      # 经营活动现金流
    "TOT_EQUITY",                 # 股东权益
]

def debug_wind_fields():
    # 启动 Wind
    if not w.isconnected():
        w.start()
    
    print("🔍 开始字段逐个排查...")
    print(f"{'Field Name':<35} | {'Status':<10} | {'Msg'}")
    print("-" * 60)
    
    valid_fields = []
    
    for field in fields_to_test:
        # 只拉取一天，一只股票，仅测试该字段
        error_code, data = w.wsd("000001.SZ", field, "2023-12-01", "2023-12-01", "priceAdj=U;Fill=Previous", usedf=True)
        
        if error_code == 0:
            print(f"{field:<35} | ✅ OK     |")
            valid_fields.append(field)
        else:
            print(f"{field:<35} | ❌ FAIL   | Error: {error_code}")
    
    print("-" * 60)
    print(f"建议修改 config.yaml，仅保留以下字段:\n{valid_fields}")

if __name__ == "__main__":
    debug_wind_fields()