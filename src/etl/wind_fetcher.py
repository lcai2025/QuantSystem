import pandas as pd
import os
import time
import yaml
from datetime import datetime, timedelta
from tqdm import tqdm
from WindPy import w

class WindDataFetcher:
    def __init__(self, config_path="config.yaml"):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
            
        with open(config_path, 'r', encoding='utf-8') as f:
            self.cfg = yaml.safe_load(f)
        
        self.raw_path = self.cfg['paths']['raw_data']
        os.makedirs(self.raw_path, exist_ok=True)
        
        if not w.isconnected():
            res = w.start()
            if res.ErrorCode != 0:
                raise RuntimeError(f"Wind start failed: {res}")
        print("✅ Wind API 已连接")

    def get_stock_list(self):
        date_str = datetime.now().strftime("%Y-%m-%d")
        sector_id = self.cfg['wind']['pool_code']
        error_code, data = w.wset("sectorconstituent", f"date={date_str};sectorid={sector_id}", usedf=True)
        if error_code != 0:
            raise Exception(f"Failed to fetch stock list. Error Code: {error_code}")
        return data['wind_code'].tolist()

    # === 核心修改：分离拉取逻辑 ===
    def fetch_single_stock(self, code, start_date, end_date):
        mkt_map = self.cfg['fields']['market_map']
        fin_map = self.cfg['fields']['financial_map']
        
        # === 1. 拉取日频行情 (Market Data) ===
        mkt_fields = list(mkt_map.keys())
        # 行情数据：不复权 (priceAdj=U)
        df_mkt = self._safe_wsd(code, ",".join(mkt_fields), start_date, end_date, "priceAdj=U")
        
        if df_mkt is None: return None
        df_mkt = self._rename_cols(df_mkt, mkt_map)

        # === 2. 拉取季频财务 (Financial Data) ===
        fin_fields = list(fin_map.keys())
        # 关键修改：
        # 1. Period=Q (只取季报)
        # 2. 向前多取 180 天 (ensure we have the last report before start_date to fill correctly)
        start_dt = pd.to_datetime(start_date)
        fin_start_str = (start_dt - timedelta(days=180)).strftime("%Y-%m-%d")
        
        # 财务数据参数: Period=Q (季频)
        fin_opt = "period=Q;unit=1;currencyType="
        df_fin = self._safe_wsd(code, ",".join(fin_fields), fin_start_str, end_date, fin_opt)
        
        if df_fin is not None:
            df_fin = self._rename_cols(df_fin, fin_map)
            # 确保索引是日期类型
            df_fin.index = pd.to_datetime(df_fin.index)
            
            # === 3. 核心技巧：本地重采样与合并 ===
            # 将行情数据的索引（日频）作为基准
            # 将财务数据 "左连接" 到日频索引上
            # 此时非财报日的财务数据会变成 NaN
            df_final = df_mkt.join(df_fin, how='left')
            
            # 执行强力前向填充 (FFill)
            # 这样 1月1日 会自动填入 去年Q3或Q4 的数据
            for col in fin_map.values():
                if col in df_final.columns:
                    df_final[col] = df_final[col].ffill()
        else:
            print(f"⚠️ Warning: No financial data for {code} (Check permissions)")
            df_final = df_mkt
            # 补齐空列
            for col in fin_map.values():
                df_final[col] = 0.0

        # === 4. 格式收尾 ===
        # 再次填充可能遗漏的头部 (如果 start_date 太早)
        df_final = df_final.fillna(0.0)
        
        df_final.index.name = 'date'
        df_final.reset_index(inplace=True)
        df_final['instrument'] = code
        df_final['date'] = pd.to_datetime(df_final['date'])
        
        return df_final

    def _safe_wsd(self, code, fields, start, end, options):
        """辅助函数：带重试的 wsd 调用"""
        retry = self.cfg['wind']['retry_count']
        for i in range(retry):
            try:
                error_code, data = w.wsd(code, fields, start, end, options, usedf=True)
                if error_code == 0:
                    return data
                elif error_code == -40520007: # No Data
                    return None
                else:
                    time.sleep(0.5)
            except Exception as e:
                print(f"⚠️ Exception: {e}")
                time.sleep(0.5)
        return None

    def _rename_cols(self, df, mapper):
        """辅助函数：统一列名重命名"""
        new_cols = []
        for c in df.columns:
            c_upper = c.upper()
            new_cols.append(mapper.get(c_upper, c.lower()))
        df.columns = new_cols
        return df

    def run(self):
        stocks = self.get_stock_list()
        start_date = self.cfg['data_scope']['start_date']
        end_date = self.cfg['data_scope']['end_date']
        
        print(f"🚀 Start fetching pipeline. Target: {len(stocks)} stocks.")
        pbar = tqdm(stocks)
        for stock in pbar:
            save_path = os.path.join(self.raw_path, f"{stock}.parquet")
            if os.path.exists(save_path):
                continue
            
            df = self.fetch_single_stock(stock, start_date, end_date)
            if df is not None:
                df.to_parquet(save_path, engine='pyarrow', compression='snappy')
            time.sleep(self.cfg['wind']['request_gap'])

if __name__ == "__main__":
    fetcher = WindDataFetcher()
    
    # === 测试模式 ===
    print("🔍 Refetching 000001.SZ with separate calls...")
    df = fetcher.fetch_single_stock("000001.SZ", "2023-01-01", "2023-12-31")

    if df is not None:
        save_path = os.path.join(fetcher.raw_path, "000001.SZ.parquet")
        df.to_parquet(save_path, engine='pyarrow', compression='snappy')
        
        # 立即验证财务数据
        print("Columns:", df.columns.tolist())
        print("Data Preview (Earnings):")
        # 检查 earnings 列是否全为空
        if 'earnings' in df.columns:
            print(df[['date', 'close', 'earnings']].head())
            print(f"Earnings Non-NaN count: {df['earnings'].count()} / {len(df)}")
            if df['earnings'].count() > 0:
                print("✅ 修复成功！财务数据已获取。")
            else:
                print("❌ 依然全为 NaN，请检查 Wind 权限或字段名。")
        else:
            print("❌ 缺少 earnings 字段。")
            
        print("💡 Now run 'converter.py' again.")
    else:
        print("❌ Fetch failed.")