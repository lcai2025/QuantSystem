import pandas as pd
import os
import sys
import yaml
import shutil
from pathlib import Path
from tqdm import tqdm
from joblib import Parallel, delayed

# === 关键修改：直接尝试导入 Qlib 的数据转换类 ===
try:
    # 尝试引用 Qlib 的数据转换核心类
    from qlib.utils import drop_nan_by_y_index
    # 注意：不同版本的 qlib，DumpData 的位置可能不同
    # 我们尝试动态寻找
    try:
        from qlib.dump_bin import DumpData
    except ImportError:
        from qlib.data.dump import DumpData
except ImportError as e:
    print("❌ 严重错误：你的 Qlib 安装不完整，缺少数据转换模块。")
    print(f"错误详情: {e}")
    print("💡 解决方案：请在终端运行: pip install -U https://github.com/microsoft/qlib/archive/main.zip")
    sys.exit(1)

class QlibConverter:
    def __init__(self, config_path="config.yaml"):
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
            
        with open(config_path, 'r', encoding='utf-8') as f:
            self.cfg = yaml.safe_load(f)
        
        self.raw_path = Path(self.cfg['paths']['raw_data'])
        self.temp_path = Path(self.cfg['paths']['temp_csv'])
        self.qlib_dir = Path(self.cfg['paths']['qlib_data'])
        
        # 清理临时目录
        if self.temp_path.exists(): 
            shutil.rmtree(self.temp_path)
        self.temp_path.mkdir(parents=True, exist_ok=True)
        self.qlib_dir.mkdir(parents=True, exist_ok=True)

    def process_single_file(self, file_path):
        try:
            df = pd.read_parquet(file_path)
            if df.empty: return
            
            # --- 清洗逻辑 ---
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date').set_index('date')
            
            # 强力填充
            df = df.ffill().fillna(0.0)
            
            if 'volume' in df.columns:
                df['volume'] = df['volume'].astype(float)
            
            # 导出 CSV
            stock_code = file_path.stem 
            # 确保文件名是 standard format (如果是数字需要注意)
            save_file = self.temp_path / f"{stock_code}.csv"
            df.to_csv(save_file)
            
        except Exception as e:
            print(f"⚠️ Error processing {file_path.name}: {e}")

    def run(self):
        # [1] Parquet -> CSV
        print(f"🔨 [1/2] Converting Parquet to CSV...")
        files = list(self.raw_path.glob("*.parquet"))
        if not files:
            print("❌ No data found. Run wind_fetcher.py first.")
            return

        Parallel(n_jobs=-1, backend="loky")(
            delayed(self.process_single_file)(f) for f in tqdm(files)
        )
        
        # [2] CSV -> Qlib Binary (直接调用 Python API)
        print("\n📦 [2/2] Building Qlib Binary Dataset (Native Mode)...")
        
        # 定义字段
        include_fields = [
            "open","high","low","close","volume","amount",
            "adj_factor","vwap","turnover","mkt_cap",
            "earnings","revenue","total_assets","op_cash_flow","total_equity"
        ]
        
        try:
            # 直接实例化 DumpData 类进行转换
            # 这种方式比 subprocess 更稳定
            DumpData(
                csv_path=str(self.temp_path),
                qlib_dir=str(self.qlib_dir),
                backup_dir=str(self.qlib_dir / "backup"), # Qlib 需要一个备份目录
                include_fields=include_fields,
                symbol_field_name="instrument",
                date_field_name="date",
                exclude_fields=[]
            ).dump(works=1) # works=1 表示单进程，避免 Windows 下多进程嵌套死锁
            
            print("\n" + "="*40)
            print(f"✅ Success! Qlib data built at: {self.qlib_dir}")
            print(f"💡 Next: Run 'python src/factors/barra_engine.py'")
            print("="*40)
            
        except Exception as e:
            print("\n❌ Qlib Dump Failed (Python API Error)")
            print(f"Detail: {e}")

if __name__ == "__main__":
    converter = QlibConverter()
    converter.run()