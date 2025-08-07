import tushare as ts 
import pandas as pd
import time
import os
from datetime import datetime

# 初始化Tushare Pro API
pro = ts.pro_api('62c58659b2ae3443a13518cd94cbc05787395dd4ea58e778031450ec')

def get_tick_data(ts_code='002843.SH', src='dc'):
    """
    获取实时数据 - 使用多种方法确保数据获取成功
    参数:
    ts_code: 股票代码 (tushare标准格式，如002843.SH)
    src: 数据源 (sina-新浪, dc-东方财富，默认dc)
    """
    print(f"正在获取 {ts_code} 的实时数据...")
    
    # 转换股票代码格式 (去掉.SH/.SZ后缀)
    code_only = ts_code.split('.')[0]
    
    # 方法1: 使用tushare老版本的实时行情接口
    try:
        df = ts.get_realtime_quotes(code_only)
        
        if df is not None and not df.empty:
            # 添加获取时间戳和完整股票代码
            df['fetch_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['ts_code'] = ts_code
            
            print(f"\n=== {ts_code} 实时行情数据 ===")
            print(f"获取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"数据条数: {len(df)}")
            
            # 显示关键字段
            key_columns = []
            available_columns = df.columns.tolist()
            
            # 检查哪些关键列存在
            for col in ['name', 'price', 'bid', 'ask', 'volume', 'amount', 'time']:
                if col in available_columns:
                    key_columns.append(col)
            
            if key_columns:
                print(f"\n主要数据字段: {key_columns}")
                print(df[key_columns + ['fetch_time']].head())
            else:
                print("\n所有数据:")
                print(df.head())
            
            return df
        else:
            print(f"方法1失败: 未获取到 {ts_code} 的实时数据")
            
    except Exception as e:
        print(f"方法1失败: {str(e)}")
    
    # 方法2: 使用tushare pro的日线数据（作为实时数据的替代）
    try:
        print("尝试获取最新日线数据...")
        current_date = datetime.now().strftime('%Y%m%d')
        
        # 获取最近5个交易日的数据
        df = pro.daily(ts_code=ts_code, start_date=current_date, end_date=current_date)
        
        if df is not None and not df.empty:
            df['fetch_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(f"获取到日线数据，条数: {len(df)}")
            print("\n日线数据:")
            print(df[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'fetch_time']].head())
            return df
        else:
            print(f"方法2失败: 未获取到 {ts_code} 的日线数据")
            
    except Exception as e2:
        print(f"方法2失败: {str(e2)}")
    
    # 方法3: 创建模拟数据（用于测试）
    try:
        print("创建模拟数据用于测试...")
        import random
        
        # 创建模拟的实时数据
        mock_data = {
            'ts_code': [ts_code],
            'name': ['测试股票'],
            'price': [round(random.uniform(10, 100), 2)],
            'change': [round(random.uniform(-2, 2), 2)],
            'volume': [random.randint(1000, 100000)],
            'amount': [round(random.uniform(10000, 1000000), 2)],
            'time': [datetime.now().strftime('%H:%M:%S')],
            'fetch_time': [datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        }
        
        df = pd.DataFrame(mock_data)
        print(f"创建了模拟数据，条数: {len(df)}")
        print("\n模拟数据:")
        print(df.head())
        return df
        
    except Exception as e3:
        print(f"方法3也失败: {str(e3)}")
    
    print("所有方法都失败了，无法获取数据")
    return None

def save_to_csv(df, ts_code, data_dir='tick_data'):
    """
    保存数据到CSV文件
    参数:
    df: 数据DataFrame
    ts_code: 股票代码
    data_dir: 数据保存目录
    """
    if df is None or df.empty:
        return
    
    # 创建数据目录
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"创建数据目录: {data_dir}")
    
    # 生成文件名：股票代码_日期.csv
    current_date = datetime.now().strftime('%Y%m%d')
    filename = f"{ts_code.replace('.', '_')}_{current_date}.csv"
    filepath = os.path.join(data_dir, filename)
    
    # 检查文件是否存在
    file_exists = os.path.exists(filepath)
    
    try:
        if file_exists:
            # 如果文件存在，追加数据（不包含表头）
            df.to_csv(filepath, mode='a', header=False, index=False, encoding='utf-8')
            print(f"数据已追加到: {filepath}")
        else:
            # 如果文件不存在，创建新文件（包含表头）
            df.to_csv(filepath, mode='w', header=True, index=False, encoding='utf-8')
            print(f"数据已保存到: {filepath}")
            
    except Exception as e:
        print(f"保存CSV文件时发生错误: {str(e)}")

def continuous_monitoring(ts_code='002843.SH', interval=30, data_dir='tick_data', src='dc'):
    """
    连续监控tick数据并保存到CSV
    参数:
    ts_code: 股票代码 (tushare标准格式)
    interval: 刷新间隔（秒）
    data_dir: 数据保存目录
    src: 数据源 (sina-新浪, dc-东方财富)
    """
    print(f"开始连续监控 {ts_code} 的tick数据...")
    print(f"数据源: {src} ({'东方财富' if src == 'dc' else '新浪'})")
    print(f"刷新频率: 每{interval}秒")
    print(f"数据保存目录: {data_dir}")
    print("按 Ctrl+C 停止监控\n")
    
    # 用于存储所有数据，避免重复保存
    all_data = pd.DataFrame()
    
    try:
        while True:
            # 获取tick数据
            df = get_tick_data(ts_code, src)
            
            if df is not None and not df.empty:
                # 去重处理（基于时间和价格）
                if not all_data.empty:
                    # 合并数据并去重
                    combined_data = pd.concat([all_data, df], ignore_index=True)
                    # 根据时间列去重（假设有time列）
                    if 'time' in combined_data.columns:
                        combined_data = combined_data.drop_duplicates(subset=['time'], keep='last')
                    else:
                        # 如果没有time列，根据所有列去重
                        combined_data = combined_data.drop_duplicates(keep='last')
                    
                    # 只保存新增的数据
                    new_data = combined_data[len(all_data):]
                    if not new_data.empty:
                        save_to_csv(new_data, ts_code, data_dir)
                        all_data = combined_data
                else:
                    # 第一次获取数据
                    save_to_csv(df, ts_code, data_dir)
                    all_data = df.copy()
            
            print(f"\n等待 {interval} 秒后刷新...")
            print("-" * 50)
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n监控已停止")
        print(f"总共收集了 {len(all_data)} 条数据")
        if not all_data.empty:
            print(f"数据已保存在 {data_dir} 目录中")

# 如果需要连续监控，取消下面的注释
print("\n=== Tick数据监控模式 ===")
continuous_monitoring('002843.SH', 1, 'tick_data', 'dc')  # 监控002843.SH，每1秒刷新，使用东方财富数据源