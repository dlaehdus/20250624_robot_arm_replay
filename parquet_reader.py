
file = '/home/limdoyeon/robot_arm_planb/dataset/23_31_321.parquet'

import os
import pandas as pd
import sys

pd.set_option('display.max_rows', None)         # 모든 행 표시
pd.set_option('display.max_columns', None)      # 모든 열 표시
pd.set_option('display.width', None)            # 너비 제한 없음
pd.set_option('display.max_colwidth', None)     # 열 너비 제한 없음

def read_parquet_data(file_path):
    """Parquet 파일을 읽어서 CSV처럼 표시합니다"""
    
    # 파일 존재 여부 확인
    if not os.path.exists(file_path):
        print(f"파일이 존재하지 않습니다: {file_path}")
        print(f"현재 작업 디렉토리: {os.getcwd()}")
        return None
    
    try:
        # Parquet 파일 읽기
        df = pd.read_parquet(file_path)
        
        print(f"파일명: {os.path.basename(file_path)}")
        print(f"전체 경로: {file_path}")
        print(f"파일 크기: {os.path.getsize(file_path):,} bytes")
        
        print(f"\n=== 파일 정보 ===")
        print(f"총 행 개수: {len(df):,}")
        print(f"컬럼 개수: {len(df.columns)}")
        print(f"컬럼명: {list(df.columns)}")
        
        # 데이터 타입 정보
        print(f"\n=== 데이터 타입 ===")
        for col in df.columns:
            print(f"{col}: {df[col].dtype}")
        
        # 시간 정보 (timestamp 컬럼이 있는 경우)
        if 'timestamp' in df.columns:
            print(f"\n=== 시간 정보 ===")
            print(f"시작 시간: {df['timestamp'].min():.2f}초")
            print(f"종료 시간: {df['timestamp'].max():.2f}초")
            print(f"총 측정 시간: {df['timestamp'].max() - df['timestamp'].min():.2f}초")
            if len(df) > 1:
                avg_interval = (df['timestamp'].iloc[-1] - df['timestamp'].iloc[0]) / (len(df) - 1)
                print(f"평균 수집 주기: {avg_interval:.3f}초")
        
        # 모터 위치 범위 정보
        motor_cols = [col for col in df.columns if 'motor' in col and 'position' in col]
        if motor_cols:
            print(f"\n=== 모터 위치 범위 ===")
            for col in motor_cols:
                print(f"{col}: {df[col].min()} ~ {df[col].max()}")
        
        # 기본 통계 정보
        print(f"\n=== 기본 통계 ===")
        print(df.describe())
        
        # 모든 데이터 출력 (변경된 부분)
        print(f"\n=== 전체 데이터 ({len(df)}개 행) ===")
        print(df.to_string(index=False))
        
        return df
        
    except Exception as e:
        print(f"파일 읽기 오류: {e}")
        return None

def save_to_csv(df, original_file_path):
    """Parquet 데이터를 CSV로 저장합니다"""
    if df is None:
        return
    
    # CSV 파일명 생성
    base_name = os.path.basename(original_file_path)
    csv_filename = base_name.replace('.parquet', '.csv')
    csv_dir = os.path.dirname(original_file_path)
    csv_path = os.path.join(csv_dir, csv_filename)
    
    try:
        df.to_csv(csv_path, index=False)
        print(f"CSV 파일로 저장되었습니다: {csv_path}")
    except Exception as e:
        print(f"CSV 저장 오류: {e}")

def show_data_analysis(df):
    """데이터 분석 옵션을 제공합니다"""
    if df is None:
        return
    
    while True:
        print(f"\n=== 추가 분석 옵션 ===")
        print("1. 특정 시간 범위 데이터 보기")
        print("2. 특정 모터 데이터만 보기") 
        print("3. CSV 파일로 저장")
        print("4. 데이터 샘플링 (N개씩 건너뛰기)")
        print("5. 종료")
        
        try:
            option = input("선택하세요 (1-5): ").strip()
            
            if option == "1":
                try:
                    start_time = float(input("시작 시간(초): "))
                    end_time = float(input("종료 시간(초): "))
                    time_filtered = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
                    print(f"\n=== {start_time}초 ~ {end_time}초 데이터 ({len(time_filtered)}개 행) ===")
                    print(time_filtered.to_string(index=False))
                except ValueError:
                    print("올바른 숫자를 입력해주세요.")
                    
            elif option == "2":
                motor_id = input("모터 ID (0-5): ").strip()
                motor_col = f'motor_{motor_id}_position'
                if motor_col in df.columns:
                    motor_data = df[['timestamp', motor_col]]
                    print(f"\n=== 🔧 Motor {motor_id} 데이터 ===")
                    print(motor_data.to_string(index=False))
                else:
                    print(f"'{motor_col}' 컬럼이 존재하지 않습니다.")
                    
            elif option == "3":
                save_to_csv(df, file)
                
            elif option == "4":
                try:
                    step = int(input("몇 개씩 건너뛸까요? (예: 10 -> 매 10번째 데이터만): "))
                    sampled_data = df[::step]
                    print(f"\n=== 샘플링된 데이터 (매 {step}번째, 총 {len(sampled_data)}개) ===")
                    print(sampled_data.to_string(index=False))
                except ValueError:
                    print("올바른 숫자를 입력해주세요.")
                    
            elif option == "5":
                break
                
            else:
                print("잘못된 선택입니다.")
                
        except KeyboardInterrupt:
            print("\n프로그램을 종료합니다.")
            break

def main():
    print("=" * 50)
    print("Parquet 파일 읽기 도구")
    print("=" * 50)
    
    # 설정된 파일 읽기
    print(f"읽을 파일: {file}")
    df = read_parquet_data(file)
    
    if df is not None:
        # 추가 분석 옵션 제공
        show_data_analysis(df)
    
    print("프로그램을 종료합니다.")

if __name__ == '__main__':
    main()
