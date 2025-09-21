import rclpy
from rclpy.node import Node
from dynamixel_sdk import *
import pandas as pd
import os
import time
import signal
import sys
import math


# 설정 변수들
PORT_CON = '/dev/ttyACM0'                                   # slave 모터용 포트
DXL_SLAVE_IDS = [10, 11, 12, 13, 14, 15]                   # 재생용 모터 ID
BAUDRATE = 57600
PLAY_TIME = 0.01


# 초기 홈 위치 (안전한 위치)
HOME_POSITION = [30000, 30000, 30000, 30000, 30000, 3000]


class SafePlaybackNode(Node):
    def __init__(self, file_path):
        super().__init__('robot_arm_safe_playback')
        
        self.file_path = file_path
        self.dxl_ids = DXL_SLAVE_IDS
        self.portHandler = PortHandler(PORT_CON)
        self.packetHandler = PacketHandler(2.0)
        self.playback_data = None
        
        # 포트 및 보드레이트 설정
        if not self.portHandler.openPort():
            self.get_logger().error("포트를 열 수 없습니다.")
            raise RuntimeError("Failed to open the port")
        if not self.portHandler.setBaudRate(BAUDRATE):
            self.get_logger().error("보드레이트 설정 실패.")
            raise RuntimeError("Failed to set baudrate")
        
        self.enable_motors()
        
        signal.signal(signal.SIGINT, self.signal_handler)
        
        self.load_playback_data()
        
        self.get_logger().info("Safe Playback node initialized.")
        
    def enable_motors(self):
        """모든 모터의 토크를 활성화합니다"""
        for dxl_id in self.dxl_ids:
            result, error = self.packetHandler.write1ByteTxRx(self.portHandler, dxl_id, 64, 1)
            if result != COMM_SUCCESS:
                self.get_logger().error(f"모터 ID {dxl_id} 토크 활성화 실패.")
                raise RuntimeError(f"Failed to enable torque for motor ID {dxl_id}")
            else:
                self.get_logger().info(f"모터 ID {dxl_id} 토크 활성화 완료")
    
    def read_current_positions(self):
        """현재 모든 모터의 위치를 읽습니다"""
        positions = []
        for dxl_id in self.dxl_ids:
            position, result, error = self.packetHandler.read4ByteTxRx(self.portHandler, dxl_id, 132)
            if result != COMM_SUCCESS:
                self.get_logger().warn(f"모터 ID {dxl_id} 위치 읽기 실패")
                position = 2048  # 기본값
            positions.append(int(position))
        return positions
    
    def smooth_move_to_position(self, target_positions, duration_seconds=3.0):
        """천천히 목표 위치로 이동합니다"""
        current_positions = self.read_current_positions()
        
        self.get_logger().info(f"천천히 이동 시작:")
        self.get_logger().info(f"현재 위치: {current_positions}")
        self.get_logger().info(f"목표 위치: {target_positions}")
        self.get_logger().info(f"소요 시간: {duration_seconds}초")
        
        step_interval = 0.05
        total_steps = int(duration_seconds / step_interval)
        
        for step in range(total_steps + 1):
            progress = step / total_steps
            
            interpolated_positions = []
            for i in range(len(target_positions)):
                current_pos = current_positions[i]
                target_pos = target_positions[i]
                
                # 선형 보간
                interp_pos = current_pos + (target_pos - current_pos) * progress
                interpolated_positions.append(int(interp_pos))
            
            # 모터에 위치 명령 전송
            self.move_motors_to_positions(interpolated_positions)
            
            # 진행 상황 표시 (20% 단위로)
            if step % (total_steps // 5) == 0:
                progress_percent = progress * 100
                self.get_logger().info(f"이동 진행: {progress_percent:.0f}% - {interpolated_positions}")
            
            # 다음 단계까지 대기
            if step < total_steps:
                time.sleep(step_interval)
        
        self.get_logger().info("천천히 이동 완료")
    
    def load_playback_data(self):
        """Parquet 파일을 로드합니다"""
        if not os.path.exists(self.file_path):
            self.get_logger().error(f"파일이 존재하지 않습니다: {self.file_path}")
            raise FileNotFoundError(f"File not found: {self.file_path}")
        
        try:
            self.playback_data = pd.read_parquet(self.file_path)
            filename = os.path.basename(self.file_path)
            self.get_logger().info(f"파일 로드 성공: {filename}")
            self.get_logger().info(f"총 {len(self.playback_data)}개의 데이터 포인트")
            self.get_logger().info(f"재생 시간: {self.playback_data['timestamp'].max():.2f}초")
            
            # 파일명에서 좌표 정보 추출해서 표시
            try:
                name_without_ext = filename.replace('.parquet', '')
                if '_' in name_without_ext:
                    coords = name_without_ext.split('_')
                    if len(coords) >= 2:
                        file_x = int(coords[0])
                        file_y = int(coords[1])
                        self.get_logger().info(f"버튼 좌표: ({file_x}, {file_y})")
            except Exception as e:
                self.get_logger().warn(f"좌표 정보 추출 실패: {e}")
        except Exception as e:
            self.get_logger().error(f"파일 로드 실패: {e}")
            raise
    
    def move_motors_to_positions(self, positions):
        """모든 모터를 지정된 위치로 이동시킵니다"""
        for i, dxl_id in enumerate(self.dxl_ids):
            if i < len(positions):
                result, error = self.packetHandler.write4ByteTxRx(
                    self.portHandler, dxl_id, 116, int(positions[i])
                )
                if result != COMM_SUCCESS:
                    self.get_logger().warn(f"모터 ID {dxl_id} 위치 설정 실패: {positions[i]}")
    
    def start_safe_playback(self):
        """안전한 재생 시퀀스를 시작합니다"""
        if self.playback_data is None:
            self.get_logger().error("재생할 데이터가 없습니다.")
            self.cleanup_and_exit()
            return
        
        try:
            # 1단계: 데이터의 첫 번째 위치 추출
            first_row = self.playback_data.iloc[0]
            first_positions = [
                first_row['motor_0_position'],
                first_row['motor_1_position'],
                first_row['motor_2_position'],
                first_row['motor_3_position'],
                first_row['motor_4_position'],
                first_row['motor_5_position']
            ]
            
            # 2단계: 데이터의 마지막 위치 추출
            last_row = self.playback_data.iloc[-1]
            last_positions = [
                last_row['motor_0_position'],
                last_row['motor_1_position'],
                last_row['motor_2_position'],
                last_row['motor_3_position'],
                last_row['motor_4_position'],
                last_row['motor_5_position']
            ]
            
            self.get_logger().info("재생 시퀀스 시작")
            
            # 3단계: 현재 위치에서 첫 데이터 위치로 천천히 이동
            self.get_logger().info("데이터 시작 위치로 이동")
            self.smooth_move_to_position(first_positions, duration_seconds=1.5)
            
            self.get_logger().info("데이터 재생 시작")
            self.execute_playback_data()
            
            # 5단계: 마지막 위치에서 홈 위치로 복귀
            self.get_logger().info("홈 위치로 복귀")
            self.smooth_move_to_position(HOME_POSITION, duration_seconds=1.5)
            
            self.get_logger().info("재생 완료 홈 위치로 복귀.")
            
        except Exception as e:
            self.get_logger().error(f"재생 중 오류 발생: {e}")
        finally:
            self.cleanup_and_exit()
    
    def execute_playback_data(self):
        for idx, row in self.playback_data.iterrows():
            time.sleep(PLAY_TIME)  # 원본 녹화 간격과 동일
            
            positions = [
                row['motor_0_position'],
                row['motor_1_position'],
                row['motor_2_position'],
                row['motor_3_position'],
                row['motor_4_position'],
                row['motor_5_position']
            ]
            
            self.move_motors_to_positions(positions)
            
            if idx % 100 == 0:
                progress = (idx / len(self.playback_data)) * 100
                timestamp = row['timestamp']
                self.get_logger().info(
                    f"재생 진행: {progress:.1f}% ({idx+1}/{len(self.playback_data)}) "
                    f"Time: {timestamp:.2f}s"
                )
    
    def cleanup_and_exit(self):
        """정리 작업 후 프로그램 종료"""
        self.get_logger().info("프로그램 종료 작업 시작...")
        
        # 모터 토크 비활성화
        for dxl_id in self.dxl_ids:
            result, error = self.packetHandler.write1ByteTxRx(self.portHandler, dxl_id, 64, 0)
            if result == COMM_SUCCESS:
                self.get_logger().info(f"모터 ID {dxl_id} 토크 비활성화 완료")
        
        # 포트 닫기
        if self.portHandler:
            self.portHandler.closePort()
            self.get_logger().info("시리얼 포트 닫기 완료")
        
        # ROS2 종료
        self.get_logger().info("모든 정리 작업 완료. 프로그램을 종료합니다.")
        rclpy.shutdown()
        sys.exit(0)
    
    def signal_handler(self, signum, frame):
        """Ctrl+C 시그널 처리 - 안전하게 홈으로 복귀 후 종료"""
        self.get_logger().info("Ctrl+C 감지! 안전하게 홈 위치로 복귀 중...")
        try:
            # 홈 위치로 복귀
            self.smooth_move_to_position(HOME_POSITION, duration_seconds=2.0)
        except:
            pass  # 복귀 실패해도 계속 진행
        self.cleanup_and_exit()


def main(args=None):
    rclpy.init(args=args)
    
    # Command line argument에서 파일 경로 받기 (필수)
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"YOLO에서 선택된 파일: {os.path.basename(file_path)}")
        print(f"전체 경로: {file_path}")
    else:
        print("오류: 파일 경로가 제공되지 않았습니다.")
        print("이 스크립트는 YOLO 노드에서 자동으로 실행되어야 합니다.")
        return
    
    # 파일 존재 확인
    if not os.path.exists(file_path):
        print(f"오류: 파일이 존재하지 않습니다: {file_path}")
        print("YOLO 노드의 파일 선택 로직에 문제가 있을 수 있습니다.")
        return
    
    try:
        playback_node = SafePlaybackNode(file_path)
        print("로봇 재생을 시작합니다...")
        # 안전한 재생 시작
        playback_node.start_safe_playback()
    except KeyboardInterrupt:
        print("프로그램이 중단되었습니다.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
