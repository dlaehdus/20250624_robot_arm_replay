"""
realsense.py에서 이 파일이 실행이 되면
master - 0,1,2,3,4,5
모터와 연결하고 G를 입력하면
master의 현재위치를 0.1초마다
[0번모터위치,1번모터위치,2번모터위치,3번모터위치,4번모터위치,5번모터위치]
이런 형식으로 ros2를 통해 발행해야함
동시에 같은 경로상의 dataset폴더 안에 해당 6개모터의 데이터를 저장해야해
예를 들어 0.1초에는 무슨값이 발행 0.2초에는 무슨값이 발행 그후 지속적으로 발행과 데이터 저장을 동시에 하면서
내가 ctrl+c를 누르면 데이터 저장과 모터 발행을 정지해야해
"""

import rclpy                                                # ROS2 Python 클라이언트 라이브러리로, 노드 생성과 ROS2 통신 기능을 제공
from rclpy.node import Node                                 # ROS2 노드 클래스의 기본 클래스로, 모든 ROS2 노드는 이를 상속
from std_msgs.msg import Int32MultiArray                    # 여러 개의 32비트 정수를 배열 형태로 전송하는 ROS2 메시지 타입
from geometry_msgs.msg import Point                         # 좌표 구독을 위한 메시지 타입 추가
from dynamixel_sdk import *                                 # Dynamixel 모터 제어를 위한 SDK로, 통신 프로토콜과 제어 함수들을 포함
import threading                                            # 키보드 입력을 위한 스레딩
import pandas as pd                                         # 데이터프레임 처리를 위한 라이브러리
import pyarrow.parquet as pq                                # Parquet 파일 저장을 위한 라이브러리
import os                                                   # 폴더 생성을 위한 라이브러리
import time                                                 # 시간 측정을 위한 라이브러리
import signal                                               # 시그널 처리를 위한 라이브러리
import sys                                                  # 시스템 종료를 위한 라이브러리


PORT_CON        = '/dev/ttyACM1'                            # 통신 포트 설정
DXL_MASTER_IDS  = [0, 1, 2, 3, 4, 5]                        # 다이나믹셀 아이디
RECODE_TIME     = 0.02                                      # 통신기 성능 최대로 사용


class MasterNode(Node):
    def __init__(self):
        super().__init__('robot_arm_master')
        self.publisher_ = self.create_publisher(Int32MultiArray, 'master_positions', 10)

        # 좌표 구독을 위한 서브스크라이버 추가
        self.coordinate_subscriber = self.create_subscription(
            Point,
            'button_coordinates',
            self.coordinate_callback,
            10
        )

        # 좌표 변수 초기화
        self.current_x = 0
        self.current_y = 0
        self.coordinate_received = False

        self.dxl_ids = DXL_MASTER_IDS
        self.portHandler = PortHandler(PORT_CON)
        self.packetHandler = PacketHandler(2.0)

        # 제어 상태 변수들
        self.is_recording = False                           # 현재 데이터 기록중인지 상태를 확인
        self.start_time = None                              # 녹화 시작 시간 저장
        self.data_records = []                              # Parquet 저장을 위한 데이터 리스트 cvs보다 70% 빠름
        self.parquet_filename = None                        # 저장될 파일 명

        if not self.portHandler.openPort():
            self.get_logger().error("포트를 열 수 없습니다.")
            raise RuntimeError("Failed to open the port")
        if not self.portHandler.setBaudRate(57600):
            self.get_logger().error("보드레이트 설정 실패.")
            raise RuntimeError("Failed to set baudrate")

        for i, dxl_id in enumerate(self.dxl_ids):
            result, error = self.packetHandler.write1ByteTxRx(self.portHandler, dxl_id, 64, 0)
            if result != COMM_SUCCESS:
                self.get_logger().error(f"모터 ID {dxl_id} 토크 비활성화 실패.")
                raise RuntimeError(f"Failed to disable torque for motor ID {dxl_id}")

        self.dataset_dir = "dataset"                                # dataset 폴더가 없으면 생성
        if not os.path.exists(self.dataset_dir):
            os.makedirs(self.dataset_dir)
        
        self.timer = None                                           # 타이머 변수 초기화
        self.input_thread = threading.Thread(target=self.keyboard_input_handler, daemon=True)
        self.input_thread.start()
        signal.signal(signal.SIGINT, self.signal_handler)
        self.get_logger().info("Master node initialized. Press 'G' to start recording.")
        self.get_logger().info("Waiting for button coordinates...")

    def coordinate_callback(self, msg):
        """좌표 메시지를 받는 콜백 함수"""
        self.current_x = int(msg.x)
        self.current_y = int(msg.y)
        self.coordinate_received = True
        # 좌표가 업데이트될 때마다 로그 출력 (필요시 주석 해제)
        # self.get_logger().info(f"Button coordinates updated: ({self.current_x}, {self.current_y})")

    def keyboard_input_handler(self):                               # 무한 루프로 사용자 입력을 감시
        while True:
            try:
                print("\n명령어를 입력하세요:")
                print("G: 녹화 시작 (좌표 수신 필요)")
                print("S: 녹화 중지")
                print("Ctrl+C: 프로그램 종료")
                
                user_input = input("입력: ").strip().upper()
                if user_input == 'G' and not self.is_recording:     # G를 누르면 기록 시작
                    if self.coordinate_received:
                        self.start_recording()
                    else:
                        self.get_logger().warn("No coordinates received yet. Make sure YOLO node is running and detecting buttons.")
                elif user_input == 'S' and self.is_recording:       # S를 누르면 기록 중지
                    self.stop_recording()
                elif user_input == 'G' and self.is_recording:
                    self.get_logger().warn("Already recording. Press 'S' to stop first.")
                elif user_input == 'S' and not self.is_recording:
                    self.get_logger().warn("Not recording. Press 'G' to start recording first.")
                else:
                    self.get_logger().info(f"Unknown command: {user_input}")
            except EOFError:
                break
            except KeyboardInterrupt:
                break

    def start_recording(self):
        self.is_recording = True                                    # 녹화 상태를 True로 
        self.start_time = time.time()                               # 현재 시간을 시작 시간으로 저장
        
        # 좌표를 파일명에 반영
        file_name = f"{self.current_x}_{self.current_y}"
        self.parquet_filename = os.path.join(self.dataset_dir, f"{file_name}.parquet")
        
        self.data_records = []
        self.timer = self.create_timer(RECODE_TIME, self.control_loop)
        
        self.get_logger().info(f"Recording started with coordinates ({self.current_x}, {self.current_y})")
        self.get_logger().info(f"Data will be saved to {self.parquet_filename}")
        self.get_logger().info("Press 'S' to stop recording or Ctrl+C to exit.")

    def stop_recording(self):
        """데이터 기록 중지 및 Parquet 파일 저장"""
        if self.is_recording:
            self.is_recording = False

            if self.timer:
                self.timer.cancel()
                self.timer = None

            if self.data_records:
                df = pd.DataFrame(self.data_records)
                df.to_parquet(self.parquet_filename, index=False)
                self.get_logger().info(f"Parquet file saved: {self.parquet_filename}")
                self.get_logger().info(f"Total records saved: {len(self.data_records)}")
            else:
                self.get_logger().warn("No data to save.")

            self.data_records = []
            self.get_logger().info("Recording stopped. Press 'G' to start again.")

    def read_motor_positions(self):
        """모든 모터의 현재 위치를 읽는 함수"""
        positions = []
        for dxl_id in self.dxl_ids:
            position, result, error = self.packetHandler.read4ByteTxRx(self.portHandler, dxl_id, 132)
            if result != COMM_SUCCESS:
                self.get_logger().warn(f"모터 ID {dxl_id} 위치 읽기 실패.")
                position = 0  
            positions.append(int(position))
        return positions

    def control_loop(self):
        """0.02초마다 실행되는 제어 루프"""
        if not self.is_recording:
            return

        positions = self.read_motor_positions()
        
        # ROS2 토픽으로 발행
        msg = Int32MultiArray()
        msg.data = positions
        self.publisher_.publish(msg)
        
        current_time = round(time.time() - self.start_time, 2)
        
        # Parquet 저장을 위한 데이터 레코드 생성
        record = {
            'timestamp': current_time,
            'button_x': self.current_x,  # 버튼 좌표도 함께 저장
            'button_y': self.current_y,
            'motor_0_position': positions[0],
            'motor_1_position': positions[1],
            'motor_2_position': positions[2],
            'motor_3_position': positions[3],
            'motor_4_position': positions[4],
            'motor_5_position': positions[5]
        }
        self.data_records.append(record)
        
        # 100개마다 중간 저장 로그 (선택사항 - 메모리 절약용)
        if len(self.data_records) % 100 == 0:
            self.get_logger().info(f"Collected {len(self.data_records)} records... Current coordinates: ({self.current_x}, {self.current_y})")
        
        # 매 루프마다 간단한 정보만 출력 (로그 스팸 방지)
        if len(self.data_records) % 50 == 0:  # 50번마다 출력
            self.get_logger().info(f"Time: {current_time:.2f}s, Positions: {positions}")

    def signal_handler(self, signum, frame):
        self.get_logger().info("Ctrl+C detected. Shutting down...")
        self.stop_recording()
        
        if self.portHandler:
            self.portHandler.closePort()
        
        rclpy.shutdown()
        sys.exit(0)

    def destroy_node(self):
        """노드 종료 시 포트 정리"""
        if hasattr(self, 'portHandler') and self.portHandler:
            self.portHandler.closePort()
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    
    try:
        master_node = MasterNode()
        print("Master node started successfully!")
        print("Waiting for button coordinates from YOLO node...")
        print("Press 'G' to start recording once coordinates are received.")
        rclpy.spin(master_node)
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received.")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()
