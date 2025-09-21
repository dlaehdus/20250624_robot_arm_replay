"""
realsense.py에서 이 파일이 실행이 되면
slave - 10,11,12,13,14,15
모터와 연결하고 dataset_master.py에서 ros 2 토픽으로 발행하는 실시간 모터 좌표 리스트를 받아서
[0번모터위치,1번모터위치,2번모터위치,3번모터위치,4번모터위치,5번모터위치]
이런 형식으로 발행되는걸 받아서
[10번모터위치,11번모터위치,12번모터위치,13번모터위치,14번모터위치,15번모터위치] 이렇게 slave모터 현재 위치에 적용해야함
"""

import rclpy
from rclpy.node import Node
from std_msgs.msg import Int32MultiArray
from dynamixel_sdk import *
import signal
import sys
import time

PORT_CON = '/dev/ttyACM0'                       # slave 모터용 포트 (master와 다른 포트)
DXL_SLAVE_IDS = [10, 11, 12, 13, 14, 15]        # slave 모터 ID
TIMEOUT_SECONDS = 2.0                           # 1초 동안 메시지가 안 오면 토크 끄기

class SlaveNode(Node):
    def __init__(self):
        super().__init__('robot_arm_slave')
        
        # ROS2 subscriber 설정
        self.subscription = self.create_subscription(
            Int32MultiArray,
            'master_positions',
            self.position_callback,
            10
        )
        
        self.dxl_ids = DXL_SLAVE_IDS
        self.portHandler = PortHandler(PORT_CON)
        self.packetHandler = PacketHandler(2.0)
        
        self.torque_enabled = False
        self.last_message_time = None
        
        if not self.portHandler.openPort():
            self.get_logger().error("포트를 열 수 없습니다.")
            raise RuntimeError("Failed to open the port")
        if not self.portHandler.setBaudRate(57600):
            self.get_logger().error("보드레이트 설정 실패.")
            raise RuntimeError("Failed to set baudrate")
        
        # 토크 상태 확인용 타이머 (0.1초마다 체크)
        self.timeout_timer = self.create_timer(0.1, self.check_timeout)
        
        # 시그널 핸들러 설정
        signal.signal(signal.SIGINT, self.signal_handler)
        
        self.get_logger().info("Slave node initialized.")
        self.get_logger().info("Waiting for master position data...")
        
    def position_callback(self, msg):
        """master 위치 데이터 수신 콜백 [web:58]"""
        # 현재 시간 업데이트
        self.last_message_time = time.time()
        
        # 데이터 유효성 검사
        if len(msg.data) != 6:
            self.get_logger().warn(f"잘못된 데이터 길이: {len(msg.data)}")
            return
        
        # 토크가 꺼져있으면 켜기
        if not self.torque_enabled:
            self.enable_torque()
            
        # 즉시 미러링 실행
        self.mirror_positions(list(msg.data))
            
    def check_timeout(self):
        """메시지 타임아웃 체크 [web:63]"""
        if self.last_message_time is None:
            return
            
        # 마지막 메시지로부터 경과 시간 계산
        elapsed_time = time.time() - self.last_message_time
        
        # 타임아웃이 발생하고 토크가 켜져있으면 끄기
        if elapsed_time > TIMEOUT_SECONDS and self.torque_enabled:
            self.disable_torque()
            self.get_logger().info("Master 발행 중지됨 - 토크 비활성화")
            
    def enable_torque(self):
        """모든 slave 모터 토크 활성화 [web:61]"""
        for dxl_id in self.dxl_ids:
            result, error = self.packetHandler.write1ByteTxRx(self.portHandler, dxl_id, 64, 1)
            if result != COMM_SUCCESS:
                self.get_logger().error(f"모터 ID {dxl_id} 토크 활성화 실패")
                
        self.torque_enabled = True
        self.get_logger().info("Master 발행 시작됨 - 토크 활성화 및 미러링 시작")
        
    def disable_torque(self):
        """모든 slave 모터 토크 비활성화 [web:61]"""
        for dxl_id in self.dxl_ids:
            result, error = self.packetHandler.write1ByteTxRx(self.portHandler, dxl_id, 64, 0)
            if result != COMM_SUCCESS:
                self.get_logger().error(f"모터 ID {dxl_id} 토크 비활성화 실패")
                
        self.torque_enabled = False
            
    def mirror_positions(self, master_positions):
        """master 위치를 slave 모터에 적용"""
        try:
            for i, dxl_id in enumerate(self.dxl_ids):
                target_position = master_positions[i]
                
                # Goal Position 설정 (기존 모터 모드 그대로 사용)
                result, error = self.packetHandler.write4ByteTxRx(self.portHandler, dxl_id, 116, target_position)
                
                if result != COMM_SUCCESS:
                    self.get_logger().warn(f"모터 ID {dxl_id} 위치 설정 실패: {target_position}")

            current_time_ms = int(time.time() * 1000)
            if current_time_ms % 500 < 50:
                self.get_logger().info(f"Mirroring: {master_positions}")
                
        except Exception as e:
            self.get_logger().error(f"미러링 중 오류: {e}")
            
    def signal_handler(self, signum, frame):
        """Ctrl+C 처리"""
        self.get_logger().info("Ctrl+C detected. Shutting down...")
        
        # 모든 모터 토크 비활성화
        self.disable_torque()
                
        if self.portHandler:
            self.portHandler.closePort()
            
        rclpy.shutdown()
        sys.exit(0)

def main(args=None):
    rclpy.init(args=args)
    
    try:
        slave_node = SlaveNode()
        rclpy.spin(slave_node)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if rclpy.ok():
            rclpy.shutdown()

if __name__ == '__main__':
    main()
