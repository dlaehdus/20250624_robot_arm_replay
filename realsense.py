"""
이걸 실행하면 
cartesian_x = x
cartesian_y = y
로 내가 지정한 좌표 변수를 만들고
이 파일을 실행하면 x, y, z 좌표를 기반으로 하는 텍스트 파일을 만들고 파일명은 해당 입력된 좌표값에 따라서 x_y_z로 저장되어야하고 그 저장된 파일을
dataset 폴더 안에 저장이 되어야해 그 후
ros를 키며 dataset_master.py 파일, dataset_slave.py을 실행시켜야해  
"""
import sys                                  # 시스템 레벨 기능
import rclpy                                # ros 노드 생성                               
from rclpy.node import Node                 # ros 노드 클래스
from sensor_msgs.msg import Image           # ros2 이미지 메시지 타입 정의
from geometry_msgs.msg import Point         # 좌표 발행을 위한 메시지 타입 추가
from cv_bridge import CvBridge              # ROS2 Image 메시지 ↔ OpenCV 이미지 변환
import cv2                                  # OpenCV - 이미지 처리, 화면 표시, 그래픽 그리기
from ultralytics import YOLO            
import subprocess                           # 새 터미널 실행용
import time                                 # 딜레이용
import os                                   # 폴더 생성용
import threading                            # 키보드 입력을 위한 스레딩
import signal                               # 시그널 처리용
import math                                 # 유클리드 거리 계산용
import glob                                 # 파일 검색용

BUTTON = 2

class YOLOv8Node(Node):
    def __init__(self):
        super().__init__('yolov8_center_node')
        self.model = YOLO("button.pt")      # button 가중치 파일로 버튼 검출용 YOLOv8 모델 로드
        self.bridge = CvBridge()            # ROS2↔OpenCV 이미지 변환 객체 생성
        self.cartesian_x = 0
        self.cartesian_y = 0
        self.should_exit = False
        self.realsense_process = None
        self.create_dataset_folder()
        
        # 좌표 발행을 위한 퍼블리셔 추가
        self.coordinate_publisher = self.create_publisher(Point, 'button_coordinates', 10)
        
        self.subscription = self.create_subscription(
            Image,                                  # 매시지 타입
            '/camera/camera/color/image_raw',       # 사용중인 토픽 그대로
            self.listener_callback,                 # 콜백 함수
            10                                      # 큐 크기 10
        )
        self.start_keyboard_thread()

    def find_closest_dataset_file(self, target_x, target_y):
        """현재 좌표와 가장 가까운 dataset 파일을 찾습니다"""
        dataset_path = "/home/limdoyeon/robot_arm_planb/dataset"

        if not os.path.exists(dataset_path):
            print(f"Dataset 폴더가 존재하지 않습니다: {dataset_path}")
            return None

        # dataset 폴더에서 모든 parquet 파일 찾기
        parquet_files = glob.glob(os.path.join(dataset_path, "*.parquet"))

        if not parquet_files:
            print("Dataset 폴더에 parquet 파일이 없습니다.")
            print("먼저 's' 키를 눌러 데이터셋을 수집해주세요.")
            return None

        print(f"\n=== 좌표 매칭 시작 ===")
        print(f"목표 좌표: ({target_x}, {target_y})")
        print(f"찾은 dataset 파일들: {[os.path.basename(f) for f in parquet_files]}")

        closest_file = None
        min_distance = float('inf')
        exact_match = None
        valid_files = []  # 좌표 추출이 성공한 파일들

        for file_path in parquet_files:
            filename = os.path.basename(file_path)  # 파일명만 추출

            # 파일명에서 좌표 추출 (예: "320_240.parquet" -> x=320, y=240)
            try:
                name_without_ext = filename.replace('.parquet', '')
                if '_' in name_without_ext:
                    coords = name_without_ext.split('_')
                    if len(coords) >= 2:
                        file_x = int(coords[0])
                        file_y = int(coords[1])

                        valid_files.append((filename, file_x, file_y))

                        # 정확한 매칭 확인
                        if file_x == target_x and file_y == target_y:
                            exact_match = file_path
                            print(f"정확한 매칭 파일 발견: {filename} ({file_x}, {file_y})")
                            break
                        
                        # 유클리드 거리 계산
                        distance = math.sqrt((file_x - target_x)**2 + (file_y - target_y)**2)

                        if distance < min_distance:
                            min_distance = distance
                            closest_file = file_path

                        print(f"파일: {filename} - 좌표: ({file_x}, {file_y}), 거리: {distance:.2f}")
            except (ValueError, IndexError) as e:
                print(f"좌표 추출 실패 - 파일: {filename}, 오류: {e}")
                continue
            
        if not valid_files:
            print("유효한 좌표를 가진 dataset 파일이 없습니다.")
            print("파일명이 'X_Y.parquet' 형식인지 확인해주세요.")
            return None

        # 정확한 매칭이 있으면 그것을 반환, 없으면 가장 가까운 파일 반환
        result_file = exact_match if exact_match else closest_file

        print(f"\n=== 매칭 결과 ===")
        if result_file:
            result_filename = os.path.basename(result_file)
            if exact_match:
                print(f"선택된 파일: {result_filename} (정확한 매칭)")
            else:
                print(f"선택된 파일: {result_filename} (가장 가까운 좌표, 거리: {min_distance:.2f})")
                # 가장 가까운 파일의 좌표 정보 표시
                for fname, fx, fy in valid_files:
                    if fname == result_filename:
                        print(f"선택된 파일의 좌표: ({fx}, {fy})")
                        break
        else:
            print("적절한 dataset 파일을 찾을 수 없습니다.")

        print(f"==================\n")
        return result_file

    def start_keyboard_thread(self):
        """키보드 입력 스레드 시작"""
        def keyboard_input():
            while not self.should_exit:
                try:
                    print("\n명령어를 입력하세요:")
                    print("ESC 또는 q: 프로그램 종료 (RealSense 터미널도 함께 종료)")
                    print("r: return.py 실행 후 종료 (RealSense 터미널도 함께 종료)")
                    print("s: 데이터셋 수집 후 종료 (RealSense 터미널도 함께 종료)")
                    print(f"현재 측정된 좌표: ({self.cartesian_x}, {self.cartesian_y})")
                    
                    key = input("입력: ").strip().lower()
                    
                    if key in ['esc', 'q', 'exit']:
                        self.clean_exit()
                        break
                    elif key == 'r':
                        self.run_return_script_and_exit()
                        break
                    elif key == 's':
                        self.handle_s_key()
                        break
                    else:
                        print(f"알 수 없는 명령어: {key}")
                        
                except EOFError:
                    break
                except KeyboardInterrupt:
                    self.clean_exit()
                    break
        
        thread = threading.Thread(target=keyboard_input, daemon=True)
        thread.start()

    def clean_exit(self):
        """깔끔한 종료 (q키 방식)"""
        print("모든 프로세스를 종료합니다...")
        self.should_exit = True
        
        # OpenCV 윈도우 먼저 닫기
        cv2.destroyAllWindows()
        
        # ROS 종료
        if rclpy.ok():
            rclpy.shutdown()
        
        # RealSense 프로세스 종료
        self.terminate_all_processes()

    def terminate_all_processes(self):
        """모든 관련 프로세스 종료"""
        try:
            subprocess.run(["pkill", "-f", "realsense2_camera"], check=False)
            subprocess.run(["pkill", "-f", "rs_launch.py"], check=False)
            subprocess.run(["pkill", "-f", "gnome-terminal.*realsense"], check=False)
            if hasattr(self, 'realsense_process') and self.realsense_process:
                try:
                    self.realsense_process.terminate()
                    time.sleep(1)
                    if self.realsense_process.poll() is None:
                        self.realsense_process.kill()
                except:
                    pass
            
        except Exception as e:
            print(f"프로세스 종료 중 오류: {e}")

    def handle_s_key(self):
        """S 키 처리 - dataset 수집 후 모든 프로세스 종료"""
        print("데이터셋 수집을 시작하고 모든 프로세스를 종료합니다...")
        self.launch_dataset_scripts_in_terminals()
        
        # 잠시 대기 후 깔끔한 종료
        time.sleep(1)
        self.clean_exit()

    def create_dataset_folder(self):
        """dataset 폴더 생성"""
        if not os.path.exists("dataset"):
            os.makedirs("dataset")
            print("dataset 폴더가 생성되었습니다.")

    def launch_dataset_scripts_in_terminals(self):
        """별도 터미널에서 dataset 스크립트들 실행"""
        try:
            # dataset_master.py 실행을 위한 터미널
            master_script_content = '''#!/bin/bash
echo "Dataset Master를 시작합니다"
source /home/limdoyeon/miniconda3/etc/profile.d/conda.sh
conda activate env_planb
source /opt/ros/humble/setup.bash
cd /home/limdoyeon/robot_arm_planb
python3 dataset_master.py
echo "Dataset Master가 종료되었습니다."
exec bash
'''
            master_script_path = "/tmp/launch_dataset_master.sh"
            with open(master_script_path, "w") as f:
                f.write(master_script_content)
            subprocess.run(["chmod", "+x", master_script_path])

            # dataset_slave.py 실행을 위한 터미널
            slave_script_content = '''#!/bin/bash
echo "Dataset Slave를 시작합니다"
source /home/limdoyeon/miniconda3/etc/profile.d/conda.sh
conda activate env_planb
source /opt/ros/humble/setup.bash
cd /home/limdoyeon/robot_arm_planb
python3 dataset_slave.py
echo "Dataset Slave가 종료되었습니다."
exec bash
'''
            slave_script_path = "/tmp/launch_dataset_slave.sh"
            with open(slave_script_path, "w") as f:
                f.write(slave_script_content)
            subprocess.run(["chmod", "+x", slave_script_path])

            # dataset_master.py 터미널 실행
            print("dataset_master.py를 새 터미널에서 실행합니다...")
            subprocess.Popen(['gnome-terminal', '--title=Dataset Master', '--', 'bash', master_script_path])
            time.sleep(0.5)
            
            # dataset_slave.py 터미널 실행
            print("dataset_slave.py를 새 터미널에서 실행합니다...")
            subprocess.Popen(['gnome-terminal', '--title=Dataset Slave', '--', 'bash', slave_script_path])
            
        except Exception as e:
            print(f"스크립트 실행 중 오류: {e}")

    def run_return_script_and_exit(self):
        """별도 터미널에서 return.py 실행 후 모든 프로세스 종료"""
        try:
            print(f"현재 측정된 좌표: ({self.cartesian_x}, {self.cartesian_y})")
            print("매칭되는 dataset 파일을 검색 중...")
            
            # 가장 가까운 dataset 파일 찾기
            closest_file = self.find_closest_dataset_file(self.cartesian_x, self.cartesian_y)
            
            if closest_file is None:
                print("실행할 dataset 파일을 찾을 수 없습니다.")
                return

            print(f"return.py를 실행합니다. 파일: {os.path.basename(closest_file)}")

            # return.py 실행을 위한 터미널 스크립트 생성 (파일 경로를 argument로 전달)
            return_script_content = f'''#!/bin/bash
echo "Return 스크립트를 시작합니다"
echo "사용할 파일: {closest_file}"
source /home/limdoyeon/miniconda3/etc/profile.d/conda.sh
conda activate env_planb
source /opt/ros/humble/setup.bash
cd /home/limdoyeon/robot_arm_planb
python3 return.py "{closest_file}"
echo "Return 스크립트가 종료되었습니다."
exec bash
'''
            return_script_path = "/tmp/launch_return.sh"
            with open(return_script_path, "w") as f:
                f.write(return_script_content)
            subprocess.run(["chmod", "+x", return_script_path])

            # return.py 터미널 실행
            subprocess.Popen(['gnome-terminal', '--title=Return Script', '--', 'bash', return_script_path])

            # 잠시 대기 후 깔끔한 종료
            time.sleep(1)
            self.clean_exit()

        except Exception as e:
            print(f"return.py 실행 중 오류: {e}")
            self.clean_exit()

    def listener_callback(self, msg):
        if self.should_exit:
            return
            
        frame = self.bridge.imgmsg_to_cv2(msg, desired_encoding="bgr8")
        results = self.model.predict(source=frame, classes = BUTTON,conf=0.75, imgsz=640, verbose=False)
        
        for result in results:
            boxes = result.boxes
            if boxes is None or len(boxes) == 0:
                continue
            
            for box in boxes:
                x1, y1, x2, y2 = box.xyxy[0].tolist()
                conf = float(box.conf[0])
                cls_id = int(box.cls[0])
                cls_name = result.names[cls_id]
                
                cx = int((x1 + x2) / 2.0)
                cy = int((y1 + y2) / 2.0)
                
                # 좌표 변수에 저장
                self.cartesian_x = cx
                self.cartesian_y = cy
                
                # ROS2 토픽으로 좌표 발행
                coordinate_msg = Point()
                coordinate_msg.x = float(cx)
                coordinate_msg.y = float(cy)
                coordinate_msg.z = 0.0  # 2D이므로 z는 0
                self.coordinate_publisher.publish(coordinate_msg)
                
                cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), (0, 255, 0), 2)
                cv2.circle(frame, (cx, cy), 3, (255, 0, 0), -1)
                cv2.putText(frame, f"{cls_name} {conf:.2f} ({cx},{cy})",(int(x1), int(y1) - 5), cv2.FONT_HERSHEY_SIMPLEX,0.5, (0, 0, 0), 2)
        
        cv2.imshow("YOLOv8 RealSense", frame)
        cv2.waitKey(1)


def launch_realsense():
    """새 터미널에서 RealSense 카메라 시작"""
    print("RealSense 카메라를 새 터미널에서 시작합니다...")
    
    script_content = '''#!/bin/bash
echo "RealSense 카메라를 시작합니다"
source /home/limdoyeon/miniconda3/etc/profile.d/conda.sh
conda activate env_planb
source /opt/ros/humble/setup.bash
ros2 launch realsense2_camera rs_launch.py
echo "RealSense 카메라가 종료되었습니다."
'''
    script_path = "/tmp/launch_realsense.sh"
    with open(script_path, "w") as f:
        f.write(script_content)
    subprocess.run(["chmod", "+x", script_path])
    try:
        process = subprocess.Popen(['gnome-terminal', '--title=RealSense Camera', '--', 'bash', script_path])
        print("RealSense 터미널이 시작되었습니다.")
        time.sleep(5)
        return process
    except FileNotFoundError:
        print("터미널을 자동으로 열 수 없습니다.")
        input("준비가 완료되면 Enter 키를 누르세요")
        return None


def main(args=None):
    realsense_process = launch_realsense()
    
    rclpy.init(args=args)
    node = YOLOv8Node()
    node.realsense_process = realsense_process
    
    try:
        print("YOLO 노드를 시작합니다. 카메라 토픽을 기다리는 중")
        print("키보드 입력 스레드가 시작됩니다")
        rclpy.spin(node)
    except KeyboardInterrupt:
        print("KeyboardInterrupt 발생")
    finally:
        node.clean_exit()
        node.destroy_node()
        print("모든 프로세스가 완전히 종료되었습니다.")


if __name__ == '__main__':
    main()
