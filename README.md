lerobot을 이용한 모방학습이 학습데이터를 수집하는 시간이 부족하여 대체할수 있는 방안을 만들기 위해 만든 코드

<img width="693" height="145" alt="image" src="https://github.com/user-attachments/assets/4ef94edb-d662-49b0-98dc-243530a0902d" />

<img width="460" height="101" alt="image" src="https://github.com/user-attachments/assets/7f40c6ae-0b16-44c8-91d2-e8d633396524" />

realsense.py
  데이터 수집, 로봇팔 실행을 하는 메인 코드
  button push작업을 진행하기 위한 버튼 인식 가중치 파일
  실행순서
  1. 이 파일을 실행하면 코드상에서 해로운 터미널을 열고 realsens카메라와 연동
  2. <img width="1391" height="568" alt="image" src="https://github.com/user-attachments/assets/e46204f2-a7da-4ad2-8503-f0f1c4a93f98" />
  3. 버튼과의 x, y위치를 지속적으로 측정함
  4. 키보드 입력 스레드를 시작함 q는 종료, r은 기록된 파일 재생, s는 데이터 수집
  5. <img width="465" height="107" alt="image" src="https://github.com/user-attachments/assets/cf4b6f3c-65da-4060-a87f-637a6cda5f0b" />
  q를 입력시 <img width="454" height="157" alt="image" src="https://github.com/user-attachments/assets/ca1cdda2-11b8-46dc-b253-1af4f81ea422" />
  s를 입력시 <img width="1457" height="466" alt="image" src="https://github.com/user-attachments/assets/0528d271-e4c5-4398-83fc-bbde67c1171f" />
  <img width="454" height="179" alt="image" src="https://github.com/user-attachments/assets/00ecabcc-e772-4a56-9f6b-2aee30f00c3e" />
  r을 입력시 로봇팔 데이터 수집된것중 가장 가까운 좌표값을 가진것을 측정하고 재생


dataset_master.py
  데이터를 slave로 전송하며 해당 좌표값을 측정하는 코드
  master는 모터의 토크를 제거후 실시간 위치값만 읽어서 slave에 전송함 이렇게 하면
  slave는 모터의 오차를 고려해서 원하는 행동을 한 좌표값을 측정하기 때문에 모터의 힘이 부족해서 모터의 오차가 누적되어 로봇팔의 처짐이 발생해도 그것까지 고려한 파일을 만들수 있어 정확한 파일 형성이 가능함
  데이터를 slave에 0.02초마다 보냄과 동시에 해당 시간간격을 기록하며 parquet파일로 만들어짐 csv보다 용량이 적음
  실행순서
  1. 키 입력 g를 입력하면 발행과 동시에 해당 데이터가 저장이 됨 
  
  



  
