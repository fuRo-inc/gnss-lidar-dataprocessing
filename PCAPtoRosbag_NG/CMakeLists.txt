cmake_minimum_required(VERSION 3.8.11)

project(PCAPtoRosbag)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_C_STANDARD 99)

find_package(ament_cmake REQUIRED)
find_package(rclcpp REQUIRED)
find_package(std_msgs REQUIRED)
find_package(builtin_interfaces REQUIRED)
find_package(rosidl_default_generators REQUIRED)
find_package(rosbag2_cpp REQUIRED)
find_package(sensor_msgs REQUIRED)
find_package(yaml-cpp REQUIRED)

# ★ これ追加（shareディレクトリ検索に使う）
find_package(ament_index_cpp REQUIRED)

find_package(OpenSSL REQUIRED)

add_definitions(-DQT_MESSAGELOGCONTEXT)
add_subdirectory(libhesai)

add_library(hesai_sdk_lib
  driver/hesai_lidar_sdk.cc
)
target_link_libraries(hesai_sdk_lib PUBLIC lidar_lib ${OTHER_LIB})

# ★ ここを 14→17 に直す（重要）
set_target_properties(hesai_sdk_lib
  PROPERTIES CXX_STANDARD_REQUIRED TRUE CXX_STANDARD 17
)

target_include_directories(hesai_sdk_lib PUBLIC
  ${OTHER_INCLUDE_DIRS}
  driver
  ./
)

add_executable(PCAPtoBag main.cc)
target_include_directories(PCAPtoBag PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/driver

  # libhesai top-level
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/Common/include
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/Container/include
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/Lidar
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/Logger/include
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/PtcClient/include
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/PtcParser/include
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/SerialClient/include
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/Source/include

  # Udp parser & protocol
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/UdpParser
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/UdpParser/include
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/UdpParser/src
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/UdpProtocol

  # (optional) GPU parser headers (無くてもCPUなら大抵通るが、念のため)
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/UdpParserGpu
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/UdpParserGpu/include
  ${CMAKE_CURRENT_SOURCE_DIR}/libhesai/UdpParserGpu/src

  # hesai ros msgs (今の include 構成なら必要な場合あり)
  ${CMAKE_CURRENT_SOURCE_DIR}/hesai_ros_driver
)
target_link_directories(PCAPtoBag PUBLIC
  /opt/ros/humble/lib/
)

target_link_libraries(PCAPtoBag
  source_lib
  ptcClient_lib
  serialClient_lib
  log_lib
  rclcpp
  ${OPENSSL_LIBRARIES}
  yaml-cpp
)

ament_target_dependencies(PCAPtoBag
  rclcpp
  rosbag2_cpp
  sensor_msgs
  ament_index_cpp       # ★ 追加
  yaml-cpp
)

# 実行ファイルを install へ
install(TARGETS PCAPtoBag
  DESTINATION lib/${PROJECT_NAME}
)
install(TARGETS
  log_lib
  source_lib
  ptcClient_lib
  serialClient_lib
  lidar_lib
  DESTINATION lib
)
set_target_properties(PCAPtoBag PROPERTIES
  INSTALL_RPATH "\$ORIGIN/.."
)
# ★ correction を share にインストール（②で使う）
install(DIRECTORY correction
  DESTINATION share/${PROJECT_NAME}
)
install(DIRECTORY launch
  DESTINATION share/${PROJECT_NAME}
)


ament_package()
