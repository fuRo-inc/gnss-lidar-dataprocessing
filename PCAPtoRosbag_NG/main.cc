#include "driver/hesai_lidar_sdk.hpp"
#include "libhesai/lidar_types.h"
#include <rosbag2_cpp/writer.hpp>
#include <std_msgs/msg/header.hpp>
//#include "../../HesaiLidar_ROS_2.0/src/manager/source_driver_ros2.hpp"
#include <string>
#include <rosbag2_storage/storage_options.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp/serialization.hpp>
#include <rclcpp/serialized_message.hpp>
#include <sensor_msgs/point_cloud2_iterator.hpp>
#include <std_msgs/msg/u_int8_multi_array.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <sstream>
#include <hesai_ros_driver/msg/udp_frame.hpp>
#include <hesai_ros_driver/msg/udp_packet.hpp>
#include <hesai_ros_driver/msg/ptp.hpp>
#include <hesai_ros_driver/msg/firetime.hpp>
#include <hesai_ros_driver/msg/loss_packet.hpp>
#include <ament_index_cpp/get_package_share_directory.hpp>
#include <fstream>
#include <memory>
#include <chrono>
#include <string>
#include <functional>
#include <regex>
#include <csignal>
#include <boost/thread.hpp>
#include <filesystem>
#include <cmath>
#include <cctype>
#include <algorithm>
namespace stdfs = std::filesystem;
uint32_t last_frame_time = 0;
uint32_t cur_frame_time = 0;
rosbag2_cpp::Writer writer;
bool endflag = false;

struct NodeConfig {
  double start_angle_deg = 0.0;
  bool apply_axis_remap = true;
  std::string lidar_model = "PandarXT";
  std::string timestamp_type = "realtime";
  std::string frame_id = "hesai_lidar";
  std::string publish_type = "points";
  bool write_point_clouds = true;
};

std::unique_ptr<NodeConfig> ParseConfig(int argc, char *argv[]) {
  auto config = std::make_unique<NodeConfig>();
  auto to_lower = [](std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return value;
  };
  auto parse_bool = [&](const std::string &value, bool default_value) {
    auto lower = to_lower(value);
    if (lower == "true" || lower == "1" || lower == "yes") return true;
    if (lower == "false" || lower == "0" || lower == "no") return false;
    return default_value;
  };
  for (int i = 2; i < argc; ++i) {
    std::string arg(argv[i]);
    auto eq_pos = arg.find('=');
    std::string key = arg;
    std::string value;
    if (eq_pos != std::string::npos) {
      key = arg.substr(0, eq_pos);
      value = arg.substr(eq_pos + 1);
    }
    if (key == "--start_angle_deg" || key == "--start-angle-deg") {
      try {
        config->start_angle_deg = std::stod(value);
      } catch (...) {
        std::cerr << "Invalid start_angle_deg value: " << value << std::endl;
      }
    } else if (key == "--apply_axis_remap" || key == "--apply-axis-remap") {
      config->apply_axis_remap = parse_bool(value, config->apply_axis_remap);
    } else if (key == "--lidar_model" || key == "--lidar-model") {
      config->lidar_model = value;
    } else if (key == "--timestamp_type" || key == "--timestamp-type") {
      config->timestamp_type = value;
    } else if (key == "--frame_id" || key == "--frame-id") {
      config->frame_id = value;
    } else if (key == "--publish_type" || key == "--publish-type") {
      config->publish_type = to_lower(value);
      if (config->publish_type != "points" &&
          config->publish_type != "both" &&
          config->publish_type != "raw") {
        std::cerr << "Unsupported publish_type '" << value
                  << "', falling back to 'points'" << std::endl;
        config->publish_type = "points";
      }
      if (config->publish_type == "raw") {
        std::cerr << "raw publish_type is not implemented, "
                  << "recording point clouds instead" << std::endl;
        config->publish_type = "points";
      }
    }
  }
  config->write_point_clouds =
      (config->publish_type == "points" || config->publish_type == "both");
  std::cout << "[CONFIG] start_angle_deg=" << config->start_angle_deg
            << " apply_axis_remap=" << std::boolalpha
            << config->apply_axis_remap << " lidar_model=" << config->lidar_model
            << " timestamp_type=" << config->timestamp_type
            << " frame_id=" << config->frame_id
            << " publish_type=" << config->publish_type << std::endl;
  return config;
}

int NormalizeAzimuth(int degrees_hundredths) {
  int normalized = degrees_hundredths % 36000;
  if (normalized < 0) normalized += 36000;
  return normalized;
}

class ScanSplitter {
 public:
  explicit ScanSplitter(const NodeConfig &config, rosbag2_cpp::Writer *writer)
      : config_(config),
        writer_(writer),
        serializer_(),
        start_angle_hundred_(NormalizeAzimuth(
            static_cast<int>(std::round(config.start_angle_deg * 100.0)))) {}

  void processFrame(const LidarDecodedFrame<LidarPointXYZIRT> &frame) {
    if (!config_.write_point_clouds || !writer_) {
      return;
    }
    if (frame.points_num == 0 || frame.pointData == nullptr) {
      return;
    }
    for (uint32_t i = 0; i < frame.points_num; ++i) {
      const LidarPointXYZIRT &point = frame.points[i];
      int azimuth = NormalizeAzimuth(
          static_cast<int>(std::round(frame.pointData[i].azimuth * 100.0)));
      bool should_flush = false;
      if (last_azimuth_hundred_ >= 0) {
        int diff = azimuth - last_azimuth_hundred_;
        if (diff < 0) diff += 36000;
        if (last_azimuth_hundred_ != azimuth && diff < kMaxAzimuthGap) {
          if ((last_azimuth_hundred_ > azimuth &&
               start_angle_hundred_ <= azimuth) ||
              (last_azimuth_hundred_ < start_angle_hundred_ &&
               start_angle_hundred_ <= azimuth)) {
            should_flush = true;
          }
        }
      }
      if (should_flush) {
        finalizeScan();
      }
      buffer_.push_back(point);
      last_azimuth_hundred_ = azimuth;
    }
  }

  void flush() { finalizeScan(); }

 private:
  static constexpr int kMaxAzimuthGap = 600;

  void finalizeScan() {
    if (buffer_.empty()) {
      return;
    }
    sensor_msgs::msg::PointCloud2 ros_msg;
    const size_t num_points = buffer_.size();
    ros_msg.fields.clear();
    ros_msg.fields.reserve(6);
    int offset = 0;
    offset =
        addPointField(ros_msg, "x", 1, sensor_msgs::msg::PointField::FLOAT32,
                      offset);
    offset =
        addPointField(ros_msg, "y", 1, sensor_msgs::msg::PointField::FLOAT32,
                      offset);
    offset =
        addPointField(ros_msg, "z", 1, sensor_msgs::msg::PointField::FLOAT32,
                      offset);
    offset = addPointField(ros_msg, "intensity", 1,
                           sensor_msgs::msg::PointField::UINT8, offset);
    offset = addPointField(ros_msg, "ring", 1,
                           sensor_msgs::msg::PointField::UINT16, offset);
    offset = addPointField(ros_msg, "timestamp", 1,
                           sensor_msgs::msg::PointField::FLOAT64, offset);
    ros_msg.point_step = offset;
    ros_msg.row_step = static_cast<uint32_t>(ros_msg.point_step * num_points);
    ros_msg.is_dense = false;
    ros_msg.height = 1;
    ros_msg.width = static_cast<uint32_t>(num_points);
    ros_msg.data.resize(num_points * ros_msg.point_step);
    ros_msg.header.frame_id = config_.frame_id;
    const LidarPointXYZIRT &first_point = buffer_.front();
    ros_msg.header.stamp.sec =
        static_cast<int32_t>(std::floor(first_point.timestamp));
    ros_msg.header.stamp.nanosec =
        static_cast<uint32_t>(std::round(
            (first_point.timestamp - std::floor(first_point.timestamp)) * 1e9));

    sensor_msgs::PointCloud2Iterator<float> iter_x(ros_msg, "x");
    sensor_msgs::PointCloud2Iterator<float> iter_y(ros_msg, "y");
    sensor_msgs::PointCloud2Iterator<float> iter_z(ros_msg, "z");
    sensor_msgs::PointCloud2Iterator<uint8_t> iter_intensity(ros_msg, "intensity");
    sensor_msgs::PointCloud2Iterator<uint16_t> iter_ring(ros_msg, "ring");
    sensor_msgs::PointCloud2Iterator<double> iter_timestamp(ros_msg,
                                                           "timestamp");

    for (const auto &point : buffer_) {
      float x = point.x;
      float y = point.y;
      if (config_.apply_axis_remap) {
        const float remapped_x = -y;
        const float remapped_y = x;
        x = remapped_x;
        y = remapped_y;
      }
      *iter_x = x;
      *iter_y = y;
      *iter_z = point.z;
      *iter_intensity = point.intensity;
      *iter_ring = point.ring;
      *iter_timestamp = point.timestamp;
      ++iter_x;
      ++iter_y;
      ++iter_z;
      ++iter_intensity;
      ++iter_ring;
      ++iter_timestamp;
    }

    rclcpp::SerializedMessage serialized_msg;
    serializer_.serialize_message(&ros_msg, &serialized_msg);
    auto message_ptr = std::make_shared<rclcpp::SerializedMessage>(serialized_msg);
    writer_->write(message_ptr, "/points_raw", "sensor_msgs/msg/PointCloud2",
                   ros_msg.header.stamp);
    buffer_.clear();
  }

  NodeConfig config_;
  rosbag2_cpp::Writer *writer_;
  rclcpp::Serialization<sensor_msgs::msg::PointCloud2> serializer_;
  std::vector<LidarPointXYZIRT> buffer_;
  int last_azimuth_hundred_ = -1;
  const int start_angle_hundred_;
};

std::unique_ptr<ScanSplitter> g_scan_splitter;

//log info, display frame message
void lidarCallback(const LidarDecodedFrame<LidarPointXYZIRT> &frame) {
  cur_frame_time = GetMicroTickCount();
  if (last_frame_time == 0) last_frame_time = GetMicroTickCount();
  if (cur_frame_time - last_frame_time > kMaxTimeInterval) {
    printf("Time between last frame and cur frame is: %u us\n",
           (cur_frame_time - last_frame_time));
  }
  last_frame_time = cur_frame_time;
  if (frame.points_num > 0) {
    double first_time = frame.points[0].timestamp;
    double last_time = frame.points[frame.points_num - 1].timestamp;
    printf("frame:%d points:%u packet:%u start time:%lf end time:%lf\n",
           frame.frame_index, frame.points_num, frame.packet_num, first_time,
           last_time);
  } else {
    printf("frame:%d no points packet:%u\n", frame.frame_index, frame.packet_num);
  }
  if (g_scan_splitter) {
    g_scan_splitter->processFrame(frame);
  }
}

void faultMessageCallback(const FaultMessageInfo& fault_message_info) {
  // Use fault message messages to make some judgments
  fault_message_info.Print();
  return;
}

// Determines whether the PCAP is finished playing
bool IsPlayEnded(HesaiLidarSdk<LidarPointXYZIRT>& sdk)
{
  return sdk.lidar_ptr_->IsPlayEnded();
}

// Signal handler
void signalHandler(int signum) {
    endflag = true;
}

int main(int argc, char *argv[])
{
  if (argc < 2) {
    std::cerr << "Usage: ros2 run PCAPtoRosbag PCAPtoBag <pcap_path> [--start_angle_deg=0.0] "
                 "[--apply_axis_remap=true] [--lidar_model=PandarXT] "
                 "[--timestamp_type=realtime] [--frame_id=hesai_lidar] "
                 "[--publish_type=points]\n";
    return 1;
  }

  auto config = ParseConfig(argc, argv);
  rclcpp::init(argc, argv);
  const std::string pcap_path_string = argv[1];
  std::cout << "[INFO] pcap_path = " << pcap_path_string << std::endl;
  stdfs::path pcap_path = stdfs::path(argv[1]);


  // 例: /path/to/20251221_152940/hesai.pcap -> /path/to/20251221_152940/outputs/lidar
  stdfs::path output_dir = pcap_path.parent_path() / "outputs" / "lidar";

  // lidar そのものは作らない！ 親だけ作る
  stdfs::create_directories(output_dir.parent_path());
  if (stdfs::exists(output_dir)) {
    stdfs::remove_all(output_dir);   // 上書き許可（危険なので注意）
  }
  rosbag2_storage::StorageOptions storage_options;
  storage_options.uri = output_dir.string();
  storage_options.storage_id = "mcap";

  rosbag2_cpp::ConverterOptions converter_options;
  writer.open(storage_options, converter_options);
  g_scan_splitter = std::make_unique<ScanSplitter>(*config, &writer);
  

  HesaiLidarSdk<LidarPointXYZIRT> sample;
  DriverParam param;
  param.input_param.source_type = DATA_FROM_PCAP;
  param.input_param.pcap_path = pcap_path; 

  // input/correction/firetime paths
  const auto share_dir = ament_index_cpp::get_package_share_directory("PCAPtoRosbag");
  stdfs::path correction_dir = stdfs::path(share_dir) / "correction";

  stdfs::path correction_csv =
    correction_dir / "angle_correction" / "XT32M2X_Angle Correction File.csv";
  stdfs::path firetime_csv =
    correction_dir / "firetime_correction" / "PandarXT-32M2X_Firetime Correction File.csv";

  // 存在チェック（無いとまた silent に空bag作るので早めに落とす）
  if (!stdfs::exists(correction_csv)) {
    std::cerr << "Correction CSV not found: " << correction_csv << std::endl;
    return 1;
  }
  if (!stdfs::exists(firetime_csv)) {
    std::cerr << "Firetime CSV not found: " << firetime_csv << std::endl;
    return 1;
  }

  param.input_param.correction_file_path = correction_csv.string();
  param.input_param.firetimes_path = firetime_csv.string();
  // other parameters
  param.decoder_param.distance_correction_flag = false;
  param.decoder_param.pcap_play_synchronization = false; // for speed up
std::cout << "[INFO] param.input_param.pcap_path = " << param.input_param.pcap_path << std::endl;
std::cout << "[INFO] correction = " << param.input_param.correction_file_path << std::endl;
std::cout << "[INFO] firetimes  = " << param.input_param.firetimes_path << std::endl;
  // init lidar with param
  sample.Init(param);

  // assign callback fuction
  sample.RegRecvCallback(lidarCallback);
  sample.RegRecvCallback(faultMessageCallback);

  // signal handler
  signal(SIGINT, signalHandler);

  sample.Start();

  // You can select the parameters in while():
  // 1.[IsPlayEnded(sample)]: adds the ability for the PCAP to automatically quit after playing the program
  // 2.[1                  ]: the application will not quit voluntarily
  while (!endflag && (!IsPlayEnded(sample) || GetMicroTickCount() - last_frame_time < 10*1e3))
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  if (g_scan_splitter) {
    g_scan_splitter->flush();
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  printf("The PCAP file has been converted and we will exit the program.\n");
  rclcpp::shutdown();
  return 0;
}
