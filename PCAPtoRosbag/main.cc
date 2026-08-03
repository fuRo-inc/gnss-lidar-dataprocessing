#include <hesai_lidar_sdk.hpp>
#include <lidar_types.h>

#include <rosbag2_cpp/writer.hpp>
#include <rosbag2_storage/storage_options.hpp>

#include <rclcpp/rclcpp.hpp>
#include <rclcpp/serialization.hpp>
#include <rclcpp/serialized_message.hpp>

#include <sensor_msgs/point_cloud2_iterator.hpp>
#include <sensor_msgs/msg/imu.hpp>
#include <std_msgs/msg/u_int8_multi_array.hpp>

#include <hesai_ros_driver/msg/udp_frame.hpp>
#include <hesai_ros_driver/msg/udp_packet.hpp>
#include <hesai_ros_driver/msg/ptp.hpp>
#include <hesai_ros_driver/msg/firetime.hpp>
#include <hesai_ros_driver/msg/loss_packet.hpp>

#include <ament_index_cpp/get_package_share_directory.hpp>

#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace stdfs = std::filesystem;

// ====== Settings ======
static constexpr int64_t kCutoffEpochSec = 1766847600;  // 2025-12-28 00:00:00 JST

// キューが無限に膨らむとメモリが死ぬので上限（環境に合わせて調整）
static constexpr size_t kQueueMax = 256;

// バックプレッシャ（完全性優先）：これ以上溜まったら callback 側で待つ
// ※ kQueueMax より小さくする（満杯寸前で止める）
static constexpr size_t kBackpressureHighWater = 200;
static constexpr int kBackpressureSleepMs = 1;

// PERFログをうるさくしたくない場合は閾値を上げる
static constexpr int64_t kPerfWarnFillUs = 5000;
static constexpr int64_t kPerfWarnSerUs  = 5000;

// ====== Globals ======
uint32_t last_frame_time = 0;
uint32_t cur_frame_time = 0;

std::string frame_id = "hesai_lidar";
rosbag2_cpp::Writer writer;

std::atomic<bool> endflag{false};

// ====== Writer thread queue ======
struct BagItem {
  rclcpp::SerializedMessage serialized;
  rclcpp::Time stamp;
  std::string topic;
  std::string type;
};

std::mutex g_q_mtx;
std::condition_variable g_q_cv;
std::deque<BagItem> g_q;
std::atomic<bool> g_writer_stop{false};

// スタンプ単調性チェック（コールバック側でチェックしてログ）
std::mutex g_stamp_mtx;
rclcpp::Time g_prev_stamp(0, 0, RCL_ROS_TIME);

// 監視用カウンタ
std::atomic<uint64_t> g_drop_queue_full{0};   // 原則ゼロになるはず（完全性優先）
std::atomic<uint64_t> g_drop_cutoff{0};
std::atomic<uint64_t> g_drop_time_reverse{0};
std::atomic<uint64_t> g_warn_stamp_reverse{0};

void writerThreadFunc() {
  while (!g_writer_stop.load(std::memory_order_relaxed)) {
    BagItem item;
    {
      std::unique_lock<std::mutex> lk(g_q_mtx);
      g_q_cv.wait(lk, [] {
        return g_writer_stop.load(std::memory_order_relaxed) || !g_q.empty();
      });

      if (g_writer_stop.load(std::memory_order_relaxed) && g_q.empty()) break;

      item = std::move(g_q.front());
      g_q.pop_front();
    }

    // writer.write はこのスレッドだけが呼ぶ（スレッドセーフ性の不確実性を回避）
    auto msg_ptr = std::make_shared<rclcpp::SerializedMessage>(std::move(item.serialized));
    writer.write(msg_ptr, item.topic, item.type, item.stamp);
  }
}

// log info, display frame message
void lidarCallback(const LidarDecodedFrame<LidarPointXYZIRT> &frame) {
  // ---- callback間隔の監視（既存） ----
  cur_frame_time = GetMicroTickCount();
  if (last_frame_time == 0) last_frame_time = GetMicroTickCount();
  if (cur_frame_time - last_frame_time > kMaxTimeInterval) {
    printf("Time between last frame and cur frame is: %u us\n",
           (cur_frame_time - last_frame_time));
  }
  last_frame_time = cur_frame_time;

  if (frame.points_num == 0) {
    printf("frame:%d no points packet:%u\n", frame.frame_index, frame.packet_num);
    return;
  }

  const double t_first = frame.points[0].timestamp;
  const double t_last  = frame.points[frame.points_num - 1].timestamp;

  // どっちかが古いなら捨てる（境目で混ざるフレームも落とす）
  if (t_first < static_cast<double>(kCutoffEpochSec) ||
      t_last  < static_cast<double>(kCutoffEpochSec)) {
    g_drop_cutoff.fetch_add(1, std::memory_order_relaxed);
    fprintf(stderr,
            "[DROP<2025-12-28] frame:%d points:%u packet:%u first:%lf last:%lf\n",
            frame.frame_index, frame.points_num, frame.packet_num, t_first, t_last);
    return;
  }

  // フレーム内で時刻が逆行してたら捨てる（変な結合の検出）
  if (t_last < t_first) {
    g_drop_time_reverse.fetch_add(1, std::memory_order_relaxed);
    fprintf(stderr,
            "[DROP time_reverse] frame:%d points:%u packet:%u first:%lf last:%lf\n",
            frame.frame_index, frame.points_num, frame.packet_num, t_first, t_last);
    return;
  }

  // （任意）フレーム情報表示（うるさければ消してOK）
  printf("frame:%d points:%u packet:%u start time:%lf end time:%lf\n",
         frame.frame_index, frame.points_num, frame.packet_num, t_first, t_last);

  const auto t0 = std::chrono::steady_clock::now();

  // conversion here
  sensor_msgs::msg::PointCloud2 ros_msg;

  int fields = 6;
  ros_msg.fields.clear();
  ros_msg.fields.reserve(fields);
  ros_msg.width  = frame.points_num;
  ros_msg.height = 1;

  int offset = 0;
  offset = addPointField(ros_msg, "x", 1, sensor_msgs::msg::PointField::FLOAT32, offset);
  offset = addPointField(ros_msg, "y", 1, sensor_msgs::msg::PointField::FLOAT32, offset);
  offset = addPointField(ros_msg, "z", 1, sensor_msgs::msg::PointField::FLOAT32, offset);
  offset = addPointField(ros_msg, "intensity", 1, sensor_msgs::msg::PointField::UINT8, offset);
  offset = addPointField(ros_msg, "ring", 1, sensor_msgs::msg::PointField::UINT16, offset);
  offset = addPointField(ros_msg, "timestamp", 1, sensor_msgs::msg::PointField::FLOAT64, offset);

  ros_msg.point_step = offset;
  ros_msg.row_step   = ros_msg.width * ros_msg.point_step;
  ros_msg.is_dense   = false;
  ros_msg.data.resize(static_cast<size_t>(frame.points_num) * ros_msg.point_step);

  // header
  ros_msg.header.frame_id = frame_id;
  ros_msg.header.stamp.sec =
      (int32_t)std::floor(frame.points[0].timestamp);
  ros_msg.header.stamp.nanosec =
      (uint32_t)std::llround(
          (frame.points[0].timestamp - std::floor(frame.points[0].timestamp)) * 1e9);

  // スタンプ単調性ログ（再生/後段がstamp基準で壊れるのを検出）
  {
    std::lock_guard<std::mutex> lk(g_stamp_mtx);
    rclcpp::Time cur(ros_msg.header.stamp.sec, ros_msg.header.stamp.nanosec, RCL_ROS_TIME);
    if (g_prev_stamp.nanoseconds() != 0 && cur < g_prev_stamp) {
      g_warn_stamp_reverse.fetch_add(1, std::memory_order_relaxed);
      fprintf(stderr,
              "[WARN stamp_reverse] frame:%d prev=%ld cur=%ld (ns)\n",
              frame.frame_index,
              (long)g_prev_stamp.nanoseconds(),
              (long)cur.nanoseconds());
    }
    g_prev_stamp = cur;
  }

  sensor_msgs::PointCloud2Iterator<float>    iter_x_(ros_msg, "x");
  sensor_msgs::PointCloud2Iterator<float>    iter_y_(ros_msg, "y");
  sensor_msgs::PointCloud2Iterator<float>    iter_z_(ros_msg, "z");
  sensor_msgs::PointCloud2Iterator<uint8_t>  iter_intensity_(ros_msg, "intensity");
  sensor_msgs::PointCloud2Iterator<uint16_t> iter_ring_(ros_msg, "ring");
  sensor_msgs::PointCloud2Iterator<double>   iter_timestamp_(ros_msg, "timestamp");

  for (size_t i = 0; i < frame.points_num; i++) {
    const LidarPointXYZIRT &point = frame.points[i];
    *iter_x_         = point.x;
    *iter_y_         = point.y;
    *iter_z_         = point.z;
    *iter_intensity_ = point.intensity;
    *iter_ring_      = point.ring;
    *iter_timestamp_ = point.timestamp;

    ++iter_x_;
    ++iter_y_;
    ++iter_z_;
    ++iter_intensity_;
    ++iter_ring_;
    ++iter_timestamp_;
  }

  const auto t1 = std::chrono::steady_clock::now();

  // Serialize（writeは別スレッド）
  rclcpp::Serialization<sensor_msgs::msg::PointCloud2> serializer;
  rclcpp::SerializedMessage serialized_msg;
  serializer.serialize_message(&ros_msg, &serialized_msg);

  const auto t2 = std::chrono::steady_clock::now();

  // ====== 完全性優先：バックプレッシャ ======
  // キューが溜まりすぎたら、writer が追いつくまで待つ（dropしない）
  while (true) {
    if (g_writer_stop.load(std::memory_order_relaxed)) {
      // 終了処理中はこれ以上ためない
      return;
    }
    size_t qsz;
    {
      std::lock_guard<std::mutex> lk(g_q_mtx);
      qsz = g_q.size();
    }
    if (qsz < kBackpressureHighWater) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(kBackpressureSleepMs));
  }

  // キューへ push（原則dropしないが、念のため満杯ガードは残す）
  {
    std::lock_guard<std::mutex> lk(g_q_mtx);

    if (g_q.size() >= kQueueMax) {
      // ここに来るのは「writerが完全に停止/詰まり続ける」などの異常系が多い
      g_drop_queue_full.fetch_add(1, std::memory_order_relaxed);
      fprintf(stderr, "[DROP queue_full] size=%zu frame=%d\n", g_q.size(), frame.frame_index);
      return;
    }

    BagItem item;
    item.serialized = std::move(serialized_msg);
    item.stamp = rclcpp::Time(ros_msg.header.stamp.sec, ros_msg.header.stamp.nanosec, RCL_ROS_TIME);
    item.topic = "/points_raw";
    item.type  = "sensor_msgs/msg/PointCloud2";
    g_q.push_back(std::move(item));
  }
  g_q_cv.notify_one();

  // PERFログ（重い箇所がどこか可視化）
  const auto fill_us =
      std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
  const auto ser_us =
      std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();

  if (fill_us > kPerfWarnFillUs || ser_us > kPerfWarnSerUs) {
    // キュー長も出すと「writeが詰まってキューが増えてる」が見える
    size_t qsz = 0;
    {
      std::lock_guard<std::mutex> lk(g_q_mtx);
      qsz = g_q.size();
    }
    fprintf(stderr, "[PERF] frame=%d fill=%ldus serialize=%ldus q=%zu\n",
            frame.frame_index, (long)fill_us, (long)ser_us, qsz);
  }
}

void faultMessageCallback(const FaultMessageInfo &fault_message_info) {
  fault_message_info.Print();
}

// Determines whether the PCAP is finished playing
bool IsPlayEnded(HesaiLidarSdk<LidarPointXYZIRT> &sdk) {
  return sdk.lidar_ptr_->IsPlayEnded();
}

// Signal handler
void signalHandler(int /*signum*/) { endflag.store(true); }

int main(int argc, char *argv[]) {
  if (argc < 2) {
    std::cout << "Usage: ros2 run PCAPtoRosbag PCAPtoBag <pcap_path>\n";
    return 1;
  }

  rclcpp::init(argc, argv);

  // PCAP path
  const std::string pcap_path_string = argv[1];
  stdfs::path pcap_path = stdfs::path(argv[1]);
  std::cout << "[INFO] pcap_path = " << pcap_path << std::endl;

  // Output dir (new style): <pcap_dir>/outputs/lidar
  stdfs::path output_dir = pcap_path.parent_path() / "outputs" / "lidar";

  // lidar そのものは作らない！ 親だけ作る
  stdfs::create_directories(output_dir.parent_path());
  if (stdfs::exists(output_dir)) {
    stdfs::remove_all(output_dir);  // 上書き（必要なら外してね）
  }

  rosbag2_storage::StorageOptions storage_options;
  storage_options.uri        = output_dir.string();
  storage_options.storage_id = "mcap";

  rosbag2_cpp::ConverterOptions converter_options;
  writer.open(storage_options, converter_options);

  // writer thread start（writer.write を別スレッドに逃がす）
  std::thread writer_thread(writerThreadFunc);

  // correction/firetime paths (new style): from package share directory
  const auto share_dir = "/home/wataru-furo/sruppto_ws/src/gnss-lidar-dataprocessing/PCAPtoRosbag";
  stdfs::path correction_dir = stdfs::path(share_dir) / "correction";

  stdfs::path correction_csv =
      correction_dir / "angle_correction" / "XT32M2X_Angle_Correction_File.csv";
  stdfs::path firetime_csv =
      correction_dir / "firetime_correction" / "PandarXT-32M2X_Firetime Correction File.csv";

  if (!stdfs::exists(correction_csv)) {
    std::cerr << "Correction CSV not found: " << correction_csv << std::endl;
    g_writer_stop.store(true);
    g_q_cv.notify_all();
    if (writer_thread.joinable()) writer_thread.join();
    return 1;
  }
  if (!stdfs::exists(firetime_csv)) {
    std::cerr << "Firetime CSV not found: " << firetime_csv << std::endl;
    g_writer_stop.store(true);
    g_q_cv.notify_all();
    if (writer_thread.joinable()) writer_thread.join();
    return 1;
  }

  // Hesai SDK init (original behavior)
  HesaiLidarSdk<LidarPointXYZIRT> sample;
  DriverParam param;
  param.input_param.source_type = DATA_FROM_PCAP;
  param.input_param.pcap_path = pcap_path;
  param.input_param.correction_file_path = correction_csv.string();
  param.input_param.firetimes_path = firetime_csv.string();

  param.decoder_param.distance_correction_flag = false;

  // ★完全性優先なら、PCAPの再生同期をtrueにするのが最も自然（必要なら戻せる）
  //   これにより「PCAPを爆速で流して書き込みが詰まる」状況が起きにくくなる
  param.decoder_param.pcap_play_synchronization = false;

  std::cout << "[INFO] output_dir = " << output_dir << std::endl;
  std::cout << "[INFO] correction = " << param.input_param.correction_file_path << std::endl;
  std::cout << "[INFO] firetimes  = " << param.input_param.firetimes_path << std::endl;

  sample.Init(param);

  sample.RegRecvCallback(lidarCallback);
  sample.RegRecvCallback(faultMessageCallback);

  signal(SIGINT, signalHandler);

  sample.Start();

  while (!endflag.load() &&
         (!IsPlayEnded(sample) || GetMicroTickCount() - last_frame_time < 10 * 1e3)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // ここで少し待って、キューを吐き切る猶予を与える（必要なら調整）
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // writer thread stop
  g_writer_stop.store(true);
  g_q_cv.notify_all();
  if (writer_thread.joinable()) writer_thread.join();

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  printf("The PCAP file has been converted and we will exit the program.\n");

  // 統計表示（原因切り分け用）
  fprintf(stderr,
          "[STATS] drop_cutoff=%lu drop_time_reverse=%lu drop_queue_full=%lu warn_stamp_reverse=%lu\n",
          (unsigned long)g_drop_cutoff.load(),
          (unsigned long)g_drop_time_reverse.load(),
          (unsigned long)g_drop_queue_full.load(),
          (unsigned long)g_warn_stamp_reverse.load());

  rclcpp::shutdown();
  return 0;
}
