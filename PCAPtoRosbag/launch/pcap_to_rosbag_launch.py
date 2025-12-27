from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument, OpaqueFunction
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    pcap_path = LaunchConfiguration("pcap_path", default="")
    start_angle = LaunchConfiguration("start_angle_deg", default="0.0")
    apply_axis_remap = LaunchConfiguration("apply_axis_remap", default="true")
    lidar_model = LaunchConfiguration("lidar_model", default="PandarXT")
    timestamp_type = LaunchConfiguration("timestamp_type", default="realtime")
    frame_id = LaunchConfiguration("frame_id", default="hesai_lidar")
    publish_type = LaunchConfiguration("publish_type", default="points")

    def _create_node(context, *_, **__):
        resolved_args = [
            context.perform_substitution(pcap_path),
            f"--start_angle_deg={context.perform_substitution(start_angle)}",
            f"--apply_axis_remap={context.perform_substitution(apply_axis_remap)}",
            f"--lidar_model={context.perform_substitution(lidar_model)}",
            f"--timestamp_type={context.perform_substitution(timestamp_type)}",
            f"--frame_id={context.perform_substitution(frame_id)}",
            f"--publish_type={context.perform_substitution(publish_type)}",
        ]
        node = Node(
            package="PCAPtoRosbag",
            executable="PCAPtoBag",
            name="pcap_to_rosbag",
            output="screen",
            arguments=resolved_args,
        )
        return [node]

    return LaunchDescription(
        [
            DeclareLaunchArgument(
                "pcap_path",
                description="Path to the Hesai PCAP file",
            ),
            DeclareLaunchArgument(
                "start_angle_deg",
                default_value="0.0",
                description="Start angle in degrees for scan splitting",
            ),
            DeclareLaunchArgument(
                "apply_axis_remap",
                default_value="true",
                description="Enable PandarXT-style axis remapping",
            ),
            DeclareLaunchArgument(
                "lidar_model",
                default_value="PandarXT",
                description="Lidar model used for parameterizing decoding",
            ),
            DeclareLaunchArgument(
                "timestamp_type",
                default_value="realtime",
                description="Timestamp type delegated to the bag file header",
            ),
            DeclareLaunchArgument(
                "frame_id",
                default_value="hesai_lidar",
                description="Frame ID assigned to the generated PointCloud2",
            ),
            DeclareLaunchArgument(
                "publish_type",
                default_value="points",
                description="Which topics to store (currently points only)",
            ),
            OpaqueFunction(function=_create_node),
        ]
    )
