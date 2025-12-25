clear; close all; clc;

dirname  = "20251221_152940";
filename = "../lidar_gnss_log/"+dirname+"/astrx.sbf_SBF_ExtSensorMeas1.txt";

%% -------- Read CSV robustly (27 columns, header + dashed line) --------
names = [ ...
"TOW [s]","WNc [w]","N","Source","SensorModel","Type","ObsInfo", ...
"Temperature compensated","Sensor Saturated","Encrypted","Driving Direction","Sanity Check", ...
"AccelerationX [m/s²]","AccelerationY [m/s²]","AccelerationZ [m/s²]", ...
"AngularRateX [°/s]","AngularRateY [°/s]","AngularRateZ [°/s]", ...
"SensorTemperature [°C]","SequenceNumber","VelocityX [m/s]","VelocityY [m/s]","VelocityZ [m/s]", ...
"StdDevX [m/s]","StdDevY [m/s]","StdDevZ [m/s]","ZeroVelocityFlag" ];

opts = delimitedTextImportOptions("Delimiter", ",");
opts.VariableNames = names;
opts.VariableNamesLine = 0;      % use the names defined above (do not read header as names)
opts.DataLines = [3 Inf];        % line1=header, line2=dashes, line3..=data
opts.ExtraColumnsRule = "ignore";
opts.EmptyLineRule = "read";

% Types (27 columns)
opts.VariableTypes = [ ...
    repmat({'double'},1,3), ...   % TOW, WNc, N
    repmat({'string'},1,3), ...   % Source, SensorModel, Type
    repmat({'string'},1,5), ...   % ObsInfo, Temp compensated, Sensor Saturated, Encrypted, Driving Direction
    repmat({'string'},1,1), ...   % Sanity Check
    repmat({'double'},1,3), ...   % Acc X,Y,Z
    repmat({'double'},1,3), ...   % Gyro X,Y,Z
    repmat({'double'},1,1), ...   % SensorTemperature
    repmat({'double'},1,1), ...   % SequenceNumber
    repmat({'double'},1,3), ...   % Velocity X,Y,Z
    repmat({'double'},1,3), ...   % StdDev X,Y,Z
    repmat({'double'},1,1)  ...   % ZeroVelocityFlag
];

T = readtable(filename, opts);

% Quick sanity
fprintf("Loaded: rows=%d cols=%d\n", height(T), width(T));
disp(T(1:3, 1:6));

%% -------- Find actual MATLAB variable names (they may be sanitized) --------
vn = string(T.Properties.VariableNames);
pick = @(prefix) vn(find(startsWith(vn, prefix), 1, "first"));

colTOW = pick("TOW");
colWNc = pick("WNc");
colAX  = pick("AccelerationX");
colAY  = pick("AccelerationY");
colAZ  = pick("AccelerationZ");
colGX  = pick("AngularRateX");
colGY  = pick("AngularRateY");
colGZ  = pick("AngularRateZ");
colType = pick("Type");

assert(~isempty(colTOW) && ~isempty(colWNc), "Could not find TOW/WNc columns.");
assert(~isempty(colAX) && ~isempty(colAY) && ~isempty(colAZ), "Could not find Acc columns.");
assert(~isempty(colGX) && ~isempty(colGY) && ~isempty(colGZ), "Could not find Gyro columns.");
assert(~isempty(colType), "Could not find Type column.");

%% -------- Split ACC / RATE and align by (WNc,TOW) --------
typeStr = string(T.(colType));
idxacc  = (typeStr == "ACC");
idxrate = (typeStr == "RATE");

assert(any(idxacc),  "No ACC rows found.");
assert(any(idxrate), "No RATE rows found.");

tow_acc  = T.(colTOW)(idxacc);
wnc_acc  = T.(colWNc)(idxacc);
tow_rate = T.(colTOW)(idxrate);
wnc_rate = T.(colWNc)(idxrate);

ax = T.(colAX)(idxacc);
ay = T.(colAY)(idxacc);
az = T.(colAZ)(idxacc);

gx_deg = T.(colGX)(idxrate);
gy_deg = T.(colGY)(idxrate);
gz_deg = T.(colGZ)(idxrate);

A = table(wnc_acc, tow_acc, ax, ay, az, 'VariableNames', ["WNc","TOW","ax","ay","az"]);
R = table(wnc_rate, tow_rate, gx_deg, gy_deg, gz_deg, 'VariableNames', ["WNc","TOW","gx_deg","gy_deg","gz_deg"]);

M = innerjoin(A, R, "Keys", ["WNc","TOW"]);
assert(height(M) > 0, "No matched timestamps between ACC and RATE.");

%% -------- Time: GPS week + TOW[s] -> Unix time (UTC) --------
gps0  = datetime(1980,1,6,0,0,0,"TimeZone","UTC"); % GPS epoch
t_gps = gps0 + days(7*M.WNc) + seconds(M.TOW);

toUTCT = 18;  % GPS-UTC (seconds)
unix_time = posixtime(t_gps - seconds(toUTCT));

fprintf("Merged messages=%d, dt_median=%.6f s\n", numel(unix_time), median(diff(unix_time)));

%% -------- Optional plots --------
tiledlayout(2,1,"TileSpacing","compact");
nexttile; plot([M.ax M.ay M.az]); grid on; title("Acceleration"); ylabel("m/s^2"); legend("x","y","z");
nexttile; plot([M.gx_deg M.gy_deg M.gz_deg]); grid on; title("Angular rate"); ylabel("deg/s"); legend("x","y","z");

%% -------- Write ROS 2 bag (MCAP) --------
outdir = "../lidar_gnss_log/"+dirname+"/outputs/imu";

% ros2bagwriter requires empty output directory
if isfolder(outdir)
    rmdir(outdir, "s");
end

bagWriter = ros2bagwriter(outdir, "StorageFormat", "mcap");

for i = 1:numel(unix_time)
    msg = ros2message("sensor_msgs/Imu");
    msg.header.frame_id = 'imu';

    sec  = floor(unix_time(i));
    nsec = uint32(round((unix_time(i) - sec)*1e9));
    msg.header.stamp.sec = int32(sec);
    msg.header.stamp.nanosec = nsec;

    % Gyro: deg/s -> rad/s
    msg.angular_velocity.x = deg2rad(M.gx_deg(i));
    msg.angular_velocity.y = deg2rad(M.gy_deg(i));
    msg.angular_velocity.z = deg2rad(M.gz_deg(i));

    % Acc: m/s^2
    msg.linear_acceleration.x = M.ax(i);
    msg.linear_acceleration.y = M.ay(i);
    msg.linear_acceleration.z = M.az(i);

    % Use ros2time for bag timestamp
    write(bagWriter, "/imu/data", ros2time(unix_time(i)), msg);
end

delete(bagWriter);
clear bagWriter;

disp("Done. Check with: ros2 bag info imu");
