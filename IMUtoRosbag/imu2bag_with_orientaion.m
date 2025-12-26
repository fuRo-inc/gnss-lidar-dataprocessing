clear; close all; clc;

dirname  = "20251221_152940";
filename = "../lidar_gnss_log/"+dirname+"/astrx.sbf_SBF_ExtSensorMeas1.txt";

%% -------- Read CSV (27 columns) --------
names = [ ...
"TOW [s]","WNc [w]","N","Source","SensorModel","Type","ObsInfo", ...
"Temperature compensated","Sensor Saturated","Encrypted","Driving Direction","Sanity Check", ...
"AccelerationX [m/s²]","AccelerationY [m/s²]","AccelerationZ [m/s²]", ...
"AngularRateX [°/s]","AngularRateY [°/s]","AngularRateZ [°/s]", ...
"SensorTemperature [°C]","SequenceNumber","VelocityX [m/s]","VelocityY [m/s]","VelocityZ [m/s]", ...
"StdDevX [m/s]","StdDevY [m/s]","StdDevZ [m/s]","ZeroVelocityFlag" ];

opts = delimitedTextImportOptions("Delimiter", ",");
opts.VariableNames = names;
opts.VariableNamesLine = 0;
opts.DataLines = [3 Inf];
opts.ExtraColumnsRule = "ignore";
opts.EmptyLineRule = "read";

opts.VariableTypes = [ ...
    repmat({'double'},1,3), ...
    repmat({'string'},1,3), ...
    repmat({'string'},1,5), ...
    repmat({'string'},1,1), ...
    repmat({'double'},1,3), ...
    repmat({'double'},1,3), ...
    repmat({'double'},1,1), ...
    repmat({'double'},1,1), ...
    repmat({'double'},1,3), ...
    repmat({'double'},1,3), ...
    repmat({'double'},1,1) ];

T = readtable(filename, opts);
fprintf("Loaded: rows=%d cols=%d\n", height(T), width(T));

%% -------- Column pick --------
vn = string(T.Properties.VariableNames);
pick = @(p) vn(find(startsWith(vn,p),1));

colTOW  = pick("TOW");
colWNc  = pick("WNc");
colAX   = pick("AccelerationX");
colAY   = pick("AccelerationY");
colAZ   = pick("AccelerationZ");
colGX   = pick("AngularRateX");
colGY   = pick("AngularRateY");
colGZ   = pick("AngularRateZ");
colType = pick("Type");

%% -------- Split ACC / RATE --------
typeStr = string(T.(colType));
idxacc  = typeStr=="ACC";
idxrate = typeStr=="RATE";

A = table(T.(colWNc)(idxacc), T.(colTOW)(idxacc), ...
          T.(colAX)(idxacc), T.(colAY)(idxacc), T.(colAZ)(idxacc), ...
          'VariableNames',["WNc","TOW","ax","ay","az"]);

R = table(T.(colWNc)(idxrate), T.(colTOW)(idxrate), ...
          T.(colGX)(idxrate), T.(colGY)(idxrate), T.(colGZ)(idxrate), ...
          'VariableNames',["WNc","TOW","gx_deg","gy_deg","gz_deg"]);

M = innerjoin(A,R,"Keys",["WNc","TOW"]);
N = height(M);

%% -------- Time (GPS -> Unix UTC) --------
gps0 = datetime(1980,1,6,0,0,0,"TimeZone","UTC");
t_gps = gps0 + days(7*M.WNc) + seconds(M.TOW);
unix_time = posixtime(t_gps - seconds(18));

dt = median(diff(unix_time));
fs = 1/dt;

fprintf("Merged=%d samples, fs=%.2f Hz\n", N, fs);

%% =========================================================
%            Orientation estimation (Madgwick-like)
% =========================================================
beta = 0.05;     % gain (tune if needed)

% quaternion [w x y z]
q = [1 0 0 0];

qmul = @(a,b)[ ...
 a(1)*b(1)-a(2)*b(2)-a(3)*b(3)-a(4)*b(4), ...
 a(1)*b(2)+a(2)*b(1)+a(3)*b(4)-a(4)*b(3), ...
 a(1)*b(3)-a(2)*b(4)+a(3)*b(1)+a(4)*b(2), ...
 a(1)*b(4)+a(2)*b(3)-a(3)*b(2)+a(4)*b(1)];
qnorm = @(q) q./norm(q);
qrot  = @(q,v) qmul(qmul(q,[0 v]),[q(1) -q(2) -q(3) -q(4)]);

Q = zeros(N,4);   % store quaternion history

for i=1:N
    w = deg2rad([M.gx_deg(i) M.gy_deg(i) M.gz_deg(i)]);
    a = [M.ax(i) M.ay(i) M.az(i)];
    if norm(a)>0, a=a/norm(a); end

    % gyro integration
    qdot = 0.5*qmul(q,[0 w]);
    qg = qnorm(q + qdot*dt);

    % gravity correction
    g_pred = qrot(qg,[0 0 -1]); g_pred = g_pred(2:4);
    if norm(a)>0
        e = cross(g_pred,-a);
        dq = [1 0.5*beta*e];
        q = qnorm(qmul(qg,dq));
    else
        q = qg;
    end
    Q(i,:) = q;
end

%% -------- Covariance (datasheet based) --------
nd_g = deg2rad(0.15/60);      % rad/s/sqrt(Hz)
sigma_g = nd_g*sqrt(fs/2);
gyro_cov = diag([sigma_g^2 sigma_g^2 sigma_g^2]);

nd_a = 57e-6*9.80665;
sigma_a = nd_a*sqrt(fs/2);
acc_cov = diag([sigma_a^2 sigma_a^2 sigma_a^2]);

ori_cov = diag([1e-4 1e-4 1e-2]);   % yaw looser

%% -------- Write ROS2 bag --------
outdir = "../lidar_gnss_log/"+dirname+"/outputs/imu";
if isfolder(outdir), rmdir(outdir,"s"); end
bagWriter = ros2bagwriter(outdir,"StorageFormat","mcap");

for i=1:N
    msg = ros2message("sensor_msgs/Imu");
    msg.header.frame_id = 'imu_link';

    sec = floor(unix_time(i));
    msg.header.stamp.sec = int32(sec);
    msg.header.stamp.nanosec = uint32((unix_time(i)-sec)*1e9);

    % orientation (ROS: x,y,z,w)
    msg.orientation.w = Q(i,1);
    msg.orientation.x = Q(i,2);
    msg.orientation.y = Q(i,3);
    msg.orientation.z = Q(i,4);
    msg.orientation_covariance = ori_cov(:)';

    % angular velocity
    msg.angular_velocity.x = deg2rad(M.gx_deg(i));
    msg.angular_velocity.y = deg2rad(M.gy_deg(i));
    msg.angular_velocity.z = deg2rad(M.gz_deg(i));
    msg.angular_velocity_covariance = gyro_cov(:)';

    % linear acceleration
    msg.linear_acceleration.x = M.ax(i);
    msg.linear_acceleration.y = M.ay(i);
    msg.linear_acceleration.z = M.az(i);
    msg.linear_acceleration_covariance = acc_cov(:)';

    write(bagWriter,"/imu/data",ros2time(unix_time(i)),msg);
end

delete(bagWriter);
disp("Done: IMU bag with orientation + covariance");
