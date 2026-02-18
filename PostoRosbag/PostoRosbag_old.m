clear; close all; clc;

%% -------- User settings --------
dirname  = "20251231_163720";
posFile  = "../lidar_gnss_log/"+dirname+"/astrx.pos";   % input .pos

outdir    = "../lidar_gnss_log/"+dirname+"/outputs/gnss"; % output bag folder (IMUと同階層)
topicName = "/gnss/fix";                                  % output topic
frameId   = "gps";                                       % header.frame_id
storage   = "mcap";                                      % "mcap" or "sqlite3"
minQ      = 1;                                           % keep rows with Q >= minQ (0=keep all)
toUTCT    = 18;                                          % GPST->UTC (2025は通常18秒)

%% -------- Prepare output folder (must be empty) --------
if isfolder(outdir)
    rmdir(outdir, "s");
end

%% -------- Read .pos (skip comment lines starting with %) --------
fid = fopen(posFile, "r");
assert(fid > 0, "Failed to open: %s", posFile);

fmt = "%s %s %f %f %f %d %d %f %f %f %f %f %f %f %f";
C = textscan(fid, fmt, ...
    "CommentStyle", "%", ...
    "MultipleDelimsAsOne", true);

fclose(fid);

dateStr = string(C{1});
timeStr = string(C{2});
lat     = C{3};
lon     = C{4};
hgt     = C{5};
Q       = C{6};
sdn     = C{8};
sde     = C{9};
sdu     = C{10};
sdne    = C{11};
sdeu    = C{12};
sdun    = C{13};

assert(~isempty(lat), "No data rows parsed. Check .pos format/delimiters.");

%% -------- Filter by RTKLIB quality flag Q --------
keep = (Q >= minQ);
dateStr = dateStr(keep); timeStr = timeStr(keep);
lat = lat(keep); lon = lon(keep); hgt = hgt(keep);
Q   = Q(keep);
sdn = sdn(keep); sde = sde(keep); sdu = sdu(keep);
sdne= sdne(keep); sdeu = sdeu(keep); sdun = sdun(keep);

N = numel(lat);
fprintf("Loaded rows=%d (after Q filter)\n", N);

%% -------- Parse timestamp -> UNIX time (UTC) --------
dt_gpst = datetime(dateStr + " " + timeStr, ...
    "InputFormat","yyyy/MM/dd HH:mm:ss.SSS", ...
    "TimeZone","UTC");
unix_time = posixtime(dt_gpst - seconds(toUTCT));

fprintf("Time range: %.3f .. %.3f (unix)\n", unix_time(1), unix_time(end));
if N > 1
    fprintf("dt_median=%.6f s\n", median(diff(unix_time)));
end

%% -------- Write ROS 2 bag (MCAP) --------
bagWriter = ros2bagwriter(outdir, "StorageFormat", storage);

for i = 1:N
    msg = ros2message("sensor_msgs/NavSatFix");
    msg.header.frame_id = char(frameId);

    sec  = floor(unix_time(i));
    nsec = uint32(round((unix_time(i) - sec) * 1e9));
    if nsec >= 1e9
        sec = sec + 1;
        nsec = uint32(nsec - 1e9);
    end
    msg.header.stamp.sec     = int32(sec);
    msg.header.stamp.nanosec = nsec;

    % Status (simple)
    if any(Q(i) == [1 2 4 5])
        msg.status.status = int8(0);   % STATUS_FIX
    else
        msg.status.status = int8(-1);  % STATUS_NO_FIX
    end
    msg.status.service = uint16(1);    % SERVICE_GPS

    % LLA
    msg.latitude  = double(lat(i));
    msg.longitude = double(lon(i));
    msg.altitude  = double(hgt(i));

    % Covariance (ENU): x=E, y=N, z=U
    Cxx = (double(sde(i))^2);
    Cyy = (double(sdn(i))^2);
    Czz = (double(sdu(i))^2);

    % Off-diagonals: もし怪しければ 0 にしてもOK
    Cxy = double(sdne(i));
    Cxz = double(sdeu(i));
    Cyz = double(sdun(i));

    covENU = [ Cxx, Cxy, Cxz; ...
               Cxy, Cyy, Cyz; ...
               Cxz, Cyz, Czz ];

    msg.position_covariance = reshape(covENU.', 1, 9);
    msg.position_covariance_type = uint8(3);

    write(bagWriter, char(topicName), ros2time(unix_time(i)), msg);
end

delete(bagWriter);
clear bagWriter;

fprintf("Done.\n  input : %s\n  output: %s\n  topic : %s\n  msgs  : %d\n", ...
    posFile, outdir, topicName, N);

disp("Check: ros2 bag info <outdir>  and  ros2 bag play <outdir>");
