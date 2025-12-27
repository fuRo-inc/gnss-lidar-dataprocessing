%% pos_to_ros2bag.m
% RTKLIB .pos -> ROS2 bag (rosbag2) with sensor_msgs/NavSatFix
% Requires: ROS Toolbox (ros2bagwriter)
%
% ---- User settings ----
dirname  = "20251221_152940";
posFile = "../lidar_gnss_log/"+dirname+"/astrx.pos";
bagFolder = "ppk_bag_mcap";     % output folder (will be created/overwritten)
topicName = "/gnss/fix";         % output topic
frameId   = "gnss";              % header.frame_id
storage   = "mcap";             % "mcap" or "sqlite3"
minQ      = 1;                  % keep rows with Q >= minQ (0=keep all)
leapSec   = 0;                  % GPST->UTC correction if needed (try 18 if time misaligned)

% overwrite existing bag folder (careful)
if isfolder(bagFolder)
    rmdir(bagFolder, "s");
end

%% Read .pos (skip lines starting with %)
fid = fopen(posFile, "r");
assert(fid > 0, "Failed to open: %s", posFile);

cleanupObj = onCleanup(@() fclose(fid));

% RTKLIB .pos typical columns:
% date time lat lon h Q ns sdn sde sdu sdne sdeu sdun age ratio
fmt = "%s %s %f %f %f %d %d %f %f %f %f %f %f %f %f";
C = textscan(fid, fmt, ...
    "CommentStyle", "%", ...
    "MultipleDelimsAsOne", true);

dateStr = string(C{1});
timeStr = string(C{2});
lat     = C{3};
lon     = C{4};
hgt     = C{5};
Q       = C{6};
ns      = C{7}; %#ok<NASGU>
sdn     = C{8};
sde     = C{9};
sdu     = C{10};
sdne    = C{11};
sdeu    = C{12};
sdun    = C{13};
age     = C{14}; %#ok<NASGU>
ratio   = C{15}; %#ok<NASGU>

assert(~isempty(lat), "No data rows parsed. Check .pos format / delimiters.");

%% Filter by Q if requested
keep = (Q >= minQ);
dateStr = dateStr(keep); timeStr = timeStr(keep);
lat = lat(keep); lon = lon(keep); hgt = hgt(keep);
Q   = Q(keep);
sdn = sdn(keep); sde = sde(keep); sdu = sdu(keep);
sdne= sdne(keep); sdeu = sdeu(keep); sdun = sdun(keep);

%% Parse time -> UNIX seconds
% RTKLIB header says GPST; your other sensors might be UTC. If misaligned, set leapSec=18.
dt = datetime(dateStr + " " + timeStr, ...
    "InputFormat","yyyy/MM/dd HH:mm:ss.SSS", ...
    "TimeZone","UTC");

tUnix = posixtime(dt) - double(leapSec); % seconds since UNIX epoch

%% Create ROS2 bag writer
bw = ros2bagwriter(bagFolder, "StorageFormat", storage);

% Ensure bag is properly closed even if an error happens
bagCleanup = onCleanup(@() safeCloseBag(bw));

%% Write NavSatFix messages
N = numel(lat);
for i = 1:N
    msg = ros2message("sensor_msgs/NavSatFix");

    % Header stamp (sec + nanosec)
    sec  = floor(tUnix(i));
    nsec = round((tUnix(i) - sec) * 1e9);
    if nsec >= 1e9
        sec = sec + 1;
        nsec = nsec - 1e9;
    end
    msg.header.stamp.sec     = int32(sec);
    msg.header.stamp.nanosec = uint32(nsec);
    msg.header.frame_id      = char(frameId);

    % Status (simple mapping: treat known RTK solutions as fix)
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
    % sdn/sde/sdu are std-dev (m). Off-diagonals (sdne/sdeu/sdun) are used as-is.
    Cxx = (double(sde(i))^2);
    Cyy = (double(sdn(i))^2);
    Czz = (double(sdu(i))^2);
    Cxy = double(sdne(i));
    Cxz = double(sdeu(i));
    Cyz = double(sdun(i));

    covENU = [ Cxx, Cxy, Cxz; ...
               Cxy, Cyy, Cyz; ...
               Cxz, Cyz, Czz ];

    msg.position_covariance = reshape(covENU.', 1, 9); % row-major
    msg.position_covariance_type = uint8(3);           % COVARIANCE_TYPE_KNOWN

    write(bw, char(topicName), ros2time(tUnix(i)), msg);
end

% Close bag (writes metadata.yaml)
delete(bw);
clear bw;

fprintf("Done.\n  input : %s\n  output: %s\n  topic : %s\n  msgs  : %d\n", ...
    posFile, bagFolder, topicName, N);

%% helper
function safeCloseBag(bw)
try
    if ~isempty(bw)
        delete(bw);
    end
catch
end
end
