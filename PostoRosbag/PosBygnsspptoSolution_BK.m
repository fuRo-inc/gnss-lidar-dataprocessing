clear; close all; clc;

%% -------- User settings --------
username = "wataru";
dirname  = "nayoro_true_route_20260225";  % <-- CHANGE THIS TO YOUR FOLDER NAME (for file paths)
% LibGNSS++ extended .pos generated from gnssplusplus-library after adding:
% Sdn(m) Sde(m) Sdu(m) Sdne(m) Sdeu(m) Sdun(m)
%
% Change this if your file path is different.
posFile = "/home/" + username + "/ros_bags/" + dirname + "/outputs/gnsspp_rtk_kinematic_cov.pos";

% Output ROS 2 bag folder.
outdir  = "/home/" + username + "/ros_bags/" + dirname + "/outputs/gnss";
storage = "mcap";  % "mcap" or "sqlite3"

topicSol = "/gnss/solution";   % custom msg topic
topicFix = "/gnss/fix";        % optional NavSatFix topic
writeNavSatFixAlso = true;

% Match live RTK side
frameId = "gnss";
childId = "gnss";

% Time conversion (GPST -> UTC)
% GPS-UTC offset is 18 s for current data around 2026.
toUTCT = 18;  % [s]

% Keep policy.
% For RTKLIB format: raw RTKLIB Q.
% For LibGNSS++ format: raw Status.
% 0 means keep everything.
minQ_keep = 0;

% Validity thresholds.
% rank:
%   0 = invalid / no fix
%   1 = single
%   2 = DGPS
%   3 = RTK float
%   4 = RTK fixed
minRank_valid = 1;

minNs_valid   = 6;
maxSigmaH_m   = 50.0;
maxAge_s      = 999.0;

% RTKLIB .pos age can be negative depending on processing/interpolation.
% LibGNSS++ .pos has no age field, so age is set to 0.
useAbsAge = true;

customMsgType = "furos_pkg2/GnssSolution";

%% -------- Prepare output folder --------
if isfolder(outdir)
    rmdir(outdir, "s");
end

%% -------- Read .pos robustly --------
fid = fopen(posFile, "r");
assert(fid > 0, "Failed to open: %s", posFile);

lines = strings(0,1);
headerLines = strings(0,1);

while true
    tline = fgetl(fid);
    if ~ischar(tline)
        break;
    end

    tline = strip(string(tline));

    if tline == ""
        continue;
    end

    if startsWith(tline, "%")
        headerLines(end+1,1) = tline; %#ok<SAGROW>
        continue;
    end

    % Supported data rows:
    % 1) RTKLIB-like:
    %    YYYY/MM/DD HH:MM:SS(.sss) lat lon hgt Q ns sdn sde sdu sdne sdeu sdun age ratio
    %
    % 2) LibGNSS++ extended:
    %    GPS_Week GPS_TOW X Y Z Lat Lon Height Status NumSat ... Sde Sdn Sdu Sdne Sdeu Sdun
    if ~isempty(regexp(tline, "^\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}", "once")) || ...
       ~isempty(regexp(tline, "^\d+\s+\d+(\.\d+)?\s+", "once"))
        lines(end+1,1) = tline; %#ok<SAGROW>
    end
end
fclose(fid);

assert(~isempty(lines), "No valid data lines found in .pos.");

firstLine = lines(1);
isRtklibDateFormat = ~isempty(regexp(firstLine, "^\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}", "once"));

%% -------- Parse .pos --------
if isRtklibDateFormat
    inputFormat = "RTKLIB";

    % date time lat lon hgt Q ns sdn sde sdu sdne sdeu sdun age ratio
    C = textscan(join(lines, newline), ...
        "%s %s %f %f %f %d %d %f %f %f %f %f %f %f %f", ...
        "MultipleDelimsAsOne", true);

    dateStr = string(C{1});
    timeStr = string(C{2});
    lat     = C{3};
    lon     = C{4};
    hgt     = C{5};
    Q       = C{6};   % RTKLIB .pos Q
    ns      = C{7};
    sdn     = C{8};
    sde     = C{9};
    sdu     = C{10};
    sdne    = C{11};
    sdeu    = C{12};
    sdun    = C{13};
    age     = C{14};
    ratio   = C{15};

    % Timestamp parse: RTKLIB date/time is GPST.
    tstr = strip(dateStr + " " + timeStr);

    hasDot = contains(tstr, ".");
    if any(hasDot)
        parts = split(tstr(hasDot), ".");
        frac  = parts(:,2);
        frac  = extractBefore(frac, 4);      % keep up to 3 digits
        frac  = pad(frac, 3, "right", "0");  % pad to .SSS
        tstr(hasDot) = parts(:,1) + "." + frac;
    end

    dt_gpst = NaT(size(tstr), "TimeZone", "UTC");
    hasMs = contains(tstr, ".");
    dt_gpst(hasMs)  = datetime(tstr(hasMs),  "InputFormat", "yyyy/MM/dd HH:mm:ss.SSS", "TimeZone", "UTC");
    dt_gpst(~hasMs) = datetime(tstr(~hasMs), "InputFormat", "yyyy/MM/dd HH:mm:ss",     "TimeZone", "UTC");

elseif true
    inputFormat = "LIBGNSSPP_EXTENDED";

    % LibGNSS++ extended format after covariance-column patch:
    %
    %  1 GPS_Week
    %  2 GPS_TOW
    %  3 X(m)
    %  4 Y(m)
    %  5 Z(m)
    %  6 Lat(deg)
    %  7 Lon(deg)
    %  8 Height(m)
    %  9 Status
    % 10 NumSat
    % 11 PDOP
    % 12 Ratio
    % 13 Baseline(m)
    % 14 RTKIter
    % 15 RTKObs
    % 16 RTKPhaseObs
    % 17 RTKCodeObs
    % 18 RTKOutliers
    % 19 RTKPrefitRMS(m)
    % 20 RTKPrefitMax(m)
    % 21 RTKPostSuppressRMS(m)
    % 22 RTKPostSuppressMax(m)
    % 23 RTKUpdateNIS
    % 24 RTKUpdateNISPerObs
    % 25 RTKUpdateNISRejected
    % 26 Sdn(m)
    % 27 Sde(m)
    % 28 Sdu(m)
    % 29 Sdne(m)
    % 30 Sdeu(m)
    % 31 Sdun(m)

    fmt = char(join(repmat("%f", 1, 31), " "));
    C = textscan(join(lines, newline), fmt, "MultipleDelimsAsOne", true);

    % Validate required covariance columns.
    nParsed = numel(C);
    assert(nParsed >= 31 && ~isempty(C{31}), ...
        "LibGNSS++ extended .pos must have 31 columns. Regenerate .pos after adding covariance columns.");

    gpsWeek = C{1};
    gpsTow  = C{2};

    lat   = C{6};
    lon   = C{7};
    hgt   = C{8};
    Q     = int32(C{9});    % LibGNSS++ Status
    ns    = int32(C{10});
    ratio = C{12};

    % Added covariance columns.
    sdn  = C{26};
    sde  = C{27};
    sdu  = C{28};
    sdne = C{29};
    sdeu = C{30};
    sdun = C{31};

    % LibGNSS++ .pos has no age field.
    age = zeros(size(lat));

    % GPS week/TOW -> UTC.
    gpsEpoch = datetime(1980, 1, 6, 0, 0, 0, "TimeZone", "UTC");
    dt_gpst = gpsEpoch + days(7 .* double(gpsWeek)) + seconds(double(gpsTow));
end

fprintf("Input format: %s\n", inputFormat);
fprintf("Loaded data rows=%d (after dropping non-data lines)\n", numel(lat));

%% -------- Keep filter --------
keep = (Q >= minQ_keep);

dt_gpst = dt_gpst(keep);
lat   = lat(keep);
lon   = lon(keep);
hgt   = hgt(keep);
Q     = Q(keep);
ns    = ns(keep);
sdn   = sdn(keep);
sde   = sde(keep);
sdu   = sdu(keep);
sdne  = sdne(keep);
sdeu  = sdeu(keep);
sdun  = sdun(keep);
age   = age(keep);
ratio = ratio(keep);

N = numel(lat);
fprintf("Loaded rows=%d (after keep filter Q/Status>=%d)\n", N, minQ_keep);

%% -------- Timestamp finalize --------
okTime = ~isnat(dt_gpst);
badIdx = find(~okTime);

fprintf("Invalid timestamp rows: %d / %d\n", numel(badIdx), numel(dt_gpst));

if any(~okTime)
    fprintf("WARNING: %d rows have invalid timestamp. Dropping them.\n", nnz(~okTime));
end

dt_gpst = dt_gpst(okTime);

lat   = lat(okTime);
lon   = lon(okTime);
hgt   = hgt(okTime);
Q     = Q(okTime);
ns    = ns(okTime);
sdn   = sdn(okTime);
sde   = sde(okTime);
sdu   = sdu(okTime);
sdne  = sdne(okTime);
sdeu  = sdeu(okTime);
sdun  = sdun(okTime);
age   = age(okTime);
ratio = ratio(okTime);

dt_utc = dt_gpst - seconds(toUTCT);
unix_time = posixtime(dt_utc);
N = numel(unix_time);

fprintf("Time range: %.3f .. %.3f (unix)\n", unix_time(1), unix_time(end));
if N > 1
    fprintf("dt_median=%.6f s\n", median(diff(unix_time)));
end

%% -------- Normalize quality to live RTK-like rtk_q --------
% live RTK-like rtk_q:
%   0 = invalid / no fix
%   1 = single
%   2 = DGPS
%   4 = RTK fixed
%   5 = RTK float

rtk_q_out = zeros(N,1,'uint8');

if inputFormat == "RTKLIB"
    % RTKLIB .pos Q:
    %   1 = fixed
    %   2 = float
    %   4 = DGPS
    %   5 = single
    %   6 = PPP
    %   7 = dead reckoning
    rtk_q_out(Q == 1) = uint8(4);  % fixed
    rtk_q_out(Q == 2) = uint8(5);  % float
    rtk_q_out(Q == 4) = uint8(2);  % DGPS
    rtk_q_out(Q == 5) = uint8(1);  % single
    rtk_q_out(Q == 6) = uint8(1);  % PPP: no direct live RTK equivalent
    rtk_q_out(Q == 7) = uint8(0);  % dead reckoning
else
    % LibGNSS++ Status observed in RTK solve:
    %   3 = RTK float
    %   4 = RTK fixed
    %
    % Other statuses are mapped conservatively.
    rtk_q_out(Q == 1) = uint8(1);  % single
    rtk_q_out(Q == 2) = uint8(2);  % DGPS-like
    rtk_q_out(Q == 3) = uint8(5);  % RTK float
    rtk_q_out(Q == 4) = uint8(4);  % RTK fixed
    rtk_q_out(Q == 5) = uint8(1);  % PPP float: no direct live RTK equivalent
    rtk_q_out(Q == 6) = uint8(1);  % PPP fixed: no direct live RTK equivalent
end

% Convert live RTK-like rtk_q to internal quality rank.
rtk_rank_out = zeros(N,1,'uint8');
rtk_rank_out(rtk_q_out == 1) = uint8(1);
rtk_rank_out(rtk_q_out == 2) = uint8(2);
rtk_rank_out(rtk_q_out == 5) = uint8(3);
rtk_rank_out(rtk_q_out == 4) = uint8(4);

%% -------- Normalize age --------
if useAbsAge
    age_out = abs(double(age));
else
    age_out = double(age);
end

%% -------- Precompute validity + reject_mask --------
reject_mask = zeros(N,1,'uint32');
valid = true(N,1);

sigma_h = sqrt(double(sdn).^2 + double(sde).^2);

% bit0: RTK quality too low
badQ = (rtk_rank_out < minRank_valid);
reject_mask(badQ) = bitor(reject_mask(badQ), uint32(1));
valid(badQ) = false;

% bit1: satellites too few
badNs = (ns < minNs_valid);
reject_mask(badNs) = bitor(reject_mask(badNs), uint32(2));
valid(badNs) = false;

% bit2: horizontal sigma too large / invalid
badSig = ~isfinite(sigma_h) | (sigma_h > maxSigmaH_m);
reject_mask(badSig) = bitor(reject_mask(badSig), uint32(4));
valid(badSig) = false;

% bit3: age invalid / too large
badAge = ~isfinite(age_out) | (age_out > maxAge_s);
reject_mask(badAge) = bitor(reject_mask(badAge), uint32(8));
valid(badAge) = false;

fprintf("Validity: valid=%d / %d\n", nnz(valid), N);

fprintf("Raw Q/Status counts:\n");
disp(groupsummary(table(Q), "Q"));

fprintf("Normalized rtk_q counts:\n");
disp(groupsummary(table(rtk_q_out), "rtk_q_out"));

fprintf("Normalized rank counts:\n");
disp(groupsummary(table(rtk_rank_out), "rtk_rank_out"));

%% -------- Write ROS 2 bag --------
bagWriter = ros2bagwriter(outdir, "StorageFormat", storage);

for i = 1:N
    % timestamps
    sec  = floor(unix_time(i));
    nsec = uint32(round((unix_time(i) - sec) * 1e9));

    if nsec >= 1e9
        sec = sec + 1;
        nsec = uint32(nsec - 1e9);
    end

    %% ----- custom msg: GnssSolution -----
    sol = ros2message(customMsgType);

    sol.header.frame_id = char(frameId);
    sol.header.stamp.sec = int32(sec);
    sol.header.stamp.nanosec = nsec;

    sol.child_frame_id = char(childId);

    sol.latitude  = double(lat(i));
    sol.longitude = double(lon(i));
    sol.altitude  = double(hgt(i));

    % live RTK-like normalized quality
    sol.rtk_q   = uint8(rtk_q_out(i));
    sol.ns_used = uint16(ns(i));

    % normalized age
    sol.age_s = single(age_out(i));

    % saved only; not used for common validity
    sol.ratio = single(ratio(i));

    sol.sdn  = single(sdn(i));
    sol.sde  = single(sde(i));
    sol.sdu  = single(sdu(i));
    sol.sdne = single(sdne(i));
    sol.sdeu = single(sdeu(i));
    sol.sdun = single(sdun(i));

    % ENU covariance: x=E, y=N, z=U
    Cee = double(sde(i))^2;
    Cnn = double(sdn(i))^2;
    Cuu = double(sdu(i))^2;

    Cen = double(sdne(i)) * abs(double(sdne(i)));
    Ceu = double(sdeu(i)) * abs(double(sdeu(i)));
    Cnu = double(sdun(i)) * abs(double(sdun(i)));

    covENU = [ ...
        Cee, Cen, Ceu; ...
        Cen, Cnn, Cnu; ...
        Ceu, Cnu, Cuu  ...
    ];

    sol.position_covariance = reshape(covENU.', 9, 1);
    sol.position_covariance_type = uint8(3);

    sol.valid = logical(valid(i));
    sol.reject_mask = uint32(reject_mask(i));

    write(bagWriter, char(topicSol), ros2time(unix_time(i)), sol);

    %% ----- optional NavSatFix -----
    if writeNavSatFixAlso
        fix = ros2message("sensor_msgs/NavSatFix");
        fix.header = sol.header;

        if rtk_rank_out(i) >= minRank_valid
            fix.status.status = int8(0);   % STATUS_FIX
        else
            fix.status.status = int8(-1);  % STATUS_NO_FIX
        end

        fix.status.service = uint16(1);    % SERVICE_GPS

        fix.latitude  = sol.latitude;
        fix.longitude = sol.longitude;
        fix.altitude  = sol.altitude;

        fix.position_covariance = reshape(covENU.', 1, 9);
        fix.position_covariance_type = uint8(3);

        write(bagWriter, char(topicFix), ros2time(unix_time(i)), fix);
    end
end

delete(bagWriter);
clear bagWriter;

fprintf("Done.\n");
fprintf("  input : %s\n", posFile);
fprintf("  output: %s\n", outdir);
fprintf("  topic : %s\n", topicSol);
fprintf("  msgs  : %d\n", N);
