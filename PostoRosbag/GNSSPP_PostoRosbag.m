clear; close all; clc;

%% -------- User settings --------
dirname = "/20260428_155111";

% LibGNSS++ .pos
% 例:
%   output/gnsspp_base_nav.pos
%   output/rtk_solution.pos
posFile = "/home/wataru-furo/lidar_gnss_log/" + dirname + "/output/gnsspp_base_nav.pos";

% Output rosbag folder
outdir  = "/home/wataru-furo/lidar_gnss_log/" + dirname + "/outputs/gnss_libgnsspp";

storage = "mcap";   % "mcap" or "sqlite3"

topicSol = "/gnss/solution";
topicFix = "/gnss/fix";
writeNavSatFixAlso = true;

frameId = "gnss";
childId = "gnss";

customMsgType = "furos_pkg2/GnssSolution";

% GPS Time -> UTC
% 2026時点では GPS-UTC = 18 sec
toUTCT = 18;

%% -------- Validity policy --------
% internal rank:
%   0 = invalid
%   1 = single / SPP
%   2 = DGPS
%   3 = RTK float
%   4 = RTK fixed
%
% fixed + float を valid にする。
minRank_valid = 3;

minNs_valid   = 6;
maxSigmaH_m   = 50.0;
maxAge_s      = 999.0;

% LibGNSS++ posには age がないので 0.0 にする
defaultAge_s = 0.0;

%% -------- Estimated covariance model --------
% これは solver-native covariance ではなく、
% GNSSOdomNode のゲート/重み用の疑似共分散。
%
% Status:
%   4 = FIX
%   3 = FLOAT
%   2 = DGPS-like
%   1 = SPP / SINGLE
%   0 = invalid

% FIXは固定基準値。
% cm級までは信じず、ただしFLOATよりは強く使う。
sigmaH_fix_m    = 0.80;
sigmaV_fix_m    = 2.00;

% FLOATは一律にしない。
% good -> bad の範囲で、RMS/NIS/PhaseObs/NumSat から動的に決める。
floatSigmaH_good_m = 1.20;
floatSigmaV_good_m = 4.00;

floatSigmaH_bad_m  = 5.00;
floatSigmaV_bad_m  = 14.00;

% FLOAT診断値の基準。
% gnsspp_base_nav.pos の実データ分布を見た初期値。
floatRmsGood = 0.40;
floatRmsBad  = 1.80;

floatNisGood = 1.0;
floatNisBad  = 25.0;

floatPhaseGood = 18;
floatPhaseBad  = 6;

floatNsGood = 18;
floatNsBad  = 8;

% 1.0より大きいと、普通のFLOATはあまり悪化せず、
% 悪いFLOATだけ強めに広がる。
floatBadPower = 1.2;

% DGPS/SPPは基本的に弱い観測。
sigmaH_dgps_m   = 4.00;
sigmaV_dgps_m   = 10.00;

sigmaH_spp_m    = 12.00;
sigmaV_spp_m    = 25.00;

sigmaH_invalid_m = 100.0;
sigmaV_invalid_m = 100.0;

% PDOP scale
% 今回のログではPDOPがほぼ2付近なので、主役ではない。
pdopRef = 2.5;
pdopScaleMax = 3.0;

% RMS scale
% FIX/DGPS/SPP用。FLOATはbase sigma側にRMS/NISを反映する。
rmsRef_fix_m   = 0.60;
rmsRef_other_m = 1.50;
rmsScaleMax    = 3.0;

% NIS scale
% FIX/DGPS/SPP用。FLOATはbase sigma側にNISを反映する。
nisPerObsRef = 10.0;
nisScaleMax  = 3.0;

% Ratio policy
% FIXなのにratioが低い場合は怪しいので膨らませる。
fixRatioWarn = 3.0;
fixRatioBad  = 2.0;

%% -------- Prepare output folder --------
if isfolder(outdir)
    rmdir(outdir, "s");
end

%% -------- Read LibGNSS++ .pos --------
fid = fopen(posFile, "r");
assert(fid > 0, "Failed to open: %s", posFile);

lines = strings(0,1);
while true
    tline = fgetl(fid);
    if ~ischar(tline)
        break;
    end

    tline = strip(string(tline));

    if tline == "" || startsWith(tline, "%")
        continue;
    end

    % LibGNSS++ row starts with:
    % GPS_Week GPS_TOW ...
    if ~isempty(regexp(tline, "^\d+\s+\d+(\.\d+)?\s+", "once"))
        lines(end+1,1) = tline; %#ok<SAGROW>
    end
end
fclose(fid);

assert(~isempty(lines), "No valid LibGNSS++ data lines found in .pos.");

% Expected columns:
%  1 GPS_Week
%  2 GPS_TOW
%  3 X
%  4 Y
%  5 Z
%  6 Lat
%  7 Lon
%  8 Height
%  9 Status
% 10 NumSat
% 11 PDOP
% 12 Ratio
% 13 Baseline
% 14 RTKIter
% 15 RTKObs
% 16 RTKPhaseObs
% 17 RTKCodeObs
% 18 RTKOutliers
% 19 RTKPrefitRMS
% 20 RTKPrefitMax
% 21 RTKPostSuppressRMS
% 22 RTKPostSuppressMax
% 23 RTKUpdateNIS
% 24 RTKUpdateNISPerObs
% 25 RTKUpdateNISRejected

fmt = repmat('%f', 1, 25);
dataText = char(join(lines, newline));

C = textscan(dataText, fmt, ...
    'MultipleDelimsAsOne', true);

gpsWeek = C{1};
gpsTow  = C{2};

ecefX = C{3}; %#ok<NASGU>
ecefY = C{4}; %#ok<NASGU>
ecefZ = C{5}; %#ok<NASGU>

lat = C{6};
lon = C{7};
hgt = C{8};

status = C{9};
ns     = C{10};
pdop   = C{11};
ratio  = C{12};

baseline = C{13}; %#ok<NASGU>
rtkIter  = C{14}; %#ok<NASGU>
rtkObs   = C{15}; %#ok<NASGU>
rtkPhaseObs = C{16};
rtkCodeObs  = C{17}; %#ok<NASGU>
rtkOutliers = C{18};

rtkPrefitRMS = C{19};
rtkPrefitMax = C{20}; %#ok<NASGU>
rtkPostSuppressRMS = C{21};
rtkPostSuppressMax = C{22}; %#ok<NASGU>
rtkUpdateNIS = C{23}; %#ok<NASGU>
rtkUpdateNISPerObs = C{24};
rtkUpdateNISRejected = C{25}; %#ok<NASGU>

N0 = numel(lat);
fprintf("Loaded LibGNSS++ rows=%d\n", N0);

%% -------- Drop invalid numeric rows --------
ok = isfinite(gpsWeek) & isfinite(gpsTow) & ...
     isfinite(lat) & isfinite(lon) & isfinite(hgt) & ...
     isfinite(status) & isfinite(ns);

gpsWeek = gpsWeek(ok);
gpsTow  = gpsTow(ok);
lat     = lat(ok);
lon     = lon(ok);
hgt     = hgt(ok);
status  = status(ok);
ns      = ns(ok);
pdop    = pdop(ok);
ratio   = ratio(ok);

rtkPhaseObs = rtkPhaseObs(ok);
rtkOutliers = rtkOutliers(ok);
rtkPrefitRMS = rtkPrefitRMS(ok);
rtkPostSuppressRMS = rtkPostSuppressRMS(ok);
rtkUpdateNISPerObs = rtkUpdateNISPerObs(ok);

N = numel(lat);
fprintf("Rows after numeric filter=%d\n", N);

%% -------- GPS week/TOW -> unix time --------
% GPS epoch is 1980-01-06 00:00:00 GPST.
gpsEpochUnix = posixtime(datetime(1980,1,6,0,0,0,"TimeZone","UTC"));

% Convert GPST seconds to UTC unix seconds by subtracting GPS-UTC offset.
unix_time = gpsEpochUnix + double(gpsWeek) .* 604800.0 + double(gpsTow) - toUTCT;

fprintf("Time range: %.3f .. %.3f (unix UTC)\n", unix_time(1), unix_time(end));
if N > 1
    fprintf("dt_median=%.6f s\n", median(diff(unix_time)));
end

%% -------- Normalize LibGNSS++ Status to live RTK-like rtk_q --------
% live RTK-like rtk_q:
%   0 = invalid / no fix
%   1 = single
%   2 = DGPS
%   4 = RTK fixed
%   5 = RTK float

rtk_q_out = zeros(N,1,'uint8');

rtk_q_out(status == 4) = uint8(4);  % fixed
rtk_q_out(status == 3) = uint8(5);  % float
rtk_q_out(status == 2) = uint8(2);  % DGPS-like
rtk_q_out(status == 1) = uint8(1);  % single / SPP
rtk_q_out(status == 0) = uint8(0);  % invalid

% Internal rank:
%   0 = invalid
%   1 = single
%   2 = DGPS
%   3 = RTK float
%   4 = RTK fixed
rtk_rank_out = zeros(N,1,'uint8');
rtk_rank_out(rtk_q_out == 1) = uint8(1);
rtk_rank_out(rtk_q_out == 2) = uint8(2);
rtk_rank_out(rtk_q_out == 5) = uint8(3);
rtk_rank_out(rtk_q_out == 4) = uint8(4);

%% -------- Estimate covariance --------
sdn  = zeros(N,1);  % sigma north [m]
sde  = zeros(N,1);  % sigma east  [m]
sdu  = zeros(N,1);  % sigma up    [m]
sdne = zeros(N,1);  % correlation/covariance term unknown -> zero
sdeu = zeros(N,1);  % unknown -> zero
sdun = zeros(N,1);  % unknown -> zero

sigmaSourceScale = zeros(N,1);
floatBadScore = nan(N,1);

for i = 1:N
    st = round(status(i));

    %% ----- base sigma by status -----
    if st == 4
        % RTK FIX
        baseH = sigmaH_fix_m;
        baseV = sigmaV_fix_m;

    elseif st == 3
        % RTK FLOAT
        % FLOATは一律にせず、診断値からgood/badを作る。

        rmsVal = rtkPostSuppressRMS(i);
        if ~isfinite(rmsVal) || rmsVal <= 0
            rmsVal = rtkPrefitRMS(i);
        end

        if isfinite(rmsVal) && rmsVal > 0
            rmsBad = min(max((rmsVal - floatRmsGood) / ...
                             (floatRmsBad - floatRmsGood), 0.0), 1.0);
        else
            rmsBad = 0.5;
        end

        if isfinite(rtkUpdateNISPerObs(i)) && rtkUpdateNISPerObs(i) > 0
            % NISは裾が重いのでlogで評価。
            nisBad = min(max( ...
                (log1p(rtkUpdateNISPerObs(i)) - log1p(floatNisGood)) / ...
                (log1p(floatNisBad) - log1p(floatNisGood)), ...
                0.0), 1.0);
        else
            nisBad = 0.5;
        end

        if isfinite(rtkPhaseObs(i))
            phaseBad = min(max((floatPhaseGood - rtkPhaseObs(i)) / ...
                               (floatPhaseGood - floatPhaseBad), 0.0), 1.0);
        else
            phaseBad = 0.5;
        end

        if isfinite(ns(i))
            nsBad = min(max((floatNsGood - ns(i)) / ...
                            (floatNsGood - floatNsBad), 0.0), 1.0);
        else
            nsBad = 0.5;
        end

        % RMS/NISを主、観測数不足を補助にする。
        % maxを使うことで、一つでも明確に悪い要素があれば弱くする。
        floatBad = max([ ...
            0.65 * rmsBad + 0.35 * nisBad, ...
            0.70 * phaseBad, ...
            0.50 * nsBad ...
        ]);

        floatBad = min(max(floatBad, 0.0), 1.0);
        t = floatBad ^ floatBadPower;

        baseH = floatSigmaH_good_m + ...
                (floatSigmaH_bad_m - floatSigmaH_good_m) * t;

        baseV = floatSigmaV_good_m + ...
                (floatSigmaV_bad_m - floatSigmaV_good_m) * t;

        floatBadScore(i) = floatBad;

    elseif st == 2
        % DGPS-like
        baseH = sigmaH_dgps_m;
        baseV = sigmaV_dgps_m;

    elseif st == 1
        % SPP / SINGLE
        baseH = sigmaH_spp_m;
        baseV = sigmaV_spp_m;

    else
        baseH = sigmaH_invalid_m;
        baseV = sigmaV_invalid_m;
    end

    %% ----- PDOP scale -----
    if isfinite(pdop(i)) && pdop(i) > 0
        pdopScale = pdop(i) / pdopRef;
        pdopScale = min(max(pdopScale, 1.0), pdopScaleMax);
    else
        pdopScale = pdopScaleMax;
    end

    %% ----- RMS scale -----
    rmsVal = rtkPostSuppressRMS(i);
    if ~isfinite(rmsVal) || rmsVal <= 0
        rmsVal = rtkPrefitRMS(i);
    end

    if isfinite(rmsVal) && rmsVal > 0
        if st == 4
            rmsRef = rmsRef_fix_m;
        else
            rmsRef = rmsRef_other_m;
        end

        rmsScale = rmsVal / rmsRef;
        rmsScale = min(max(rmsScale, 1.0), rmsScaleMax);
    else
        rmsScale = 1.0;
    end

    %% ----- NIS scale -----
    if isfinite(rtkUpdateNISPerObs(i)) && rtkUpdateNISPerObs(i) > 0
        nisScale = sqrt(rtkUpdateNISPerObs(i) / nisPerObsRef);
        nisScale = min(max(nisScale, 1.0), nisScaleMax);
    else
        nisScale = 1.0;
    end

    %% ----- outlier scale -----
    if isfinite(rtkOutliers(i)) && rtkOutliers(i) > 0
        outlierScale = 1.0 + 0.1 * min(rtkOutliers(i), 10);
    else
        outlierScale = 1.0;
    end

    %% ----- final scale -----
    if st == 3
        % FLOATはbaseH/baseV側にRMS/NIS/観測数を反映済み。
        % 二重に膨らみすぎないよう、追加scaleはPDOP/outlierのみ。
        scale = max(1.0, pdopScale) * outlierScale;
    else
        scale = max([pdopScale, rmsScale, nisScale]) * outlierScale;
    end

    % FIXなのにratioが低い場合は追加ペナルティ。
    if st == 4
        if ~isfinite(ratio(i)) || ratio(i) < fixRatioBad
            scale = scale * 4.0;
        elseif ratio(i) < fixRatioWarn
            scale = scale * 2.0;
        end
    end

    sigmaH = baseH * scale;
    sigmaV = baseV * scale;

    sdn(i) = sigmaH;
    sde(i) = sigmaH;
    sdu(i) = sigmaV;

    sigmaSourceScale(i) = scale;
end

% 相関項は不明なので0。
sdne(:) = 0.0;
sdeu(:) = 0.0;
sdun(:) = 0.0;

%% -------- Normalize age --------
age_out = defaultAge_s * ones(N,1);

%% -------- Validity + reject_mask --------
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

fprintf("LibGNSS++ status counts:\n");
disp(groupsummary(table(status), "status"));

fprintf("Normalized rtk_q counts:\n");
disp(groupsummary(table(rtk_q_out), "rtk_q_out"));

fprintf("Normalized rank counts:\n");
disp(groupsummary(table(rtk_rank_out), "rtk_rank_out"));

fprintf("Estimated sigma summary by status:\n");
Tsig = table(status, sdn, sde, sdu, sigmaSourceScale, floatBadScore);
disp(groupsummary(Tsig, "status", ["mean","median","min","max"], ...
    ["sdn","sde","sdu","sigmaSourceScale","floatBadScore"]));

%% -------- Write ROS 2 bag --------
bagWriter = ros2bagwriter(outdir, "StorageFormat", storage);

for i = 1:N
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

    sol.age_s = single(age_out(i));
    sol.ratio = single(ratio(i));

    % Estimated sigma values.
    sol.sdn  = single(sdn(i));
    sol.sde  = single(sde(i));
    sol.sdu  = single(sdu(i));
    sol.sdne = single(sdne(i));
    sol.sdeu = single(sdeu(i));
    sol.sdun = single(sdun(i));

    % ENU covariance: x=E, y=N, z=U.
    Cee = double(sde(i))^2;
    Cnn = double(sdn(i))^2;
    Cuu = double(sdu(i))^2;

    Cen = double(sdne(i));
    Ceu = double(sdeu(i));
    Cnu = double(sdun(i));

    covENU = [ ...
        Cee, Cen, Ceu; ...
        Cen, Cnn, Cnu; ...
        Ceu, Cnu, Cuu  ...
    ];

    sol.position_covariance = reshape(covENU.', 9, 1);

    % 2 = APPROXIMATED covariance.
    % solver-native covarianceではないのでKNOWNにはしない。
    sol.position_covariance_type = uint8(2);

    sol.valid = logical(valid(i));
    sol.reject_mask = uint32(reject_mask(i));

    write(bagWriter, char(topicSol), ros2time(unix_time(i)), sol);

    %% ----- optional NavSatFix -----
    if writeNavSatFixAlso
        fix = ros2message("sensor_msgs/NavSatFix");
        fix.header = sol.header;

        if valid(i)
            fix.status.status = int8(0);   % STATUS_FIX
        else
            fix.status.status = int8(-1);  % STATUS_NO_FIX
        end

        fix.status.service = uint16(1);    % SERVICE_GPS

        fix.latitude  = sol.latitude;
        fix.longitude = sol.longitude;
        fix.altitude  = sol.altitude;

        fix.position_covariance = reshape(covENU.', 1, 9);

        % COVARIANCE_TYPE_APPROXIMATED
        fix.position_covariance_type = uint8(2);

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