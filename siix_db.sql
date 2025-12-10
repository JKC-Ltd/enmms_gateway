--
-- Table structure for table `cache`
--

CREATE TABLE `cache` (
  `key` varchar(255) NOT NULL,
  `value` mediumtext NOT NULL,
  `expiration` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `cache`
--

INSERT INTO `cache` (`key`, `value`, `expiration`) VALUES
('admin@smartpower.com|127.0.0.1', 'i:2;', 1765179894),
('admin@smartpower.com|127.0.0.1:timer', 'i:1765179893;', 1765179894),
('admin@smartpowerph.com|119.94.165.133', 'i:3;', 1764244778),
('admin@smartpowerph.com|119.94.165.133:timer', 'i:1764244778;', 1764244778),
('admin@smartpowerph.com|119.94.167.195', 'i:2;', 1747762973),
('admin@smartpowerph.com|119.94.167.195:timer', 'i:1747762973;', 1747762973),
('admin@smartpowerph.com|127.0.0.1', 'i:1;', 1765123105),
('admin@smartpowerph.com|127.0.0.1:timer', 'i:1765123104;', 1765123104),
('admin@smartpowerph.com|136.158.10.191', 'i:2;', 1750329397),
('admin@smartpowerph.com|136.158.10.191:timer', 'i:1750329397;', 1750329397),
('admin@smartpowerph.com|172.18.0.1', 'i:1;', 1764653382),
('admin@smartpowerph.com|172.18.0.1:timer', 'i:1764653381;', 1764653382),
('ghowell00@yahoo.com|2a03:b0c0:1:e0::9097:1001', 'i:1;', 1763617955),
('ghowell00@yahoo.com|2a03:b0c0:1:e0::9097:1001:timer', 'i:1763617955;', 1763617955),
('hugolehmann92@outlook.com|2a03:b0c0:1:e0::9097:1001', 'i:1;', 1763617958),
('hugolehmann92@outlook.com|2a03:b0c0:1:e0::9097:1001:timer', 'i:1763617958;', 1763617958),
('james.zarsuelo@indigo21.com|136.158.10.191', 'i:1;', 1747650159),
('james.zarsuelo@indigo21.com|136.158.10.191:timer', 'i:1747650159;', 1747650159),
('katheryn97@twinbash.co|2a03:b0c0:1:e0::9097:1001', 'i:3;', 1763617952),
('katheryn97@twinbash.co|2a03:b0c0:1:e0::9097:1001:timer', 'i:1763617952;', 1763617952),
('mrgabthan@smartpowerph.com|112.200.234.10', 'i:1;', 1751866207),
('mrgabthan@smartpowerph.com|112.200.234.10:timer', 'i:1751866207;', 1751866207),
('mrgabthan@smartpowerph.com|2001:4451:4160:7300:8926:57a7:cb0e:7ff0', 'i:1;', 1751865631),
('mrgabthan@smartpowerph.com|2001:4451:4160:7300:8926:57a7:cb0e:7ff0:timer', 'i:1751865631;', 1751865631),
('mrgbathan@jsmartpowerph.com|2001:4451:4154:2700:bd0e:b269:6d02:7c4f', 'i:2;', 1760060439),
('mrgbathan@jsmartpowerph.com|2001:4451:4154:2700:bd0e:b269:6d02:7c4f:timer', 'i:1760060439;', 1760060439),
('mrgbathan@smarpowerph.com|112.200.234.10', 'i:2;', 1748222836),
('mrgbathan@smarpowerph.com|112.200.234.10:timer', 'i:1748222836;', 1748222836),
('mrgbathan@smartpowerph.com|111.90.198.84', 'i:2;', 1756424212),
('mrgbathan@smartpowerph.com|111.90.198.84:timer', 'i:1756424212;', 1756424212),
('support@indigo21.com|180.190.121.10', 'i:1;', 1744649660),
('support@indigo21.com|180.190.121.10:timer', 'i:1744649660;', 1744649660),
('test@gmail.com|180.190.111.22', 'i:1;', 1759853306),
('test@gmail.com|180.190.111.22:timer', 'i:1759853306;', 1759853306),
('test@indigo21.com|180.190.212.116', 'i:1;', 1759243848),
('test@indigo21.com|180.190.212.116:timer', 'i:1759243848;', 1759243848),
('test@indigo21.com|180.190.73.39', 'i:2;', 1756198055),
('test@indigo21.com|180.190.73.39:timer', 'i:1756198055;', 1756198055),
('test@smartpowerph.com|180.190.111.22', 'i:3;', 1764310658),
('test@smartpowerph.com|180.190.111.22:timer', 'i:1764310658;', 1764310658),
('test@smartpowerph.com|180.190.121.57', 'i:1;', 1745864617),
('test@smartpowerph.com|180.190.121.57:timer', 'i:1745864617;', 1745864617),
('user@smartpower.com|149.30.138.176', 'i:1;', 1762343278),
('user@smartpower.com|149.30.138.176:timer', 'i:1762343278;', 1762343278),
('user@smartpower.com|210.1.64.194', 'i:1;', 1750397445),
('user@smartpower.com|210.1.64.194:timer', 'i:1750397445;', 1750397445),
('user@smartpower.com|2405:8d40:4483:1521:1840:45b9:261c:27ea', 'i:1;', 1747632825),
('user@smartpower.com|2405:8d40:4483:1521:1840:45b9:261c:27ea:timer', 'i:1747632825;', 1747632825),
('user@smartpowerp.com|111.90.195.6', 'i:2;', 1750643024),
('user@smartpowerp.com|111.90.195.6:timer', 'i:1750643024;', 1750643024),
('user@smartpowerph.com|61.245.23.170', 'i:2;', 1750321434),
('user@smartpowerph.com|61.245.23.170:timer', 'i:1750321434;', 1750321434),
('user1@example.com|210.1.64.194', 'i:1;', 1745371018),
('user1@example.com|210.1.64.194:timer', 'i:1745371018;', 1745371018);

-- --------------------------------------------------------

--
-- Table structure for table `cache_locks`
--

CREATE TABLE `cache_locks` (
  `key` varchar(255) NOT NULL,
  `owner` varchar(255) NOT NULL,
  `expiration` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `failed_jobs`
--

CREATE TABLE `failed_jobs` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `uuid` varchar(255) NOT NULL,
  `connection` text NOT NULL,
  `queue` text NOT NULL,
  `payload` longtext NOT NULL,
  `exception` longtext NOT NULL,
  `failed_at` timestamp NOT NULL DEFAULT current_timestamp()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `gateways`
--

CREATE TABLE `gateways` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `location_id` bigint(20) UNSIGNED NOT NULL,
  `customer_code` varchar(255) NOT NULL,
  `gateway` varchar(255) DEFAULT NULL,
  `gateway_code` varchar(255) NOT NULL,
  `description` longtext DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `gateways`
--

INSERT INTO `gateways` (`id`, `location_id`, `customer_code`, `gateway`, `gateway_code`, `description`, `created_at`, `updated_at`, `deleted_at`) VALUES
(1, 18, 'SIIX', '1', 'GAT-01', 'Gateway on IIDA', '2025-03-20 22:26:56', '2025-03-20 22:26:56', NULL),
(2, 19, 'SIIX', '2', 'GAT-02', 'Gateway on Canteen', '2025-03-20 22:27:47', '2025-03-20 22:27:47', NULL),
(3, 24, 'SIIX', '3', 'GAT-03', 'Gateway on EOL', '2025-03-20 22:28:08', '2025-03-20 22:28:08', NULL),
(4, 25, 'SIIX', '4', 'GAT-04', 'Gateway on EE Room', '2025-03-20 22:28:37', '2025-03-20 22:28:37', NULL),
(5, 21, 'SIIX', '5', 'GAT-05', 'Gateway on SMT Area', '2025-03-20 22:29:02', '2025-03-20 22:29:02', NULL),
(6, 26, 'SIIX', '6', 'GAT-06', 'Gateway on EE Room EMS Building 1', '2025-04-30 13:43:55', '2025-04-30 13:43:55', NULL),
(7, 8, 'SIIX', '7', 'GAT-07', 'Gateway on EE Room Building 3', '2025-11-26 19:53:32', '2025-11-26 19:53:32', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `jobs`
--

CREATE TABLE `jobs` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `queue` varchar(255) NOT NULL,
  `payload` longtext NOT NULL,
  `attempts` tinyint(3) UNSIGNED NOT NULL,
  `reserved_at` int(10) UNSIGNED DEFAULT NULL,
  `available_at` int(10) UNSIGNED NOT NULL,
  `created_at` int(10) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `job_batches`
--

CREATE TABLE `job_batches` (
  `id` varchar(255) NOT NULL,
  `name` varchar(255) NOT NULL,
  `total_jobs` int(11) NOT NULL,
  `pending_jobs` int(11) NOT NULL,
  `failed_jobs` int(11) NOT NULL,
  `failed_job_ids` longtext NOT NULL,
  `options` mediumtext DEFAULT NULL,
  `cancelled_at` int(11) DEFAULT NULL,
  `created_at` int(11) NOT NULL,
  `finished_at` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `locations`
--

CREATE TABLE `locations` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `location_code` varchar(255) NOT NULL,
  `location_name` varchar(255) NOT NULL,
  `pid` varchar(255) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `locations`
--

INSERT INTO `locations` (`id`, `location_code`, `location_name`, `pid`, `created_at`, `updated_at`, `deleted_at`) VALUES
(1, 'SEP', 'SEP', NULL, '2025-03-02 01:26:56', '2025-03-02 01:26:56', NULL),
(2, 'EMS', 'EMS', '1', '2025-03-02 01:28:08', '2025-03-02 01:28:08', NULL),
(3, 'Injection', 'Injection', '1', '2025-03-02 01:28:59', '2025-03-02 01:28:59', NULL),
(4, 'CIP2', 'CIP2', '1', '2025-03-02 01:29:11', '2025-03-02 01:29:11', NULL),
(5, 'Building 4', 'Building 4', '1', '2025-03-02 01:30:20', '2025-03-02 01:30:20', NULL),
(6, 'Building 1', 'Building 1', '2', '2025-03-02 01:30:32', '2025-03-02 01:30:32', NULL),
(7, 'Building 2', 'Building 2', '2', '2025-03-02 01:31:51', '2025-03-02 01:31:51', NULL),
(8, 'Building 3', 'Building 3', '2', '2025-03-02 01:32:04', '2025-03-02 01:32:04', NULL),
(9, '1st Floor', '1st Floor', '3', '2025-03-02 01:32:30', '2025-03-02 01:32:30', NULL),
(10, '2nd Floor', '2nd Floor', '3', '2025-03-02 01:32:49', '2025-03-02 01:32:49', NULL),
(11, '1st Floor', '1st Floor', '6', '2025-03-02 01:32:49', '2025-03-02 01:32:49', NULL),
(12, '2nd Floor', '2nd Floor', '6', '2025-03-02 01:32:49', '2025-03-02 01:32:49', NULL),
(13, '1st Floor', '1st Floor', '7', '2025-03-02 01:32:30', '2025-03-02 01:32:30', NULL),
(14, '2nd Floor', '2nd Floor', '7', '2025-03-02 01:32:49', '2025-03-02 01:32:49', NULL),
(15, '1st Floor', '1st Floor', '8', '2025-03-02 01:32:30', '2025-03-02 01:32:30', NULL),
(16, '2nd Floor', '2nd Floor', '8', '2025-03-02 01:32:49', '2025-03-02 01:32:49', NULL),
(17, 'IIDA line', 'IIDA line', '11', '2025-03-02 01:32:49', '2025-03-02 01:32:49', NULL),
(18, 'IIDA Office', 'IIDA Office', '11', '2025-03-02 01:32:49', '2025-03-02 01:32:49', NULL),
(19, 'Canteen', 'Canteen', '12', NULL, NULL, NULL),
(20, 'General Office', 'General Office', '12', NULL, NULL, NULL),
(21, 'SMT Area', 'SMT Area', '14', NULL, '2025-12-10 20:06:59', NULL),
(22, 'A1 reflow', 'A1 reflow', '25', NULL, '2025-03-23 18:50:54', '2025-03-23 18:50:54'),
(23, 'B5 reflow', 'B5 reflow', '21', NULL, '2025-03-23 18:51:04', '2025-03-23 18:51:04'),
(24, 'EOL', 'EOL', '14', NULL, '2025-03-22 10:29:04', NULL),
(25, 'EE Room', 'EE Room Bldg 2', '13', '2025-03-20 21:16:10', '2025-05-16 19:05:44', NULL),
(26, 'EE Room B1', 'EE Room B1', '6', '2025-04-30 13:39:16', '2025-04-30 13:39:16', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `migrations`
--

CREATE TABLE `migrations` (
  `id` int(10) UNSIGNED NOT NULL,
  `migration` varchar(255) NOT NULL,
  `batch` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `migrations`
--

INSERT INTO `migrations` (`id`, `migration`, `batch`) VALUES
(1, '0001_01_01_000000_create_users_table', 1),
(2, '0001_01_01_000001_create_cache_table', 1),
(3, '0001_01_01_000002_create_jobs_table', 1),
(4, '2025_01_29_090253_create_locations_table', 1),
(5, '2025_01_29_090317_create_gateways_table', 1),
(6, '2025_01_29_090335_create_sensor_types_table', 1),
(7, '2025_01_29_090336_create_sensor_models_table', 1),
(8, '2025_01_29_090340_create_sensors_table', 1),
(9, '2025_01_29_090520_create_sensor_logs_table', 1),
(10, '2025_01_30_050145_create_sensor_offlines_table', 1),
(11, '2025_02_14_042309_alter_sensor_offlines_table', 1),
(12, '2025_02_14_045008_alter_gateways_table', 1),
(13, '2025_02_17_071232_create_user_types_table', 1);

-- --------------------------------------------------------

--
-- Table structure for table `password_reset_tokens`
--

CREATE TABLE `password_reset_tokens` (
  `email` varchar(255) NOT NULL,
  `token` varchar(255) NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- --------------------------------------------------------

--
-- Table structure for table `sensors`
--

CREATE TABLE `sensors` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `slave_address` varchar(255) NOT NULL,
  `description` longtext DEFAULT NULL,
  `location_id` bigint(20) UNSIGNED NOT NULL,
  `gateway_id` bigint(20) UNSIGNED NOT NULL,
  `sensor_model_id` bigint(20) UNSIGNED NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `sensors`
--

INSERT INTO `sensors` (`id`, `slave_address`, `description`, `location_id`, `gateway_id`, `sensor_model_id`, `created_at`, `updated_at`, `deleted_at`) VALUES
(1, '2', 'IIDA PP-100V', 17, 1, 1, '2025-03-20 22:32:53', '2025-03-20 22:37:17', NULL),
(2, '3', 'IIDA PP-200V', 17, 1, 1, '2025-03-20 22:33:12', '2025-03-20 22:37:24', NULL),
(3, '1', 'IIDA PP-220V', 17, 1, 1, '2025-03-20 22:33:31', '2025-03-20 22:37:32', NULL),
(4, '6', 'IIDA PP-HDA-AP', 17, 1, 1, '2025-03-20 22:33:52', '2025-03-20 22:37:40', NULL),
(5, '4', 'PP-CANTEEN', 12, 2, 1, '2025-03-20 22:37:03', '2025-12-10 20:04:09', NULL),
(6, '5', 'PP-GED/BPO', 12, 2, 1, '2025-03-20 22:38:00', '2025-12-10 20:04:25', NULL),
(7, '7', 'PP-GenOffice', 12, 2, 1, '2025-03-20 22:38:20', '2025-12-10 20:05:17', NULL),
(8, '9', 'EOL MP-100V-2-3', 24, 3, 1, '2025-03-20 22:39:35', '2025-03-22 15:21:55', NULL),
(9, '8', 'EOL MP-2-3', 24, 3, 1, '2025-03-20 22:39:54', '2025-03-22 15:20:43', NULL),
(10, '10', 'SWS Reflow A4', 21, 4, 2, '2025-03-20 22:40:45', '2025-12-10 20:06:00', NULL),
(11, '11', 'B2 Reflow', 21, 4, 2, '2025-03-20 22:41:28', '2025-12-10 20:07:54', NULL),
(12, '12', 'A1 Reflow', 21, 4, 1, '2025-03-20 22:41:48', '2025-12-10 20:06:24', NULL),
(13, '13', 'A2 Reflow', 21, 5, 1, '2025-03-20 22:42:04', '2025-03-26 18:32:13', NULL),
(14, '14', 'A3 Reflow', 21, 5, 1, '2025-03-20 22:42:22', '2025-03-26 18:32:40', NULL),
(15, '20', 'MDP', 26, 6, 1, '2025-04-30 17:12:36', '2025-04-30 17:12:36', NULL),
(16, '30', 'CB1000 - TX1', 7, 4, 3, '2025-11-26 21:33:11', '2025-12-10 20:09:02', NULL),
(17, '31', 'CB1000 - TX2', 7, 4, 3, '2025-11-26 21:33:39', '2025-12-10 20:09:19', NULL),
(18, '32', 'CB2000 - TX3', 7, 4, 3, '2025-11-26 21:34:08', '2025-12-10 20:09:32', NULL),
(19, '33', 'MDP-2', 8, 7, 3, '2025-11-26 22:09:52', '2025-11-28 14:27:00', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `sensor_logs`
--

CREATE TABLE `sensor_logs` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `gateway_id` bigint(20) UNSIGNED NOT NULL,
  `sensor_id` bigint(20) UNSIGNED NOT NULL,
  `voltage_ab` double DEFAULT NULL,
  `voltage_bc` double DEFAULT NULL,
  `voltage_ca` double DEFAULT NULL,
  `current_a` double DEFAULT NULL,
  `current_b` double DEFAULT NULL,
  `current_c` double DEFAULT NULL,
  `real_power` double DEFAULT NULL,
  `apparent_power` double DEFAULT NULL,
  `energy` double DEFAULT NULL,
  `temperature` double DEFAULT NULL,
  `humidity` double DEFAULT NULL,
  `volume` double DEFAULT NULL,
  `flow` double DEFAULT NULL,
  `pressure` double DEFAULT NULL,
  `co2` double DEFAULT NULL,
  `pm25_pm10` double DEFAULT NULL,
  `o2` double DEFAULT NULL,
  `nox` double DEFAULT NULL,
  `co` double DEFAULT NULL,
  `s02` double DEFAULT NULL,
  `datetime_created` datetime NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Table structure for table `sensor_models`
--

CREATE TABLE `sensor_models` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `sensor_model` varchar(255) NOT NULL,
  `sensor_brand` varchar(255) NOT NULL,
  `sensor_type_id` bigint(20) UNSIGNED NOT NULL,
  `sensor_reg_address` varchar(255) NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `sensor_models`
--

INSERT INTO `sensor_models` (`id`, `sensor_model`, `sensor_brand`, `sensor_type_id`, `sensor_reg_address`, `created_at`, `updated_at`, `deleted_at`) VALUES
(1, 'PM2120', 'Schneider', 2, '3019,3021,3023,2999,3001,3003,3059,3075,2699', '2025-03-19 01:33:52', '2025-03-22 18:24:13', NULL),
(2, 'SDM120', 'Eastron', 2, '200,202,204,6,8,10,52,56,342', '2025-03-22 09:17:04', '2025-03-26 12:55:03', NULL),
(3, 'Smart X96', 'Eastron', 2, '200,202,204,6,8,10,52,56,342', '2025-11-28 14:26:02', '2025-11-28 14:26:02', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `sensor_offlines`
--

CREATE TABLE `sensor_offlines` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `query` longtext NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `gateway_id` bigint(20) UNSIGNED NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `sensor_offlines`
--
-- --------------------------------------------------------

--
-- Table structure for table `sensor_types`
--

CREATE TABLE `sensor_types` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `description` varchar(255) NOT NULL,
  `sensor_type_code` varchar(255) NOT NULL,
  `sensor_type_parameter` varchar(255) NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `sensor_types`
--

INSERT INTO `sensor_types` (`id`, `description`, `sensor_type_code`, `sensor_type_parameter`, `created_at`, `updated_at`, `deleted_at`) VALUES
(1, 'Single Phase Meter', 'SPM', 'voltage_ab,current_a,real_power,apparent_power,energy', '2025-02-14 01:04:41', '2025-02-14 01:06:28', NULL),
(2, 'Three Phase Meter', 'TPM', 'voltage_ab,voltage_bc,voltage_ca,current_a,current_b,current_c,real_power,apparent_power,energy', '2025-02-14 01:05:34', '2025-02-14 01:06:21', NULL),
(3, 'Temperature & Humidity Sensor', 'THS', 'temperature,humidity', '2025-02-14 01:06:13', '2025-02-14 01:06:13', NULL),
(4, 'Flow Meter', 'FVM', 'volume,flow', '2025-02-14 01:07:05', '2025-02-14 01:07:05', NULL),
(5, 'Pressure Meter Guage', 'PMG', 'pressure', '2025-02-14 01:07:28', '2025-02-14 01:07:28', NULL),
(6, 'Air Quality Meter', 'AQM', 'co2,pm25_pm10,o2,nox,co,s02', '2025-02-14 01:08:48', '2025-02-14 01:08:48', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `sessions`
--

CREATE TABLE `sessions` (
  `id` varchar(255) NOT NULL,
  `user_id` bigint(20) UNSIGNED DEFAULT NULL,
  `ip_address` varchar(45) DEFAULT NULL,
  `user_agent` text DEFAULT NULL,
  `payload` longtext NOT NULL,
  `last_activity` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `sessions`
--

INSERT INTO `sessions` (`id`, `user_id`, `ip_address`, `user_agent`, `payload`, `last_activity`) VALUES
('6oR6zAZpHh13y9QWbutRSAy703417jzcLoER5Iid', NULL, '157.245.124.230', 'Mozilla/5.0 (X11; Linux x86_64; rv:142.0) Gecko/20100101 Firefox/142.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiV1dvekFyN2NHTzZNcU9CMEVuM1k3YkZXWWsxdVd3VHU5c3FOYXp4aCI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765350777),
('6ydaD8algThEO59bl9fph7zL5ImOgfS6AFTWe48o', 2, '61.245.23.170', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0', 'YTo0OntzOjY6Il90b2tlbiI7czo0MDoidGIwa0RtaUlJZWNuVGlsRmNWQWVyaFVxU0dYR3B0MXJLTXRvNGxoRyI7czo1MDoibG9naW5fd2ViXzU5YmEzNmFkZGMyYjJmOTQwMTU4MGYwMTRjN2Y1OGVhNGUzMDk4OWQiO2k6MjtzOjk6Il9wcmV2aW91cyI7YToxOntzOjM6InVybCI7czo0NzoiaHR0cHM6Ly9zaWl4LnNtYXJ0cG93ZXJwaC5jb20vZW5lcmd5Q29uc3VtcHRpb24iO31zOjY6Il9mbGFzaCI7YToyOntzOjM6Im9sZCI7YTowOnt9czozOiJuZXciO2E6MDp7fX19', 1765329931),
('9jmEEJRSTTZ6Kw5VoAGZiRrT2M7LFWFuFNjzJRl7', NULL, '34.139.108.176', 'Mozilla/5.0 (compatible; CMS-Checker/1.0; +https://example.com)', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoibzl1RndaQ3V0NkZucXVDVkdCZ211bVZHQmZJdGFMSlgzWW02NkxzbSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765296117),
('COimvxCgzQVKBYXUwWP8JEbd4gNYXs51W3fMRFJq', NULL, '35.174.168.97', 'okhttp/5.3.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiYWpaNEF6dVd0Y0ZaNUtVcXo3UFhJTTdnZk5YUDRzVjJ3ZDJ0SHcwbSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mjk6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765341645),
('CVuIh4UqCMMVI6Fug2SgeOhKzZsj0WkSXD1MYpyB', NULL, '2a02:4780:11:c0de::e', 'Go-http-client/2.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiSkxTWXhSclVYck1ya3lud3IySEJwWWtreWpCczd0TzQ1b3hIajNEciI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mzg6Imh0dHBzOi8vZGV2LXNpaXguc3BwaHBvcnRhbC5zaXRlL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765334449),
('DKBTuKeUanU5VP4UimiGe1zQagsOkZU5pWOdeExf', 2, '112.199.110.203', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0', 'YTo0OntzOjY6Il90b2tlbiI7czo0MDoid3lHOWtpOW0yZTd3U2hxQzhObGM4NUpuNFJoZklnbkhwUDhzaHBnOCI7czo1MDoibG9naW5fd2ViXzU5YmEzNmFkZGMyYjJmOTQwMTU4MGYwMTRjN2Y1OGVhNGUzMDk4OWQiO2k6MjtzOjk6Il9wcmV2aW91cyI7YToxOntzOjM6InVybCI7czo0MToiaHR0cHM6Ly9zaWl4LnNtYXJ0cG93ZXJwaC5jb20vYWN0aXZlUG93ZXIiO31zOjY6Il9mbGFzaCI7YToyOntzOjM6Im9sZCI7YTowOnt9czozOiJuZXciO2E6MDp7fX19', 1765372107),
('FFiW3RBptrpxOTLlu9IUEenScyK9hXx0qKqaBxj6', NULL, '35.174.168.97', 'okhttp/5.3.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiWTFIdmYyY2FFR3FyaUJybHhuY05OMWduOW13RjZmdmc3dGdSNVRsYSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765341646),
('FJqttjEiX91cd1bBHbZFMtpxOlxAaDSyvALJR00b', 1, '136.158.39.33', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0', 'YTo0OntzOjY6Il90b2tlbiI7czo0MDoiQkozV09sVkFQZ0g3N2hiT29LSXR2bVBRRDAyY0VDdWlRMWo5bGhNcyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDc6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2xvY2F0aW9uRGFzaGJvYXJkIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319czo1MDoibG9naW5fd2ViXzU5YmEzNmFkZGMyYjJmOTQwMTU4MGYwMTRjN2Y1OGVhNGUzMDk4OWQiO2k6MTt9', 1765372376),
('fN6z7MKm6fR65l2hB1mMsQbundIq7y5Sxq4Psnu9', 1, '127.0.0.1', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36', 'YTo0OntzOjY6Il90b2tlbiI7czo0MDoiRnNRcEcyT2tTVnVaeXBiYmFveG9nQzVSVTlrV0lOV21NeVN5UE1EeiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mzk6Imh0dHA6Ly8xMjcuMC4wLjE6ODAwMC91c2VyVHlwZUxvY2F0aW9ucyI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fXM6NTA6ImxvZ2luX3dlYl81OWJhMzZhZGRjMmIyZjk0MDE1ODBmMDE0YzdmNThlYTRlMzA5ODlkIjtpOjE7fQ==', 1765370698),
('fQ1mIHQEpssHpLogk3Yx2roRipxZYnR7c0f9gA3R', 9, '210.1.64.194', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0', 'YTo0OntzOjY6Il90b2tlbiI7czo0MDoieXM4NWFrUHJEMUQ0QnpBREZlczVMck1aWDE0WTdIV040MHVqeWpiZyI7czo1MDoibG9naW5fd2ViXzU5YmEzNmFkZGMyYjJmOTQwMTU4MGYwMTRjN2Y1OGVhNGUzMDk4OWQiO2k6OTtzOjk6Il9wcmV2aW91cyI7YToxOntzOjM6InVybCI7czozOToiaHR0cHM6Ly9zaWl4LnNtYXJ0cG93ZXJwaC5jb20vZGFzaGJvYXJkIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765358161),
('gQDty9uXsjPlGfY3FUaAQejfqb9oMIQ6yr0LHHM9', NULL, '54.198.50.8', 'okhttp/5.3.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiN1lBNVRuQkJuajB0Q0FGa0FDMUwzSEY4WU4zQ1h5dWdvcjl2VkFrZyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765287215),
('hqc6pkkd9CXTrbv3ypFzBrGPuvwLivKARp4Q80YY', 8, '2001:4451:412d:2b00:dfa:c998:4747:7c41', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.1 Safari/605.1.15', 'YTo1OntzOjY6Il90b2tlbiI7czo0MDoicjZLNG53VHFuYUdOOUFuajJVa29aTjZ4T1FVOGdvTzZrbFllb09GSSI7czozOiJ1cmwiO2E6MDp7fXM6OToiX3ByZXZpb3VzIjthOjE6e3M6MzoidXJsIjtzOjM5OiJodHRwczovL3NpaXguc21hcnRwb3dlcnBoLmNvbS9kYXNoYm9hcmQiO31zOjY6Il9mbGFzaCI7YToyOntzOjM6Im9sZCI7YTowOnt9czozOiJuZXciO2E6MDp7fX1zOjUwOiJsb2dpbl93ZWJfNTliYTM2YWRkYzJiMmY5NDAxNTgwZjAxNGM3ZjU4ZWE0ZTMwOTg5ZCI7aTo4O30=', 1765337419),
('j7rZK8d8atqYgKjuWs65xl5yOL1H5CaPgfHQgmTa', NULL, '2a02:4780:11:c0de::e', 'Go-http-client/2.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoicDM0NVoyWDV5MnZYeDlZOVdSOTFiSWp5WmFhbnZpMWlnZXJoQUNNeiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzI6Imh0dHBzOi8vZGV2LXNpaXguc3BwaHBvcnRhbC5zaXRlIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765334449),
('JYbaBS6uIfEmFcuIhAbmfEZwdSaWHRhtzt1WEmDF', NULL, '34.139.108.176', 'Mozilla/5.0 (compatible; CMS-Checker/1.0; +https://example.com)', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoicUtmM0hRaTREUTRGTWpkTTZObTF5cEc3QTNyckJUM2M2bmZNbkxmWSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mjk6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765296116),
('KbRMNGC6XiK0QHd3H7t0hPWs9rYg3bdT8JtXo0vF', NULL, '216.73.216.55', 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiV0ZGb0FWd2ZTT3Z4UXlwdXR0dnJEbkRnOW04QWFqWFF5N2I5TlBXYSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDU6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2ZvcmdvdC1wYXNzd29yZCI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1765290094),
('kC2yWJJ4UVBOn4DlXJ0QPk57vs4G86N2b9NcndGw', 1, '119.94.165.133', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36', 'YTo0OntzOjY6Il90b2tlbiI7czo0MDoiYlBTM0h3aFc5eHRMVnhuWElvOTNUbzJWVlc0Z1JvYjNFWFdTaFRINiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2FjdGl2ZVBvd2VyIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319czo1MDoibG9naW5fd2ViXzU5YmEzNmFkZGMyYjJmOTQwMTU4MGYwMTRjN2Y1OGVhNGUzMDk4OWQiO2k6MTt9', 1765370409),
('lOnMVNAl8bAkjLAL9TSNRFDVNQDRJvJCtcmLJ9qW', NULL, '18.207.132.205', 'okhttp/5.3.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoickJNZEVncnA3UHRjYmNBelFlRGFoT1dzMEF2SHlpRDNNOHVOSHVuZiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mjk6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765294449),
('NuJKZ4pabXaXwa7YFXDx47i0wEJvprmxU3amXoLJ', NULL, '54.198.50.8', 'okhttp/5.3.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoibjVETUVVbHVKQ2plMm9aa0VITEIzM3g5VlQ1MTZVQ1Z0NVF2NXpNRCI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mjk6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765287214),
('oxgl7FWzMKHDsPgFRBr1DKByUarlu91zIFbpAtUs', NULL, '98.92.69.211', 'okhttp/5.3.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiTXV4Q09pMW9lOEgzQ3BZV3ZHRkZqZWtKYzdwUnRZSkxqclpDNHpKWSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mjk6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765287120),
('qcn1Flt78IBVC6czerTpKQ3VHB8XZO8G8CMgwAvv', NULL, '13.57.26.121', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36 Assetnote/1.0.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiMXZUYll6cVZSYlVnVDNUdDJLczdkTzUybXpjeVd0SWc1NWhrTDFJcyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mjk6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765332061),
('QP90e0JgzWEehPFh9sQqboDMvVE0jveW8lDGzUYE', NULL, '98.92.69.211', 'okhttp/5.3.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoibWp0cnhjc2s4bHBEZU1ENHU2SVg5QllLSkVBU3UzZWpKM2JFblJwNCI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765287121),
('SRiOuEqXNY4zt9V11aCs136MSLCFKkvqrjXBXpzx', NULL, '52.90.75.170', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/138.0.7204.23 Safari/537.36', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoicU9aVjVJS2hhVWFmTlpuaHRWaGpBQXJkTVJEcjdoOTNScTNZQXFPcSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765294497),
('UBmKCzyhBSPw6RTlWKAWOW2isq5AUW5aXZmMsGY6', 1, '180.190.111.117', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36', 'YTo0OntzOjY6Il90b2tlbiI7czo0MDoicWxDbjNHcllsTlFZUXB1M0JSVkdCVjFjeWt2MjdYcTdPZlU5NkdZWiI7czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319czo1MDoibG9naW5fd2ViXzU5YmEzNmFkZGMyYjJmOTQwMTU4MGYwMTRjN2Y1OGVhNGUzMDk4OWQiO2k6MTtzOjk6Il9wcmV2aW91cyI7YToxOntzOjM6InVybCI7czo0NzoiaHR0cHM6Ly9zaWl4LnNtYXJ0cG93ZXJwaC5jb20vbG9jYXRpb25EYXNoYm9hcmQiO319', 1765372401),
('uL0DNKCRaSw6Xt2e2odVsU7xjW2keBAZIDVYaE5L', NULL, '18.207.132.205', 'okhttp/5.3.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiNzFQOVNNS3F3YWNGalNCaDB3REVTNFVjVjdqZ1k0TFBJODhlZnBmbiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765294450),
('uW7ic82tzqV5bhIwnb9y7ZyPPh26amdTqhFqQSwn', NULL, '54.172.7.20', 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/138.0.7204.23 Safari/537.36', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiUWtLTzZJeVU1eG5mYWczdVpkd2ZldFF1Y09Obm1HaVZVZDJyQW9vNSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765287154),
('V9oSrWu2CygyZjOWMDkvct6x2Q02u82C3YDkgcjd', NULL, '216.73.216.55', 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiS29UOXZlQ3NOUmpPSnhnT3BHYng1dDFjc2ZPbk9HZk1pbnU2c01KQyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vc2lpeC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1765286701),
('VncXx2nyAgHj52qsRNslBLNLFw5QTmxVtb4eX6mv', 2, '2405:8d40:4490:586e:187f:7cd2:333d:58da', 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36', 'YTo0OntzOjY6Il90b2tlbiI7czo0MDoiUUpDR3NFdmJoQ2N6a005bk9ycE9lUWZUODZPa2Jic3RCekRnZnZDVCI7czo1MDoibG9naW5fd2ViXzU5YmEzNmFkZGMyYjJmOTQwMTU4MGYwMTRjN2Y1OGVhNGUzMDk4OWQiO2k6MjtzOjk6Il9wcmV2aW91cyI7YToxOntzOjM6InVybCI7czo0MToiaHR0cHM6Ly9zaWl4LnNtYXJ0cG93ZXJwaC5jb20vYWN0aXZlUG93ZXIiO31zOjY6Il9mbGFzaCI7YToyOntzOjM6Im9sZCI7YTowOnt9czozOiJuZXciO2E6MDp7fX19', 1765320015);

-- --------------------------------------------------------

--
-- Table structure for table `users`
--

CREATE TABLE `users` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `firstname` varchar(255) NOT NULL,
  `lastname` varchar(255) NOT NULL,
  `user_type_id` int(11) NOT NULL,
  `email` varchar(255) NOT NULL,
  `email_verified_at` timestamp NULL DEFAULT NULL,
  `password` varchar(255) NOT NULL,
  `remember_token` varchar(100) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `users`
--

INSERT INTO `users` (`id`, `firstname`, `lastname`, `user_type_id`, `email`, `email_verified_at`, `password`, `remember_token`, `created_at`, `updated_at`, `deleted_at`) VALUES
(1, 'Admin', 'Admin', 1, 'test@example.com', NULL, '$2y$12$F47rUVsa3edWbBRkM3RvxODjDKRgT.TMJiJjCoxoJ3.SA10ize7G.', NULL, '2025-03-18 05:48:11', '2025-04-01 00:56:38', NULL),
(2, 'User', 'SIIX', 2, 'user@smartpowerph.com', NULL, '$2y$12$RiwlX4W.3tXp56cOQQkiVeTV86UW7.97NRcw8aWY3DhsZN0WhsUfG', 'PmfNqObMG1Au9QQRLr3htMwr78F2nNjG5W9e8qccgpCEbOSA5Vri1hAbWEBC', '2025-03-22 11:28:13', '2025-04-01 10:35:03', NULL),
(3, 'Nicole', 'Carillo', 2, 'healer.moebus@gmail.com', NULL, '$2y$12$IKjPHLHNLlIh8PHwV2lwnOrMjmf0rJgInjW/WwzQZgOkcbHWw/1Xa', NULL, '2025-03-30 19:15:17', '2025-03-30 19:15:17', NULL),
(4, '', '', 0, '', NULL, '', NULL, '2025-03-30 19:46:52', '2025-03-30 19:46:52', '2025-03-30 19:46:52'),
(6, 'Gilbert', 'Ollet', 2, 'Gilbert.Ollet@siix-global.com', NULL, '$2y$12$IzaQzXs4OFIladNbQDcoOe8mzbYfvCkdGa7J7XNhfXHH5qN0B4wtC', NULL, '2025-04-09 11:02:27', '2025-04-09 11:02:27', NULL),
(7, 'Sammantha', 'Penalosa', 2, 'Sammantha.Penalosa@siix-global.com', NULL, '$2y$12$fTgdNf.owDPn7eyUS3rw.ejcV9qJqDx.QmDg398ucmrAEYns1nePu', NULL, '2025-04-09 11:05:27', '2025-06-20 17:02:08', NULL),
(8, 'Marvin Ryan', 'Bathan', 2, 'mrgbathan@smartpowerph.com', NULL, '$2y$12$sZoqTGmQNH8Ey64hDkutGOtEA131IKSJRtTTOkQBaSx8zYC4MQE0m', NULL, '2025-04-09 11:10:23', '2025-04-09 11:19:47', NULL),
(9, 'Jessica', 'Beltran', 2, 'Jessica.Beltran@siix-global.com', NULL, '$2y$12$5rhHCwAIUvUCyaPNvMcfdeJPNnBz2jOOxtmA3LiYk9FPEiYlFCGoG', '9FvCmNwNXXAFuL9NWGVD8rZdbVumR1kpX2qxjc8NMZFa1X3LozmI7Gf8x8z6', '2025-04-09 11:24:31', '2025-04-09 11:24:31', NULL),
(10, 'Roland', 'Abad', 2, 'Roland.Abad@siix-global.com', NULL, '$2y$12$5Rl4GIgBTG4C.pcw/qRp5eJX1itGu/Tn656BdsDjn9nTvYakag9Ae', NULL, '2025-04-09 11:27:52', '2025-04-09 11:27:52', NULL),
(11, 'Ems', 'Facility', 2, 'Ems.Facility@siix-global.com', NULL, '$2y$12$JkZpyxKFXy3Zve7ZoU6tz.ftLMtIUMQK068H1U4SV1BGGhBeiNvqu', NULL, '2025-04-09 11:28:59', '2025-04-09 11:28:59', NULL),
(12, 'Kasuyo', 'Matsuo', 2, 'Kasuyo.Matsuo@siix-global.com', NULL, '$2y$12$H5gdQlrak2HKg766npecGe1RdVpkto0OqtjPm7lBgbfetmy89Qje.', NULL, '2025-04-09 11:32:05', '2025-04-09 11:32:05', NULL),
(13, 'Marcial', 'Maglaqui', 2, 'Marcial.Maglaqui@siix-global.com', NULL, '$2y$12$UZlTeEa0esH4nQQ4WzfhzebE6U6yL59mHb0D4NgK/6E7bfyphXBae', NULL, '2025-04-09 11:33:57', '2025-04-09 11:33:57', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `user_types`
--

CREATE TABLE `user_types` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `name` varchar(255) NOT NULL,
  `created_by` varchar(255) DEFAULT NULL,
  `updated_by` varchar(255) DEFAULT NULL,
  `deleted_by` varchar(255) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `user_types`
--

INSERT INTO `user_types` (`id`, `name`, `created_by`, `updated_by`, `deleted_by`, `created_at`, `updated_at`, `deleted_at`) VALUES
(1, 'Admin', NULL, NULL, NULL, '2025-03-18 05:48:11', '2025-03-18 05:48:11', NULL),
(2, 'User', NULL, NULL, NULL, '2025-03-18 05:48:11', '2025-03-18 05:48:11', NULL);

--
-- Indexes for dumped tables
--

--
-- Indexes for table `cache`
--
ALTER TABLE `cache`
  ADD PRIMARY KEY (`key`);

--
-- Indexes for table `cache_locks`
--
ALTER TABLE `cache_locks`
  ADD PRIMARY KEY (`key`);

--
-- Indexes for table `failed_jobs`
--
ALTER TABLE `failed_jobs`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `failed_jobs_uuid_unique` (`uuid`);

--
-- Indexes for table `gateways`
--
ALTER TABLE `gateways`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `gateways_gateway_code_unique` (`gateway_code`),
  ADD KEY `gateways_location_id_foreign` (`location_id`);

--
-- Indexes for table `jobs`
--
ALTER TABLE `jobs`
  ADD PRIMARY KEY (`id`),
  ADD KEY `jobs_queue_index` (`queue`);

--
-- Indexes for table `job_batches`
--
ALTER TABLE `job_batches`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `locations`
--
ALTER TABLE `locations`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `migrations`
--
ALTER TABLE `migrations`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `password_reset_tokens`
--
ALTER TABLE `password_reset_tokens`
  ADD PRIMARY KEY (`email`);

--
-- Indexes for table `sensors`
--
ALTER TABLE `sensors`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `sensors_slave_address_unique` (`slave_address`),
  ADD KEY `sensors_location_id_foreign` (`location_id`),
  ADD KEY `sensors_gateway_id_foreign` (`gateway_id`),
  ADD KEY `sensors_sensor_model_id_foreign` (`sensor_model_id`);

--
-- Indexes for table `sensor_logs`
--
ALTER TABLE `sensor_logs`
  ADD PRIMARY KEY (`id`),
  ADD KEY `sensor_logs_gateway_id_foreign` (`gateway_id`),
  ADD KEY `sensor_logs_sensor_id_foreign` (`sensor_id`);

--
-- Indexes for table `sensor_models`
--
ALTER TABLE `sensor_models`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `sensor_models_sensor_model_unique` (`sensor_model`),
  ADD KEY `sensor_models_sensor_type_id_foreign` (`sensor_type_id`);

--
-- Indexes for table `sensor_offlines`
--
ALTER TABLE `sensor_offlines`
  ADD PRIMARY KEY (`id`),
  ADD KEY `sensor_offlines_gateway_id_foreign` (`gateway_id`);

--
-- Indexes for table `sensor_types`
--
ALTER TABLE `sensor_types`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `sensor_types_sensor_type_code_unique` (`sensor_type_code`);

--
-- Indexes for table `sessions`
--
ALTER TABLE `sessions`
  ADD PRIMARY KEY (`id`),
  ADD KEY `sessions_user_id_index` (`user_id`),
  ADD KEY `sessions_last_activity_index` (`last_activity`);

--
-- Indexes for table `users`
--
ALTER TABLE `users`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `users_email_unique` (`email`);

--
-- Indexes for table `user_types`
--
ALTER TABLE `user_types`
  ADD PRIMARY KEY (`id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `failed_jobs`
--
ALTER TABLE `failed_jobs`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `gateways`
--
ALTER TABLE `gateways`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=8;

--
-- AUTO_INCREMENT for table `jobs`
--
ALTER TABLE `jobs`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `locations`
--
ALTER TABLE `locations`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=27;

--
-- AUTO_INCREMENT for table `migrations`
--
ALTER TABLE `migrations`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=14;

--
-- AUTO_INCREMENT for table `sensors`
--
ALTER TABLE `sensors`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=28;

--
-- AUTO_INCREMENT for table `sensor_logs`
--
ALTER TABLE `sensor_logs`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1079350;

--
-- AUTO_INCREMENT for table `sensor_models`
--
ALTER TABLE `sensor_models`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- AUTO_INCREMENT for table `sensor_offlines`
--
ALTER TABLE `sensor_offlines`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=339;

--
-- AUTO_INCREMENT for table `sensor_types`
--
ALTER TABLE `sensor_types`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=7;

--
-- AUTO_INCREMENT for table `users`
--
ALTER TABLE `users`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=14;

--
-- AUTO_INCREMENT for table `user_types`
--
ALTER TABLE `user_types`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `gateways`
--
ALTER TABLE `gateways`
  ADD CONSTRAINT `gateways_location_id_foreign` FOREIGN KEY (`location_id`) REFERENCES `locations` (`id`);

--
-- Constraints for table `sensors`
--
ALTER TABLE `sensors`
  ADD CONSTRAINT `sensors_gateway_id_foreign` FOREIGN KEY (`gateway_id`) REFERENCES `gateways` (`id`),
  ADD CONSTRAINT `sensors_location_id_foreign` FOREIGN KEY (`location_id`) REFERENCES `locations` (`id`),
  ADD CONSTRAINT `sensors_sensor_model_id_foreign` FOREIGN KEY (`sensor_model_id`) REFERENCES `sensor_models` (`id`);

--
-- Constraints for table `sensor_logs`
--
ALTER TABLE `sensor_logs`
  ADD CONSTRAINT `sensor_logs_gateway_id_foreign` FOREIGN KEY (`gateway_id`) REFERENCES `gateways` (`id`),
  ADD CONSTRAINT `sensor_logs_sensor_id_foreign` FOREIGN KEY (`sensor_id`) REFERENCES `sensors` (`id`);

--
-- Constraints for table `sensor_models`
--
ALTER TABLE `sensor_models`
  ADD CONSTRAINT `sensor_models_sensor_type_id_foreign` FOREIGN KEY (`sensor_type_id`) REFERENCES `sensor_types` (`id`);

--
-- Constraints for table `sensor_offlines`
--
ALTER TABLE `sensor_offlines`
  ADD CONSTRAINT `sensor_offlines_gateway_id_foreign` FOREIGN KEY (`gateway_id`) REFERENCES `gateways` (`id`);
COMMIT;

