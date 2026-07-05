-- phpMyAdmin SQL Dump
-- version 5.2.2
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1:3306
-- Generation Time: Jul 05, 2026 at 02:52 PM
-- Server version: 11.8.8-MariaDB-log
-- PHP Version: 7.2.34

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `u565803524_dev_uratex`
--

-- --------------------------------------------------------

--
-- Table structure for table `branches`
--

CREATE TABLE `branches` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `name` varchar(255) NOT NULL,
  `branch_code` varchar(255) DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `branches`
--

INSERT INTO `branches` (`id`, `name`, `branch_code`, `deleted_at`, `created_at`, `updated_at`) VALUES
(1, 'Plaridel', 'PRL', NULL, '2026-07-05 14:35:57', '2026-07-05 14:35:57'),
(2, 'Valenzuela', 'VZA', NULL, '2026-07-05 14:36:22', '2026-07-05 14:36:22'),
(3, 'Alabang', 'ABG', NULL, '2026-07-05 14:36:38', '2026-07-05 14:36:38');

-- --------------------------------------------------------

--
-- Table structure for table `cache`
--

CREATE TABLE `cache` (
  `key` varchar(255) NOT NULL,
  `value` mediumtext NOT NULL,
  `expiration` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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
(1, 3, 'Uratex', '1', 'GAT-01', 'Gateway on Admin Building', '2026-07-05 11:43:47', '2026-07-05 11:43:47', NULL),
(2, 16, 'Uratex2', '2', 'GAT-02', 'Gateway Building2', NULL, NULL, NULL);

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
  `deleted_at` timestamp NULL DEFAULT NULL,
  `branch_id` bigint(20) UNSIGNED DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `locations`
--

INSERT INTO `locations` (`id`, `location_code`, `location_name`, `pid`, `created_at`, `updated_at`, `deleted_at`, `branch_id`) VALUES
(1, 'Uratex', 'Uratex', NULL, '2025-03-02 09:26:56', '2026-07-05 14:38:30', NULL, NULL),
(2, 'Valenzuela', 'Valenzuela', '1', '2025-03-02 09:28:08', '2025-03-02 09:28:08', NULL, NULL),
(3, 'Admin Building', 'Admin Building', '2', '2025-03-02 09:28:59', '2026-07-05 14:39:06', NULL, 2),
(4, 'Building No. 18', 'Building No. 18', '2', '2025-03-02 09:29:11', '2025-03-02 09:29:11', NULL, NULL),
(5, 'Building No. 12', 'Building No. 12', '2', '2025-03-02 09:30:20', '2025-03-02 09:30:20', NULL, NULL),
(6, 'Building No. 13', 'Building No. 13', '2', '2025-03-02 09:30:32', '2025-03-02 09:30:32', NULL, NULL),
(7, 'Building No. 11', 'Building No. 11', '2', '2025-03-02 09:31:51', '2025-03-02 09:31:51', NULL, NULL),
(8, 'Building No. 9', 'Building No. 9', '2', '2025-03-02 09:32:04', '2025-03-02 09:32:04', NULL, NULL),
(9, 'Building No. 17', 'Building No. 17', '2', '2025-03-02 09:32:30', '2025-03-02 09:32:30', NULL, NULL),
(10, 'Alabang', 'Alabang', '1', '2025-03-02 09:32:30', '2025-03-02 09:32:30', NULL, NULL),
(11, 'Powerhouse 1', 'Powerhouse 1', '10', '2025-03-02 09:32:30', '2026-07-05 14:39:24', NULL, 3),
(12, 'Powerhouse 2', 'Powerhouse 2', '10', '2025-03-02 09:32:30', '2025-03-02 09:32:30', NULL, NULL),
(13, 'Powerhouse 3', 'Powerhouse 3', '10', '2025-03-02 09:32:30', '2025-03-02 09:32:30', NULL, NULL),
(14, 'Powerhouse 4', 'Powerhouse 4', '10', '2025-03-02 09:32:30', '2025-03-02 09:32:30', NULL, NULL),
(15, 'Powerhouse 5', 'Powerhouse 5', '10', '2025-03-02 09:32:30', '2025-03-02 09:32:30', NULL, NULL),
(16, 'Plaridel', 'Plaridel', '1', '2025-03-02 09:32:30', '2025-03-02 09:32:30', NULL, NULL),
(17, 'Powerhouse 1', 'Powerhouse 1', '16', '2025-03-02 09:32:30', '2025-03-02 09:32:30', NULL, NULL),
(18, 'Powerhouse 2', 'Powerhouse 2', '16', '2025-03-02 09:32:30', '2025-03-02 09:32:30', NULL, NULL),
(19, 'Powerhouse 3', 'Powerhouse 3', '16', '2025-03-02 09:32:30', '2025-03-02 09:32:30', NULL, NULL),
(20, 'Powerhouse 4', 'Powerhouse 4', '16', '2025-03-02 09:32:30', '2025-03-02 09:32:30', NULL, NULL);

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
(13, '2025_02_17_071232_create_user_types_table', 1),
(14, '2025_12_17_042459_create_user_type_locations_table', 1),
(15, '2026_04_11_000000_create_branches_table', 1),
(16, '2026_04_12_000001_add_branch_id_to_locations_table', 1),
(17, '2026_04_12_000002_add_branch_code_to_branches_table', 1),
(18, '2026_04_14_165039_alter_users_table', 1),
(19, '2026_05_09_000001_create_user_branches_table', 1),
(20, '2026_05_09_174808_add_session_token_to_users_table', 1);

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
(1, '7', 'J-1', 3, 1, 1, '2026-07-05 11:43:47', '2026-07-05 14:39:40', NULL),
(24, '5', 'Test Gateway 2', 11, 2, 2, '2026-07-05 14:35:27', '2026-07-05 14:39:47', NULL);

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
-- Dumping data for table `sensor_logs`
--

INSERT INTO `sensor_logs` (`id`, `gateway_id`, `sensor_id`, `voltage_ab`, `voltage_bc`, `voltage_ca`, `current_a`, `current_b`, `current_c`, `real_power`, `apparent_power`, `energy`, `temperature`, `humidity`, `volume`, `flow`, `pressure`, `co2`, `pm25_pm10`, `o2`, `nox`, `co`, `s02`, `datetime_created`, `created_at`, `updated_at`) VALUES
(1, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:44:22', NULL, NULL),
(2, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:44:33', NULL, NULL),
(3, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:44:43', NULL, NULL),
(4, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:55:20', NULL, NULL),
(5, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:55:32', NULL, NULL),
(6, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:55:43', NULL, NULL),
(7, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:55:59', NULL, NULL),
(8, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:56:09', NULL, NULL),
(9, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:56:20', NULL, NULL),
(10, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:56:34', NULL, NULL),
(11, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:56:46', NULL, NULL),
(12, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:57:01', NULL, NULL),
(13, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:57:12', NULL, NULL),
(14, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:57:22', NULL, NULL),
(15, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:57:33', NULL, NULL),
(16, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:57:44', NULL, NULL),
(17, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:57:54', NULL, NULL),
(18, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:58:05', NULL, NULL),
(19, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:58:15', NULL, NULL),
(20, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:58:26', NULL, NULL),
(21, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:58:36', NULL, NULL),
(22, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:58:47', NULL, NULL),
(23, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:58:57', NULL, NULL),
(24, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:59:08', NULL, NULL),
(25, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:59:18', NULL, NULL),
(26, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:59:33', NULL, NULL),
(27, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:59:44', NULL, NULL),
(28, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 21:59:56', NULL, NULL),
(29, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:00:07', NULL, NULL),
(30, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:00:18', NULL, NULL),
(31, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:00:30', NULL, NULL),
(32, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:00:41', NULL, NULL),
(33, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:00:52', NULL, NULL),
(34, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:01:04', NULL, NULL),
(35, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:01:15', NULL, NULL),
(36, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:01:27', NULL, NULL),
(37, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:01:43', NULL, NULL),
(38, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:01:55', NULL, NULL),
(39, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:02:07', NULL, NULL),
(40, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:02:22', NULL, NULL),
(41, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:02:33', NULL, NULL),
(42, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:02:43', NULL, NULL),
(43, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:02:54', NULL, NULL),
(44, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:03:04', NULL, NULL),
(45, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:03:15', NULL, NULL),
(46, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:03:25', NULL, NULL),
(47, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:03:36', NULL, NULL),
(48, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:03:46', NULL, NULL),
(49, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:03:57', NULL, NULL),
(50, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:04:07', NULL, NULL),
(51, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:04:18', NULL, NULL),
(52, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:04:32', NULL, NULL),
(53, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:04:44', NULL, NULL),
(54, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:04:55', NULL, NULL),
(55, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:05:07', NULL, NULL),
(56, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:05:20', NULL, NULL),
(57, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:05:31', NULL, NULL),
(58, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:05:43', NULL, NULL),
(59, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:05:54', NULL, NULL),
(60, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:06:06', NULL, NULL),
(61, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:06:18', NULL, NULL),
(62, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:06:29', NULL, NULL),
(63, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:06:40', NULL, NULL),
(64, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:06:52', NULL, NULL),
(65, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:07:03', NULL, NULL),
(66, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:07:15', NULL, NULL),
(67, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:07:27', NULL, NULL),
(68, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:07:39', NULL, NULL),
(69, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:07:50', NULL, NULL),
(70, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:08:02', NULL, NULL),
(71, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:08:13', NULL, NULL),
(72, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:08:25', NULL, NULL),
(73, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:08:36', NULL, NULL),
(74, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:08:47', NULL, NULL),
(80, 2, 1, 233.87, 0, 233.82, 0.23, 0, 0, 37.04, 37.04, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:10:50', NULL, NULL),
(81, 2, 1, 233.96, 0, 233.78, 0.23, 0, 0, 36.87, 36.87, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:11:01', NULL, NULL),
(82, 2, 1, 234.26, 0, 234.21, 0.23, 0, 0, 36.83, 36.83, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:13:57', NULL, NULL),
(83, 2, 1, 234.04, 0, 233.99, 0.23, 0, 0, 36.97, 36.97, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:14:09', NULL, NULL),
(84, 2, 1, 234.02, 0, 233.97, 0.23, 0, 0, 36.83, 36.83, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:14:21', NULL, NULL),
(85, 2, 1, 234.45, 0, 234.43, 0.23, 0, 0, 36.94, 36.94, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:14:36', NULL, NULL),
(86, 2, 1, 234.06, 0, 234.02, 0.23, 0, 0, 36.83, 36.83, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:14:47', NULL, NULL),
(87, 2, 1, 234.23, 0, 234.18, 0.23, 0, 0, 36.94, 36.94, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:14:58', NULL, NULL),
(88, 2, 1, 234.42, 0, 234.48, 0.23, 0, 0, 36.9, 36.9, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:15:08', NULL, NULL),
(89, 2, 1, 234.36, 0, 234.31, 0.23, 0, 0, 36.9, 36.83, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:15:19', NULL, NULL),
(90, 2, 1, 234.44, 0, 234.4, 0.23, 0, 0, 36.8, 36.8, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:15:29', NULL, NULL),
(91, 2, 1, 234.06, 0, 234.01, 0.23, 0, 0, 36.87, 36.87, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:15:40', NULL, NULL),
(92, 2, 1, 233.89, 0, 233.84, 0.23, 0, 0, 36.87, 36.87, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:15:51', NULL, NULL),
(93, 2, 1, 233.4, 0, 233.36, 0.23, 0, 0, 36.83, 36.83, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:16:01', NULL, NULL),
(94, 2, 1, 234.44, 0, 234.34, 0.23, 0, 0, 36.87, 36.87, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:16:12', NULL, NULL),
(95, 2, 1, 234.32, 0, 234.27, 0.23, 0, 0, 36.9, 36.9, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:16:23', NULL, NULL),
(96, 2, 1, 234.54, 0, 234.49, 0.23, 0, 0, 36.87, 36.87, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:16:37', NULL, NULL),
(97, 2, 1, 234.5, 0, 234.45, 0.23, 0, 0, 36.83, 36.83, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:16:49', NULL, NULL),
(98, 2, 1, 234.49, 0, 234.44, 0.23, 0, 0, 36.83, 36.83, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:17:00', NULL, NULL),
(99, 2, 1, 234.57, 0, 234.53, 0.23, 0, 0, 36.9, 36.9, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:17:12', NULL, NULL),
(100, 2, 1, 234.4, 0, 234.35, 0.23, 0, 0, 37.01, 37.01, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:17:23', NULL, NULL),
(101, 2, 1, 234.18, 0, 234.13, 0.23, 0, 0, 36.9, 36.9, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:17:34', NULL, NULL),
(102, 2, 1, 234.03, 0, 233.98, 0.23, 0, 0, 36.94, 36.94, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:17:46', NULL, NULL),
(103, 2, 1, 232.8, 0, 232.75, 0.23, 0, 0, 36.69, 36.69, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:17:57', NULL, NULL),
(104, 2, 1, 234.27, 0, 234.22, 0.23, 0, 0, 36.9, 36.8, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:18:08', NULL, NULL),
(105, 2, 1, 234.28, 0, 234.23, 0.23, 0, 0, 36.83, 36.83, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:18:20', NULL, NULL),
(106, 2, 1, 234.09, 0, 234.05, 0.23, 0, 0, 36.87, 36.87, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:18:31', NULL, NULL),
(107, 2, 1, 234.15, 0, 234.11, 0.23, 0, 0, 36.9, 36.9, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:18:43', NULL, NULL),
(108, 2, 1, 234.8, 0, 234.75, 0.23, 0, 0, 36.83, 36.83, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:18:54', NULL, NULL),
(109, 2, 1, 234.57, 0, 234.53, 0.23, 0, 0, 36.87, 36.87, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:19:06', NULL, NULL),
(110, 2, 1, 234.57, 0, 234.53, 0.23, 0, 0, 36.94, 36.94, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:19:17', NULL, NULL),
(111, 2, 1, 234.46, 0, 234.42, 0.23, 0, 0, 36.9, 36.9, 436.16, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:19:29', NULL, NULL),
(112, 2, 1, 234.56, 0, 234.52, 0.23, 0, 0, 36.83, 36.83, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:19:40', NULL, NULL),
(113, 2, 1, 234.57, 0, 234.52, 0.23, 0, 0, 36.83, 36.87, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:19:51', NULL, NULL),
(114, 2, 1, 234.69, 0, 234.64, 0.23, 0, 0, 36.9, 36.9, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:20:03', NULL, NULL),
(115, 2, 1, 234.72, 0, 234.67, 0.23, 0, 0, 36.87, 36.87, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:20:14', NULL, NULL),
(116, 2, 1, 234.71, 0, 234.66, 0.23, 0, 0, 36.94, 36.94, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:20:26', NULL, NULL),
(117, 2, 1, 234.81, 0, 234.76, 0.23, 0.07, 0, 36.9, 36.9, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:20:37', NULL, NULL),
(118, 2, 1, 234.85, 0, 234.81, 0.23, 0, 0, 36.87, 36.87, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:20:48', NULL, NULL),
(119, 2, 1, 234.99, 0, 234.94, 0.23, 0, 0, 36.83, 36.83, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:21:00', NULL, NULL),
(120, 2, 1, 234.67, 0, 234.63, 0.23, 0, 0, 36.87, 36.87, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:21:11', NULL, NULL),
(121, 2, 1, 234.75, 0, 234.61, 0.23, 0, 0, 36.83, 36.83, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:21:23', NULL, NULL),
(122, 2, 1, 234.89, 0, 234.84, 0.23, 0, 0, 36.9, 36.9, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:21:34', NULL, NULL),
(123, 2, 1, 235.34, 0, 235.29, 0.23, 0, 0, 36.9, 36.9, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:21:46', NULL, NULL),
(124, 2, 1, 235.36, 0, 235.31, 0.23, 0, 0, 36.76, 36.76, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:21:57', NULL, NULL),
(125, 2, 1, 235.4, 0, 235.35, 0.23, 0, 0, 36.9, 36.97, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:22:08', NULL, NULL),
(126, 2, 1, 235.39, 0, 235.34, 0.23, 0, 0, 36.87, 36.87, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:22:20', NULL, NULL),
(127, 2, 1, 235.34, 0, 235.3, 0.23, 0, 0, 36.87, 36.87, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:22:31', NULL, NULL),
(128, 2, 1, 235.48, 0, 235.43, 0.23, 0, 0, 36.87, 36.87, 436.17, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:22:43', NULL, NULL),
(130, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:42:56', NULL, NULL),
(131, 1, 1, 0, 0, 0, 220, 5, 1, 0, 0, 0, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2026-07-05 22:43:08', NULL, NULL);

-- --------------------------------------------------------

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
(1, 'Test', 'Schneider', 2, '200,202,204,6,8,10,52,56,342', '2026-07-05 11:43:47', '2026-07-05 14:34:25', NULL),
(2, 'SDM630', 'Eastron', 2, '200,202,204,6,8,10,52,56,342', '2026-07-05 14:34:34', '2026-07-05 14:34:34', NULL);

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
(1, 'Single Phase Meter', 'SPM', 'voltage_ab,current_a,real_power,apparent_power,energy', '2025-02-14 09:04:41', '2025-02-14 09:06:28', NULL),
(2, 'Three Phase Meter', 'TPM', 'voltage_ab,voltage_bc,voltage_ca,current_a,current_b,current_c,real_power,apparent_power,energy', '2025-02-14 09:05:34', '2025-02-14 09:06:21', NULL),
(3, 'Temperature & Humidity Sensor', 'THS', 'temperature,humidity', '2025-02-14 09:06:13', '2025-02-14 09:06:13', NULL),
(4, 'Flow Meter', 'FVM', 'volume,flow', '2025-02-14 09:07:05', '2025-02-14 09:07:05', NULL),
(5, 'Pressure Meter Guage', 'PMG', 'pressure', '2025-02-14 09:07:28', '2025-02-14 09:07:28', NULL),
(6, 'Air Quality Meter', 'AQM', 'co2,pm25_pm10,o2,nox,co,s02', '2025-02-14 09:08:48', '2025-02-14 09:08:48', NULL);

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
('26W7HxS2kg6i33WbHG8UVQw1NSfeO8FKnWa0CUYq', NULL, '23.27.145.144', 'Mozilla/5.0 (X11; Linux i686; rv:109.0) Gecko/20100101 Firefox/120.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiQ20zRE9ZdUJGNzNwSHpGWHFrUWY0cEg3Y1RaRVk1V2VqdDVSV1VtciI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783260527),
('3KGfnHTPi7MAx8fmZkQ8NoVWhaJ1nesUqERr70BV', NULL, '35.91.33.22', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiTEN4dFV1S2EyWU1aVVFLRnc1dlhsQ2tDZDNOZUZNUWRpU2JxUWNxTiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mzk6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbSI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783261655),
('46G7BVrjhIU4y7AeWQaQ9aSe7uiddxxlnxxVhh5v', NULL, '52.33.72.134', 'Mozilla/5.0 (Linux; Android 8.0.0; SM-G965U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.7871.46 Mobile Safari/537.36', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiQm5pdEFZSm15VzdiS00xVDk1UU1sd2VRVVpsaXFuaGxXUHFoenZ3NSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783260290),
('5TnIbKnIlyakxtXOE7g9WzwozPM3t1Qz2KBXX7Dy', NULL, '34.248.137.227', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiUWs0em1JM2p5eXZEMDM0dlp2ZHd1OFJKemNrQ2dBRzI4dkdpQWpDRiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDU6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbS9sb2dpbiI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783262070),
('7d29UFC9nB97YjrKiwaAEu1sv50fddogS4w8V7RV', NULL, '52.33.3.252', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoid2xFYTBzaHcxNTlKMmJ1dkdmSEF5RUFBcnBISGxZcDk4SXZLMW0wciI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783259552),
('8rY7QNjsPhciQqCv0F8MCtJxPpkT4Ja6eF5lUUrR', NULL, '91.196.152.149', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiZWRWZjM2TlJIUVB3YUpJaVpsNUcyNU1DcTBOek5BRjFvN29JVkpyRiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783256780),
('bmHqLnyaRZRWNpggzSGwV0OxUAe1iOHfJtQgNEog', NULL, '52.33.3.252', 'Mozilla/5.0 (Linux; Android 8.0.0; SM-G965U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.111 Mobile Safari/537.36', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiUWhORmtzc3RQV0UyQVRlTWRzMTRReXc4TlVTdWJNSVlQM05Ec0ZlUiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783259552),
('br1N3WhgEVnsVMRBLkdfULofA4GwRrxkRg9XB522', NULL, '216.73.217.148', 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiNnk1MEVpczNRa0N6bkszZ21JSmwzZ2hDVzRvMDVBV2E4YUxTZVNtaSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDU6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbS9sb2dpbiI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783256447),
('bRGVhYQACg2RhmDcrktGfyb733w5gDwX5QNvJjcU', NULL, '52.33.3.252', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiSVIwYml2MUEwUWw5NnNTaG52OE1HTHlrdll1UWJDMGZCSnNaTjJrbyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783259552),
('bUp3yaNOd2ZdeNb0EXWcVSmkFgX9aSk2ifqm22x5', NULL, '2a03:b0c0:2:f0:0:1:be0b:3001', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 +https://forestengine.net/#opt-out', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiSThLTjVQdUYyRm44d1hWbkhHbXc1bFd2aWVpeTRWUnd2VHVGaUkyNyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mzk6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbSI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783258059),
('CJ8VfHBJgca2hTbwk8yAyqx9CIVVxoEmpv0bkThx', 1, '136.158.10.63', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36', 'YTo0OntzOjY6Il90b2tlbiI7czo0MDoiRGVEZDAyakZkRm1PS3BDaTNmb3dIUzV4RDFwVEtldW1BeXJkdDFwbSI7czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDQ6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2JyYW5jaGVzIjt9czo1MDoibG9naW5fd2ViXzU5YmEzNmFkZGMyYjJmOTQwMTU4MGYwMTRjN2Y1OGVhNGUzMDk4OWQiO2k6MTt9', 1783262138),
('DEL2ZhMzUfcDAiompmoUsYi6EaenevTUepthIVEN', NULL, '91.231.89.96', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiUXZpTnhrUTVpOHE3djh4ejNjTGNmRmtLQVBvNk5TVnViZHhFVTVJRCI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mzk6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbSI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783256660),
('Ftq0m9qZhYJnoC2FUdOAnVYEow1edaqEW0xs4KPL', NULL, '52.13.16.67', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.7871.46 Safari/537.36 Edge/18.19582', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoib0ZkOVJycGVlOWx6a1FIT1Jrd1lOQmN3Y0NGOXpwOG5VMHFOV3hOYSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783262814),
('htYcVHwBQPO4OBgwmBzvwx9J3ELxR9pFydkBnsaa', NULL, '51.254.49.98', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiaHNNS1FMcnF5M3ZVUURUem05OTRCQU9PdE43YUo1SzhKTFI2dFZwbCI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mzk6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbSI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783255959),
('IJZTgz8toBTK1SYnBH2AbZ90m56HEoHQBaAuT6VI', NULL, '91.196.152.53', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiVmYwWnEwMmNTZElwVlF4V3JwaW9NUFVJcHZ2OHVhUmxRMHJRZUhyRCI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783256887),
('k1OdEUZlDK3iFknAZVXMJUbZY2IVHjUOr8eyzPCr', NULL, '35.91.33.22', 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Mobile/15E148 Safari/604.1', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiUEJlRGx0ZWVzeU1WZ3dLclJRcmZ0Q1BHRUdTdEluWHlYZ3owYlY0MCI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6Mzk6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbSI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783261656),
('LLitRrDvEzJpUg7Ox7xjmg3I7B1BK1OMrEYRwcCs', 1, '136.158.39.197', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0', 'YTo1OntzOjY6Il90b2tlbiI7czo0MDoiWFdnbHlvcE5uOWNyWVdzN3gzdFdmNHpzTElwbTRiWVgzOWRKcUZJUiI7czozOiJ1cmwiO2E6MDp7fXM6OToiX3ByZXZpb3VzIjthOjE6e3M6MzoidXJsIjtzOjQzOiJodHRwczovL2Rldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbS9zZW5zb3JzIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319czo1MDoibG9naW5fd2ViXzU5YmEzNmFkZGMyYjJmOTQwMTU4MGYwMTRjN2Y1OGVhNGUzMDk4OWQiO2k6MTt9', 1783262743),
('lXj6kBSvdpnmkh5DN6h1Dd6GB1fteqQE483HnFr8', NULL, '35.91.33.22', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.19582', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiQlpTV2h4RHRRV2RhcWVhRjhMbWdzVXN4V3Z5aWhxazJ3cmtQREw3WiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDU6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbS9sb2dpbiI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783261655),
('MEdDCAC0Zdpjb7A7U8K07qtJaVADKzKZZYYQixNW', NULL, '34.34.17.27', 'Scrapy/2.16.0 (+https://scrapy.org)', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiMTh2S2czWnBzaE05TkkwNTFNaGMyOHdmMFUyeHl3a1dwRTNscm9OWCI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783258166),
('MzJIlA7pBJ4X2i6hoYYpvHv7wfr9bruI1U8ihdBd', NULL, '34.248.137.227', 'Mozilla/5.0 (X11; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoibkF1cU8wN1pHM1dsaGhNcXUzc3ZPWFdTWkx6eElNYW5qMG1teVhtZyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783262083),
('nazH66VrJqxYD3EQKVLadU3OuZbTBSIiTXSEUsQT', NULL, '91.196.152.149', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoib1VrZFJ0WnZVTjIxS2VjazlKUUZUNGU3Sm16YVhPZ3gwWm9nSTlqZyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDU6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbS9sb2dpbiI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783256858),
('NiqjLaR3D6crOC3WKB9njZ5sU4V3CEzOV2Z4gNY0', NULL, '216.73.217.148', 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoidFZ5WlFoSlRyaHNpQUVqV0hXUkJoZnFidHpSTEJ0cU5vZUJWV2NvVCI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NTE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2ZvcmdvdC1wYXNzd29yZCI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783261276),
('nosgUo5C3EEUBPnARvt1dyGqqWjj2prUNlZ4yAXh', NULL, '34.34.17.27', 'Scrapy/2.16.0 (+https://scrapy.org)', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiRGVvTlJsZ05FeEFaTmFnOU9GMWgxazVMTjg4VklkS0ZteklxOGtkWiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDU6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbS9sb2dpbiI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783258201),
('PgGuO5nmSFNlCH4SG9lx9E3pCWUcYVkto6JFSh6U', NULL, '34.248.137.227', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiUmE3eU1YQlMyb1VmRWRMOUVsSTV6Z2NMc20wYUxBNFU1bDBwY3N3SCI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783262084),
('PsXOmWa1cbLGtv0YhMr42olfyWBApzUJFi6zAmlu', NULL, '216.73.217.148', 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiRmV0c3lrVG9lOGl2QTV5ZFJuOXB4RUxBbjNwcnREczBETGNZZFZPMyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NTU6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbS9mb3Jnb3QtcGFzc3dvcmQiO31zOjY6Il9mbGFzaCI7YToyOntzOjM6Im9sZCI7YTowOnt9czozOiJuZXciO2E6MDp7fX19', 1783261382),
('R8q3G4aEj5Z5Iaf7lkMAVHUMi08ABdDSMUb6Q44d', NULL, '52.33.3.252', 'Mozilla/5.0 (Linux; Android 8.0.0; SM-G965U Build/R16NW) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.111 Mobile Safari/537.36', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiMUk4d0xRdThraEtveUFoYkl3SFBGcWI1RDR3VGd0TXlXd1ROZDlrUyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783259552),
('VjnImvsHdGFXIb9vBa49mJ3pJPYTzRH4CujBIjgG', NULL, '35.91.33.22', 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Mobile/15E148 Safari/604.1', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiZTllVEtKeTdUYUFJVDZGQ1FOcENVWHg3QUNIUmFTRlEwYWIyTmlYUiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDU6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbS9sb2dpbiI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783261656),
('vJSYMJSunjkDGLVxSOYBSRwwAeSU00UtXi5dIn9w', NULL, '52.33.72.134', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.7871.46 Safari/537.36', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiTnl0S2VxVFdpNk1lMzZzVDFGZGRubWJuUmhScUlLSm9FR3JBUUhQQyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783260286),
('VmiCWeTkegG9H04j6J2Z38zF42nxALpSvmDDqUl7', NULL, '2a03:b0c0:2:f0:0:1:be0b:5001', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 +https://forestengine.net/#opt-out', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoidGRkdVB5MkU4NzRNUjdyRnRObEZVYWtqR2VMQVhVSUN3MlAwbHB2bCI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783257972),
('VwzK0wU1KumuTu1zy5rZc42IeLTkoZityuMf9L3c', NULL, '216.73.217.148', 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; ClaudeBot/1.0; +claudebot@anthropic.com)', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoiMUR0YnozTUdDQUE1TDM1eFhxbXB0T0k0NkRKdXM5cnMzUzVLMjg2QyI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDE6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tL2xvZ2luIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783256094),
('WtrbW6PY7nLf8U2qkjC3Ips0xht7WHCgdGOacET5', NULL, '34.248.137.227', 'Mozilla/5.0 (X11; Linux x86_64; rv:83.0) Gecko/20100101 Firefox/83.0', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoidWIzMTBCdWszWGdNSmI4aUtUREh3YzQzTHFISGVGM3RucUhDWVJZSiI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6NDU6Imh0dHBzOi8vd3d3LmRldi11cmF0ZXguc21hcnRwb3dlcnBoLmNvbS9sb2dpbiI7fXM6NjoiX2ZsYXNoIjthOjI6e3M6Mzoib2xkIjthOjA6e31zOjM6Im5ldyI7YTowOnt9fX0=', 1783262069),
('ziYfVCu2WN9SP1efeIIb2q0vqAbkMSb2GnOcbfRI', NULL, '178.128.254.234', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 +https://forestengine.net/#opt-out', 'YTozOntzOjY6Il90b2tlbiI7czo0MDoieHVVVE8wWW9CVkJVTzU4T1hmRFloeDlYTG1HODREVmk3TGtDUUZiWSI7czo5OiJfcHJldmlvdXMiO2E6MTp7czozOiJ1cmwiO3M6MzU6Imh0dHBzOi8vZGV2LXVyYXRleC5zbWFydHBvd2VycGguY29tIjt9czo2OiJfZmxhc2giO2E6Mjp7czozOiJvbGQiO2E6MDp7fXM6MzoibmV3IjthOjA6e319fQ==', 1783257968);

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
  `session_token` varchar(255) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Dumping data for table `users`
--

INSERT INTO `users` (`id`, `firstname`, `lastname`, `user_type_id`, `email`, `email_verified_at`, `password`, `remember_token`, `session_token`, `created_at`, `updated_at`, `deleted_at`) VALUES
(1, 'Admin', 'Admin', 1, 'admin@smartpowerph.com', NULL, '$2y$12$Nt3hSGBKJWJl6LOyZ4OWy.3IJR1AldE.TmgvjFa0XyGGjcaKuXLxK', NULL, NULL, '2026-07-05 11:43:47', '2026-07-05 11:43:47', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `user_branches`
--

CREATE TABLE `user_branches` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `user_id` bigint(20) UNSIGNED NOT NULL,
  `branch_id` bigint(20) UNSIGNED NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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
(1, 'Admin', NULL, NULL, NULL, '2026-07-05 11:43:47', '2026-07-05 11:43:47', NULL),
(2, 'User', NULL, NULL, NULL, '2026-07-05 11:43:47', '2026-07-05 11:43:47', NULL);

-- --------------------------------------------------------

--
-- Table structure for table `user_type_locations`
--

CREATE TABLE `user_type_locations` (
  `id` bigint(20) UNSIGNED NOT NULL,
  `user_type_id` bigint(20) UNSIGNED NOT NULL,
  `locations_list` varchar(255) NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  `deleted_at` timestamp NULL DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

--
-- Indexes for dumped tables
--

--
-- Indexes for table `branches`
--
ALTER TABLE `branches`
  ADD PRIMARY KEY (`id`);

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
  ADD PRIMARY KEY (`id`),
  ADD KEY `locations_branch_id_foreign` (`branch_id`);

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
-- Indexes for table `user_branches`
--
ALTER TABLE `user_branches`
  ADD PRIMARY KEY (`id`),
  ADD UNIQUE KEY `user_branches_user_id_branch_id_unique` (`user_id`,`branch_id`),
  ADD KEY `user_branches_branch_id_foreign` (`branch_id`);

--
-- Indexes for table `user_types`
--
ALTER TABLE `user_types`
  ADD PRIMARY KEY (`id`);

--
-- Indexes for table `user_type_locations`
--
ALTER TABLE `user_type_locations`
  ADD PRIMARY KEY (`id`),
  ADD KEY `user_type_locations_user_type_id_foreign` (`user_type_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `branches`
--
ALTER TABLE `branches`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- AUTO_INCREMENT for table `failed_jobs`
--
ALTER TABLE `failed_jobs`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `gateways`
--
ALTER TABLE `gateways`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=18;

--
-- AUTO_INCREMENT for table `jobs`
--
ALTER TABLE `jobs`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `locations`
--
ALTER TABLE `locations`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=21;

--
-- AUTO_INCREMENT for table `migrations`
--
ALTER TABLE `migrations`
  MODIFY `id` int(10) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=21;

--
-- AUTO_INCREMENT for table `sensors`
--
ALTER TABLE `sensors`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=25;

--
-- AUTO_INCREMENT for table `sensor_logs`
--
ALTER TABLE `sensor_logs`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=132;

--
-- AUTO_INCREMENT for table `sensor_models`
--
ALTER TABLE `sensor_models`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- AUTO_INCREMENT for table `sensor_offlines`
--
ALTER TABLE `sensor_offlines`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=25;

--
-- AUTO_INCREMENT for table `sensor_types`
--
ALTER TABLE `sensor_types`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=7;

--
-- AUTO_INCREMENT for table `users`
--
ALTER TABLE `users`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT for table `user_branches`
--
ALTER TABLE `user_branches`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `user_types`
--
ALTER TABLE `user_types`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=3;

--
-- AUTO_INCREMENT for table `user_type_locations`
--
ALTER TABLE `user_type_locations`
  MODIFY `id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `gateways`
--
ALTER TABLE `gateways`
  ADD CONSTRAINT `gateways_location_id_foreign` FOREIGN KEY (`location_id`) REFERENCES `locations` (`id`);

--
-- Constraints for table `locations`
--
ALTER TABLE `locations`
  ADD CONSTRAINT `locations_branch_id_foreign` FOREIGN KEY (`branch_id`) REFERENCES `branches` (`id`);

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

--
-- Constraints for table `user_branches`
--
ALTER TABLE `user_branches`
  ADD CONSTRAINT `user_branches_branch_id_foreign` FOREIGN KEY (`branch_id`) REFERENCES `branches` (`id`) ON DELETE CASCADE,
  ADD CONSTRAINT `user_branches_user_id_foreign` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE;

--
-- Constraints for table `user_type_locations`
--
ALTER TABLE `user_type_locations`
  ADD CONSTRAINT `user_type_locations_user_type_id_foreign` FOREIGN KEY (`user_type_id`) REFERENCES `user_types` (`id`);
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
