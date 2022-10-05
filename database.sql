SET NAMES utf8;
SET time_zone = '+00:00';
SET foreign_key_checks = 0;
SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';

SET NAMES utf8mb4;

DROP TABLE IF EXISTS `bot_settings`;
CREATE TABLE `bot_settings` (
  `name` varchar(64) NOT NULL,
  `value` text DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO `bot_settings` (`name`, `value`) VALUES
('min_gas_move_nft',	'{\"ETH\": 0.005, \"MATIC\": 0.02}'),
('eth_gas_tracker',	'{\"LastBlock\": \"15679631\", \"SafeGasPrice\": \"7\", \"ProposeGasPrice\": \"8\", \"FastGasPrice\": \"10\", \"suggestBaseFee\": \"6.010468023\", \"gasUsedRatio\": \"0.0551893666666667,0.9562653,0.329520533333333,0.2400164,0.9397873\", \"last_update\": 1664945449}'),
('matic_gas_tracker',	'{\"safeLow\": 32.5, \"standard\": 50, \"fast\": 50.5, \"fastest\": 50.5, \"blockTime\": 1, \"blockNumber\": 33948507, \"last_update\": 1664945449}');

DROP TABLE IF EXISTS `metamask_v1`;
CREATE TABLE `metamask_v1` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `address` varchar(42) NOT NULL,
  `publicName` tinytext DEFAULT NULL,
  `discord_id` tinytext DEFAULT NULL,
  `discord_name` tinytext DEFAULT NULL,
  `discord_verified_date` int(11) DEFAULT NULL,
  `verified_by_key` tinyint(4) NOT NULL DEFAULT 0,
  `secret_key` tinytext NOT NULL,
  `user_server` varchar(32) NOT NULL DEFAULT 'DISCORD',
  `nonce` tinytext DEFAULT NULL,
  `is_signed` tinyint(4) NOT NULL DEFAULT 0,
  `created` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`ID`),
  UNIQUE KEY `address` (`address`),
  UNIQUE KEY `secret_key` (`secret_key`) USING HASH
) ENGINE=MyISAM DEFAULT CHARSET=latin1;


DROP TABLE IF EXISTS `nft_credit`;
CREATE TABLE `nft_credit` (
  `nft_id` int(11) NOT NULL AUTO_INCREMENT,
  `network` varchar(16) NOT NULL,
  `token_address` varchar(42) CHARACTER SET ascii NOT NULL,
  `token_id_int` bigint(20) DEFAULT NULL,
  `token_id_hex` varchar(256) CHARACTER SET ascii NOT NULL,
  `credited_user_id` varchar(32) CHARACTER SET ascii NOT NULL,
  `credited_user_server` varchar(32) CHARACTER SET ascii NOT NULL,
  `amount` int(11) NOT NULL,
  `is_frozen` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`nft_id`),
  UNIQUE KEY `token_address_token_id_hex_credited_user_id_network` (`token_address`,`token_id_hex`,`credited_user_id`,`network`),
  KEY `token_id_hex` (`token_id_hex`),
  KEY `is_frozen` (`is_frozen`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `nft_erc1155_list`;
CREATE TABLE `nft_erc1155_list` (
  `vtk_id` int(11) NOT NULL AUTO_INCREMENT,
  `network` varchar(16) NOT NULL,
  `name` tinytext NOT NULL,
  `token_address` varchar(42) NOT NULL,
  `token_id_hex` varchar(256) NOT NULL,
  `token_uri` text NOT NULL,
  `metadata` text DEFAULT NULL,
  `thumb_url` tinytext DEFAULT NULL,
  `is_verified` tinyint(4) NOT NULL DEFAULT 0,
  `inserted_date` int(11) NOT NULL,
  `check_later_date` int(11) NOT NULL,
  `verified_date` int(11) DEFAULT NULL,
  `verified_by_uid` int(11) DEFAULT NULL,
  PRIMARY KEY (`vtk_id`),
  UNIQUE KEY `token_address_token_id_hex` (`token_address`,`token_id_hex`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `nft_info_contract`;
CREATE TABLE `nft_info_contract` (
  `contract_id` int(11) NOT NULL AUTO_INCREMENT,
  `collection_name` varchar(128) NOT NULL,
  `alias_name` varchar(128) DEFAULT NULL,
  `desc` text NOT NULL,
  `market_link` text DEFAULT NULL,
  `url` text DEFAULT NULL,
  `explorer_link` text DEFAULT NULL,
  `contract_height` int(11) NOT NULL,
  `contract` varchar(128) NOT NULL,
  `contract_extra` varchar(128) DEFAULT NULL,
  `contract_type` varchar(16) NOT NULL DEFAULT 'ERC721',
  `collection_thumb_url` varchar(512) DEFAULT NULL,
  `network` varchar(16) NOT NULL DEFAULT 'ETHEREUM',
  `need_verify_token_id` tinyint(4) NOT NULL DEFAULT 0,
  `is_enable` tinyint(4) NOT NULL DEFAULT 0,
  `enable_rarity` tinyint(4) NOT NULL,
  PRIMARY KEY (`contract_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `nft_item_list`;
CREATE TABLE `nft_item_list` (
  `item_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL,
  `contract` varchar(128) NOT NULL,
  `contract_extra` varchar(128) DEFAULT NULL,
  `network` varchar(32) NOT NULL DEFAULT 'ETHEREUM',
  `asset_id` varchar(32) DEFAULT NULL,
  `asset_id_hex` varchar(128) NOT NULL,
  `meta` text DEFAULT NULL,
  `meta_url` varchar(512) DEFAULT NULL,
  `thumb_url` varchar(512) NOT NULL,
  PRIMARY KEY (`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `nft_main_wallet_nft_tx`;
CREATE TABLE `nft_main_wallet_nft_tx` (
  `nft_tx_id` int(11) NOT NULL AUTO_INCREMENT,
  `network` varchar(32) NOT NULL,
  `block_number` int(11) NOT NULL,
  `block_timestamp` varchar(32) NOT NULL,
  `block_time` int(11) NOT NULL,
  `block_hash` varchar(70) NOT NULL,
  `transaction_hash` varchar(70) NOT NULL,
  `transaction_index` smallint(6) DEFAULT NULL,
  `log_index` smallint(6) NOT NULL,
  `value` varchar(32) NOT NULL,
  `contract_type` varchar(16) NOT NULL,
  `transaction_type` varchar(16) NOT NULL,
  `token_address` varchar(42) NOT NULL,
  `token_id_int` bigint(20) DEFAULT NULL,
  `token_id_hex` varchar(256) NOT NULL,
  `from_address` varchar(42) NOT NULL,
  `to_address` varchar(42) NOT NULL,
  `amount` bigint(20) NOT NULL,
  `verified` tinyint(4) DEFAULT NULL,
  `inserted_credited` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`nft_tx_id`),
  UNIQUE KEY `transaction_hash_log_index` (`transaction_hash`,`log_index`),
  KEY `token_address` (`token_address`),
  KEY `from_address` (`from_address`),
  KEY `to_address` (`to_address`)
) ENGINE=InnoDB DEFAULT CHARSET=ascii;


DROP TABLE IF EXISTS `nft_main_wallet_tx`;
CREATE TABLE `nft_main_wallet_tx` (
  `tx_id` int(11) NOT NULL AUTO_INCREMENT,
  `network` varchar(32) NOT NULL,
  `hash` varchar(70) NOT NULL,
  `nonce` int(11) NOT NULL,
  `transaction_index` smallint(6) NOT NULL,
  `from_address` varchar(42) NOT NULL,
  `to_address` varchar(42) NOT NULL,
  `value` bigint(20) NOT NULL,
  `gas` bigint(20) NOT NULL,
  `gas_price` bigint(20) NOT NULL,
  `input` text DEFAULT NULL,
  `receipt_cumulative_gas_used` bigint(20) NOT NULL,
  `receipt_gas_used` bigint(20) NOT NULL,
  `receipt_status` tinyint(4) NOT NULL,
  `block_timestamp` varchar(32) NOT NULL,
  `block_time` int(11) NOT NULL,
  `block_number` int(11) NOT NULL,
  `block_hash` varchar(70) NOT NULL,
  `is_credited` tinyint(4) NOT NULL DEFAULT 0,
  `credited_user_id` varchar(32) DEFAULT NULL,
  `credited_user_server` varchar(32) DEFAULT NULL,
  `credited_time` int(11) DEFAULT NULL,
  PRIMARY KEY (`tx_id`),
  UNIQUE KEY `hash_transaction_index` (`hash`,`transaction_index`),
  KEY `from_address` (`from_address`),
  KEY `to_address` (`to_address`),
  KEY `is_credited` (`is_credited`)
) ENGINE=InnoDB DEFAULT CHARSET=ascii;


DROP TABLE IF EXISTS `nft_rarity`;
CREATE TABLE `nft_rarity` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `nft_info_contract_id` int(11) NOT NULL,
  `contract` varchar(42) NOT NULL,
  `collection_name` varchar(128) NOT NULL,
  `rarity_json` longtext NOT NULL,
  `update_date` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `contract` (`contract`),
  KEY `nft_info_contract_id` (`nft_info_contract_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `nft_rarity_items`;
CREATE TABLE `nft_rarity_items` (
  `rarity_item_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `nft_info_contract_id` int(11) NOT NULL,
  `nft_item_list_id` int(11) NOT NULL,
  `name` varchar(512) DEFAULT NULL,
  `attributes_dump` text NOT NULL,
  `missing_traits_dump` text NOT NULL,
  `trait_count_dump` text NOT NULL,
  `trait_count` int(11) NOT NULL,
  `rarity_score` float NOT NULL,
  `rank` int(11) NOT NULL,
  `updated_date` int(11) NOT NULL,
  PRIMARY KEY (`rarity_item_id`),
  KEY `nft_info_contract_id` (`nft_info_contract_id`),
  KEY `nft_item_list_id` (`nft_item_list_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `nft_tip_logs`;
CREATE TABLE `nft_tip_logs` (
  `tip_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `from_user_id` varchar(32) NOT NULL,
  `from_user_server` varchar(32) NOT NULL,
  `to_user_id` varchar(32) NOT NULL,
  `to_user_server` varchar(32) NOT NULL,
  `date` int(11) NOT NULL,
  `nft_info_contract_id` int(11) NOT NULL,
  `token_id_int` bigint(20) DEFAULT NULL,
  `token_id_hex` varchar(256) NOT NULL,
  `amount` int(11) NOT NULL,
  PRIMARY KEY (`tip_id`),
  KEY `from_user_id` (`from_user_id`),
  KEY `to_user_id` (`to_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `nft_withdraw`;
CREATE TABLE `nft_withdraw` (
  `withdraw_id` int(11) NOT NULL AUTO_INCREMENT,
  `network` varchar(32) NOT NULL,
  `token_address` varchar(42) NOT NULL,
  `token_id_hex` varchar(256) NOT NULL,
  `amount` int(11) NOT NULL DEFAULT 1,
  `user_id` varchar(32) NOT NULL,
  `user_server` varchar(32) NOT NULL,
  `withdrew_to` varchar(42) NOT NULL,
  `withdrew_tx` varchar(70) NOT NULL,
  `withdrew_status` enum('PENDING','CONFIRMED','FAILED') DEFAULT NULL,
  `withdrew_gas_price` bigint(20) DEFAULT NULL,
  `withdrew_gas_used` bigint(20) DEFAULT NULL,
  `withdrew_tx_real_fee` float DEFAULT NULL,
  `withdrew_date` int(11) NOT NULL,
  PRIMARY KEY (`withdraw_id`),
  KEY `user_id` (`user_id`),
  KEY `user_server` (`user_server`),
  KEY `withdrew_status` (`withdrew_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


DROP TABLE IF EXISTS `tbl_users`;
CREATE TABLE `tbl_users` (
  `inserted_date` int(11) NOT NULL,
  `user_id` varchar(32) NOT NULL,
  `user_server` varchar(32) NOT NULL,
  `total_tipped` int(11) NOT NULL DEFAULT 0,
  `total_received` int(11) NOT NULL DEFAULT 0,
  `is_frozen` tinyint(4) NOT NULL DEFAULT 0,
  `eth_gas` float NOT NULL DEFAULT 0,
  `matic_gas` float NOT NULL DEFAULT 0,
  `eth_nft_deposited` int(11) NOT NULL DEFAULT 0,
  `matic_nft_deposited` int(11) NOT NULL DEFAULT 0,
  `eth_nft_withdrew` int(11) NOT NULL DEFAULT 0,
  `matic_nft_withdrew` int(11) NOT NULL DEFAULT 0,
  `is_first_time` tinyint(4) NOT NULL DEFAULT 1,
  `command_called` int(11) NOT NULL DEFAULT 0,
  UNIQUE KEY `user_id_user_server` (`user_id`,`user_server`),
  KEY `user_id` (`user_id`),
  KEY `user_server` (`user_server`),
  KEY `is_frozen` (`is_frozen`),
  KEY `is_first_time` (`is_first_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
