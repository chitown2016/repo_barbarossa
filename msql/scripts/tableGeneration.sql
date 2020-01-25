#CREATE DATABASE futures_master;
#USE securities_master;

CREATE TABLE `exchange` (
  `id` int NOT NULL AUTO_INCREMENT,
  `abbrev` varchar(32) NOT NULL,
  `name` varchar(255) NOT NULL,
  `city` varchar(255) NULL,
  `country` varchar(255) NULL,
  `currency` varchar(64) NULL,
  `timezone_offset` time NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=UTF8MB4;

CREATE TABLE `data_vendor` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(64) NOT NULL,
  `website_url` varchar(255) NULL,
  `support_email` varchar(255) NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=UTF8MB4;

CREATE TABLE `symbol` (
  `id` int NOT NULL AUTO_INCREMENT,
  `exchange_id` int NULL,
  `ticker` varchar(32) NOT NULL,
  `ticker_head` varchar(32) NOT NULL,
  `ticker_year` int NOT NULL,
  `ticker_month` int NOT NULL,
  `expiration_date` date NOT NULL,
  `instrument` varchar(64) NOT NULL,
  `name` varchar(255) NULL,
  `ticker_class` varchar(255) NULL,
  `currency` varchar(32) NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `index_exchange_id` (`exchange_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=UTF8MB4;

ALTER TABLE `symbol`
ADD UNIQUE INDEX `unique_ticker` (`ticker`,`instrument`);

CREATE TABLE `daily_price` (
  `id` int NOT NULL AUTO_INCREMENT,
  `data_vendor_id` int NOT NULL,
  `ticker_head` varchar(32) NOT NULL,
  `ticker_month` int NOT NULL,
  `symbol_id` int NOT NULL,
  `price_date` datetime NOT NULL,
  `cal_dte` int NOT NULL,
  `tr_dte` int NOT NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  `open_price` decimal(19,8) NULL,
  `high_price` decimal(19,8) NULL,
  `low_price` decimal(19,8) NULL,
  `close_price` decimal(19,8) NULL,
  `volume` bigint NULL,
  `open_interest` bigint NULL,
  PRIMARY KEY (`id`),
  KEY `index_data_vendor_id` (`data_vendor_id`),
  KEY `index_synbol_id` (`symbol_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=UTF8MB4;


ALTER TABLE `daily_price`
ADD UNIQUE INDEX `ticker_day` (`symbol_id`,`price_date`);


CREATE TABLE `daily_option_price` (
  `id` int NOT NULL AUTO_INCREMENT,
  `data_vendor_id` int NOT NULL,
  `ticker_head` varchar(32) NOT NULL,
  `ticker_month` int NOT NULL,
  `ticker_year` int NOT NULL,
  `ticker` varchar(32) NOT NULL,
  `option_type` varchar(32) NOT NULL,
  `strike` decimal(19,4) NULL,
  `price_date` datetime NOT NULL,
  `cal_dte` int NOT NULL,
  `tr_dte` int NOT NULL,
  `created_date` datetime NOT NULL,
  `last_updated_date` datetime NOT NULL,
  `close_price` decimal(15,8) NULL,
  `imp_vol` decimal(15,8) NULL,
  `delta` decimal(15,8) NULL,
  `vega` decimal(15,8) NULL,
  `gamma` decimal(15,8) NULL,
  `theta` decimal(15,8) NULL,
  `close_price_clean` decimal(15,8) NULL,
  `imp_vol_clean` decimal(15,8) NULL,
  `delta_clean` decimal(15,8) NULL,
  `vega_clean` decimal(15,8) NULL,
  `gamma_clean` decimal(15,8) NULL,
  `theta_clean` decimal(15,8) NULL,
  `option_pnl` decimal(15,8) NULL,
  `delta_pnl` decimal(15,8) NULL,
  `vega_pnl` decimal(15,8) NULL,
  `gamma_pnl` decimal(15,8) NULL,
  `theta_pnl` decimal(15,8) NULL,
  `option_pnl5` decimal(15,8) NULL,
  `delta_pnl5` decimal(15,8) NULL,
  `vega_pnl5` decimal(15,8) NULL,
  `gamma_pnl5` decimal(15,8) NULL,
  `theta_pnl5` decimal(15,8) NULL,
  `option_pnl10` decimal(15,8) NULL,
  `delta_pnl10` decimal(15,8) NULL,
  `vega_pnl10` decimal(15,8) NULL,
  `gamma_pnl10` decimal(15,8) NULL,
  `theta_pnl10` decimal(15,8) NULL,
  `option_pnl20` decimal(15,8) NULL,
  `delta_pnl20` decimal(15,8) NULL,
  `vega_pnl20` decimal(15,8) NULL,
  `gamma_pnl20` decimal(15,8) NULL,
  `theta_pnl20` decimal(15,8) NULL,
  `volume` bigint NULL,
  `open_interest` bigint NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ticker_day` (`ticker`,`option_type`,`strike`,`price_date`),
  KEY `index_data_vendor_id` (`data_vendor_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=UTF8MB4;


CREATE TABLE `option_ticker_indicators`(
`id` int NOT NULL AUTO_INCREMENT,
`ticker` varchar(32) NOT NULL,
`ticker_head` varchar(32) NOT NULL,
`ticker_month` int NOT NULL,
`ticker_year` int NOT NULL,
`cal_dte` int NULL,
`tr_dte` int NULL,
`imp_vol` decimal(7,3) NULL,
`delta` decimal(5,3) NULL,
`strike` decimal(19,4) NULL,
`theta` decimal(15,8) NULL,
`close2close_vol20` decimal(6,3) NULL,
`option_pnl5` decimal(15,8) NULL,
`delta_pnl5` decimal(15,8) NULL,
`aux_ind1` decimal(5,3) NULL,
`aux_ind2` decimal(5,3) NULL,
`aux_ind3` decimal(5,3) NULL,
`aux_ind4` decimal(5,3) NULL,
`aux_ind5` decimal(5,3) NULL,
`volume` bigint NULL,
`open_interest` bigint NULL,
`price_date` datetime NOT NULL,
`created_date` datetime NOT NULL,
`last_updated_date` datetime NOT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY `ticker_day` (`ticker`,`price_date`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=UTF8MB4;




CREATE TABLE `strategy` (
`id` int NOT NULL AUTO_INCREMENT,
`alias` varchar(255) NOT NULL,
`open_date` datetime NOT NULL,
`close_date` datetime NOT NULL,
`pnl` decimal(19,4) NULL,
`created_date` datetime NOT NULL,
`last_updated_date` datetime NOT NULL,
`description_string` varchar(1000) NOT NULL,
PRIMARY KEY (`id`),
UNIQUE KEY (`alias`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=UTF8MB4;




CREATE TABLE `trades` (
`id` int NOT NULL AUTO_INCREMENT,
`ticker` varchar(32) NOT NULL,
`option_type` varchar(32) NULL,
`strike_price` decimal(19,4) NULL,
`strategy_id` int NOT NULL,
`trade_price` decimal(19,4) NULL,
`trade_quantity` decimal(19,2) NULL,
`trade_date` datetime NOT NULL,
`instrument` varchar(64) NOT NULL,
`real_tradeQ` tinyint NOT NULL,
`created_date` datetime NOT NULL,
`last_updated_date` datetime NOT NULL,
PRIMARY KEY (`id`),
KEY `index_strategy_id` (`strategy_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=UTF8MB4;











