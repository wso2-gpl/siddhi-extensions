-- DROP DATABASE IF EXISTS wso2_geo;
-- CREATE DATABASE wso2_geo DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;
-- USE wso2_geo;

-- Create syntax for TABLE 'alerts_history'
CREATE TABLE `alerts_history` (
  `id` varchar (255) not null ,
  `state` varchar (100),
  `timeStamp` varchar (255),
  `information` text,
  `longitude` float,
  `latitude` float,
  primary key (`id`,`timeStamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;

-- Create syntax for TABLE 'webMapService'
CREATE TABLE `webMapService` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `serviceUrl` MEDIUMTEXT NOT NULL,
  `name` VARCHAR (255),
  `layers` VARCHAR (255),
  `version` VARCHAR (255),
  `format` VARCHAR (255),
  KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;

-- Create syntax for TABLE 'tileServers'
CREATE TABLE `tileServers` (
  `id` INT NOT NULL AUTO_INCREMENT ,
  `url` MEDIUMTEXT NOT NULL,
  `name` VARCHAR (255),
  `subdomains` VARCHAR (255),
  `attribution` MEDIUMTEXT ,
  `maxzoom` INT ,
  PRIMARY KEY (`id`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE utf8_unicode_ci;
