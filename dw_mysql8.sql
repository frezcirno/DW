ALTER TABLE `Actin` DROP FOREIGN KEY `fk_Direct_Movie_1_copy_1`;
ALTER TABLE `Actin` DROP FOREIGN KEY `fk_Direct_Person_1_copy_1`;
ALTER TABLE `Direct` DROP FOREIGN KEY `fk_Direct_Movie_1`;
ALTER TABLE `Direct` DROP FOREIGN KEY `fk_Direct_Person_1`;
ALTER TABLE `Movie` DROP FOREIGN KEY `fk_Movie_Genres_1`;
ALTER TABLE `Review` DROP FOREIGN KEY `fk_Review_Movie_1`;
ALTER TABLE `Review` DROP FOREIGN KEY `fk_Review_User_1`;
ALTER TABLE `Star` DROP FOREIGN KEY `fk_Direct_Movie_1_copy_2`;
ALTER TABLE `Star` DROP FOREIGN KEY `fk_Direct_Person_1_copy_2`;

DROP TABLE `Act`;
DROP TABLE `Actin`;
DROP TABLE `Direct`;
DROP TABLE `Genres`;
DROP TABLE `Movie`;
DROP TABLE `Person`;
DROP TABLE `Review`;
DROP TABLE `Star`;
DROP TABLE `table_1`;
DROP TABLE `User`;

CREATE TABLE `Act`  ();

CREATE TABLE `Actin`  (
  `asin` varchar(255) NOT NULL COMMENT '电影id',
  `pid` int NOT NULL COMMENT '人id',
  PRIMARY KEY (`asin`, `pid`)
) COMMENT = '出演';

CREATE TABLE `Direct`  (
  `asin` varchar(255) NOT NULL COMMENT '电影id',
  `pid` int NOT NULL COMMENT '人id',
  PRIMARY KEY (`asin`, `pid`)
);

CREATE TABLE `Genres`  (
  `id` int NOT NULL,
  `name` varchar(255) NULL COMMENT '题材名',
  PRIMARY KEY (`id`)
);

CREATE TABLE `Movie`  (
  `asin` varchar(255) NOT NULL COMMENT 'ASIN码',
  `title` varchar(255) NULL COMMENT '电影名',
  `score` float NULL COMMENT '打分',
  `release_date` datetime NULL COMMENT '上映时间',
  `genre_id` int NULL COMMENT '题材id',
  PRIMARY KEY (`asin`)
);

CREATE TABLE `Person`  (
  `id` int NOT NULL,
  `name` varchar(255) NULL COMMENT '人名',
  PRIMARY KEY (`id`)
) COMMENT = '导演或演员';

CREATE TABLE `Review`  (
  `id` int NOT NULL,
  `asin` varchar(255) NULL COMMENT '电影',
  `uid` varchar(255) NULL COMMENT '用户id',
  `helpfulness` int NULL,
  `time` datetime NULL,
  `text` varchar(255) NULL,
  `summary` varchar(255) NULL,
  PRIMARY KEY (`id`)
);

CREATE TABLE `Star`  (
  `asin` varchar(255) NOT NULL COMMENT '电影id',
  `pid` int NOT NULL COMMENT '人id',
  PRIMARY KEY (`asin`, `pid`)
) COMMENT = '主演';

CREATE TABLE `table_1`  ();

CREATE TABLE `User`  (
  `id` varchar(255) NOT NULL,
  `profile_name` varchar(255) NULL,
  PRIMARY KEY (`id`)
) COMMENT = '看电影的人';

ALTER TABLE `Actin` ADD CONSTRAINT `fk_Direct_Movie_1_copy_1` FOREIGN KEY (`asin`) REFERENCES `Movie` (`asin`);
ALTER TABLE `Actin` ADD CONSTRAINT `fk_Direct_Person_1_copy_1` FOREIGN KEY (`pid`) REFERENCES `Person` (`id`);
ALTER TABLE `Direct` ADD CONSTRAINT `fk_Direct_Movie_1` FOREIGN KEY (`asin`) REFERENCES `Movie` (`asin`);
ALTER TABLE `Direct` ADD CONSTRAINT `fk_Direct_Person_1` FOREIGN KEY (`pid`) REFERENCES `Person` (`id`);
ALTER TABLE `Movie` ADD CONSTRAINT `fk_Movie_Genres_1` FOREIGN KEY (`genre_id`) REFERENCES `Genres` (`id`);
ALTER TABLE `Review` ADD CONSTRAINT `fk_Review_Movie_1` FOREIGN KEY (`asin`) REFERENCES `Movie` (`asin`);
ALTER TABLE `Review` ADD CONSTRAINT `fk_Review_User_1` FOREIGN KEY (`id`) REFERENCES `User` (`id`);
ALTER TABLE `Star` ADD CONSTRAINT `fk_Direct_Movie_1_copy_2` FOREIGN KEY (`asin`) REFERENCES `Movie` (`asin`);
ALTER TABLE `Star` ADD CONSTRAINT `fk_Direct_Person_1_copy_2` FOREIGN KEY (`pid`) REFERENCES `Person` (`id`);

