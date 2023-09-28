# 运行方式
## 克隆项目
```
git clone https://github.com/Raopend/movies_analysis.git
```
## 安装 sbt
sbt 官网安装指南：https://www.scala-sbt.org/download.html
## 新建数据库 movies，并运行 sql 脚本
```
create_table.sql
USE movies;
CREATE TABLE `ten_movies_averagerating` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `movieId` int(11) NOT NULL COMMENT '电影id',
  `title` varchar(100) NOT NULL COMMENT '电影名称',
  `avgRating` decimal(10,2) NOT NULL COMMENT '平均评分',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `movie_id_UNIQUE` (`movieId`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;


-- -------------------------------------------------------------------------------

CREATE TABLE genres_average_rating (
    `id` INT ( 11 ) NOT NULL AUTO_INCREMENT COMMENT '自增id',
    `genres` VARCHAR ( 100 ) NOT NULL COMMENT '电影类别',
    `avgRating` DECIMAL ( 10, 2 ) NOT NULL COMMENT '电影类别平均评分',
    `update_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
PRIMARY KEY ( `id` ),
UNIQUE KEY `genres_UNIQUE` ( `genres` )
) ENGINE = INNODB DEFAULT CHARSET = utf8;

-- ------------------------------------------------------------------------------


CREATE TABLE ten_most_rated_films (
    `id` INT ( 11 ) NOT NULL AUTO_INCREMENT COMMENT '自增id',
    `movieId` INT ( 11 ) NOT NULL COMMENT '电影Id',
    `title` varchar(100) NOT NULL COMMENT '电影名称',
    `ratingCnt` INT(11) NOT NULL COMMENT '电影被评分的次数',
    `update_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
PRIMARY KEY ( `id` ),
UNIQUE KEY `movie_id_UNIQUE` ( `movieId` )
) ENGINE = INNODB DEFAULT CHARSET = utf8;

-- ------------------------------------------------------------------------------
```
## 修改数据库配置和数据文件路径
```
DemoMainApp.scala
// 文件路径
private val MOVIES_CSV_FILE_PATH = "data/movies.csv"
private val RATINGS_CSV_FILE_PATH = "data/ratings.csv"
```
```
JDBCUtil.scala
val dataSource = new ComboPooledDataSource()

val user = "root"
val password = "123456"
val url = "jdbc:mysql://localhost:3306/movies"

dataSource.setUser(user)
dataSource.setPassword(password)
dataSource.setDriverClass("com.mysql.jdbc.Driver")
dataSource.setJdbcUrl(url)
dataSource.setAutoCommitOnClose(false)
```
## 运行
```
sbt run
```