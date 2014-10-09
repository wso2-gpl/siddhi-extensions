#!/usr/bin/env bash

# Default values for database variables.
dbhost="localhost"
dbport=3306
dbname="wso2_geo"
dir=$( cd "$( dirname "$0" )" && pwd )

logo() {
	echo "================================================================================================"
	echo "                           W S O 2    G E O    D A T A    S E T U P                        "
	echo "================================================================================================"
}

usage() {
	logo
    echo " Parameters indicates the following information:"
	echo "    -u <user>     User name to access database server."
	echo "    -p <password> User password to access database server."
	echo "    -h <host>     Data Base Server address (default: localhost)."
	echo "    -r <port>     Data Base Server Port (default: 3306)"
	echo "    -n <dbname>  Data Base Name for the geonames.org data (default: wso2_geo)"
	echo "================================================================================================"
    exit -1
}

if [ $# -lt 1 ]; then
	usage
	exit 1
fi

logo

# Parses command line parameters.
while getopts "u:p:h:r:n:" opt;
do
    case $opt in
        u) dbusername=$OPTARG ;;
        p) dbpassword=$OPTARG ;;
        h) dbhost=$OPTARG ;;
        r) dbport=$OPTARG ;;
        n) dbname=$OPTARG ;;
    esac
done

if [ -z $dbusername ]; then
    echo "No user name provided for accessing the database. Please write some value in parameter -u..."
    exit 1
fi

if [ -z $dbpassword ]; then
    echo "No user password provided for accessing the database. Please write some value in parameter -p..."
    exit 1
fi

echo "Database parameters being used..."
echo "UserName: " $dbusername
echo "Password: " $dbpassword
echo "DB Host: " $dbhost
echo "DB Port: " $dbport
echo "DB Name: " $dbname


echo "Creating database $dbname..."
mysql -h $dbhost -P $dbport -u $dbusername -p$dbpassword -Bse "DROP DATABASE IF EXISTS $dbname;"
mysql -h $dbhost -P $dbport -u $dbusername -p$dbpassword -Bse "CREATE DATABASE $dbname DEFAULT CHARACTER SET utf8;"

echo "Creating tables for database $dbname..."
mysql -h $dbhost -P $dbport -u $dbusername -p$dbpassword -Bse "USE $dbname;"
mysql -h $dbhost -P $dbport -u $dbusername -p$dbpassword $dbname < $dir/geo_dashboard_db_struct.sql


if [ $? == 0 ]; then 
	echo "[OK]"
else
	echo "[FAILED]"
fi

exit 0
