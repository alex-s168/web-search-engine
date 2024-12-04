if [ -z $folder ]; then
	echo "you need to set \$folder to a valid s3 dir in the bucket"
fi

clickhouse-client "BACKUP DATABASE pages TO S3('https://search-engine-backups.s3.eu-north-1.amazonaws.com/$folder/', 'AKIAZAI4HA2RMRR3VVPY', 'D7KcKk52Xom5EJjzOe9Hl/HUI82mOR/1EvuRH+tw')
"
