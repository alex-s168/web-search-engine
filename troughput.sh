echo ""
while true; do
	t=20
	old=$(clickhouse-client "SELECT COUNT(*) FROM pages.scraped")
	sleep $t
	new=$(clickhouse-client "SELECT COUNT(*) FROM pages.scraped")
	num=$(bc <<< "scale=2; ($new - $old)/$t")
	printf "\r$num pages per second"
done
