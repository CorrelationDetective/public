wait=0.6
dir=raw_$1
if [ ! -d $dir ]; then mkdir raw_$1; fi
while IFS=',' read -ra line; do
    for id in "${line[@]}"; do
	file=raw_$1/$id.gz
	if  [ ! -f $file ]; then
	        curl "https://api.coingecko.com/api/v3/coins/$id/market_chart?vs_currency=usd&days=90" | gzip --stdout > $file
	        sleep $wait
	else
		echo "$file already exists"
	fi
    done
done < coingecko_coinids_$1.csv