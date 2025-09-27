# !/bin/bash

filename="pages.txt"

while read -r line; do
    min=("$(echo "$line" | cut -d',' -f1)")
    max=("$(echo "$line" | cut -d',' -f2)")
    echo "Scraping pages from $min to $max"
    # python3 scrape_waltermart_shop.py "-p $min-$max" "-o waltermart_products_$min-$max.csv" &
    # python3 scrape_waltermart_shop.py --debug --stagnant-limit 6 --max-dom-nodes 1500 "-p $min-$max" "-o waltermart_products_$min-$max.csv" &
    python3 scrape_waltermart_shop.py --fresh-session "-p $min-$max" "-o waltermart_products_$min-$max.csv" &
done < "$filename"

# wait until all background processes are done
echo "Waiting for all scraping processes to finish..."
wait

for file in $(ls | grep waltermart_products_ | awk '{print $9}'); do
    echo "Deduplicating $file"
    python3 dedup_waltermart_csv.py "$file" "waltermart_products_deduped_$file"
    mv "$file" ../data/
done

echo "All done!"
