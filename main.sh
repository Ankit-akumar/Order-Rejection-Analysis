#!/usr/bin/env bash

# Clear files
> errorOrder.json
> executeBulkRequest.json
> ids.txt

# Filter known errors
grep -v -e 'Zone location not found for' -e 'is not serviceable' -e 'InProgress Task Found' errorOrder.csv > errorOrder.json

# Get all the IDs from error logs
awk -F'generic,' '{split($2, a, ","); print a[1]}' errorOrder.json | sort -u > ids.txt

# Grep all error IDs from bulk request
grep -Fwf ids.txt executeBulkRequest.csv > executeBulkRequest.json

# Creating json objects from bulk requests
awk '{
  match($0, /\{data=.*?, validations/);
  if (RSTART > 0) {
    s = substr($0, RSTART+6, RLENGTH-19);
    print s
  }
}' executeBulkRequest.json > tmpfile_raw

sed 's/""/"/g; s/\\u0027/'"'"'/g' tmpfile_raw > tmpfile_clean

{ echo "["; paste -sd, tmpfile_clean; echo "]"; } > executeBulkRequest.json


##############

echo "Checking for missing values in JSON..."
grep -oP '"[^"]+":\s*}' executeBulkRequest.json | while read -r line; do
  key=$(echo "$line" | grep -oP '"[^"]+"')
  echo "Missing value for key: $key — replacing with null"
done

sed -i 's/\("[^"]*"\):[[:space:]]*}/\1: "null_ptr"}/g' executeBulkRequest.json

###############

rm tmpfile_raw tmpfile_clean ids.txt

#jq . executeBulkRequest.json

# Creating list of all external ids
errorOrder_array=$(jq -r '.[].externalServiceRequestId' executeBulkRequest.json | paste -sd, - | sed "s/^/ARRAY['/; s/,/','/g; s/\$/']/")
#echo -e "$errorOrder_array"

# Get missing orders in SRMS from the external ids array
readarray -t missing_orders < <(PGPASSWORD=1f23c9fe0381aef1 psql -h 10.170.247.9 -U postgres -d platform_srms -t -A -c "SELECT missing_id FROM unnest(array[($errorOrder_array)]) AS missing_id LEFT JOIN service_request sr ON sr.external_service_request_id = missing_id WHERE sr.id IS NULL;")

# echo "Missing IDs:"
# for id in "${missing_orders[@]}"; do
#   echo "$id"
# done

# Collecting data of the missing orders from bulk request json objects
missing_orders_data_array=()

for id in "${missing_orders[@]}"; do
  result=$(jq -c --arg id "$id" '
    .[]
    | select(.externalServiceRequestId == $id)
    | if .type == "PICK" then
        {
          externalServiceRequestId,
          type,
          pallet_number: (.attributes.pallet_number // .attributes.order_options.pallet_number),
          fullAddresses: [ .serviceRequests[].attributes.location.fullAddress ],
          product_skus: (
            [ .serviceRequests[].expectations.containers[].products[].productAttributes.filter_parameters[] ]
            | map(
                sub("product_sku *= *'"'"'"; "") 
                | sub("'"'"'"; "")
              )
          )
        }
      else
        {
          externalServiceRequestId,
          type,
          origin: .attributes.origin,
          destination: .attributes.destination
        }
      end
  ' executeBulkRequest.json)

  if [[ -n "$result" ]]; then
    missing_orders_data_array+=("$result")
  fi
done

missing_orders_data_array=($(printf "%s\n" "${missing_orders_data_array[@]}" | sort -u))

#printf "%s\n" "${missing_orders_data_array[@]}" | jq -s .



echo -e "\nVerifying skus in pick requests..."

for json in "${missing_orders_data_array[@]}"; do
 id=$(echo "$json" | jq -r '.externalServiceRequestId')

 skus=$(echo "$json" | jq -r 'select(.product_skus) | .product_skus[]')

 if [[ -n "$skus" ]]; then
  # Convert to single-quoted, comma-separated list
  sku_list=()
  while IFS= read -r sku; do
    sku_list+=("'$sku'")
  done <<< "$skus"

  sql_sku_list=$(IFS=,; echo "${sku_list[*]}")

  # Get missing sku
  readarray -t missing_sku < <(PGPASSWORD=1f23c9fe0381aef1 psql -h 10.170.247.9 -U postgres -d wms_masterdata -t -A -c "select sku as missing_sku from unnest(array[${sql_sku_list}]) as sku(sku) left join item i on i.productattributes->>'product_sku' = sku where i.id is null;")
    if (( ${#missing_sku[@]} != 0 )); then
    t=$(echo "$json" | jq -r '.type')
    pn=$(echo "$json" | jq -r '.pallet_number')
    p_id=$(echo "$(grep "$id" executeBulkRequest.csv)" | awk -F'generic,' '{split($2,a,","); print a[1]}')
    error_log=$(grep "$p_id" errorOrder.json)

    echo -e "\nMissing SKU in External Service Request ID: $id, type=$t, pallet_number=$pn"
    for i in "${missing_sku[@]}"; do
        echo "$i"
    done
    echo "$error_log"
    fi
 fi
done


echo -e "\nGetting Pallet height exceed rejections..."


mapfile -t process_ids < <(awk -F 'generic,' '/Pallet height exceeds the maximum limit/ {split($2,a,","); print a[1]}' errorOrder.json)

if [ ${#process_ids[@]} -eq 0 ]; then
  echo "No pallet was rejected due to height exceed limit error"
else
  for process_id in "${process_ids[@]}"; do
    
    request=$(grep -F "$process_id" executeBulkRequest.csv)
    
    if [ -n "$request" ]; then
      ext_id=$(echo "$request" | awk -F'externalServiceRequestId"": ""' '{split($2, uuid, "\""); print uuid[1]}')
      
      if [ -n "$ext_id" ]; then
        request_attributes=$(jq -c --arg ext_id "$ext_id" '
        .[]
        | select(.externalServiceRequestId == $ext_id)
        |
          {
            type,
            externalServiceRequestId,
            pallet_number: (.attributes.pallet_number // .attributes.order_options.pallet_number)
          }
        ' executeBulkRequest.json)
        printf "%s\n" "$request_attributes"
	echo "$(grep -F "$process_id" errorOrder.json)"
      else
        echo "Warning: Could not extract externalServiceRequestId from line: $request"
      fi
    else
      echo "Warning: Could not find the bulk request for process ID: $process_id"
    fi
  done
fi
