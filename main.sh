#!/usr/bin/env bash

# Clear files
> executeBulkRequest.json
bulk_requests_file="Bulk requests.csv"
error_orders_file="Error Requests.csv"

# Write to output csv
echo "Order type,Order id,Received_on (utc),Pallet number,Rejection Category,Logs" > output.csv
function write_to_csv () {
  local json="$1"
  local id=$(echo "$json" | jq -r '.externalServiceRequestId')
  local t=$(echo "$json" | jq -r '.type')
  local pn=$(echo "$json" | jq -r '.pallet_number')
  local p_id=$(echo "$(grep -m 1 "$id" "${bulk_requests_file}")" | awk -F'generic,' '{split($2,a,","); print a[1]}')
  local error_log=$(grep "$p_id" "${error_orders_file}")
  local received_at=$(echo "$error_log" | grep -o '[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}T[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}\.[0-9]\{3\}Z' | head -n 1)
  local message="$2"

  printf -v line "%s,%s,%s,%s,%s,%s" "$t" "$id" "$received_at" "$pn" "$message" "$error_log"
  echo "$line" >> output.csv
}

# Get all the IDs from error logs
awk -F'generic,' '{split($2, a, ","); print a[1]}' "${error_orders_file}" | sort -u > ids.txt

# Grep all error IDs from bulk request
grep -Fwf ids.txt "${bulk_requests_file}" > executeBulkRequest.json

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
  echo "Missing value for key: $key — replacing with null_ptr"
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

echo -e "\nVerifying locations..."

for json in "${missing_orders_data_array[@]}"; do
  id=$(echo "$json" | jq -r '.externalServiceRequestId')
  locations=()
  while IFS= read -r loc; do
    locations+=("$loc")
  done < <(echo "$json" | jq -r 'select(.fullAddresses) | .fullAddresses[]')

  if [[ "${#locations[@]}" -gt 0 ]]; then
    declare -i ctr=1
    test_json="{}"

    for loc in "${locations[@]}"; do
      test_loc=$(echo "$loc" | cut -d '-' -f 1-2)
      test_json=$(echo "$test_json" | jq --arg key "$test_loc" --argjson value "$ctr" '. + { ($key): $value }')
      ((ctr++))
    done
    invalid_locations=$(curl --silent --location 'https://adams-internal.greymatter.greyorange.com/zone-manager/api/v1/validate/locations' \
    --header 'accept: application/json' \
    --header 'Content-Type: application/json' \
    --header 'X-CSRFToken: LbIOP1XgXuWt3m1b9D35KZzFvYychlIkIqrB8SLQImo4jyOuNMMWhtezvEQsLj0Z' \
    --data "$test_json" | jq -r '.invalid_locations[]')

    if [[ -n "$invalid_locations" ]]; then
      invalid_locations_string="Invalid locations - $invalid_locations"
      write_to_csv "$json" "$invalid_locations_string"
    fi
  fi
done


echo -e "\nVerifying skus in pick requests..."

for json in "${missing_orders_data_array[@]}"; do
  id=$(echo "$json" | jq -r '.externalServiceRequestId')
  sku_list=()
  while IFS= read -r sku; do
    sku_list+=("\"$sku\"")
  done < <(echo "$json" | jq -r 'select(.product_skus) | .product_skus[]')

  if [[ ${#sku_list[@]} -gt 0 ]]; then
    json_sku_list=$(printf "%s," "${sku_list[@]}")
    json_sku_list=${json_sku_list%,}
    data_payload="{\"filter_params\": [{\"key\": \"product_sku\",\"operator\": \"in\",\"value\": [${json_sku_list}]}]}"

    valid_skus=$(curl --silent --location 'https://adams-internal.greymatter.greyorange.com/api-gateway/mdm-service/wms-masterdata/item/search_v2' \
    --header 'Content-Type: application/json' \
    --data "$data_payload" | jq -r '.[].productAttributes.product_sku')

    missing_sku=()
    for quoted_sku in "${sku_list[@]}"; do
      sku=$(echo "$quoted_sku" | tr -d '"')
      if ! echo "$valid_skus" | grep -q "^$sku$"; then
        missing_sku+=("$sku")
      fi
    done

    if (( ${#missing_sku[@]} != 0 )); then
      #Writing error to output.csv
      missing_sku_string=$(printf " %s" "${missing_sku[@]}")
      missing_sku_string="Missing SKU in MDM -${missing_sku_string}"
      write_to_csv "$json" "$missing_sku_string"
    fi
  fi
done



echo -e "\nGetting Pallet height exceed rejections..."

mapfile -t process_ids < <(awk -F 'generic,' '/Pallet height exceeds the maximum limit/ {split($2,a,","); print a[1]}' "${error_orders_file}")

if [ ${#process_ids[@]} -eq 0 ]; then
  echo "No pallet was rejected due to height exceed limit error"
else
  for process_id in "${process_ids[@]}"; do

    request=$(grep -F "$process_id" "${bulk_requests_file}")

    if [ -n "$request" ]; then
      ext_id=$(echo "$request" | awk -F'externalServiceRequestId"": ""' '{split($2, uuid, "\""); print uuid[1]}')
      if [ -z "$ext_id" ]; then
	ext_id=$(echo "$request" | awk -F'externalServiceRequestId"":""' '{split($2, uuid, "\""); print uuid[1]}')
      fi

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
	      write_to_csv "$request_attributes" "Breach max height"
      else
        echo "Warning: Could not extract externalServiceRequestId from line: $request"
      fi
    else
      echo "Warning: Could not find the bulk request for process ID: $process_id"
    fi
  done
fi