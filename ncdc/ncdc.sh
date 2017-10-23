#!/usr/bin/env bash

# global parameters
g_tmp_folder="ncdc_tmp";
g_output_folder="ncdc_data";

g_remote_host="ftp.ncdc.noaa.gov";
g_remote_path="pub/data/noaa";


# $1: folder_path
function create_folder {
    if [ -d "$1" ]; then
        rm -rf "$1";
    fi
    mkdir "$1"
}

# $1: year to download
function download_data {
    local source_url="ftp://$g_remote_host/$g_remote_path/$1"
    wget -r -c -q --no-parent -P "$g_tmp_folder" "$source_url";
}

# $1: year to process
function process_data {
    local year="$1"
    local local_path="$g_tmp_folder/$g_remote_host/$g_remote_path/$year"
    local tmp_output_file="$g_tmp_folder/$year"
    for file in $local_path/*; do
        gunzip -c $file >> "$tmp_output_file"
    done
    zipped_file="$g_output_folder/$year.gz"
    gzip -c "$tmp_output_file" >> "$zipped_file"
    echo "created file: $zipped_file"

    rm -rf "$local_path"
    rm "$tmp_output_file"
}

# $1 - start year
# $2 - finish year
function main {
    local start_year=2000
    local finish_year=2017

    if [ -n "$1" ]; then
        start_year=$1
    fi

    if [ -n "$2" ]; then
        finish_year=$2
    fi

    create_folder $g_tmp_folder
    create_folder $g_output_folder

    for year in `seq $start_year $finish_year`; do
        download_data $year
        process_data $year
    done

    rm -rf "$g_tmp_folder"
}

main $1 $2