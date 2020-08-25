#!/usr/bin/bash

function check_lvs_usage_or_remove(){
    find_output=`findmnt $1`
    if [ -z "$find_output" ]
    then
        echo "$1 does not has a mount point, removing the lvs..."
        lvremove -fy $1
    else
        echo "$1 is in use: $(lvdisplay --columns -o lv_size --no-headings)"
    fi
}


LV_LIST=$(lvdisplay --columns -o lv_path --no-headings)

while read line
do
    check_lvs_usage_or_remove ${line}
done <<< $LV_LIST

