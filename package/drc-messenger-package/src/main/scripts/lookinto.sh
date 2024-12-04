# parse options
gtid=$1
hours=$2
if [ ! $2 ]; then
    $2=168
fi
let minutes=hours*60;

#create context
if [ ! -d "temp" ];then
mkdir temp
fi

#unzip & cache
if [ ! -f "temp/.$1" ];then
    echo -n "[matching in $hours hours] "
    files=`find ./* -mmin -$minutes -name '*log*' | grep -v temp`
    echo $files
    files=`zgrep -il "$1" $files`
    echo -n "[matched] "
    echo $files
    echo "[caching...]"
    touch temp/."$1"
    for file in $files
    do
        file=${file#*./}
        echo -n `du -sh $file`
        result=$(echo $file | grep ".gz$")
        if [ -n "$result" ]; then
            unzipped=temp/"${file%.gz*}"
            gunzip -c $file > $unzipped
            echo $unzipped >> temp/."$1"
            echo " -> "$unzipped
        else
            cp $file temp/
            echo "temp/$file" >> temp/."$1"
            echo " -> temp/$file"
        fi
    done
fi

#search log
targets=`cat temp/."$1"`
echo "[searching cache] "$targets
grep "$1" $targets
