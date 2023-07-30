rm ../data-for-correctness/condensed*
rm ../data-for-correctness/sorted*
rm ../data-for-correctness/final*

rm ../preprocessed-small-watdiv/condensed*
rm ../preprocessed-small-watdiv/sorted*
rm ../preprocessed-small-watdiv/final*

go build main.go sort-merge.go hash-join.go file-utils.go
time ./main
