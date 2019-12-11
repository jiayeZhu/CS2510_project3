time hadoop jar project3.jar Stage1 input/ output/stage1/ $1 $2 $3 1>log_$1_$2_$3.txt 2>&1
time hadoop jar project3.jar Stage2 input/ output/stage2/ $1 $2 $3 1>>log_$1_$2_$3.txt 2>&1
time hadoop jar project3.jar Stage3 output/stage2/ output/stage3/ $1 $2 $3 1>>log_$1_$2_$3.txt 2>&1
time hadoop jar project3.jar Stage4 output/stage3/ output/stage4/ $1 $2 $3 1>>log_$1_$2_$3.txt 2>&1
