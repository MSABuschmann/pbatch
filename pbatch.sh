#!/bin/sh

i=0
while true; do
OUTPUT_FOLDER=./job_output_$i
if [ ! -d $OUTPUT_FOLDER ]; then
mkdir $OUTPUT_FOLDER
break
fi
i=$[$i+1]
done

SCRIPT_FOLDER=$OUTPUT_FOLDER/scripts
mkdir $SCRIPT_FOLDER
PRESAMP=gen_pbatch

python $PBATCH_DIRECTORY/pbatch.py $PBATCH_DIRECTORY $OUTPUT_FOLDER $SCRIPT_FOLDER $PRESAMP $@

if [ $? -eq 0 ]; then

chmod +x $SCRIPT_FOLDER/${PRESAMP}*.sh
cp $SCRIPT_FOLDER/${PRESAMP}*.sl .
for sl in `ls ${PRESAMP}*.sl`; do
sbatch $sl
done
rm ${PRESAMP}*.sl
echo 'Job output will be saved under '$OUTPUT_FOLDER

else
echo "Unexpected parsing error...clearly your fault"
fi

