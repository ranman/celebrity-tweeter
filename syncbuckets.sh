#REGIONLIST=(`echo $(aws ec2 describe-regions --output text --query 'Regions[*].RegionName' | tr -s '\t' ' ')`)
REGIONLIST=(us-east-1 us-west-2 eu-west-1)
aws s3 mb "s3://$1" > /dev/null || true
aws s3 cp "template-us-east-1.yaml" "s3://$1/" --acl "public-read" &
aws s3 cp "template-us-west-2.yaml" "s3://$1/" --acl "public-read" &
aws s3 cp "template-eu-west-1.yaml" "s3://$1/" --acl "public-read" &
aws s3 cp "stream.py" "s3://$1/" --acl "public-read" &
for REGION in "${REGIONLIST[@]}"; do
    #aws s3 mb "s3://${1}-${REGION}" --region "${REGION}"
    aws s3 cp --quiet "${2}" "s3://${1}-${REGION}" --acl public-read --region "${REGION}" &
done
wait
