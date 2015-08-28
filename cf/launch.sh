#!/bin/bash
set -e
export AWS_REGION=us-east-1
export DEPLOYMENT=development
export INSTANCE_TYPE=r3.xlarge
mode=$1

export SPONGE_VPC_NAME=us-east-1-dev-core
export SPONGE_ROLE=webhdfs-dataloader
export SPONGE_NODE_NAME=${SPONGE_ROLE}-dev.spongecell.net

STACK_NAME=$SPONGE_VPC_NAME-$SPONGE_ROLE
PARAMS_FILE=${STACK_NAME}.${DEPLOYMENT}.json

echo "Generating cftemplate..."
./yaml_to_json.rb ${SPONGE_ROLE}.yaml > ${SPONGE_ROLE}.cftemplate
echo "Generating params file..."
ruby aws_parameter_lookup.rb > $PARAMS_FILE

if [[ x"$mode" != x"delete" ]]; then
   caps="--capabilities CAPABILITY_IAM"
   tems="--template-body file://$SPONGE_ROLE.cftemplate"
   pars="--parameters file://$PARAMS_FILE"
fi

echo "Calling AWS cloudformation cli..."
aws --color on --region $AWS_REGION cloudformation $mode-stack \
    $caps \
--stack-name $STACK_NAME-$DEPLOYMENT \
--region $AWS_REGION \
    $tems \
    $pars
echo "Done."

# END
