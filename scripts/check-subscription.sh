# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

#!/bin/bash

set -e

# Get the directory that this script is in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "${DIR}/load-env.sh"

figlet Check Subscription

# This is called if we are in a CI system and we will login
# with a Service Principal.
if [ -n "${IN_AUTOMATION}" ]
then

    if [ -n "${AZURE_ENVIRONMENT}" ] && [[ $AZURE_ENVIRONMENT == "AzureUSGovernment" ]]; then
        az cloud set --name AzureUSGovernment
    fi

    az login --identity
    az account set -s "$ARM_SUBSCRIPTION_ID"
fi

if ! CURRENT_SUBSCRIPTION_ID=$(az account show --query id --output tsv 2>/dev/null); then
    echo -e "ERROR: Please run 'az login' to setup account."
    exit 1
fi
export CURRENT_SUBSCRIPTION_ID
echo -e "Using subscription id ${CURRENT_SUBSCRIPTION_ID}"

# If the ARM_SUBSCRIPTION_ID is set, compare it with the current subscription
if [ -n "$ARM_SUBSCRIPTION_ID" ] && [ "$CURRENT_SUBSCRIPTION_ID" != "$ARM_SUBSCRIPTION_ID" ]; then
    echo -e "*** INCORRECT SUBSCRIPTION ***."
    echo -e "Either use subscription id $ARM_SUBSCRIPTION_ID, or unset the ARM_SUBSCRIPTION_ID environment variable in your .env"
    exit 1
fi
