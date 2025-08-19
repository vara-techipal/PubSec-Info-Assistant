# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

#!/bin/bash
set -e

figlet Infrastructure

# Get the directory that this script is in
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
source "${DIR}/load-env.sh"
source "${DIR}/prepare-tf-variables.sh"
pushd "$DIR/../infra" > /dev/null

# reset the current directory on exit using a trap so that the directory is reset even on error
function finish {
  popd > /dev/null
}
trap finish EXIT

if [ -n "${IN_AUTOMATION}" ]; then
  export TF_VAR_isInAutomation=true
fi

if [ -n "${IN_AUTOMATION}" ]
then

    if [ -n "${AZURE_ENVIRONMENT}" ] && [[ $AZURE_ENVIRONMENT == "AzureUSGovernment" ]]; then
        az cloud set --name AzureUSGovernment 
    fi

    az login --service-principal -u "$ARM_CLIENT_ID" -p "$ARM_CLIENT_SECRET" --tenant "$ARM_TENANT_ID"
    az account set -s "$ARM_SUBSCRIPTION_ID"
fi

#If you are unable to obtain the permission at the tenant level described in Azure account requirements, you can set the following to true provided you have created Azure AD App Registrations. 

#export TF_VAR_isInAutomation=true


# Check for existing DDOS Protection Plan and use it if available
if [[ "$SECURE_MODE" == "true" ]]; then
    if [[ "$ENABLE_DDOS_PROTECTION_PLAN" == "true" ]]; then
      if [[ -z "$DDOS_PLAN_ID" ]]; then
          # No DDOS_PLAN_ID provided in the environment, look up Azure for an existing DDOS plan
          DDOS_PLAN_ID=$(az network ddos-protection list --query "[?contains(name, 'ddos')].id | [0]" --output tsv)
          
          if [[ -z "$DDOS_PLAN_ID" ]]; then
              echo -e "\e[31mNo existing DDOS protection plan found. Terraform will create a new one.\n\e[0m"
          else
              echo "Found existing DDOS Protection Plan: $DDOS_PLAN_ID"
              read -p "Do you want to use this existing DDOS Protection Plan (y/n)? " use_existing
              if [[ "$use_existing" =~ ^[Yy]$ ]]; then
                  echo -e "Using existing DDOS Protection Plan: $DDOS_PLAN_ID\n"
                  export TF_VAR_ddos_plan_id="$DDOS_PLAN_ID"

                  echo -e "-------------------------------------\n"
                  echo "DDOS_PLAN_ID is set to: $DDOS_PLAN_ID"
                  echo -e "-------------------------------------\n"

              else
                  export TF_VAR_ddos_plan_id=""  # Clear the variable to indicate that a new plan should be created
                  echo "A new DDOS Protection Plan will be created by Terraform."
              fi
          fi
      else
          echo -e "Using provided DDOS Protection Plan ID from environment: $DDOS_PLAN_ID\n"
          export TF_VAR_ddos_plan_id="$DDOS_PLAN_ID"
      fi
    else
        echo "DDOS Protection Plan is disabled. No DDOS Protection Plan will be created."
        export TF_VAR_ddos_plan_id=""
    fi
fi

# Set the expiration date for the Key Vault secret. The value must be in the format of seconds since 1970-01-01T00:00:00Z
# The below syntax takes the calculated date and converts it to the number of seconds since the Unix epoch.
# The number of days is set in the SECRET_EXPIRATION_DAYS environment variable.
kv_secret_expiration=$(date -d "$(date -d "+$SECRET_EXPIRATION_DAYS days" +%Y-%m-%d)" +%s)
export TF_VAR_kv_secret_expiration="$kv_secret_expiration"
echo "Key Vault secret expiration date set to: $(date -d @$kv_secret_expiration)"

# Create our application configuration file before starting infrastructure
${DIR}/configuration-create.sh

# Initialise Terraform with the correct path
${DIR}/terraform-init.sh "$DIR/../infra/"

${DIR}/terraform-plan-apply.sh -d "$DIR/../infra" -p "infoasst" -o "$DIR/../inf_output.json"