#!/bin/bash


# Fonction pour ajouter ou mettre à jour les variables d'environnement dans ~/.bashrc
add_or_update_bashrc() {
    local variable_name=$1
    local variable_value=$2

    # Vérifier si la variable existe déjà dans ~/.bashrc
    if grep -qF "${variable_name}" ~/.bashrc; then
        # La variable existe, donc remplacer sa valeur
        sed -i "s|^export ${variable_name}=.*$|export ${variable_name}=${variable_value}|" ~/.bashrc
    else
        # La variable n'existe pas, donc l'ajouter à ~/.bashrc
        echo "export ${variable_name}=${variable_value}" >> ~/.bashrc
    fi
}

# Définir les variables d'environnement pour l'environnement de développement (dev)
add_or_update_bashrc "SNOWFLAKE_ACCOUNT"                   "FEGEGTN-VT61244"
add_or_update_bashrc "SNOWFLAKE_USER"                      "USR_DPF_DBT_SRV_DEV"
add_or_update_bashrc "SNOWFLAKE_PRIVATE_KEY_PATH"          "$HOME/secret_key/rsa_key.p8"
add_or_update_bashrc "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"    "TdJqYN2445j7"
add_or_update_bashrc "SNOWFLAKE_ROLE"                      "RL_DPF_RW_ALL_DEV"
add_or_update_bashrc "SNOWFLAKE_DATABASE"                  "MARKETING_DEV"
add_or_update_bashrc "SNOWFLAKE_WAREHOUSE"                 "COMPUTE_WH"
add_or_update_bashrc "SNOWFLAKE_SCHEMA"                    "PUBLIC"
add_or_update_bashrc "SNOWFLAKE_THREADS"                   "10"
add_or_update_bashrc "SNOWFLAKE_CLIENT_SESSION_KEEP_ALIVE" "False"
add_or_update_bashrc "SNOWFLAKE_QUERY_TAG"                 "DBT_ENV_DEV"

echo "Variables d'environnement ajoutées au fichier ~/.bashrc."

# Recharger les configurations
source ~/.bashrc
source ~/.bashrc
