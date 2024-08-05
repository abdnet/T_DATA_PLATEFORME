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
add_or_update_bashrc "DBT_ACCOUNT"                  "khwrkrv-ku13850"
add_or_update_bashrc "DBT_USER"                     "USR_DPF_DBT_SRV_DEV"
add_or_update_bashrc "DBT_PRIVATE_KEY_PATH"         "$HOME/secret_key/rsa_secret_key/rsa_key.p8"
add_or_update_bashrc "DBT_PRIVATE_KEY_PASSPHRASE"   "TdJqYN2445j7"
add_or_update_bashrc "DBT_ROLE"                     "RL_DPF_RW_ALL_DEV"
add_or_update_bashrc "DBT_DATABASE"                 "marketing"
add_or_update_bashrc "DBT_WAREHOUSE"                "compute_wh"
add_or_update_bashrc "DBT_SCHEMA"                   "DEV_ANALYTICS"
add_or_update_bashrc "DBT_THREADS"                  "10"
add_or_update_bashrc "DBT_CLIENT_SESSION_KEEP_ALIVE" "False"
add_or_update_bashrc "DBT_QUERY_TAG"                "DBT_ENV_DEV"

echo "Variables d'environnement ajoutées au fichier ~/.bashrc."

# Recharger les configurations
source ~/.bashrc
source ~/.bashrc
