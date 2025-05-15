FROM apache/airflow:2.10.5

WORKDIR /opt/airflow
USER root

# Installer les outils nécessaires
RUN echo 'Acquire::AllowInsecureRepositories "true";' > /etc/apt/apt.conf.d/99insecure \
 && echo 'Acquire::AllowDowngradeToInsecureRepositories "true";' >> /etc/apt/apt.conf.d/99insecure

RUN apt-get update && apt-get install -y curl gnupg ca-certificates

# Ajouter les clés GPG manquantes (exemples pour MariaDB, Docker, PostgreSQL)
RUN curl -fsSL https://mariadb.org/mariadb_release_signing_key.asc | gpg --dearmor -o /usr/share/keyrings/mariadb.gpg \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker.gpg \
    && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/postgresql.gpg

# (Optionnel) tu peux aussi modifier les fichiers de sources s’ils utilisent `signed-by`

# Maintenant on peut mettre à jour sans erreur
RUN apt-get update

# Installer des dépendances python
COPY requirements.txt /opt/airflow/requirements.txt
USER airflow

# Installer Cosmos
RUN pip install --upgrade pip \
    && pip install --no-cache-dir astronomer-cosmos==1.9.2

# Créer un environnement virtuel et installer les packages
RUN python -m venv dbt_venv \
    && . dbt_venv/bin/activate \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r /opt/airflow/requirements.txt
