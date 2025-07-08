import requests, json
import psycopg2
from datetime import datetime, timezone, timedelta
import time


# Configurações
APIS = [
    {"url": "https://xxxx.somosessentia.com.br/instance/fetchInstances", "apikey": "xxxx"},
    {"url": "http://xxxx.somosessentia.com.br:8080/instance/fetchInstances", "apikey": "xxxx"},
    {"url": "https://xxxx.somosessentia.com.br/instance/fetchInstances", "apikey": "xxxx"},
    {"url": "https://xxxx.somosessentia.com.br/instance/fetchInstances", "apikey": "xxxx"},
    {"url": "https://xxxx.somosessentia.com.br/instance/fetchInstances", "apikey": "xxxx"}
]

DATABASE_CONFIG = {
    'dbname': 'xxxxk',
    'user': 'xxxx',
    'password': 'xxxx',
    'host': 'xxxx.xxxx.xxxx.azure.com',
    'port': xxxx
}

def fetch_instances(api_url, api_key):
    """Consome a API para buscar os instances."""
    headers = {"apiKey": api_key}
    response = requests.get(api_url, headers=headers)
    response.raise_for_status()
    instances = response.json()

    if not isinstance(instances, list):
        raise ValueError(f"Estrutura inesperada de dados da API {api_url}. Esperado uma lista.")
    return instances

def fetch_existing_instances(cursor):
    """Busca todos os `instance_id` existentes no banco de dados com seus dados."""
    cursor.execute("""
        SELECT instance_id, instance_name, owner, profile_name, profile_picture_url, apikey, webhook_wa_business, status, updated_at
        FROM evolution_instances
    """)
    return {row[0]: row for row in cursor.fetchall()}

def map_instance_data(instance, api_url):
    """Mapeia os dados da instância com base na API."""
    if api_url in [
    "https://xxxx.somosessentia.com.br/instance/fetchInstances"]:
        return {
            "instance_id": instance.get("id"),
            "instance_name": instance.get("name", "N/A"),
            "status": instance.get("connectionStatus", "unknown"),
            "owner": instance.get("ownerJid", None),
            "profile_name": instance.get("profileName", None),
            "profile_picture_url": instance.get("profilePicUrl", None),
            "apikey": instance.get("token", "N/A"),
            "webhook_wa_business": f"https://xxxx.somosessentia.com.br/webhook/{instance.get('name', '')}"
            }
    elif api_url == "https://xxxx.somosessentia.com.br/instance/fetchInstances":
        return {
            "instance_id": instance.get("id"),
            "instance_name": instance.get("name", "N/A"),
            "status": instance.get("connectionStatus", "unknown"),
            "owner": instance.get("ownerJid", None),
            "profile_name": instance.get("profileName", None),
            "profile_picture_url": instance.get("profilePicUrl", None),
            "apikey": instance.get("token", "N/A"),
            "webhook_wa_business": f"https://xxxx.somosessentia.com.br/webhook/{instance.get('name', '')}"
            }
    else:
        instance_data = instance.get("instance", {})
        integration_data = instance_data.get("integration", {})
        return {
            "instance_id": instance_data.get("InstanceId") or instance_data.get("instanceId"),
            "instance_name": instance_data.get("instanceName", "N/A"),
            "status": instance_data.get("status", "unknown"),
            "owner": instance_data.get("owner", None),
            "profile_name": instance_data.get("profileName", None),
            "profile_picture_url": instance_data.get("profilePictureUrl", None),
            "apikey": instance_data.get("apikey", "N/A"),
            "webhook_wa_business": integration_data.get("webhook_wa_business")
        }

def insert_status_change(cursor, instance_id, status):
    """Insere uma mudança de status na tabela evolution_instances_status."""
    now = datetime.now(timezone.utc) - timedelta(hours=3)
    cursor.execute("""
        INSERT INTO evolution_instances_status (id, created_at, instance_id, status)
        VALUES (gen_random_uuid(), %s, %s, %s)
    """, (now, instance_id, status))


def notify_status_change(instance_name, status, webhook_wa_business, now):
    """
    Envia uma notificação ao Slack sobre a mudança de status de uma instância.

    :param now:
    :param webhook_wa_business:
    :param instance_name: Nome da instância
    :param status: Novo status da instância
    """
    webhook_url = "https://hooks.slack.com/services/xxxx/xxxx/xxxx"
    # Formatar o horário
    timestamp = (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%d/%m/%Y %H:%M:%S")
    # Lógica para exibir "Conectado" em verde e outros status em vermelho
    if status == "open":
        status_display = "Conectado"
        icone = ":white_check_mark:"
    elif status == "close":
        status_display = "Desconectado"
        icone = ":no_entry:"
    else:
        status_display = status
        icone = ":warning:"

    message = f"{icone} A instância *{instance_name}* mudou de status. O status atual dela é *{status_display}*. \n   O webhook é {webhook_wa_business} \n   Foi detectado no monitoramento em *{timestamp}* "

    payload = {"text": message}
    headers = {"Content-type": "application/json"}

    try:
        response = requests.post(webhook_url, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            print("Notificação enviada com sucesso.")
        else:
            print(f"Erro ao enviar notificação: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Erro ao conectar ao Slack: {e}")

def insert_or_update_instance(cursor, data, existing_instance):
    """Insere ou atualiza uma instância no banco de dados."""
    now = datetime.now(timezone.utc) - timedelta(hours=3)
    status_changed = False

    if existing_instance:
        # Verificar alterações
        changes = (
            data["instance_name"] != existing_instance[1] or
            data["owner"] != existing_instance[2] or
            data["profile_name"] != existing_instance[3] or
            data["profile_picture_url"] != existing_instance[4] or
            data["apikey"] != existing_instance[5] or
            data["webhook_wa_business"] != existing_instance[6]
        )
        status_changed = data["status"] != existing_instance[7]

        if changes or status_changed:
            cursor.execute("""
                UPDATE evolution_instances
                SET instance_name = %s, owner = %s, profile_name = %s, profile_picture_url = %s, apikey = %s, webhook_wa_business = %s, status = %s, updated_at = %s
                WHERE instance_id = %s
            """, (
                data["instance_name"], data["owner"], data["profile_name"], data["profile_picture_url"],
                data["apikey"], data["webhook_wa_business"], data["status"], now, data["instance_id"]
            ))
            if status_changed:
                # Registrar a alteração no status
                insert_status_change(cursor, data["instance_id"], data["status"])

                # Enviar notificação ao Slack
                notify_status_change(data["instance_name"], data["status"], data["webhook_wa_business"], now)
            return f"Instance {data['instance_id']} atualizada com sucesso."
        return f"Instance {data['instance_id']} não sofreu alterações."
    else:
        # Inserir nova instância
        cursor.execute("""
            INSERT INTO evolution_instances (
                instance_id, instance_name, owner, profile_name, profile_picture_url, apikey, integration, token, webhook_wa_business, status, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data["instance_id"], data["instance_name"], data["owner"], data["profile_name"],
            data["profile_picture_url"], data["apikey"], "WHATSAPP-BAILEYS", data["apikey"],
            data["webhook_wa_business"], data["status"], now
        ))
        insert_status_change(cursor, data["instance_id"], data["status"])
        return f"Instance {data['instance_id']} inserida com sucesso."

def process_instances(api_url, api_key, cursor, existing_instances):
    """Processa as instâncias para inserção ou atualização no banco de dados."""
    instances = fetch_instances(api_url, api_key)
    now = datetime.now(timezone.utc) - timedelta(hours=3)  # Timestamp atual para o campo verified_at

    for instance in instances:
        data = map_instance_data(instance, api_url)
        if not data["instance_id"]:
            print(f"Falha: Campo 'instance_id' ausente, pulando...")
            continue

        # Atualizar o campo verified_at para a instância
        cursor.execute("""
            UPDATE evolution_instances
            SET verified_at = %s
            WHERE instance_id = %s
        """, (now, data["instance_id"]))

        message = insert_or_update_instance(cursor, data, existing_instances.get(data["instance_id"]))
        print(message)

def main_loop():
    """Executa o processo principal em loop a cada 30 segundos."""
    while True:
        print("Iniciando o processamento...")
        try:
            connection = psycopg2.connect(**DATABASE_CONFIG)
            cursor = connection.cursor()

            cursor.execute("BEGIN")
            existing_instances = fetch_existing_instances(cursor)
            print(f"{len(existing_instances)} instances já existem no banco.")

            for api in APIS:
                print(f"Conectando à API: {api['url']}...")
                process_instances(api["url"], api["apikey"], cursor, existing_instances)

            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            print(f"Erro durante o processamento: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

        print("Processamento concluído. Aguardando 30 segundos...")
        time.sleep(90)  # Pausa de 30 segundos

if __name__ == "__main__":
    main_loop()
