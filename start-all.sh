#!/bin/bash

# Script para iniciar todos os serviços de uma vez
# Uso: ./start-all.sh

echo "🚀 Iniciando World Time Streaming com Kafka"
echo "=============================================="
echo ""

# Cores para output

# Verifica se o Docker está rodando
if ! docker info > /dev/null 2>&1; then
    echo -e "❌ Docker não está rodando!"
    exit 1
fi

# 1. Inicia Kafka e Zookeeper
echo -e "📦 Iniciando Kafka ..."
docker compose up -d

echo "⏳ Aguardando Kafka inicializar (30 segundos)..."
sleep 30

# 2. Cria o tópico com 5 partições
echo -e "📋 Criando tópico 'world-times' com 5 partições..."
docker exec kafka kafka-topics \
  --create \
  --topic world-times \
  --bootstrap-server localhost:9092 \
  --partitions 5 \
  --replication-factor 1 \
  --if-not-exists

echo -e "✅ Tópico criado com sucesso!"
echo ""

# 3. Mostra informações do tópico
echo -e "📊 Informações do tópico:"
docker exec kafka kafka-topics \
  --describe \
  --topic world-times \
  --bootstrap-server localhost:9092

echo ""
echo -e "=============================================="
echo "✅ Kafka está pronto!"
echo "=============================================="
echo ""
echo "Agora abra 3 terminais e execute:"
echo ""
echo "  Terminal 1: node consumer/index.js"
echo "  Terminal 2: node producer/index.js$"
echo "  Terminal 3: ./monitor.sh"
echo ""
echo "Para parar tudo: docker-compose down"
echo ""