#!/bin/bash

# Script para iniciar todos os serviços de uma vez
# Uso: ./start-all.sh

echo "🚀 Iniciando World Time Streaming com Kafka"
echo "=============================================="
echo ""

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Verifica se o Docker está rodando
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker não está rodando!${NC}"
    exit 1
fi

# 1. Inicia Kafka e Zookeeper
echo -e "${YELLOW}📦 Iniciando Kafka ...${NC}"
docker compose up -d

echo "⏳ Aguardando Kafka inicializar (30 segundos)..."
sleep 30

# 2. Cria o tópico com 5 partições
echo -e "${YELLOW}📋 Criando tópico 'world-times' com 5 partições...${NC}"
docker exec kafka kafka-topics \
  --create \
  --topic world-times \
  --bootstrap-server localhost:9092 \
  --partitions 5 \
  --replication-factor 1 \
  --if-not-exists

echo -e "${GREEN}✅ Tópico criado com sucesso!${NC}"
echo ""

# 3. Mostra informações do tópico
echo -e "${YELLOW}📊 Informações do tópico:${NC}"
docker exec kafka kafka-topics \
  --describe \
  --topic world-times \
  --bootstrap-server localhost:9092

echo ""
echo -e "${GREEN}=============================================="
echo "✅ Kafka está pronto!"
echo "=============================================="
echo ""
echo "Agora abra 5 terminais e execute:"
echo ""
echo "  Terminal 1: ${YELLOW}node consumer/index.js america_sp${NC}"
echo "  Terminal 2: ${YELLOW}node consumer/index.js america_ny${NC}"
echo "  Terminal 3: ${YELLOW}node consumer/index.js europe${NC}"
echo "  Terminal 4: ${YELLOW}node consumer/index.js asia${NC}"
echo "  Terminal 5: ${YELLOW}node consumer/index.js oceania${NC}"
echo ""
echo "Para parar tudo: ${RED}docker-compose down${NC}"
echo ""