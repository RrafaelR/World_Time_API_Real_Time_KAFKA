#!/bin/bash

# Script para monitorar todos os consumer groups
# Uso: ./monitor.sh

echo "📊 Monitorando Consumer Groups do Kafka"
echo "========================================"
echo ""

while true; do
    clear
    echo "🕐 $(date '+%H:%M:%S') - Pressione Ctrl+C para sair"
    echo "=================================================="
    echo ""
    
    # Lista todos os consumer groups
    echo "📋 Consumer Groups Ativos:"
    docker exec kafka kafka-consumer-groups \
      --list \
      --bootstrap-server localhost:9092
    
    echo ""
    echo "=================================================="
    echo ""
    
    # Detalhes de cada consumer group
    for group in group-americas-sp group-americas-ny group-europe group-asia group-oceania; do
        echo "🌍 Detalhes: $group"
        echo "--------------------------------------------------"
        docker exec kafka kafka-consumer-groups \
          --describe \
          --group $group \
          --bootstrap-server localhost:9092 2>/dev/null || echo "  ❌ Group não ativo"
        echo ""
    done
    
    echo "=================================================="
    echo "Atualizando em 10 segundos..."
    sleep 10
done