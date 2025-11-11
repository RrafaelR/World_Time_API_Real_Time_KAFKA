const { Kafka} = require('kafkajs');

const REGIONS = {
    america_sp: {
        groupId: 'group-americas-sp',
        clientId: 'world-time-consumer-americas-sp',
        partition: [0],
        emoji: '🌎',
        name: 'AMERICAS-SP'
    },
    america_ny: {
        groupId: 'group-americas-ny',
        clientId: 'world-time-consumer-americas-ny',
        partition: [1],
        emoji: '🌎',
        name: 'AMERICAS-NY'
    },
    europe: {
        groupId: 'group-europe',
        clientId: 'world-time-consumer-europe',
        partition: [2],
        emoji: '🇪🇺',
        name: 'EUROPE'
    },
    asia: {
        groupId: 'group-asia',
        clientId: 'world-time-consumer-asia',
        partition: [3],
        emoji: '🌏',
        name: 'ASIA'
    },
    oceania: {
        groupId: 'group-oceania',
        clientId: 'world-time-consumer-oceania',
        partition: [4],
        emoji: '🌊',
        name: 'OCEANIA'
    }
};

const regionKey = process.argv[2];

if (!regionKey || !REGIONS[regionKey]) {
    console.error('❌ Use: node index.js <region>');
    console.error('     Available regions:', Object.keys(REGIONS).join(", "));
}

const config = REGIONS[regionKey];

const kafka = new Kafka({
    clientId: config.clientId,
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({
    groupId: config.groupId,
    sessionTimeout: 40000,
    heartbeatInterval: 3000
});

const TOPIC = "world-times";
let messageCount = 0;

async function startConsumer() {
    try {
        await consumer.connect();
        console.log(`✅ Consumer ${config.name} connected to Kafka`);

        await consumer.subscribe({
            topic: TOPIC,
            fromBeginning: false
        });


        console.log(`${config.emoji} Monitoring timezones of ${config.name}...`);
        console.log(`📍 Partitions: ${config.partitions.join(', ')}`);
        console.log('⏹️  Press Ctrl+C to shutdown\n');

        await consumer.run({
            eachMessage: async ({topic, partition, message}) => {
                try {
                    const data = JSON.parse(message.value.toString());

                    if (config.partition.includes(partition)) {
                        messageCount++;

                        const time = new Date(data.datetime).toLocaleTimeString('pt-BR', {
                            hour: '2-digit',
                            minute: '2-digit',
                            second: '2-digit'
                        });

                        console.log(
                            `${config.emoji} [${new Date().toLocaleTimeString('pt-BR')}] ` +
                            `${data.timezone}: ${time} ` +
                            `(UTC ${data.utc_offset}) ` +
                            `[Partition ${partition}] ` +
                            `[Msg number #${messageCount}]`
                        );
                    }
                } catch (error) {
                    console.error('❌ Error processing the message:', error);
                }
            }
        });
    } catch (error) {
        console.error('❌ Consumer fatal error:', error);
        await shutdown();
    }
}

async function shutdown() {
  console.log(`\n\n🛑 Shutting down consumer ${config.name}...`);
  
  try {
    await consumer.disconnect();
    console.log('✅ Consumer disconnected successfully');
    console.log(`📊 Total of processed messages: ${messageCount}`);
    process.exit(0);
  } catch (error) {
    console.error('❌ Fatal error trying to disconnect:', error);
    process.exit(1);
  }
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection:', reason);
});

startConsumer();