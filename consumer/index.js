const { Kafka} = require('kafkajs');

const TOPIC = "world-times";

const REGIONS = [
    // {
    //     groupId: 'group-americas-sp',
    //     clientId: 'world-time-consumer-americas-sp',
    //     partition: [0],
    //     emoji: '🌎',
    //     name: 'AMERICAS-SP'
    // },
    // {
    //     groupId: 'group-americas-ny',
    //     clientId: 'world-time-consumer-americas-ny',
    //     partition: [1],
    //     emoji: '🌎',
    //     name: 'AMERICAS-NY'
    // },
    // {
    //     groupId: 'group-europe',
    //     clientId: 'world-time-consumer-europe',
    //     partition: [2],
    //     emoji: '🇪🇺',
    //     name: 'EUROPE'
    // },
    // {
    //     groupId: 'group-asia',
    //     clientId: 'world-time-consumer-asia',
    //     partition: [3],
    //     emoji: '🌏',
    //     name: 'ASIA'
    // },
    {
        groupId: 'group-oceania',
        clientId: 'world-time-consumer-oceania',
        partition: [4],
        emoji: '🌊',
        name: 'OCEANIA'
    }
];

consumers = [];
messageCounters = {};

async function createConsumer(config) {
    messageCounters[config.name] = 0;
    const kafka = new Kafka({
        clientId: config.clientId,
        brokers: ['localhost:9092']
    });

    const consumer = kafka.consumer({
        groupId: config.groupId,
        sessionTimeout: 40000,
        heartbeatInterval: 3000
    });
    try {
        await consumer.connect();
        console.log(`✅ Consumer ${config.name} connected to Kafka`);

        await consumer.subscribe({
            topic: TOPIC,
            fromBeginning: false
        });


        console.log(`${config.emoji} Monitoring timezones of ${config.name}...`);
        console.log(`📍 Partition: ${config.partition}`);
        console.log('⏹️  Press Ctrl+C to shutdown\n');

        await consumer.run({
            eachMessage: async ({topic, partition, message}) => {
                try {
                    const data = JSON.parse(message.value.toString());

                    if (config.partition.includes(partition)) {
                        messageCounters[config.name]++;
                        const date = new Date(data.datetime);

                        const time = new Intl.DateTimeFormat('pt-BR', {
                            timeZone: data.timezone,      // Mantém o fuso horário original
                            day: '2-digit',
                            month: '2-digit',
                            year: 'numeric',
                            hour: '2-digit',
                            minute: '2-digit',
                            second: '2-digit'
                        }).format(date);


                        console.log(
                            `${config.emoji} [${new Date().toLocaleTimeString('pt-BR')}] ` +
                            `${data.timezone}: ${time} ` +
                            `(UTC ${data.utc_offset}) ` +
                            `[Partition ${partition}] ` +
                            `[Msg number #${messageCounters[config.name]}]`
                        );
                    }
                } catch (error) {
                    console.error('❌ Error processing the message:', error.message);
                }
            }
        }).catch(error => {
            console.error(`❌ Error running Consumer (${config.name}):`, error);
        });

        return consumer
    } catch (error) {
        console.error(`❌ Error connecting the Consumer ${config.name}: `, error);
        await shutdown();
    }
}


async function startAllConsumers() {
    console.log("🚀 Starting all consumers!\n");
    console.log("==========================\n");

    try {
        const consumersPromises = REGIONS.map(config => createConsumer(config));
        const consumersConnected = await Promise.all(consumersPromises);

        consumers.push(...consumersConnected);

        console.log('\n=====================================');
        console.log('✅ All consumers connected!');
        console.log('⏹️  Press Ctrl+C to shutdown');
        console.log('=====================================\n');
    } catch (error) {
        console.error('❌ Error starting all consumers: ', error);
        await shutdown()
    }
}

async function shutdown() {
  console.log('\n\n🛑 Shutting down all consumers ...');
  
  try {
    await Promise.all(
        consumers.map(consumer => consumer.disconnect())
    );
    console.log('✅ All Consumer disconnected successfully');
    console.log('\n📊 Final Statistics: ');
    let total =0;
    for (const [region, count] of Object.entries(messageCounters)) {
        console.log(`   ${region}: ${count} menssages. \n`);
        total+=count;
    }
    console.log(`📊 Total of processed messages: ${total}`);
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
  shutdown();
});

startAllConsumers();