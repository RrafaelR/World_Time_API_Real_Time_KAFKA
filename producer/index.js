const { Kafka } = require('kafkajs');
const axios = require('axios');
// import { shutdown } from '../utils/shared_function';

const kafka = new Kafka({
    clientid: "world-time-producer",
    brokers: ['localhost:9092']
});

const producer = kafka.producer();
const TOPIC = "world-times";

const timezones = [
  { zone: 'America/Sao_Paulo', partition: 0, group: 'Américas' },
  { zone: 'America/New_York', partition: 1, group: 'Américas' },
  { zone: 'Europe/London', partition: 2, group: 'Europa' },
  { zone: 'Asia/Tokyo', partition: 3, group: 'Ásia' },
  { zone: 'Australia/Sydney', partition: 4, group: 'Oceania' }
];

let isRunning = true;
let messageCount = 0;

async function fetchAndPublish(){
    try{
        await producer.connect();
        console.log("✅ Connected to producer!");
        console.log("\n📡 Start getting time zones!");
        console.log("⏹️ Press CRTL + C to finish!\n")

        while(isRunning){
            for(const {zone, partition, group} of timezones) {
                try {
                    const response = await axios.get(`http://worldtimeapi.org/api/timezone/${zone}`,
                        {timeout: 5000}
                    );
                    const date = response.data;
                    const payload = {
                        timezone: date.timezone,
                        datetime: date.datetime,
                        utc_offset: date.utc_offset,
                        day_of_week: date.day_of_week,
                        group: group
                    };

                    await producer.send({
                        topic: TOPIC,
                        messages: [{
                            key: zone,
                            value: JSON.stringify(payload),
                            partition: partition
                        }]
                    });

                    messageCount++;
                    console.log(
                        `📤 [${new Date().toLocaleTimeString('pt-BR')}] ` +
                        `Published: ${zone} (Partition ${partition}) - ${data.datetime}`
                    );
                } catch (error) {
                    console.log(`❌ Error trying to get ${zone}: `, error);
                }
            }
            console.log(`\n📊 Total of sent messages: ${messageCount}\n`);
            await new Promise(resolve => setTimeout(resolve, 10000));
        }
    } catch (error) {
        console.log(`❌ Fatal error in producer: `, error);
    }
}

async function shutdown() {
    console.log('\n\n🛑 Shutting down producer ...');
    isRunning = false;

    try {
        await producer.disconnect();
        console.log('✅ Producer disconnected correctly!');
        console.log(`📊 Total of sent messages: ${messageCount}`);
        process.exit(0);
    } catch (error) {
        console.log("❌ Error shutting down the producer: ", error)
        process.exit(1);
    }
}

// Capture CRTL C to stop the process

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// Start producer
fetchAndPublish().catch(error => {
    console.log('❌ Error trying to start producer: ', error);
    process.exit(1);
});