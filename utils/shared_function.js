export async function shutdown() {
    console.log('\n\n Shutting down producer ...');
    isRunning = false;

    try {
        await producer.disconnect();
        console.log('Producer disconnected correctly!');
        console.log(`Total of sent messages: ${messageCount}`);
        process.exit(0);
    } catch (error) {
        console.log("Error shutting down the producer: ", error)
        process.exit(1);
    }
}