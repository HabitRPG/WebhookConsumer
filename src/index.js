require('dotenv').config();
const Kafka = require('node-rdkafka');
const got = require('got');

const kafkaConf = {
  'group.id': process.env.CLOUDKARAFKA_GROUP_ID,
  'metadata.broker.list': process.env.CLOUDKARAFKA_BROKERS.split(','),
  'socket.keepalive.enable': true,
  'security.protocol': 'SASL_SSL',
  'sasl.mechanisms': 'SCRAM-SHA-256',
  'sasl.username': process.env.CLOUDKARAFKA_USERNAME,
  'sasl.password': process.env.CLOUDKARAFKA_PASSWORD,
  debug: 'generic,broker,security',
};

const prefix = process.env.CLOUDKARAFKA_TOPIC_PREFIX;
const topics = [`${prefix}-default`];
const consumer = new Kafka.KafkaConsumer(kafkaConf, {
  'auto.offset.reset': 'beginning',
});

consumer.on('error', err => console.error(err));

consumer.on('ready', (arg) => {
  console.log(`Consumer ${arg.name} - ${topics} ready.`);
  consumer.subscribe(topics);
  consumer.consume();
});

consumer.on('data', (m) => {
  const value = m.value.toString();
  const { url, body } = JSON.parse(value);

  got.post(url, {
    body,
    json: true,
  }).catch(err => console.error(err));
});

consumer.on('disconnected', () => process.exit());

consumer.on('event.error', (err) => {
  console.error(err);
  process.exit(1);
});

/* consumer.on('event.log', (log) => console.log(log)); */

consumer.connect();

process.on('exit', () => {
  consumer.disconnect();
});
